"""
GIWA Launch Watch
Phase 1: hunts for the GIWA mainnet RPC until it goes live.
Phase 2: watches every block for new DEX pairs, and for deployers you
         already know from the testnet scanner.

Run:   python giwa_launchwatch.py
Test:  python giwa_launchwatch.py --test     (config + discovery check)
"""

import json
import logging
import os
import sys
import time

import requests
from Crypto.Hash import keccak
from dotenv import load_dotenv

load_dotenv("config.env")

MANUAL_RPC   = os.getenv("MAINNET_RPC", "").strip()
WEBHOOK      = os.getenv("MAINNET_WEBHOOK_URL", "").strip() or os.getenv("DISCORD_WEBHOOK_URL", "")
SCOUT_DB     = os.getenv("SCOUT_DB", "giwa_scout.db")     # testnet deployer watchlist
STATE_FILE   = os.getenv("WATCH_STATE", "launchwatch.json")
TESTNET_CHAIN_ID = 91342

DISCOVER_EVERY = 300      # seconds between discovery attempts
BATCH_BLOCKS   = 20
MAX_DEPLOY_LIST = 12      # deployments listed per batched alert

CANDIDATE_RPCS = [
    "https://rpc.giwa.io",
    "https://mainnet-rpc.giwa.io",
    "https://rpc-mainnet.giwa.io",
    "https://mainnet.giwa.io",
    "https://giwa-rpc.giwa.io",
    "https://rpc.mainnet.giwa.io",
]
CHAIN_REGISTRY = "https://chainid.network/chains.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("launchwatch")
session = requests.Session()


def topic(text):
    k = keccak.new(digest_bits=256)
    k.update(text.encode())
    return "0x" + k.hexdigest()


PAIR_CREATED = topic("PairCreated(address,address,address,uint256)")
POOL_CREATED = topic("PoolCreated(address,address,uint24,int24,address)")

# OP Stack predeploys — identical on every OP Stack chain, live from block one
L2_BRIDGE       = "0x4200000000000000000000000000000000000010"
MINTABLE_FACTORY = "0x4200000000000000000000000000000000000012"

BRIDGE_TOPICS = [
    topic("DepositFinalized(address,address,address,address,uint256,bytes)"),
    topic("ERC20BridgeFinalized(address,address,address,address,uint256,bytes)"),
    topic("ETHBridgeFinalized(address,address,uint256,bytes)"),
]
TOKEN_CREATED = [
    topic("OptimismMintableERC20Created(address,address,address)"),
    topic("StandardL2TokenCreated(address,address)"),
]

RELAYER_MIN_RECIPIENTS = 15    # distinct recipients that mark a wallet as a bridge relayer
RELAYER_WINDOW_BLOCKS  = 300

# ─── RPC ──────────────────────────────────────────────────────────────────────

def rpc_batch(url, calls, tries=4):
    if not calls:
        return []
    payload = [{"jsonrpc": "2.0", "id": i, "method": m, "params": p}
               for i, (m, p) in enumerate(calls)]
    for attempt in range(tries):
        try:
            r = session.post(url, json=payload, timeout=30)
            if r.status_code == 429:
                raise RuntimeError("rate limited")
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict):
                raise RuntimeError(data.get("error", "batch rejected"))
            by_id = {d.get("id"): d for d in data}
            return [by_id.get(i, {}).get("result") for i in range(len(calls))]
        except (requests.RequestException, ValueError, RuntimeError) as e:
            if attempt == tries - 1:
                raise
            log.warning("RPC problem (%s) — retrying", e)
            time.sleep(2 ** attempt)


def rpc(url, method, params):
    return rpc_batch(url, [(method, params)])[0]

# ─── State ────────────────────────────────────────────────────────────────────

def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=1)

# ─── Discord ──────────────────────────────────────────────────────────────────

def discord(title, description, color=0x5865F2, ping=False):
    body = {"embeds": [{"title": title, "description": description[:4000], "color": color}]}
    if ping:
        body["content"] = "@everyone"
    for _ in range(4):
        try:
            r = session.post(WEBHOOK, json=body, timeout=15)
            if r.status_code == 429:
                time.sleep(float(r.json().get("retry_after", 2)))
                continue
            return r.ok
        except requests.RequestException as e:
            log.warning("Discord problem: %s", e)
            time.sleep(3)
    return False


def short(a):
    return f"{a[:6]}…{a[-4:]}" if a and len(a) > 12 else (a or "?")

# ─── Phase 1: find the mainnet RPC ────────────────────────────────────────────

def validate(url):
    """A usable GIWA mainnet endpoint: not the testnet, and producing blocks now."""
    try:
        chain_id, block = rpc_batch(url, [("eth_chainId", []), ("eth_blockNumber", [])], tries=1)
        cid = int(chain_id, 16)
        if cid == TESTNET_CHAIN_ID or not block or int(block, 16) < 1:
            return None
        head = rpc(url, "eth_getBlockByNumber", ["latest", False])
        age = time.time() - int(head["timestamp"], 16)
        if age > 3600:
            log.info("%s looks stale (%.0f min behind)", url, age / 60)
            return None
        return {"rpc": url, "chain_id": cid, "block": int(block, 16)}
    except Exception:
        return None


def from_registry():
    """Chain registries list new networks within hours of launch."""
    try:
        r = session.get(CHAIN_REGISTRY, timeout=45)
        r.raise_for_status()
        for chain in r.json():
            name = (chain.get("name", "") + " " + chain.get("shortName", "")).lower()
            if "giwa" not in name or chain.get("chainId") == TESTNET_CHAIN_ID:
                continue
            if "test" in name or "sepolia" in name:
                continue
            for url in chain.get("rpc", []):
                if url.startswith("https://") and "${" not in url:
                    yield url
    except (requests.RequestException, ValueError) as e:
        log.warning("Registry lookup failed: %s", e)


def discover():
    """Returns endpoint details once mainnet is live, else None."""
    if MANUAL_RPC:
        found = validate(MANUAL_RPC)
        if found:
            return found
        log.warning("MAINNET_RPC is set but not responding as a live mainnet")

    for url in from_registry():
        found = validate(url)
        if found:
            return found

    for url in CANDIDATE_RPCS:
        found = validate(url)
        if found:
            return found
    return None

# ─── Phase 2: watch the chain ─────────────────────────────────────────────────

def testnet_deployers():
    """Every address that deployed a contract on the testnet — your watchlist."""
    import sqlite3
    try:
        db = sqlite3.connect(f"file:{SCOUT_DB}?mode=ro", uri=True)
        rows = db.execute(
            "SELECT DISTINCT LOWER(deployer) FROM contracts WHERE deployer IS NOT NULL")
        found = {r[0] for r in rows if r[0]}
        db.close()
        log.info("Watchlist: %s testnet deployers", f"{len(found):,}")
        return found
    except Exception as e:
        log.warning("Could not read testnet database (%s) — watchlist empty", e)
        return set()


def token_label(url, addr):
    """name / symbol for a token address, best effort."""
    try:
        name, sym = rpc_batch(url, [
            ("eth_call", [{"to": addr, "data": "0x06fdde03"}, "latest"]),
            ("eth_call", [{"to": addr, "data": "0x95d89b41"}, "latest"]),
        ], tries=1)
    except Exception:
        return short(addr)

    def dec(res):
        if not res or res == "0x":
            return ""
        b = bytes.fromhex(res[2:])
        try:
            if len(b) >= 96:
                off = int.from_bytes(b[:32], "big")
                ln = int.from_bytes(b[off:off + 32], "big")
                raw = b[off + 32:off + 32 + ln]
            else:
                raw = b[:32].rstrip(b"\0")
            return raw.decode("utf-8", "ignore").strip()[:24]
        except Exception:
            return ""

    sym, name = dec(sym), dec(name)
    if sym and name:
        return f"{sym} ({name})"
    return sym or name or short(addr)


def alert_pairs(url, logs, explorer):
    for entry in logs:
        topics = entry.get("topics", [])
        if len(topics) < 3:
            continue
        a = "0x" + topics[1][-40:]
        b = "0x" + topics[2][-40:]
        kind = "V2 pair" if topics[0] == PAIR_CREATED else "V3 pool"
        body = (f"**{token_label(url, a)}** ⇄ **{token_label(url, b)}**\n"
                f"[{short(a)}]({explorer}{a}) · [{short(b)}]({explorer}{b})\n"
                f"block {int(entry['blockNumber'], 16):,}")
        discord(f"🚨 New {kind} created", body, color=0xED4245, ping=True)
        log.info("Pair alert: %s / %s", a, b)


def alert_deploys(url, deploys, watchlist, explorer):
    known = [d for d in deploys if d["from"] in watchlist]
    others = [d for d in deploys if d["from"] not in watchlist]

    for d in known:
        discord("⭐ Known testnet deployer just deployed on mainnet",
                f"Deployer [{short(d['from'])}]({explorer}{d['from']})\n"
                f"Contract [{short(d['contract'])}]({explorer}{d['contract']})\n"
                f"block {d['block']:,}",
                color=0xFEE75C, ping=True)
        log.info("Watchlist deploy: %s by %s", d["contract"], d["from"])

    if others:
        listed = "\n".join(f"[{short(d['contract'])}]({explorer}{d['contract']}) "
                           f"by {short(d['from'])}" for d in others[:MAX_DEPLOY_LIST])
        extra = f"\n…and {len(others) - MAX_DEPLOY_LIST} more" if len(others) > MAX_DEPLOY_LIST else ""
        discord(f"{len(others)} new contracts deployed", listed + extra, color=0x4F545C)


def alert_bridges(url, state, logs, explorer):
    """Canonical bridge deposits and newly bridged token representations."""
    for entry in logs:
        topics = entry.get("topics", [])
        if not topics:
            continue
        addr = entry.get("address", "").lower()
        blk = int(entry["blockNumber"], 16)

        if topics[0] in BRIDGE_TOPICS and addr == L2_BRIDGE:
            if not state.get("first_bridge"):
                state["first_bridge"] = blk
                save_state(state)
                discord("🌉 FIRST BRIDGE DEPOSIT ON GIWA MAINNET",
                        f"The canonical bridge is carrying real deposits.\n"
                        f"block {blk:,} · [tx]({explorer.replace('/address/', '/tx/')}"
                        f"{entry['transactionHash']})",
                        color=0x00B0F4, ping=True)
                log.info("First canonical bridge deposit at block %s", blk)

        if topics[0] in TOKEN_CREATED and addr == MINTABLE_FACTORY:
            l2 = "0x" + topics[1][-40:] if len(topics) > 1 else ""
            seen = set(state.get("bridged_tokens", []))
            if l2 and l2 not in seen:
                seen.add(l2)
                state["bridged_tokens"] = sorted(seen)
                save_state(state)
                discord("🪙 New bridged token on GIWA",
                        f"**{token_label(url, l2)}**\n[{short(l2)}]({explorer}{l2})\n"
                        f"block {blk:,} · {len(seen)} bridged assets so far",
                        color=0x00B0F4, ping=len(seen) <= 3)
                log.info("Bridged token created: %s", l2)


def track_relayers(state, blocks, window, explorer):
    """A wallet paying many unrelated recipients is behaving like a bridge relayer."""
    for b in blocks:
        if b is None:
            continue
        blk = int(b["number"], 16)
        for tx in b["transactions"]:
            if tx.get("to") is None or tx.get("type") == "0x7e":
                continue
            sender = tx["from"].lower()
            window.setdefault(sender, {"to": set(), "first": blk})["to"].add(tx["to"].lower())

    flagged = set(state.get("relayers", []))
    for sender, seen in list(window.items()):
        if len(seen["to"]) >= RELAYER_MIN_RECIPIENTS and sender not in flagged:
            flagged.add(sender)
            state["relayers"] = sorted(flagged)
            save_state(state)
            first = not state.get("first_relayer")
            if first:
                state["first_relayer"] = sender
                save_state(state)
            discord("🌉 Possible third-party bridge or distributor" if not first
                    else "🌉 FIRST THIRD-PARTY BRIDGE ACTIVITY",
                    f"[{short(sender)}]({explorer}{sender}) paid "
                    f"{len(seen['to'])} distinct recipients in under "
                    f"{RELAYER_WINDOW_BLOCKS} blocks.\n"
                    f"Could be Relay, deBridge, Orbiter — or an airdrop. Check it.",
                    color=0x00B0F4, ping=first)
            log.info("Relayer pattern: %s (%d recipients)", sender, len(seen["to"]))

    # keep the window small
    if len(window) > 20000:
        window.clear()


def watch(endpoint, state):
    url = endpoint["rpc"]
    explorer = os.getenv("MAINNET_EXPLORER", "").rstrip("/")
    explorer = (explorer + "/address/") if explorer else "https://sepolia-explorer.giwa.io/address/"
    watchlist = testnet_deployers()
    nxt = state.get("next_block") or endpoint["block"]
    window, window_start = {}, nxt

    while True:
        try:
            head = int(rpc(url, "eth_blockNumber", []), 16)
            if nxt > head:
                time.sleep(2)
                continue
            end = min(nxt + BATCH_BLOCKS - 1, head)

            logs = rpc(url, "eth_getLogs", [{
                "fromBlock": hex(nxt), "toBlock": hex(end),
                "topics": [[PAIR_CREATED, POOL_CREATED]]}]) or []
            if logs:
                alert_pairs(url, logs, explorer)

            bridge_logs = rpc(url, "eth_getLogs", [{
                "fromBlock": hex(nxt), "toBlock": hex(end),
                "topics": [BRIDGE_TOPICS + TOKEN_CREATED]}]) or []
            if bridge_logs:
                alert_bridges(url, state, bridge_logs, explorer)

            blocks = rpc_batch(url, [("eth_getBlockByNumber", [hex(n), True])
                                     for n in range(nxt, end + 1)])
            pending, last = [], nxt - 1
            for b in blocks:
                if b is None:
                    break
                last = int(b["number"], 16)
                for tx in b["transactions"]:
                    if tx.get("to") is None and tx.get("type") != "0x7e":
                        pending.append((tx["hash"], tx["from"].lower(), last))
            if last < nxt:
                time.sleep(2)
                continue

            track_relayers(state, blocks, window, explorer)
            if end - window_start > RELAYER_WINDOW_BLOCKS:
                window.clear()
                window_start = end

            if pending:
                receipts = rpc_batch(url, [("eth_getTransactionReceipt", [h])
                                           for h, _, _ in pending])
                deploys = [{"contract": r["contractAddress"].lower(), "from": f, "block": blk}
                           for r, (_, f, blk) in zip(receipts, pending)
                           if r and r.get("contractAddress") and r.get("status") == "0x1"]
                if deploys:
                    alert_deploys(url, deploys, watchlist, explorer)

            nxt = last + 1
            state["next_block"] = nxt
            save_state(state)

        except KeyboardInterrupt:
            raise
        except Exception:
            log.exception("Watch loop error — continuing in 10s")
            time.sleep(10)

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not WEBHOOK:
        sys.exit("❌ No webhook configured (MAINNET_WEBHOOK_URL or DISCORD_WEBHOOK_URL)")

    if "--test" in sys.argv:
        print(f"✅ Webhook configured")
        print(f"   PairCreated topic {PAIR_CREATED}")
        print(f"   Bridge topics: {len(BRIDGE_TOPICS + TOKEN_CREATED)} signatures armed")
        print(f"   Watchlist: {len(testnet_deployers()):,} testnet deployers")
        found = discover()
        print(f"✅ Mainnet found: {found}" if found else "ℹ️  Mainnet not live yet — as expected")
        discord("Launch Watch test", "Configuration OK. Standing by for GIWA mainnet.",
                color=0x57F287)
        return

    state = load_state()
    endpoint = state.get("endpoint")

    while not endpoint:
        endpoint = discover()
        if endpoint:
            break
        log.info("Mainnet not live — checking again in %d min", DISCOVER_EVERY // 60)
        time.sleep(DISCOVER_EVERY)

    if not state.get("endpoint"):
        state["endpoint"] = endpoint
        save_state(state)
        discord("🟢 GIWA MAINNET IS LIVE",
                f"Chain ID **{endpoint['chain_id']}**\nRPC `{endpoint['rpc']}`\n"
                f"Block {endpoint['block']:,}\n\nNow watching every block for pair "
                f"creations and known testnet deployers.",
                color=0x57F287, ping=True)
        log.info("Mainnet found: %s (chain %s)", endpoint["rpc"], endpoint["chain_id"])

    try:
        watch(endpoint, state)
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
