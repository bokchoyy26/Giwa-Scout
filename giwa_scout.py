"""
GIWA Scout
Watches GIWA Sepolia, ranks contracts by unique wallets each hour,
fingerprints them from bytecode, and posts the top 20 to Discord.

Run:   python giwa_scout.py          (runs forever)
Test:  python giwa_scout.py --test   (checks RPC + Discord, then exits)
"""

import json
import logging
import os
import re
import sqlite3
import sys
import time

import requests
from dotenv import load_dotenv

# ─── Config (edit config.env, not this file) ──────────────────────────────────

load_dotenv("config.env")

RPC_URL       = os.getenv("RPC_URL", "https://sepolia-rpc.giwa.io")
WEBHOOK       = os.getenv("DISCORD_WEBHOOK_URL", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")          # optional
DB_PATH       = os.getenv("DB_PATH", "giwa_scout.db")
BLOCK_BATCH   = int(os.getenv("BLOCK_BATCH", "10"))         # blocks per request

TOP_N       = 20      # contracts per report
MIN_CALLERS = 3       # ignore contracts with fewer unique wallets per hour
BOT_RATIO   = 20      # txs per wallet above this = flagged as bot-heavy
KEEP_DAYS   = 7       # activity history kept in the database
CONSUMER_ONLY = os.getenv("CONSUMER_ONLY", "1") == "1"   # hide infra/stablecoins
CANDIDATES  = 60      # contracts examined before filtering down to TOP_N
EXPLORER    = "https://sepolia-explorer.giwa.io/address/"
EXPLORER_API = "https://sepolia-explorer.giwa.io/api/v2"
AI_MODEL    = "claude-haiku-4-5-20251001"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("scout")

# ─── Known function selectors ─────────────────────────────────────────────────

SELECTORS = {
    # ERC-20
    "transfer": "a9059cbb", "approve": "095ea7b3", "totalSupply": "18160ddd",
    "balanceOf": "70a08231", "transferFrom": "23b872dd", "decimals": "313ce567",
    "mint(address,uint256)": "40c10f19", "mint(uint256)": "a0712d68",
    "owner": "8da5cb5b", "renounceOwnership": "715018a6", "wethDeposit": "d0e30db0",
    # NFTs
    "ownerOf": "6352211e", "tokenURI": "c87b56dd", "safeTransferFrom": "42842e0e",
    "setApprovalForAll": "a22cb465", "safeBatchTransferFrom": "2eb2c2d6",
    "balanceOfBatch": "4e1273f4",
    # DEX
    "createPair": "c9c65396", "createPool": "a1671295", "getReserves": "0902f1ac",
    "pairSwap": "022c0d9f", "slot0": "3850c7bd", "addLiquidity": "e8e33700",
    "addLiquidityETH": "f305d719", "swapExactTokensForTokens": "38ed1739",
    "swapExactETHForTokens": "7ff36ab5", "exactInputSingle": "414bf389",
    "exactInputSingle02": "04e45aaf",
    # Infra / DeFi
    "upgradeToAndCall": "4f1ef286", "upgradeTo": "3659cfe6",
    "multicall": "ac9650d8", "aggregate3": "82ad56cb",
    "latestRoundData": "feaf968c", "vaultDeposit": "6e553f65", "asset": "38d52e0f",
    "stake": "a694fc3a", "withdraw": "2e1a7d4d", "getReward": "3d18b912",
    "handleOps06": "1fad948c", "handleOps07": "765e827f",
    "execTransaction": "6a761202",
    # Admin / proxy patterns
    "transferOwnership": "f2fde38b", "pause": "8456cb59", "unpause": "3f4ba83a",
    "paused": "5c975abb", "proxyAdmin": "f851a440", "implementation": "5c60da1b",
    "fee": "ddca3f43",
}
SEL_NAMES = {v: k for k, v in SELECTORS.items()}
S = SELECTORS

PREDEPLOY   = "0x42000000000000000000000000000000000000"
BORING_KIND = {"Wrapped ETH", "Utility: multicall", "Infra: AA EntryPoint",
               "Smart wallet (Safe)", "Oracle / price feed"}
BORING_SYM  = {"WETH", "ETH", "USDC", "USDT", "DAI", "WBTC", "USDBC", "TUSDT", "TUSDC"}
MAJOR_SYM   = {"USDC", "USDT", "WETH", "DAI", "WBTC", "UNI", "AAVE", "LINK", "ETH"}

EIP1967_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
EIP1167_TAG  = "363d3d373d3d3d363d73"

# ─── RPC ──────────────────────────────────────────────────────────────────────

session = requests.Session()


def rpc_batch(calls):
    """Send [(method, params), ...] as one JSON-RPC batch. Failed items return None."""
    if not calls:
        return []
    payload = [{"jsonrpc": "2.0", "id": i, "method": m, "params": p}
               for i, (m, p) in enumerate(calls)]
    for attempt in range(6):
        try:
            r = session.post(RPC_URL, json=payload, timeout=30)
            if r.status_code == 429:
                raise RuntimeError("rate limited")
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict):
                raise RuntimeError(data.get("error", "batch rejected"))
            by_id = {d.get("id"): d for d in data}
            return [by_id.get(i, {}).get("result") for i in range(len(calls))]
        except (requests.RequestException, ValueError, RuntimeError) as e:
            wait = 2 ** attempt
            log.warning("RPC problem (%s) — retrying in %ss", e, wait)
            time.sleep(wait)
    raise RuntimeError("RPC failed 6 times in a row")


def rpc_many(calls, size=20):
    out = []
    for i in range(0, len(calls), size):
        out += rpc_batch(calls[i:i + size])
    return out


def rpc(method, params):
    return rpc_batch([(method, params)])[0]

# ─── Database ─────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS state (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS contracts (
    address     TEXT PRIMARY KEY,
    is_contract INTEGER,
    first_seen  INTEGER,
    deployer    TEXT,
    kind        TEXT,
    name        TEXT,
    symbol      TEXT,
    proxy       TEXT,
    selectors   TEXT,
    ai_note     TEXT,
    ex_name     TEXT,
    verified    INTEGER,
    token_type  TEXT,
    holders     INTEGER,
    socials     TEXT,
    enriched    INTEGER,
    is_factory  INTEGER
);
CREATE TABLE IF NOT EXISTS callers (
    contract TEXT, hour INTEGER, sender TEXT,
    PRIMARY KEY (contract, hour, sender)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS callers_hour ON callers(hour);
CREATE TABLE IF NOT EXISTS txs (
    contract TEXT, hour INTEGER, n INTEGER,
    PRIMARY KEY (contract, hour)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS contracts_deployer ON contracts(deployer);
"""


def migrate(db):
    """Add v2 columns to a database created by v1."""
    have = {r[1] for r in db.execute("PRAGMA table_info(contracts)")}
    for col, decl in (("ex_name", "TEXT"), ("verified", "INTEGER"), ("token_type", "TEXT"),
                      ("holders", "INTEGER"), ("socials", "TEXT"), ("enriched", "INTEGER"),
                      ("is_factory", "INTEGER")):
        if col not in have:
            db.execute(f"ALTER TABLE contracts ADD COLUMN {col} {decl}")
    db.commit()


def get_state(db, k):
    row = db.execute("SELECT v FROM state WHERE k=?", (k,)).fetchone()
    return row[0] if row else None


def set_state(db, k, v):
    db.execute("INSERT INTO state VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)))


def lookup(db, addrs):
    """address -> is_contract, for addresses already in the database."""
    addrs, out = list(addrs), {}
    for i in range(0, len(addrs), 500):
        chunk = addrs[i:i + 500]
        q = f"SELECT address, is_contract FROM contracts WHERE address IN ({','.join('?' * len(chunk))})"
        out.update({r[0]: r[1] for r in db.execute(q, chunk)})
    return out

# ─── Block ingestion ──────────────────────────────────────────────────────────

def process_blocks(db, start, end):
    """Ingest blocks start..end. Returns (last block processed, its hour)."""
    blocks = rpc_batch([("eth_getBlockByNumber", [hex(n), True]) for n in range(start, end + 1)])

    activity, creations = {}, []
    last, hour = start - 1, None

    for b in blocks:
        if b is None:                      # block not available yet
            break
        last = int(b["number"], 16)
        ts = int(b["timestamp"], 16)
        hour = ts // 3600
        for tx in b["transactions"]:
            if tx.get("type") == "0x7e":   # L1 deposit / system tx — noise
                continue
            sender = tx["from"].lower()
            if tx.get("to") is None:
                creations.append((tx["hash"], sender, ts))
                continue
            slot = activity.setdefault((tx["to"].lower(), hour), [set(), 0])
            slot[0].add(sender)
            slot[1] += 1

    if last < start:
        return last, None

    # Contract deployments
    if creations:
        receipts = rpc_many([("eth_getTransactionReceipt", [h]) for h, _, _ in creations])
        rows = [(r["contractAddress"].lower(), ts, sender)
                for r, (_, sender, ts) in zip(receipts, creations)
                if r and r.get("contractAddress") and r.get("status") == "0x1"]
        db.executemany("""
            INSERT INTO contracts(address, is_contract, first_seen, deployer) VALUES(?,1,?,?)
            ON CONFLICT(address) DO UPDATE SET is_contract=1,
                first_seen=excluded.first_seen, deployer=excluded.deployer""", rows)

    # Which destinations are contracts? Check new ones once, then cache.
    addrs = {a for a, _ in activity}
    known = lookup(db, addrs)
    unknown = [a for a in addrs if a not in known]
    codes = rpc_many([("eth_getCode", [a, "latest"]) for a in unknown])
    fresh = [(a, int(c != "0x")) for a, c in zip(unknown, codes) if c is not None]
    db.executemany("INSERT OR IGNORE INTO contracts(address, is_contract) VALUES(?,?)", fresh)
    known.update(dict(fresh))

    # Record activity for contracts only
    db.executemany("INSERT OR IGNORE INTO callers VALUES(?,?,?)",
                   [(a, h, s) for (a, h), (senders, _) in activity.items()
                    if known.get(a) for s in senders])
    db.executemany("""
        INSERT INTO txs VALUES(?,?,?)
        ON CONFLICT(contract, hour) DO UPDATE SET n = n + excluded.n""",
                   [(a, h, n) for (a, h), (_, n) in activity.items() if known.get(a)])
    return last, hour

# ─── Fingerprinting ───────────────────────────────────────────────────────────

def extract_selectors(code_hex):
    """Every PUSH4 value in the bytecode — function selectors live here."""
    code = bytes.fromhex((code_hex or "0x")[2:])
    found, i = set(), 0
    while i < len(code):
        op = code[i]
        if 0x60 <= op <= 0x7f:             # PUSH1..PUSH32
            size = op - 0x5f
            if op == 0x63 and i + 5 <= len(code):
                found.add(code[i + 1:i + 5].hex())
            i += 1 + size
        else:
            i += 1
    return found


def classify(sels):
    has = lambda *names: all(S[n] in sels for n in names)
    any_ = lambda *names: any(S[n] in sels for n in names)

    if any_("handleOps06", "handleOps07"):          return "Infra: AA EntryPoint"
    if has("execTransaction"):                      return "Smart wallet (Safe)"
    if any_("createPair", "createPool"):            return "DEX factory"
    if has("pairSwap", "getReserves") or has("slot0"): return "DEX pool"
    if any_("swapExactTokensForTokens", "exactInputSingle", "exactInputSingle02"):
        return "DEX router"
    if has("latestRoundData"):                      return "Oracle / price feed"
    if has("vaultDeposit", "asset"):                return "Vault (ERC-4626)"
    if has("stake") and any_("getReward", "withdraw"): return "Staking"
    if has("safeBatchTransferFrom", "balanceOfBatch"): return "NFT (ERC-1155)"
    if has("ownerOf", "tokenURI"):                  return "NFT (ERC-721)"
    erc20_hits = sum(S[n] in sels for n in
                     ("transfer", "approve", "totalSupply", "balanceOf", "transferFrom"))
    if erc20_hits >= 3:
        if has("wethDeposit"):
            return "Wrapped ETH"
        tags = []
        if any_("mint(address,uint256)", "mint(uint256)"):
            tags.append("mintable")
        if has("renounceOwnership"):
            tags.append("ownable")
        base = "Token — simple, meme-style" if len(sels) <= 25 else "Token — custom logic"
        return base + (f" ({', '.join(tags)})" if tags else "")
    if any_("multicall", "aggregate3"):             return "Utility: multicall"
    if len(sels) > 40:                              return "Protocol (unknown, complex)"
    return "Unknown"


def decode_string(res):
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
        return raw.decode("utf-8", "ignore").strip()[:32]
    except Exception:
        return ""


def is_boring(c):
    """Infrastructure and stablecoins — real, but not what we are hunting."""
    if c["address"].startswith(PREDEPLOY):
        return True
    if (c["kind"] or "") in BORING_KIND:
        return True
    if (c["symbol"] or "").upper() in BORING_SYM and c["verified"]:
        return True
    return False


def suspicion_flags(c, socials):
    """Inconsistencies worth a second look. Not proof of anything."""
    flags = []
    sym = (c["symbol"] or "").upper()
    if sym in MAJOR_SYM and not c["address"].startswith(PREDEPLOY):
        flags.append("⚠️ impersonating")
    name = "".join(ch for ch in (c["symbol"] or c["ex_name"] or "").lower() if ch.isalnum())
    if socials and name and len(name) > 2:
        if not any(name in link.lower().replace("-", "") for link in socials):
            flags.append("⚠️ link mismatch")
    return flags


def fingerprint(db, addr):
    row = db.execute("SELECT kind FROM contracts WHERE address=?", (addr,)).fetchone()
    if row and row[0]:
        return
    code, slot, name, symbol = rpc_batch([
        ("eth_getCode", [addr, "latest"]),
        ("eth_getStorageAt", [addr, EIP1967_SLOT, "latest"]),
        ("eth_call", [{"to": addr, "data": "0x06fdde03"}, "latest"]),
        ("eth_call", [{"to": addr, "data": "0x95d89b41"}, "latest"]),
    ])
    code = code or "0x"
    proxy = None
    if EIP1167_TAG in code:
        i = code.index(EIP1167_TAG) + len(EIP1167_TAG)
        proxy = "0x" + code[i:i + 40]
    elif slot and int(slot, 16) != 0:
        proxy = "0x" + slot[-40:]

    impl_code = rpc("eth_getCode", [proxy, "latest"]) if proxy else code
    sels = extract_selectors(impl_code)
    kind = classify(sels) + (" [proxy]" if proxy else "")
    db.execute("""UPDATE contracts SET kind=?, name=?, symbol=?, proxy=?, selectors=?
                  WHERE address=?""",
               (kind, decode_string(name), decode_string(symbol), proxy,
                json.dumps(sorted(sels)), addr))


# ─── Explorer enrichment ──────────────────────────────────────────────────────

BOILERPLATE = ("openzeppelin", "soliditylang", "ethereum.org", "eips.ethereum",
               "consensys", "hardhat.org", "github.com/ethereum", "gnu.org",
               "spdx.org", "solidity.readthedocs", "creativecommons")
URL_RE     = re.compile(r"https?://[\w./\-?=#%&]+", re.I)
HANDLE_RE  = re.compile(r"(?:twitter\.com|x\.com|t\.me)/[\w./\-]+", re.I)


def explorer(path):
    """GET one explorer API path. Returns a dict, or None on any failure."""
    try:
        r = session.get(f"{EXPLORER_API}{path}", timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
        return data if isinstance(data, dict) and "message" not in data else None
    except (requests.RequestException, ValueError):
        return None


def find_socials(source):
    """Project links from verified source, minus library boilerplate."""
    found = []
    for hit in HANDLE_RE.findall(source) + URL_RE.findall(source):
        clean = hit.rstrip(".,);\"'*/ ")
        low = clean.lower()
        if any(b in low for b in BOILERPLATE) or len(clean) > 90:
            continue
        if clean not in found:
            found.append(clean)
        if len(found) == 2:
            break
    return found


def enrich(db, addr):
    """Pull name, verification, deployer, token data and socials from the explorer."""
    row = db.execute("SELECT enriched FROM contracts WHERE address=?", (addr,)).fetchone()
    if row and row[0]:
        return

    info = explorer(f"/addresses/{addr}") or {}
    verified = int(bool(info.get("is_verified")))
    ex_name = info.get("name") or ""
    creator = (info.get("creator_address_hash") or "").lower() or None

    token = explorer(f"/tokens/{addr}") if info.get("has_tokens") or info.get("token") else None
    token = token or {}
    token_type = token.get("type") or ""
    holders = int(token.get("holders_count") or 0)
    if token.get("name"):
        ex_name = ex_name or token["name"]

    socials = []
    if verified:
        sc = explorer(f"/smart-contracts/{addr}") or {}
        source = sc.get("source_code") or ""
        ex_name = sc.get("name") or ex_name
        if source:
            socials = find_socials(source)

    db.execute("""UPDATE contracts SET ex_name=?, verified=?, token_type=?, holders=?,
                  socials=?, enriched=1, deployer=COALESCE(deployer, ?) WHERE address=?""",
               (ex_name[:40], verified, token_type, holders,
                json.dumps(socials), creator, addr))

    # A creator that is itself a contract is a factory — i.e. a launchpad
    if creator:
        row = db.execute("SELECT is_contract, is_factory FROM contracts WHERE address=?",
                         (creator,)).fetchone()
        if not row or row["is_factory"] is None:
            code = rpc("eth_getCode", [creator, "latest"])
            factory = int(bool(code and code != "0x"))
            db.execute("""INSERT INTO contracts(address, is_contract, is_factory) VALUES(?,?,?)
                          ON CONFLICT(address) DO UPDATE SET is_factory=excluded.is_factory,
                          is_contract=MAX(COALESCE(contracts.is_contract, 0), excluded.is_contract)""",
                       (creator, factory, factory))

# ─── Optional Claude verdicts ─────────────────────────────────────────────────

def add_ai_notes(db, rows):
    if not ANTHROPIC_KEY:
        return
    todo = []
    for r in rows:
        c = db.execute("SELECT * FROM contracts WHERE address=?", (r["contract"],)).fetchone()
        if c["ai_note"]:
            continue
        sels = json.loads(c["selectors"] or "[]")
        todo.append({
            "address": c["address"], "heuristic": c["kind"],
            "name": c["name"], "symbol": c["symbol"], "is_proxy": bool(c["proxy"]),
            "known_functions": [SEL_NAMES[s] for s in sels if s in SEL_NAMES],
            "unknown_selector_count": sum(s not in SEL_NAMES for s in sels),
            "wallets_last_hour": r["callers"], "txs_last_hour": r["n"],
        })
    if not todo:
        return
    prompt = (
        "You classify smart contracts on GIWA Sepolia, an OP Stack L2 testnet. "
        "Names may be fake. For each contract give a verdict under 12 words: what it most "
        "likely is, and whether it looks like a memecoin, infrastructure, an app, or airdrop-farming bait. "
        "Reply ONLY with a JSON object mapping each address to its verdict. No markdown.\n\n"
        + json.dumps(todo)
    )
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": AI_MODEL, "max_tokens": 1500,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=60,
        )
        r.raise_for_status()
        text = "".join(b.get("text", "") for b in r.json().get("content", []))
        text = text.strip().removeprefix("```json").removesuffix("```").strip()
        for addr, note in json.loads(text).items():
            db.execute("UPDATE contracts SET ai_note=? WHERE address=?",
                       (str(note)[:100], addr.lower()))
    except Exception as e:
        log.warning("Claude verdicts skipped: %s", e)

# ─── Discord ──────────────────────────────────────────────────────────────────

def discord(embeds):
    for _ in range(5):
        r = requests.post(WEBHOOK, json={"embeds": embeds}, timeout=15)
        if r.status_code == 429:
            time.sleep(float(r.json().get("retry_after", 2)))
            continue
        if not r.ok:
            log.error("Discord said %s: %s", r.status_code, r.text[:200])
        return r.ok
    return False


def short(addr):
    return f"{addr[:6]}…{addr[-4:]}" if addr else "?"


def report(db, hour):
    rows = db.execute("""
        SELECT c.contract, COUNT(*) AS callers,
               (SELECT n FROM txs t WHERE t.contract = c.contract AND t.hour = ?) AS n,
               (SELECT COUNT(*) FROM callers p WHERE p.contract = c.contract AND p.hour = ?) AS prev
        FROM callers c
        WHERE c.hour = ?
        GROUP BY c.contract
        HAVING callers >= ?
        ORDER BY callers DESC
        LIMIT ?""", (hour, hour - 1, hour, MIN_CALLERS, CANDIDATES)).fetchall()

    new_deploys = db.execute(
        "SELECT COUNT(*) FROM contracts WHERE deployer IS NOT NULL AND first_seen / 3600 = ?",
        (hour,)).fetchone()[0]

    kept, skipped = [], 0
    for r in rows:
        try:
            fingerprint(db, r["contract"])
            enrich(db, r["contract"])
        except Exception as e:
            log.warning("Lookup failed for %s: %s", r["contract"], e)
        c = db.execute("SELECT * FROM contracts WHERE address=?", (r["contract"],)).fetchone()
        if CONSUMER_ONLY and is_boring(c):
            skipped += 1
            continue
        kept.append(r)
        if len(kept) == TOP_N:
            break
    rows = kept
    add_ai_notes(db, rows)
    db.commit()

    lines = []
    for i, r in enumerate(rows, 1):
        c = db.execute("SELECT * FROM contracts WHERE address=?", (r["contract"],)).fetchone()
        label = c["symbol"] or c["ex_name"] or c["name"] or short(c["address"])
        kind = c["token_type"] or c["kind"] or "Unknown"
        if c["token_type"] and c["kind"] and "Token" in c["kind"]:
            kind = c["kind"]
        n = r["n"] or 0
        growth = "🆕 first hour" if not r["prev"] else f"{(r['callers'] - r['prev']) / r['prev']:+.0%}"

        flags = ""
        if c["verified"]:
            flags += " ✅ verified"
        if n / r["callers"] > BOT_RATIO:
            flags += " 🤖 bot-heavy"
        elif r["callers"] and 0.9 <= n / r["callers"] <= 1.1 and r["callers"] > 500:
            flags += " 🌾 farm pattern"
        if c["first_seen"] and hour - c["first_seen"] // 3600 < 24:
            flags += " 🐣 <24h old"
        if c["is_factory"]:
            flags += " 🏭 factory"
        links = json.loads(c["socials"] or "[]")
        flags += "".join(" " + f for f in suspicion_flags(c, links))

        line = (f"**{i}. {label}** · {kind}{flags}\n"
                f"{r['callers']:,} wallets ({growth}) · {n:,} txs")
        if c["holders"]:
            line += f" · {c['holders']:,} holders"
        line += f"\n[{short(c['address'])}]({EXPLORER}{c['address']})"
        if c["deployer"]:
            count = db.execute("SELECT COUNT(*) FROM contracts WHERE deployer=?",
                               (c["deployer"],)).fetchone()[0]
            line += f" · by {short(c['deployer'])} ({count} contracts)"
        for link in links:
            line += f"\n🔗 {link}"
        if c["ai_note"]:
            line += f"\n> {c['ai_note']}"
        lines.append(line)

    stamp = time.strftime("%d %b %H:00 UTC", time.gmtime(hour * 3600))
    if not lines:
        lines = [f"No contract reached {MIN_CALLERS}+ unique wallets this hour."]

    embeds, chunk = [], ""
    for line in lines:
        if len(chunk) + len(line) > 2800:
            embeds.append(chunk)
            chunk = ""
        chunk += line + "\n\n"
    embeds.append(chunk)

    payload = [{"description": text[:4000], "color": 0x5865F2} for text in embeds]
    payload[0]["title"] = f"GIWA top {len(rows)} · {stamp}"

    pads = launchpad_board(db, hour)
    if pads:
        payload.append({"title": "🏭 Launchpads & factories (24h)", "description": pads[:4000],
                        "color": 0xEB459E})

    board = deployer_board(db, [r["contract"] for r in rows])
    if board:
        payload.append({"title": "Deployers behind this hour", "description": board[:4000],
                        "color": 0xFEE75C})
    payload[-1]["footer"] = {
        "text": f"{new_deploys} new deployments · {skipped} infra/stablecoin entries hidden"}
    discord(payload)
    log.info("Report sent for %s (%d contracts)", stamp, len(rows))


def launchpad_board(db, hour):
    """Contracts that deploy other contracts — token factories and launchpads."""
    rows = db.execute("""
        SELECT c.deployer AS pad, COUNT(*) AS children,
               SUM(CASE WHEN c.first_seen / 3600 > ? THEN 1 ELSE 0 END) AS recent
        FROM contracts c
        JOIN contracts p ON p.address = c.deployer AND p.is_factory = 1
        GROUP BY c.deployer
        HAVING children >= 2
        ORDER BY recent DESC, children DESC
        LIMIT 5""", (hour - 24,)).fetchall()
    out = []
    for i, r in enumerate(rows, 1):
        p = db.execute("SELECT ex_name, verified FROM contracts WHERE address=?",
                       (r["pad"],)).fetchone()
        label = (p["ex_name"] if p and p["ex_name"] else short(r["pad"]))
        tick = " ✅" if p and p["verified"] else ""
        out.append(f"**{i}. {label}**{tick} — {r['children']} contracts deployed "
                   f"({r['recent']} in 24h)\n[{short(r['pad'])}]({EXPLORER}{r['pad']})")
    return "\n".join(out)


def deployer_board(db, contracts):
    """Who is behind this hour's active contracts, ranked by contract count."""
    if not contracts:
        return ""
    q = f"""SELECT deployer, COUNT(*) AS n FROM contracts
            WHERE deployer IS NOT NULL AND address IN ({','.join('?' * len(contracts))})
            GROUP BY deployer ORDER BY n DESC LIMIT 5"""
    out = []
    for i, row in enumerate(db.execute(q, contracts), 1):
        total, verified = db.execute(
            "SELECT COUNT(*), COALESCE(SUM(verified), 0) FROM contracts WHERE deployer=?",
            (row[0],)).fetchone()
        out.append(f"**{i}.** [{short(row[0])}]({EXPLORER}{row[0]}) — "
                   f"{row[1]} in this top 20 · {total} total, {verified} verified")
    return "\n".join(out)


def prune(db, hour):
    cutoff = hour - 24 * KEEP_DAYS
    db.execute("DELETE FROM callers WHERE hour < ?", (cutoff,))
    db.execute("DELETE FROM txs WHERE hour < ?", (cutoff,))
    db.commit()

# ─── Main loop ────────────────────────────────────────────────────────────────

def self_test():
    head = int(rpc("eth_blockNumber", []), 16)
    print(f"✅ RPC works — latest block {head:,}")
    batch = rpc_batch([("eth_blockNumber", []), ("eth_chainId", [])])
    print(f"✅ Batch requests work — chain ID {int(batch[1], 16)}")
    ok = discord([{"title": "GIWA Scout test", "description": "Webhook connected. ✅", "color": 0x57F287}])
    print("✅ Discord message sent" if ok else "❌ Discord failed — check the webhook URL")


def main():
    if not WEBHOOK:
        sys.exit("❌ DISCORD_WEBHOOK_URL is missing from config.env")
    if "--test" in sys.argv:
        self_test()
        return

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA)
    migrate(db)

    head = int(rpc("eth_blockNumber", []), 16)
    saved = get_state(db, "next_block")
    nxt = int(saved) if saved else head
    if head - nxt > 3600:
        log.warning("Offline too long — skipping %s blocks to catch up", f"{head - nxt:,}")
        nxt = head

    last_hour = int(get_state(db, "last_hour") or 0)
    discord([{"title": "GIWA Scout online",
              "description": f"Watching from block {nxt:,}. Reports arrive on the hour.",
              "color": 0x57F287}])
    log.info("Scanning from block %s", f"{nxt:,}")

    while True:
        try:
            head = int(rpc("eth_blockNumber", []), 16)
            if nxt > head:
                time.sleep(1)
                continue
            if head - nxt > 300:
                log.warning("Falling behind by %s blocks", f"{head - nxt:,}")

            done, hour = process_blocks(db, nxt, min(nxt + BLOCK_BATCH - 1, head))
            if done < nxt:
                time.sleep(1)
                continue
            nxt = done + 1
            set_state(db, "next_block", nxt)

            if not last_hour:
                last_hour = hour
            elif hour > last_hour:          # an hour just finished on-chain
                report(db, last_hour)
                prune(db, hour)
                last_hour = hour
            set_state(db, "last_hour", last_hour)
            db.commit()

        except KeyboardInterrupt:
            log.info("Stopped.")
            break
        except Exception:
            log.exception("Loop error — continuing in 5s")
            db.rollback()
            time.sleep(5)


if __name__ == "__main__":
    main()
