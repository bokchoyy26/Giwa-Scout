"""
Zilk Sniper
Polls the zilkroad market API and pings Discord the moment a zkSNARK
is listed at or below your price threshold.

Run:   python zilk_sniper.py
Test:  python zilk_sniper.py --test    (shows floor + cheapest listings, no monitoring)
"""

import json
import logging
import os
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv("config.env")

API_TOKENS = "https://zilkroad.com/api/market/tokens"
API_RATE   = "https://zilkroad.com/api/market/rate"
TOKEN_URL  = os.getenv("ZILK_TOKEN_URL", "https://zilkroad.com/#/token/{id}")
MARKET_URL = "https://zilkroad.com/#/for-sale"

WEBHOOK   = os.getenv("ZILK_WEBHOOK_URL", "").strip() or os.getenv("MAINNET_WEBHOOK_URL", "").strip() \
            or os.getenv("DISCORD_WEBHOOK_URL", "")
THRESHOLD = float(os.getenv("ZILK_MAX_PRICE", "1.0"))    # alert at or below this ask
INTERVAL  = int(os.getenv("ZILK_INTERVAL", "5"))         # seconds between polls
STATE     = os.getenv("ZILK_STATE", "zilk_sniper.json")
ALERT_FLOOR = os.getenv("ZILK_ALERT_FLOOR", "1") == "1"  # also alert on new all-time floor

HEADERS = {"accept": "application/json",
           "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/152.0.0.0"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("zilk")
session = requests.Session()


def load():
    try:
        with open(STATE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save(state):
    with open(STATE, "w") as f:
        json.dump(state, f)


def discord(title, description, color=0x57F287, ping=False):
    body = {"embeds": [{"title": title, "description": description[:3800], "color": color}]}
    if ping:
        body["content"] = "@everyone"
    for _ in range(4):
        try:
            r = session.post(WEBHOOK, json=body, timeout=10)
            if r.status_code == 429:
                time.sleep(float(r.json().get("retry_after", 2)))
                continue
            return r.ok
        except requests.RequestException as e:
            log.warning("Discord problem: %s", e)
            time.sleep(2)
    return False


_etag = {"tokens": None}
_stats = {"polls": 0, "changed": 0, "unchanged": 0}


def get_json(url, timeout=20):
    r = session.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.json()


def get_tokens():
    """Conditional fetch. Returns None when the data has not changed since last poll.

    Their cache sets max-age=15, so most polls come back 304 for a few hundred
    bytes. Polling faster than that catches the refresh sooner, not fresher data.
    """
    headers = dict(HEADERS)
    if _etag["tokens"]:
        headers["if-none-match"] = _etag["tokens"]
    r = session.get(API_TOKENS, headers=headers, timeout=20)
    _stats["polls"] += 1

    if r.status_code == 304:
        _stats["unchanged"] += 1
        return None
    r.raise_for_status()
    _etag["tokens"] = r.headers.get("etag")
    _stats["changed"] += 1
    return r.json()


def zec_usd():
    try:
        return float(get_json(API_RATE, timeout=10).get("zecUsd") or 0)
    except Exception:
        return 0.0


def listings(data=None):
    """Every currently listed token, as {id: {...}}. None means unchanged since last poll."""
    if data is None:
        data = get_tokens()
        if data is None:
            return None
    out = {}
    for t in data.get("tokens", []):
        if not t.get("listed") or t.get("ask") in (None, ""):
            continue
        try:
            ask = float(t["ask"])
        except (TypeError, ValueError):
            continue
        out[str(t["id"])] = {
            "ask": ask,
            "listing": t.get("listingId"),
            "rank": t.get("rank"),
            "auction": bool(t.get("auction")),
            "traits": t.get("traits", {}),
        }
    return out


def describe(tid, item, rate):
    traits = item.get("traits", {})
    nice = " · ".join(f"{v}" for k, v in list(traits.items())[:4] if v and v != "None")
    usd = f" (~${item['ask'] * rate:,.0f})" if rate else ""
    return (f"**#{tid}** — **{item['ask']:g} ZEC**{usd}\n"
            f"rank {item.get('rank', '?')} · {nice or 'no traits'}"
            f"{' · AUCTION' if item['auction'] else ''}\n"
            f"[open listing]({TOKEN_URL.format(id=tid)}) · [market]({MARKET_URL})")


def check(state, seen, rate):
    current = listings()
    if current is None:          # nothing changed on their side
        return 0, state.get("listed_count", 0)
    alerts = 0

    for tid, item in current.items():
        if item["ask"] > THRESHOLD:
            continue
        key = f"{tid}:{item['listing']}:{item['ask']:g}"
        if key in seen:
            continue
        seen.add(key)
        discord(f"💎 SUB-{THRESHOLD:g} ZEC LISTING", describe(tid, item, rate),
                color=0x57F287, ping=True)
        log.info("Alert: #%s at %s ZEC", tid, item["ask"])
        alerts += 1

    if current:
        floor_id, floor = min(current.items(), key=lambda kv: kv[1]["ask"])
        prev = state.get("floor")
        if ALERT_FLOOR and prev is not None and floor["ask"] < prev and floor["ask"] > THRESHOLD:
            discord("📉 New floor", describe(floor_id, floor, rate),
                    color=0xFEE75C, ping=False)
            log.info("New floor: #%s at %s ZEC", floor_id, floor["ask"])
        state["floor"] = floor["ask"]
        state["listed_count"] = len(current)

    # keep the dedupe set from growing forever
    if len(seen) > 5000:
        seen.clear()
    return alerts, len(current)


def main():
    if not WEBHOOK:
        sys.exit("❌ No webhook configured")

    if "--test" in sys.argv:
        rate = zec_usd()
        current = listings(get_json(API_TOKENS))
        print(f"✅ API reachable — {len(current):,} tokens currently listed")
        print(f"   ZEC/USD: ${rate:,.2f}")
        cheapest = sorted(current.items(), key=lambda kv: kv[1]["ask"])[:5]
        print("   Cheapest listings right now:")
        for tid, item in cheapest:
            print(f"     #{tid:<6} {item['ask']:>10g} ZEC   (~${item['ask'] * rate:,.0f})"
                  f"   rank {item.get('rank')}")
        under = [t for t, i in current.items() if i["ask"] <= THRESHOLD]
        print(f"   At or below your {THRESHOLD:g} ZEC threshold: {len(under)}")
        discord("Zilk Sniper test",
                f"Watching {len(current):,} listings. Floor "
                f"{cheapest[0][1]['ask']:g} ZEC. Threshold {THRESHOLD:g} ZEC.",
                color=0x57F287)
        print("✅ Discord message sent")
        return

    state = load()
    seen = set(state.get("seen", []))
    rate, rate_at = zec_usd(), time.time()

    log.info("Watching zilkroad · threshold %s ZEC · every %ss", THRESHOLD, INTERVAL)
    discord("🎯 Zilk Sniper armed",
            f"Alerting on any listing at or below **{THRESHOLD:g} ZEC**.\n"
            f"Checking every {INTERVAL} seconds.", color=0x57F287)

    while True:
        try:
            if time.time() - rate_at > 600:
                rate, rate_at = zec_usd(), time.time()
            alerts, listed = check(state, seen, rate)
            state["seen"] = list(seen)[-5000:]
            save(state)
            if alerts:
                log.info("%d alert(s) · %d listed", alerts, listed)
            if _stats["polls"] % 600 == 0:
                log.info("%d polls · %d changed · %d unchanged (%.0f%% saved)",
                         _stats["polls"], _stats["changed"], _stats["unchanged"],
                         100 * _stats["unchanged"] / max(_stats["polls"], 1))
        except KeyboardInterrupt:
            log.info("Stopped.")
            break
        except requests.RequestException as e:
            log.warning("Fetch failed: %s", e)
        except Exception:
            log.exception("Loop error")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
