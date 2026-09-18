"""
Site Watch
Monitors a website for launch signals: text changes, new build assets,
paths going live, and status changes. Pings Discord when something moves.

Run:   python site_watch.py
Test:  python site_watch.py --test
"""

import hashlib
import json
import logging
import os
import re
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv("config.env")

SITE     = os.getenv("WATCH_SITE", "https://zilkroad.com/").rstrip("/")
WEBHOOK  = os.getenv("MAINNET_WEBHOOK_URL", "").strip() or os.getenv("DISCORD_WEBHOOK_URL", "")
STATE    = os.getenv("SITE_STATE", "site_watch.json")
INTERVAL = int(os.getenv("WATCH_INTERVAL", "60"))

# Paths that would appear when a marketplace goes live
PROBE_PATHS = [
    "/marketplace", "/market", "/trade", "/app", "/collection", "/collections",
    "/listings", "/explore", "/mint", "/dashboard", "/api/listings", "/api/collections",
]

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; site-watch/1.0)"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("sitewatch")
session = requests.Session()

TAG_RE    = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.S | re.I)
STRIP_RE  = re.compile(r"<[^>]+>")
NONCE_RE  = re.compile(r"(nonce|csrf|token|build|hash)[\"'=:\s]+[\w\-]{8,}", re.I)
ASSET_RE  = re.compile(r"""(?:src|href)=["']([^"']+\.(?:js|css|json))["']""", re.I)
KEYWORDS  = ("marketplace", "trade", "buy now", "list", "sell", "floor", "offers",
             "connect wallet", "live", "opensea", "collection")


def load():
    try:
        with open(STATE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save(state):
    with open(STATE, "w") as f:
        json.dump(state, f, indent=1)


def discord(title, description, color=0x5865F2, ping=False):
    body = {"embeds": [{"title": title, "description": description[:3800], "color": color}]}
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


def visible_text(html):
    """Readable text only, with volatile tokens removed so nonces don't cause false alarms."""
    body = TAG_RE.sub(" ", html)
    body = STRIP_RE.sub(" ", body)
    body = NONCE_RE.sub("", body)
    return " ".join(body.split())


def fetch(url):
    try:
        r = session.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        return r.status_code, r.text, r.url
    except requests.RequestException as e:
        log.warning("Fetch failed for %s: %s", url, e)
        return None, "", url


def check_home(state):
    status, html, final = fetch(SITE + "/")
    if status is None:
        return
    text = visible_text(html)
    digest = hashlib.sha256(text.encode()).hexdigest()[:16]
    assets = sorted(set(ASSET_RE.findall(html)))
    hits = sorted({k for k in KEYWORDS if k in text.lower()})

    first = "home_hash" not in state
    if first:
        state.update({"home_hash": digest, "assets": assets, "keywords": hits,
                      "status": status, "text_len": len(text)})
        save(state)
        log.info("Baseline captured: %d chars, %d assets, keywords %s",
                 len(text), len(assets), hits)
        return

    if status != state.get("status"):
        discord("🌐 Site status changed",
                f"{SITE} now returns **{status}** (was {state['status']})",
                color=0xFEE75C, ping=True)
        state["status"] = status

    new_assets = [a for a in assets if a not in state.get("assets", [])]
    if new_assets:
        listed = "\n".join(f"`{a[:90]}`" for a in new_assets[:8])
        discord("📦 New build deployed",
                f"{SITE} shipped new files — usually the first sign of a launch.\n\n{listed}",
                color=0xEB459E, ping=True)
        log.info("New assets: %s", new_assets[:3])
        state["assets"] = assets

    new_words = [k for k in hits if k not in state.get("keywords", [])]
    if new_words:
        discord("🔑 New wording on the site",
                f"Appeared: **{', '.join(new_words)}**\n{SITE}",
                color=0xED4245, ping=True)
        state["keywords"] = hits

    if digest != state["home_hash"]:
        delta = len(text) - state.get("text_len", 0)
        discord("✏️ Page content changed",
                f"{SITE}\nText length {delta:+,} characters.\n"
                f"Open it — something moved.",
                color=0x5865F2, ping=abs(delta) > 200)
        log.info("Content changed (%+d chars)", delta)
        state["home_hash"] = digest
        state["text_len"] = len(text)

    save(state)


def check_paths(state):
    """A path counts as live only if it serves its own content — not a redirect
    or a catch-all showing the homepage."""
    live = set(state.get("live_paths", []))
    home_hash = state.get("home_hash")
    for path in PROBE_PATHS:
        status, html, final = fetch(SITE + path)
        if status is None:
            continue

        genuine = False
        if status < 400:
            landed = final.rstrip("/").lower()
            asked = (SITE + path).rstrip("/").lower()
            same_page = hashlib.sha256(
                visible_text(html).encode()).hexdigest()[:16] == home_hash
            genuine = landed.endswith(path.lower()) and landed == asked and not same_page

        if genuine and path not in live:
            live.add(path)
            state["live_paths"] = sorted(live)
            save(state)
            discord("🚀 NEW PAGE IS LIVE",
                    f"**{SITE}{path}** is serving its own page (HTTP {status}).\n{final}",
                    color=0x57F287, ping=True)
            log.info("Path live: %s (%s)", path, status)
        elif not genuine and path in live:
            live.discard(path)
            state["live_paths"] = sorted(live)
            save(state)


def main():
    if not WEBHOOK:
        sys.exit("❌ No webhook configured")

    if "--test" in sys.argv:
        status, html, _ = fetch(SITE + "/")
        text = visible_text(html)
        print(f"✅ {SITE} returned {status}, {len(text):,} chars of text")
        print(f"   Assets found: {len(set(ASSET_RE.findall(html)))}")
        print(f"   Keywords present: {sorted({k for k in KEYWORDS if k in text.lower()})}")
        discord("Site Watch test", f"Monitoring {SITE} every {INTERVAL}s.", color=0x57F287)
        print("✅ Discord message sent")
        return

    state = load()
    log.info("Watching %s every %ds", SITE, INTERVAL)
    if not state:
        discord("👀 Site Watch started",
                f"Monitoring {SITE} for launch signals every {INTERVAL} seconds.",
                color=0x57F287)

    cycle = 0
    while True:
        try:
            check_home(state)
            if cycle % 5 == 0:          # probe paths every 5th cycle
                check_paths(state)
            cycle += 1
        except KeyboardInterrupt:
            log.info("Stopped.")
            break
        except Exception:
            log.exception("Loop error")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
