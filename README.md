# GIWA Scout

On-chain monitoring for the GIWA network (Upbit's OP Stack L2), plus a website launch watcher.
Three services, running on a VPS under systemd, alerting to Discord.

| Service | Purpose | Output |
|---|---|---|
| `giwa_scout.py` | Ranks testnet contracts by unique wallets, classifies them from bytecode and explorer data | Hourly Discord report |
| `giwa_launchwatch.py` | Discovers the mainnet RPC automatically, then watches for pairs, bridges and known deployers | Instant alerts |
| `site_watch.py` | Detects website launches via content, build assets and path probing | Instant alerts |

## How it works

**Contract classification** — extracts PUSH4 function selectors from bytecode and matches them
against known signatures (ERC-20, ERC-721, DEX routers, staking, proxies). Explorer data takes
priority where a contract is verified.

**Launchpad detection** — a contract whose creator is itself a contract is a factory. Tracking
factories catches every token they deploy afterwards.

**Farm filtering** — roughly one transaction per wallet at scale means airdrop farming, not usage.
These get flagged and can be filtered out.

**Deployer watchlist** — every testnet deployer is recorded. If one of those addresses deploys on
mainnet, it alerts immediately. Teams reuse deployer keys.

**Bridge detection** — OP Stack predeploys are at fixed addresses, so canonical bridge deposits and
bridged token creations are detectable from block one. Third-party bridges are caught behaviourally:
one wallet paying many unrelated recipients.

## Setup

```bash
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
cp config.env.example config.env    # then fill in your webhook and RPC
./venv/bin/python giwa_scout.py --test
```

Install as services with the included `.service` files:

```bash
cp *.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now giwa-scout giwa-launchwatch site-watch
```

## Configuration

All settings live in `config.env` (never committed — see `config.env.example`).

| Setting | Meaning |
|---|---|
| `DISCORD_WEBHOOK_URL` | Hourly reports |
| `MAINNET_WEBHOOK_URL` | Launch alerts (separate channel recommended) |
| `RPC_URL` | Testnet endpoint |
| `MAINNET_RPC` | Manual override; blank means auto-discover |
| `CONSUMER_ONLY` | `1` hides stablecoins and infrastructure |
| `BLOCK_BATCH` | Blocks per RPC request |
| `WATCH_SITE` | Site to monitor |

## Limitations

- Classification is heuristic. Labels are hints, not proof.
- Social links found in verified source cannot be proven to belong to the project.
- GIWA has no public mempool, so only mined blocks are visible.
- Testnet activity predicting mainnet outcomes is an untested assumption.

## Stack

Python 3, SQLite, systemd. JSON-RPC batching, Blockscout API, Discord webhooks.
