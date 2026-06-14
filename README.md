# Polymarket Insider Tracker

**Detect informed money before the market moves.**

[![CI](https://github.com/pselamy/polymarket-insider-tracker/actions/workflows/ci.yml/badge.svg)](https://github.com/pselamy/polymarket-insider-tracker/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Real-time detection of suspicious trading patterns on Polymarket: fresh wallets, unusual sizing, niche-market activity, and funding chain analysis. Streams trades via WebSocket, profiles wallets on-chain (Polygon), scores risk with ML + heuristics, and dispatches alerts to Discord/Telegram.

---

## Quick Start (< 2 minutes)

### 1. Install

```bash
# Requires: Python 3.11+, Docker
git clone https://github.com/pselamy/polymarket-insider-tracker.git
cd polymarket-insider-tracker
uv sync --all-extras          # or: pip install -e ".[dev]"
```

### 2. Start infrastructure

```bash
docker compose up -d           # PostgreSQL 15 + Redis 7
docker compose ps              # wait for healthy
```

### 3. Configure

```bash
cp .env.example .env
# Edit .env — only DATABASE_URL and REDIS_URL are required for local dev
# (defaults in .env.example work with docker compose)
```

### 4. Run migrations + start

```bash
uv run alembic upgrade head
uv run python -m polymarket_insider_tracker
```

You should see live trades within seconds:

```
INFO  Connection state: disconnected -> connecting
INFO  Connected to wss://ws-live-data.polymarket.com and subscribed to trades
DEBUG Trade: BUY 450 @ 1.00 on fifwc-ger-kor-2026-06-14-ger
DEBUG Trade: SELL 5 @ 0.86 on chi1-cd1-cdl-2026-06-14-draw
```

### CLI Options

```bash
python -m polymarket_insider_tracker --help
  --version          Show version
  --config-check     Validate configuration and exit
  --log-level DEBUG  Override log level
  --dry-run          Run pipeline without sending alerts
  --health-port 8080 Override health check port
```

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string |
| `REDIS_URL` | No | `redis://localhost:6379` | Redis connection string |
| `POLYGON_RPC_URL` | No | `https://polygon-rpc.com` | Polygon RPC (public default works) |
| `POLYGON_FALLBACK_RPC_URL` | No | — | Fallback RPC endpoint |
| `POLYMARKET_WS_URL` | No | `wss://ws-live-data.polymarket.com` | WebSocket endpoint |
| `POLYMARKET_API_KEY` | No | — | Optional API key for higher rate limits |
| `DISCORD_WEBHOOK_URL` | No | — | Discord alerts |
| `TELEGRAM_BOT_TOKEN` | No | — | Telegram alerts (needs `TELEGRAM_CHAT_ID` too) |
| `TELEGRAM_CHAT_ID` | No | — | Telegram chat for alerts |
| `LOG_LEVEL` | No | `INFO` | Logging level |
| `DRY_RUN` | No | `false` | Skip sending alerts |
| `HEALTH_PORT` | No | `8080` | Health check HTTP port |

No API keys are needed for basic operation — the Polymarket WebSocket and CLOB REST APIs are public.

---

## What It Detects

| Signal | Detection Method | Threshold |
|--------|-----------------|-----------|
| **Fresh Wallets** | Wallet age < 48h, nonce <= 5, making trades > $1k | Confidence 0.5-0.9 |
| **Size Anomalies** | Trade size > 2% of 24h volume or > 5% of order book | Weighted by niche factor |
| **Niche Markets** | Low-volume markets (< $50k daily) with specific outcomes | 1.5x risk multiplier |
| **Funding Chains** | Trace wallet funding to known entities (exchanges, etc.) | On-chain lineage |
| **Sniper Clusters** | DBSCAN clustering of wallets entering within minutes | Coordinated behavior |

Risk scoring combines signals with configurable weights (default threshold: 0.6). Multi-signal bonuses: 2 signals +20%, 3+ signals +30%.

### Sample Alert

```
SUSPICIOUS ACTIVITY DETECTED

Wallet: 0x7a3...f91 (Age: 2 hours, 3 transactions)
Market: "Will X announce Y by March 2026?"
Action: BUY YES @ $0.075
Size: $15,000 USDC (8.2% of daily volume)

Risk Signals:
  [x] Fresh Wallet (fewer than 5 transactions lifetime)
  [x] Niche Market (less than $50k daily volume)
  [x] Large Position (more than 2% order book impact)

Funding Trail:
  --> 0xdef...789 (2-year-old wallet, 500+ txns)
      --> Binance Hot Wallet

Confidence: HIGH (3/4 signals triggered)
```

---

## Architecture

```
Polymarket WebSocket ──> Ingestor ──> Profiler ──> Detector ──> Alerter
(wss://ws-live-data)    (trades)    (on-chain)   (scoring)   (Discord/TG)
                                        |
                                   Polygon RPC
```

### Components

| Module | Purpose |
|--------|---------|
| `ingestor/` | WebSocket trade stream + CLOB REST client with rate limiting |
| `profiler/` | Polygon wallet analysis, entity identification, funding chain tracing |
| `detector/` | Fresh wallet, size anomaly, sniper cluster detection, composite risk scorer |
| `alerter/` | Multi-channel dispatch (Discord webhooks, Telegram bot) with dedup |
| `storage/` | SQLAlchemy ORM + Alembic migrations (PostgreSQL) |
| `pipeline.py` | Orchestrator wiring all components together |
| `shutdown.py` | Graceful SIGTERM/SIGINT handling with cleanup callbacks |

---

## Development

```bash
uv run pytest                        # run tests
uv run ruff check src/ tests/        # lint
uv run ruff format src/ tests/       # format
uv run mypy src/                     # type check (strict mode)
```

### Docker Services

| Service | Port | Description |
|---------|------|-------------|
| PostgreSQL 15 | 5432 | Primary database |
| Redis 7 | 6379 | Caching and pub/sub |
| Adminer | 8080 | Database admin UI (optional, `--profile tools`) |
| RedisInsight | 5540 | Redis admin UI (optional, `--profile tools`) |

---

## Troubleshooting

**No trades received / silent connection**
The WebSocket subscription requires `action: "subscribe"` in the envelope. If you're on an older version, update — this was fixed in the WebSocket protocol alignment (see #89).

**Connection timeout / DNS errors**
Verify `wss://ws-live-data.polymarket.com` is reachable from your network. Some corporate firewalls block WebSocket connections.

**Database migration errors**
Ensure PostgreSQL is running (`docker compose ps`) and `DATABASE_URL` matches your docker-compose config. Run `uv run alembic upgrade head` after any schema changes.

**Rate limiting on Polygon RPC**
The default public RPC (`https://polygon-rpc.com`) has low rate limits. For production use, set `POLYGON_RPC_URL` to a dedicated provider (Alchemy, QuickNode, etc.).

---

## Disclaimer

This software is provided for **educational and research purposes only**.

- Trading prediction markets involves significant financial risk
- This tool does not constitute financial advice
- Insider trading is illegal in regulated markets; this tool is for transparency and research
- Users are responsible for compliance with applicable laws

## License

MIT License - see [LICENSE](LICENSE) for details.
