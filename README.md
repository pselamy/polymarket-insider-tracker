# Polymarket Insider Tracker

**Detect informed money before the market moves.**

[![CI](https://github.com/pselamy/polymarket-insider-tracker/actions/workflows/ci.yml/badge.svg)](https://github.com/pselamy/polymarket-insider-tracker/actions/workflows/ci.yml)
[![Python 3.11–3.13](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Real-time detection of suspicious trading patterns on Polymarket: fresh wallets, unusual sizing, niche-market activity, and funding chain analysis. Streams trades via WebSocket, profiles wallets on-chain (Polygon), scores risk with ML + heuristics, and dispatches alerts to Discord/Telegram.

---

## Supported Runtime

- CPython 3.11, 3.12, and 3.13 are supported. Python 3.10 and 3.14+ are intentionally rejected by
  project metadata until their complete locked matrices are approved.
- Ubuntu 24.04 x86_64 is the blocking Linux reference environment. This does not certify every Linux
  distribution, libc, or architecture.
- Apple Silicon macOS (`arm64`) is supported and receives advisory compatibility automation plus local
  release evidence with Docker services.
- uv `>=0.11,<0.12` and the checked-in `uv.lock` define reproducible installation. CI installs uv
  0.11.26 exactly; later uv versions must first be admitted by an approved support change.

Run `uv run --env-file .env python scripts/verify.py --profile all` for the complete local gate, or select `static`,
`compatibility`, or `services` for the independently runnable profiles described below.

---

## Foundation Quick Start (under 5 minutes)

The measured foundation path includes locked Python dependency installation and excludes only the first
download of the PostgreSQL and Redis container images. It verifies the local runtime and services; it
does not contact Polymarket, Polygon, Discord, or Telegram.

### 1. Install

```bash
# Requires: Python 3.11, 3.12, or 3.13; uv 0.11; Docker with Compose
git clone https://github.com/pselamy/polymarket-insider-tracker.git
cd polymarket-insider-tracker
uv sync --locked --all-extras
```

The checked-in `uv.lock` is the reproducible dependency authority. Editable pip installation is an
unsupported convenience and is not equivalent verification evidence.

### 2. Configure

```bash
cp .env.example .env
# Edit .env if the default loopback ports or development credentials conflict locally.
```

New configurations use one Psycopg 3 URL for the application and Alembic:

```text
postgresql+psycopg://tracker:dev_password@localhost:5432/polymarket_tracker
```

### 3. Start infrastructure

```bash
docker compose up -d --wait postgres redis
docker compose ps
```

### 4. Verify services and migrations safely

```bash
uv run --env-file .env python scripts/runtime_services.py --phase all
```

This probes PostgreSQL and Redis, then runs upgrade → downgrade → upgrade against a generated disposable
loopback database. It always attempts to remove that database and never downgrades or drops the normal
application database named in `.env`.

### 5. Migrate the application database and start

```bash
uv run --env-file .env alembic upgrade head
uv run --env-file .env python -m polymarket_insider_tracker --config-check
uv run --env-file .env python -m polymarket_insider_tracker
```

The final command contacts configured external services. It is outside the deterministic foundation
verification above.

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
uv run python scripts/verify.py --profile static
uv run python scripts/verify.py --profile compatibility
uv run --env-file .env python scripts/verify.py --profile services
uv run --env-file .env python scripts/verify.py --profile all
```

The `static` profile checks the lock, Black formatting across the repository, Ruff lint/import rules,
strict mypy, strict Pyright, Vulture dead code detection, and Complexipy cognitive complexity analysis. Pyright is an additional checker, not a mypy replacement. Its canonical
scope is the complete production package, configured for the lowest supported Python version:

```bash
uv run --isolated --locked --all-extras --python 3.11 pyright src/polymarket_insider_tracker
```

`pyproject.toml` keeps that scope in strict mode with no exclusions or baseline. The local stubs under
`typings/` describe only the external `py-clob-client` and scikit-learn APIs consumed by the package.
Vulture runs as its own required CI job and verifier gate over `src`, `tests`, `scripts`, `alembic`, and
`conftest.py`, which together hold every tracked repository Python file. It runs at its default confidence
with no baseline, allowlist, ignore list, decorator exemption, or path exclusion:

```bash
uv run --isolated --locked --all-extras --python 3.11 vulture src tests scripts alembic conftest.py
```

Complexipy runs as its own required CI job and verifier gate over `src`, `tests`, `scripts`, `alembic`, and
`conftest.py`, which together hold every tracked repository Python file. It enforces a strict maximum cognitive
complexity of 5 with `--no-ignore` enabled, rejecting inline suppression comments and configuration escape hatches:

```bash
uv run --isolated --locked --all-extras --python 3.11 complexipy src tests scripts alembic conftest.py --max-complexity-allowed 5 --no-ignore
```

The `compatibility` profile checks locked imports and the deterministic test suite. The `services`
profile performs real local probes and the disposable migration cycle. Individual commands remain
visible in verifier output and `--help`.

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
Ensure PostgreSQL is healthy with `docker compose ps` and that `.env` contains a loopback
`postgresql+psycopg://` `DATABASE_URL` matching the Compose credentials. Run
`uv run --env-file .env python scripts/runtime_services.py --phase probe` first, then
`uv run --env-file .env alembic upgrade head`. Bare `postgresql://` and legacy
`postgresql+asyncpg://` values are temporarily normalized with a deprecation warning; migrate them to
the canonical Psycopg spelling. Driver-specific asyncpg query options are rejected before connection.

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
