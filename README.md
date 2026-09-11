# Polymarket Insider Tracker

**Detect informed money before the market moves.**

[![CI](https://github.com/pselamy/polymarket-insider-tracker/actions/workflows/ci.yml/badge.svg)](https://github.com/pselamy/polymarket-insider-tracker/actions/workflows/ci.yml)
[![Python 3.11–3.13](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Near-real-time detection of suspicious trading patterns on Polymarket: fresh wallets, unusual sizing, niche-market activity, and funding chain analysis. Polls the documented public trades query for wallet-bearing trades, profiles wallets on-chain (Polygon), scores risk with ML + heuristics, and dispatches alerts to Discord/Telegram.

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
application database named in `.env`. The `services` verification profile additionally runs the shared
Redis behavioral contract (`tests/integration/test_redis_contract.py`) against that loopback Redis inside
a disposable key namespace, proving the in-memory `fakeredis` used by unit tests matches the real service.

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
  --config-check     Validate offline configuration syntax and exit
  --log-level DEBUG  Override log level
  --dry-run          Run pipeline without sending alerts or writing dedup keys
  --health-port 8080 Override health check HTTP port
```

### Health & Observability Endpoints

When running, the tracker serves HTTP health and metrics endpoints on `--health-port` (default `8080` or `HEALTH_PORT`):

- **`/live`**: HTTP 200 `{"live": true}` while the event loop runs.
- **`/ready`**: HTTP 200 `{"ready": true}` when PostgreSQL, Redis, and trade acquisition can make progress; HTTP 503 when a required dependency is unavailable, ingestion has terminally failed, or the trade source has no recent successful acquisition (a source that never connected — or whose last completed fetch is older than the staleness threshold — is `down`, not ready). A recoverable `degraded` or `possible-data-loss` source with fresh successful acquisitions stays ready but is reported as `"degraded"` in the components summary with its error in `/health`.
- **`/health`**: Detailed JSON report with component statuses (`up`, `down`, `degraded`), probe latencies, successful-acquisition timestamps, quiet-period indicators, and a top-level `last_error` carrying the most recent worker or per-trade processing error (`null` when none). URLs and error text are centrally redacted, so credentials never appear in health output or logs.
- **`/metrics`**: Prometheus metrics (`polymarket_events_total`, `polymarket_events_per_second`, `polymarket_stream_status`, `polymarket_last_event_timestamp`, `polymarket_health_status`). The overall-health gauge is computed from the same combined snapshot as `/health`, so the two can never disagree about the same state.

### Safe Alert Delivery & Deduplication

- **`--dry-run`**: Processes all trades and persists risk assessments with `delivery_disposition="dry_run"` while making zero external notification calls and writing zero deduplication keys to Redis.
- **Channel-Scoped Deduplication**: Delivery history is tracked per channel (`alert:dedup:<channel>:<wallet>:<market>`). If delivery fails for one channel, it remains eligible for retry while successful channels are deduplicated.
- **Ambiguity Suppression Window**: On channel delivery timeout, a 60-second window (`alert:ambiguous:<channel>:<wallet>:<market>`) suppresses immediate duplicate alerts while downstream delivery is indeterminate. The same key is acquired atomically before every send attempt (and released on a confirmed outcome), so two concurrent dispatches of one wallet/market identity cannot both deliver. A send that exceeds its 45s logical deadline is classified ambiguous regardless of any late result, and if it resists cancellation the claim is renewed until the send truly terminates — the claim can never expire while a send is still in flight.

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string |
| `REDIS_URL` | No | `redis://localhost:6379` | Redis connection string |
| `POLYGON_RPC_URL` | No | `https://polygon-rpc.com` | Polygon RPC (public default works) |
| `POLYGON_FALLBACK_RPC_URL` | No | — | Fallback RPC endpoint |
| `POLYMARKET_TRADES_URL` | No | `https://data-api.polymarket.com/trades` | Documented anonymous public trades query |
| `POLYMARKET_TRADES_COVERAGE` | No | `all` | `all` public participant observations, or the provider's `taker-only` subset |
| `POLYMARKET_TRADES_POLL_INTERVAL_SECONDS` | No | `5` | Seconds between acquisition cycles (1–60) |
| `POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS` | No | `600` | Identity retention and loss-detection horizon (60–3600) |
| `POLYMARKET_WS_URL` | No | — | **Deprecated.** Accepted only with `ws://`/`wss://`, warns, never used |
| `POLYMARKET_API_KEY` | No | — | Optional API key consumed by the CLOB client only |
| `DISCORD_WEBHOOK_URL` | No | — | Discord alerts |
| `TELEGRAM_BOT_TOKEN` | No | — | Telegram alerts (needs `TELEGRAM_CHAT_ID` too) |
| `TELEGRAM_CHAT_ID` | No | — | Telegram chat for alerts |
| `LOG_LEVEL` | No | `INFO` | Logging level |
| `DRY_RUN` | No | `false` | Skip sending alerts |
| `HEALTH_PORT` | No | `8080` | Health check HTTP port |

No API keys are needed for basic operation — the Polymarket public trades query and CLOB REST APIs are
public, and no credential is ever sent to the trades endpoint.

### Trade Ingestion

The tracker acquires trades by **near-real-time polling** of the documented public trades query, not by
push delivery. Every cycle (default every 5 seconds, about 1% of the published limit of 200 requests per
10 seconds; the configurable 1-second minimum stays at 5%) requests one full 10,000-row page with a
strictly increasing `end` cutoff so the provider's shared cache never serves a stale page, sorts and
validates rows on the client, de-duplicates by a composite observation identity, and delivers each new
observation to the detection pipeline exactly once, oldest first. Coverage defaults to all public
participant observations (`takerOnly=false`); several wallet rows per transaction are legitimate and
distinct, while exact repeats are suppressed.

A durable complete-through boundary, the recent identity window, and loss events live in Redis. A page
advances the boundary only when it provably reaches the previous boundary (its oldest row is older than
the boundary and every retained newer identity reappears); otherwise the tracker fetches the single
documented recovery page (`offset=10000`) and, if the page still cannot be proven, enters the visible
`possible-data-loss` state with a frozen boundary while new observations keep flowing. A gap that ages
past the recovery horizon, or a restart older than it, is written as a durable, logged loss event
(`horizon-expired`, `restart-beyond-horizon`, `continuity-mismatch`) before the boundary re-anchors.
The 10-minute horizon is a retention and loss-detection bound: reachable depth is two 10,000-row pages
(roughly five to seven minutes at observed rates), so an outage can produce a recorded loss event before
it is 10 minutes old. Acquisition starts before the market-metadata crawl finishes; metadata only
repairs missing outcomes and enriches detection.

Status exposes the lifecycle state (`running`, `degraded`, `possible-data-loss`, `failed`), last success
and last trade times, provider timestamp lag (freshness, not first-publication latency), duplicate and
invalid-row counts, and recent loss events; Prometheus metrics use the `polymarket_ingest_` prefix.
`POLYMARKET_WS_URL` is deprecated: it is accepted only with a WebSocket scheme, produces an actionable
warning at load and in `--config-check`, and is never used or reinterpreted as an HTTP source.

A bounded, aggregate-only live-safe smoke check is available on demand and never runs in tests or CI:

```bash
uv run --env-file .env python scripts/trades_smoke.py --live --window-seconds 5 --json
```

---

## What It Detects

| Signal | Detection Method | Threshold |
|--------|-----------------|-----------|
| **Fresh Wallets** | Wallet age < 48h, nonce <= 5, making trades > $1k | Confidence 0.5-0.9 |
| **Size Anomalies** | Trade size > 2% of 24h volume or > 5% of order book | Weighted by niche factor |
| **Niche Markets** | Low-volume markets (< $50k daily) with specific outcomes | 1.5x risk multiplier |
| **Funding Chains** | Trace wallet funding to known entities (exchanges, etc.) | On-chain lineage |
| **Sniper Clusters** | DBSCAN clustering of wallets entering within minutes | Coordinated behavior |

Risk scoring combines signals with the algorithm's immutable default weights (alert threshold 0.80, tunable via `DETECTOR_ALERT_THRESHOLD`). Multi-signal bonuses: 2 signals +20%, 3+ signals +30%. Every persisted assessment records its `scoring_algorithm_version` and the exact `scoring_config` (weights, bonuses, threshold, quantum) that produced it, so it can be replayed exactly from its own stored values; rows created before versioning existed are labeled `legacy-unversioned`. The library-level `weights=`/`set_weights()` configuration API is deprecated (one compatibility window, `DeprecationWarning`) and unused by the wired pipeline.

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
Public trades query ──> Ingestor ──> Profiler ──> Detector ──> Alerter
(data-api, polled)     (poller)    (on-chain)   (scoring)   (Discord/TG)
        |                                |
   Redis boundary                   Polygon RPC
```

### Components

| Module | Purpose |
|--------|---------|
| `ingestor/` | Near-real-time trade poller (strict rows, durable Redis boundary, loss detection) + CLOB REST client with rate limiting; deprecated WebSocket handler retained |
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
strict mypy, strict Pyright, Vulture dead code detection, and Complexipy cognitive complexity analysis.
Unit tests use working fakes and real values instead of `unittest.mock`; an AST policy test rejects any
spelling of that framework, Redis is `fakeredis` verified by the services-profile contract, and only
external boundaries (alert channels, the CLOB SDK, the Polygon RPC, webhook servers) are faked. Pyright is an additional checker, not a mypy replacement. Its canonical
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
complexity of 5 for functions and module-level control flow. Explicit flags disable inline suppression,
report-only mode, automatic snapshots, snapshot creation, and cwd-config exclusions:

```bash
uv run --isolated --locked --all-extras --python 3.11 python scripts/complexipy_gate.py src tests scripts alembic conftest.py --max-complexity-allowed 5 --no-ignore --ignore-complexity=false --snapshot-ignore=true --snapshot-create=false --exclude=. --check-script=true
```

The launcher validates the exact visible arguments, resolves only the five scope paths to absolute paths,
and invokes the pinned CLI from a fresh temporary working directory. That prevents any repository-working-
directory TOML—including `[diff]` settings—from changing success semantics. `--exclude=.` is a non-matching
CLI pattern and does not exclude repository files.

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

**No trades received**
Check the ingestion state in the logs. `degraded` means transient provider failures are being retried
with bounded backoff; `failed` means a terminal response (HTTP 400/401/403/404/410 or an incompatible
body) stopped acquisition and needs a configuration or provider-contract fix. A quiet interval shows
`last_success_at` advancing while `last_trade_at` does not. Run the live-safe smoke check above to
confirm reachability and schema compatibility.

**`possible-data-loss` in the logs**
Two full pages could not prove continuity to the durable boundary, usually because more than 20,000
rows were published since it. Monitoring continues behind a frozen boundary; once the gap ages past the
recovery horizon it is recorded as a loss event and the boundary re-anchors. Investigate the recorded
interval rather than assuming coverage.

**`POLYMARKET_WS_URL` deprecation warning**
The WebSocket setting is no longer used. Remove it and configure the `POLYMARKET_TRADES_*` settings.

**Connection timeout / DNS errors**
Verify `https://data-api.polymarket.com` is reachable from your network.

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
