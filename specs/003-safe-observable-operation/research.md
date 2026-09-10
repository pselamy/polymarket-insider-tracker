# Research: Safe Observable Operation

**Feature**: `specs/003-safe-observable-operation`
**Date**: 2026-09-10
**Author**: Implementation Owner

This document resolves the technical context, patterns, and design decisions for slice 003.

---

## 1. Health & Metrics Server Architecture (FR-001, FR-002, FR-003, FR-004, G-014, G-015)

### Decision
Integrate an asynchronous HTTP server managed by `HealthMonitor` into `InsiderTrackerPipeline`. The server binds to `0.0.0.0` on the effective health port (default `8080`, configurable via `HEALTH_PORT` environment variable or `--health-port` CLI flag).

### Endpoints
- **`GET /live`**: Process liveness probe. Always returns HTTP 200 `{"live": true}` if the event loop and HTTP server are responsive. Never probes dependencies.
- **`GET /ready`**: Readiness probe for orchestrators / Kubernetes. Performs bounded checks (timeout <= 1.0s) against required dependencies:
  1. PostgreSQL connection query.
  2. Redis ping.
  3. Ingestion poller worker state (must be active and not in terminal error).
  Returns HTTP 200 `{"ready": true, "components": {...}}` when all pass; HTTP 503 `{"ready": false, "reason": "...", "components": {...}}` if any required dependency is down or poller has crashed.
- **`GET /health`**: Detailed diagnostic health report. Distinguishes:
  - `status`: "healthy" | "degraded" | "unhealthy"
  - `uptime_seconds`: float
  - `last_acquisition_time`: float timestamp of last API poll cycle (even if empty)
  - `last_trade_time`: float timestamp of newest trade event received
  - `acquisition_freshness_seconds`: elapsed time since last poll
  - `trade_freshness_seconds`: elapsed time since last trade
  - `dependencies`: status and latency of PostgreSQL, Redis, Trade Source
  - `last_error`: string or null
- **`GET /metrics`**: Prometheus metrics endpoint using `prometheus_client.generate_latest()`.

### Rationale
Separating `/live` from `/ready` prevents cascading pod restarts while downstream dependencies recover. Distinguishing acquisition time from trade time prevents quiet markets from being flagged as source disconnects (SC-001, SC-007).

---

## 2. Background Task Failure Propagation & Nonzero Exit (FR-006, SC-002, G-016)

### Decision
The pipeline maintains references to background tasks (`_poller_task`, `_metadata_task`). A dedicated task supervisor or `add_done_callback` monitors the tasks. If the required trade poller task terminates with an exception or unexpected completion:
1. The pipeline records the exception in `_stats.last_error`.
2. The pipeline transitions state to `PipelineState.ERROR`.
3. Readiness probe immediately fails (HTTP 503).
4. The pipeline triggers its internal shutdown event, cancels active background tasks, cleans up connections, and exits.
5. In `__main__.py`, `run_pipeline` awaits the pipeline completion and returns `EXIT_ERROR` (code 1) when the pipeline stopped due to error.

### Alternatives Considered
- *Polling worker status in a loop*: Higher CPU overhead and slower reaction time.
- *Ignoring worker death*: Status quo; leads to zombie processes that appear running but ingest nothing.

---

## 3. Configuration Validation vs Runtime Readiness (FR-005, SC-007, G-014)

### Decision
`--config-check` is strictly an offline configuration and syntax validation utility. It does not initiate network connections. Its terminal output is updated to state:
```text
Configuration is valid! Offline syntax and structure checks passed.
Note: --config-check validates syntax and configuration only; runtime readiness requires reachable PostgreSQL, Redis, and trade acquisition sources.
```
This directly resolves G-014 by eliminating false "Ready to run" claims when external services may be unreachable.

---

## 4. Default Polygon Endpoint & URL Validation (FR-007, G-017, G-030a)

### Decision
1. **Default Polygon RPC URL**:
   The default `rpc_url` in `PolygonSettings` is set to `https://polygon-rpc.com`, with fallback `https://polygon-bor.publicnode.com`. Both are verified free public RPC endpoints that answer block number requests without an API key.
2. **Diagnostic URL Validation**:
   `_validate_http_url` and `_validate_websocket_url` in `config.py` are refactored to parse the URL using `urllib.parse.urlsplit`:
   - Enforce exact scheme (`http`/`https` for HTTP, `ws`/`wss` for WebSocket).
   - Ensure `hostname` is present and non-empty.
   - Detect and reject embedded secondary schemes in netloc or path (such as `wss://https://...` or `https://wss://...`), raising an explicit, actionable `ValueError` with clear diagnostics.

---

## 5. Delivery Deduplication Separation & Multi-Channel Semantics (FR-008, FR-009, FR-010, FR-011, FR-017, FR-018, SC-003, SC-004, SC-008, G-018, G-019, G-020)

### Decision
1. **Decouple Scorer from Deduplication**:
   Remove all Redis dedup writing and checking from `RiskScorer.assess()`. `RiskScorer` assesses signals, weights, and thresholds to produce an objective risk qualification.
2. **Authoritative Delivery Deduplication in Alerter**:
   Delivery deduplication is owned solely by `AlertHistory` (or `AlertDispatcher`).
   - Deduplication key: `alert:dedup:{channel}:{wallet}:{market}`.
   - Window: `dedup_window_seconds` (default 3600s / 1 hour).
   - Identity: `channel` + normalized lowercase `wallet` + `market`. Side and score deltas do not alter the identity.
3. **Dry-Run Mode**:
   When `dry_run=True`:
   - No external HTTP calls to Discord or Telegram are made.
   - ZERO keys are written to Redis (`alert:dedup:*`).
   - The assessment is persisted with `delivery_disposition="dry_run"`.
4. **Channel-Scoped Outcomes**:
   - **Success**: Write `alert:dedup:{channel}:{wallet}:{market}` with TTL.
   - **Confirmed Failure** (HTTP 4xx/5xx or transport connection error): Do NOT write dedup key. Channel remains immediately eligible for retry.
   - **Ambiguous Outcome** (HTTP read timeout where remote endpoint may have received the payload): Write `alert:ambiguous:{channel}:{wallet}:{market}` with 60-second TTL.
     - Within 60 seconds: Dispatch to this channel is suppressed to prevent double-send.
     - After 60 seconds: Ambiguity expires; dispatch becomes eligible with an explicit `possible_duplicate=True` flag and log.
5. **Deduplication Path Unification**:
   Retire or delegate competing dedup methods in `AlertHistory` so that only the canonical channel-scoped contract is operational.

---

## 6. Persisted Assessment Schema & Migration (FR-012, FR-013, FR-019, G-033)

### Decision
Slice 003 creates Alembic migration `003_safe_observable_operation` modifying `risk_assessments`.

### Added Columns:
- `delivery_disposition` (VARCHAR(32), nullable=False, default="dry_run")
- `delivery_channels` (TEXT/JSON, nullable=True)
- `dry_run` (BOOLEAN, nullable=False, default=False)
- `volume_available` (BOOLEAN, nullable=True)
- `market_daily_volume` (NUMERIC(20, 6), nullable=True)
- `book_depth_available` (BOOLEAN, nullable=True)
- `wallet_tx_count` (INTEGER, nullable=True)
- `wallet_age_known` (BOOLEAN, nullable=True)

### Indexes:
- `idx_risk_assessments_disposition` on (`delivery_disposition`)

Both `upgrade()` and `downgrade()` are implemented and tested with Psycopg 3 on PostgreSQL.

---

## 7. Reusable Deterministic End-to-End Pipeline Harness (FR-014, FR-020, SC-005, G-021)

### Decision
Provide `tests/integration/test_end_to_end.py` that wires the entire pipeline with deterministic fakes:
- `FakeTradesSource` / `FakeTradePoller` with predictable trade fixtures.
- `FakeGammaClient` for market metadata.
- `FakeWeb3Client` for wallet profiling.
- `fakeredis` or real loopback Redis.
- SQLite or disposable PostgreSQL.
- `FakeAlertChannel` recording deliver/fail/timeout calls.

Exercises:
1. Ingest trade -> Profile -> Detect -> Score -> Persist assessment -> Suppress or fake deliver.
2. Verified dry-run: 0 external calls, 0 dedup keys, explainable persisted assessment.
3. Verified failure: persistence failure does not block alert; channel failure leaves dedup clean.
4. Clean shutdown without orphaned background workers.
