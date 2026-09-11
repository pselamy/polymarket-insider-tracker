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
   The default `rpc_url` in `PolygonSettings` is set to `https://polygon-rpc.com`. The code default for `fallback_rpc_url` remains unset; the documented `.env.example` supplies `https://polygon-bor.publicnode.com` as the fallback. Both are verified free public RPC endpoints that answer block number requests without an API key.
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
   - **Success**: Write `alert:dedup:{channel}:{wallet}:{market}` with TTL, then release the in-flight claim.
   - **Confirmed Failure** (HTTP 4xx/5xx or transport connection error): Do NOT write dedup key; release the in-flight claim. Channel remains immediately eligible for retry.
   - **Ambiguous Outcome** (HTTP read timeout where remote endpoint may have received the payload): Retain `alert:ambiguous:{channel}:{wallet}:{market}` refreshed to the 60-second TTL.
     - Within 60 seconds: Dispatch to this channel is suppressed to prevent double-send.
     - After 60 seconds: Ambiguity expires and dispatch becomes eligible; the `ambiguous` disposition is persisted and a possible-duplicate warning is logged at ambiguity time (the expired key leaves no retry-time marker).
5. **Atomic In-Flight Claim** (prevents concurrent duplicate delivery):
   The ambiguity key doubles as the per-identity send claim, acquired with `SET NX EX
   claim_ttl` (default 60 s) before an attempt and with the dedup key re-checked under the
   claim. Check-then-send is therefore atomic against a shared Redis: two concurrent
   dispatches of one identity cannot both deliver. Claim-state errors degrade toward
   delivery with a possible-duplicate warning, matching the general dedup-state degradation
   rule. Two properties keep the claim sound across lease expiry (round-3 repair): every
   claim stores a unique ownership token and is released only by a `WATCH`/`MULTI`
   compare-and-delete (a stale owner can never delete a newer claim), and every channel send
   runs under a `send_deadline_seconds` bound strictly smaller than the claim lease, so a
   slow send is cut off as an ambiguous outcome before its claim can expire — no send ever
   outlives its claim. Channel-internal retry, backoff, and rate-limit waits are therefore
   allowed to exceed the lease only by becoming ambiguous, never by sending concurrently.
6. **Deduplication Path Unification**:
   Retire or delegate competing dedup methods in `AlertHistory` so that only the canonical channel-scoped contract is operational.

---

## 6. Persisted Assessment Schema & Migration (FR-012, FR-013, FR-019, G-033)

### Decision
Slice 003 creates Alembic migration `003_safe_observable_operation` modifying `risk_assessments`.

### Added Columns:
- `delivery_disposition` (VARCHAR(32), nullable=False, default="unrecorded" — pre-existing rows have no recorded delivery outcome, so the backfill must not claim `dry_run` while `dry_run` backfills false)
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

---

## 8. Reproducible Scoring at Persisted Precision (FR-012, Constitution IV)

### Decision
The alert decision is made at the same NUMERIC(4,3) precision the assessment schema
persists. `RiskScorer` quantizes each signal confidence and the alert threshold to three
decimals (`quantize_score_value`, banker's rounding), combines them in exact decimal
arithmetic with the code-pinned weights and multi-signal bonuses, and quantizes the final
score before comparing it to the quantized threshold. Assessment persistence uses the same
quantizer, so the stored confidences, score, and threshold reproduce `should_alert` exactly.

### Rationale
The previous float comparison could decide `0.7999999999999999 < 0.8` while persisting
`0.800 >= 0.800` with `should_alert=false` — a durable research record that contradicted
its own decision and could not be replayed. Deciding at persisted precision changes
behavior only within half a thousandth of the threshold and makes the record
self-explanatory; the earlier float-artifact regression pin in `tests/detector/test_scorer.py`
was replaced accordingly.

*Superseded in part (2026-09-11, round-4 repair under Patrick's schema decision):* the
round-3 approach removed weight configurability entirely because the approved schema stored
neither weights nor a version, which also broke the public `weights=`/`set_weights()` API.
The schema now persists `scoring_algorithm_version` and the canonical `scoring_config`
JSON per row (legacy rows are labeled `legacy-unversioned` with NULL config), the default
weights are immutable, and the pre-slice-003 weight API is restored for one deprecation
window — rows scored under custom weights replay from their own recorded configuration.
See `contracts/assessment-storage.md` §4.

---

## 9. Loopback Isolation Hardening for Service Verification (Constitution IV, AGENTS.md safe effects)

### Decision
`scripts/runtime_services.py` rejects, as prerequisites, every route libpq could take past
the loopback-resolved host component: the `host`, `hostaddr`, `port`, and `service`
DATABASE_URL query parameters, and the `PGHOSTADDR`, `PGSERVICE`, and `PGSERVICEFILE`
environment variables. The Alembic migration subprocess additionally runs with every
`PG*`-prefixed variable stripped from its environment; the disposable-database URL carries
the complete credentials.

### Rationale
libpq documents that `hostaddr` (parameter or environment default) determines the actual
connection address while `host` is used only for authentication purposes, and that a
service file can supply unspecified parameters. Without these checks a loopback-validated
URL could still create, migrate, and force-drop databases on a remote server.

---

## 10. Truthful Degraded Health, Detector-Failure Evidence & Single-Deadline Shutdown (FR-002, FR-003, FR-006, US1-2/3, US3-3; round-3 repairs)

### Decision
1. **Degraded ingestion is visible, not hidden and not fatal.** The ingestion component
   reports `degraded` (with the poller's `last_error`, or `ingestion state: <state>` when
   the state itself is the condition) for `IngestionState.DEGRADED` and
   `POSSIBLE_DATA_LOSS`. Readiness fails (`503`) only for a `down` component. FR-002
   enumerates the readiness-failure conditions — unavailable dependency, terminal ingestion
   failure, blocked progress — and a recoverable degraded source satisfies none of them,
   while US1 scenario 3 requires the degradation and its error to be identified. The
   `/ready` components summary shows `degraded`; `/health` reports the error and an overall
   `degraded` status with HTTP 200. (The earlier contract phrase "degraded → 503"
   contradicted FR-002 and was corrected; specs own what/why.)
2. **Stream staleness follows source progress, not trade silence.** The staleness basis is
   the freshest of trade arrival and successful acquisition, so a quiet market with fresh
   acquisitions stays `active` (US1 scenario 2); the stream goes `stale` only when both age
   past the threshold.
3. **Detector failures are counted and durably explained.** Wallet-profiling failures
   propagate out of the fresh-wallet detector (swallowing them was indistinguishable from
   "wallet not fresh"); the pipeline counts each failed detector into
   `PipelineStats.errors`/`last_error` (surfaced at `/health`). A trade left with no signal
   and at least one detector failure persists a `detector_failure` skip row so the absent
   evidence is explained (US3 scenario 3); with a surviving signal, scoring proceeds and
   NULL evidence columns show what was absent.
4. **Shutdown consumes one deadline.** `run_pipeline` and the registered shutdown cleanup
   share a single-stop guard: `pipeline.stop()` is attempted exactly once under one
   `shutdown_timeout`, so a hung stop exits with code 1 after ~1x the timeout instead of 2x.

### Rationale
Round-3 phase-3 review reproduced `up`/`None` ingestion health for both failing states, a
stale quiet stream with a 2-second-old acquisition, vanishing detector failures
(`errors=0`, no assessment), and a doubled shutdown deadline. Each repair follows the
narrower truthful reading of the governing spec text rather than adding new authority.
