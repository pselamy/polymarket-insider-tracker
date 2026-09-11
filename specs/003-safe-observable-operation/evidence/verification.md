# Verification Evidence: Safe Observable Operation

**Slice**: `specs/003-safe-observable-operation`
**Date**: 2026-09-10
**Branch**: `feat/slice-003-safe-observable-operation`

All verification commands were executed in the repository worktree `/home/dev/worktrees/polymarket-insider-tracker/slice003-safe-observable-operation` using Python 3.13.14 on Linux `x86_64`.

---

## 1. Quality Gates Summary

All verification profiles pass cleanly with zero warnings or errors.

| Profile / Gate | Command | Result | Duration | Notes |
|---|---|---|---|---|
| **Static Profile** | `uv run python scripts/verify.py --profile static` | **PASS** | 10.25s | lock, format (black), lint (ruff), strict-types (mypy), pyright, vulture, complexipy |
| **Compatibility Profile** | `uv run python scripts/verify.py --profile compatibility` | **PASS** | 23.46s | 1,145 passed across Python runtime |
| **Services Profile** | `uv run --env-file .env.example python scripts/verify.py --profile services` | **PASS** | 11.39s | PostgreSQL probe, Redis probe, 37 redis contract tests, Alembic 3-step cycle (up -> down -> re-up) |
| **All Profile** | `uv run --env-file .env.example python scripts/verify.py --profile all` | **PASS** | 38.04s | All 12 gates green |
| **Cognitive Complexity Gate** | `uv run python scripts/complexipy_gate.py src tests scripts alembic conftest.py --max-complexity-allowed 5 ...` | **PASS** | 0.67s | All functions <= 5 cognitive complexity |
| **Test Suite & Coverage** | `uv run pytest --cov=polymarket_insider_tracker` | **PASS** | 27.80s | 1,145 passed, 2 skipped, 92% coverage |

---

## 2. User Story 1: Observable Operational Runtime

### 2.1 Health HTTP Server & Metrics
- **Endpoints**: `/live`, `/ready`, `/health`, `/metrics` implemented on `HealthMonitor` in `src/polymarket_insider_tracker/ingestor/health.py`.
- **Liveness vs Readiness**: `/live` returns 200 `{ "live": true }` while event loop runs; `/ready` returns 200 `{ "ready": true }` only when all core components (`database`, `redis`, `ingestion`) report `status="up"`.
- **Port Override**: Configured default `8080` (or `HEALTH_PORT` env), overridable via CLI `--health-port`.
- **Test Evidence**:
  - `tests/ingestor/test_health_server.py`: 8 passed
  - `tests/test_main.py`: Flag parsing and banner summary verified

### 2.2 Worker Supervision & Exit Code 1
- **Failure Propagation**: When `TradePoller` enters `IngestionState.FAILED` or background task encounters a terminal exception, `Pipeline._handle_worker_failure` transitions pipeline state to `PipelineState.ERROR` and signals `stop_event`.
- **Main CLI**: Returns `EXIT_ERROR` (1) on worker failure.
- **Test Evidence**:
  - `tests/test_pipeline.py::TestWorkerSupervision::test_worker_crash_transitions_pipeline_to_error_and_fails_readiness`: PASSED
  - `tests/integration/test_end_to_end.py::TestEndToEndPipelineHarness::test_end_to_end_worker_crash_causes_pipeline_error_and_unready`: PASSED

### 2.3 Strict URL Validation & Polygon Defaults
- Default RPC URL set to `https://polygon-rpc.com`.
- Hardened URL parsing in `PolygonSettings` rejects malformed schemes like `wss://https://...`.
- Clarified `--config-check` as configuration validation without false runtime readiness claims.
- **Test Evidence**:
  - `tests/test_config.py::TestPolygonSettings`: PASSED
  - `tests/test_main.py::TestRunConfigCheck`: PASSED

---

## 3. User Story 2: Safe Deduplication & Delivery Accounting

### 3.1 Scorer Decoupling
- `RiskScorer.assess` evaluates `weighted_score >= alert_threshold` purely in memory without Redis side-effects.
- Populates signal availability diagnostics (`volume_available`, `market_daily_volume`, `book_depth_available`, `wallet_tx_count`, `wallet_age_known`).
- **Test Evidence**:
  - `tests/detector/test_scorer.py`: All 14 tests pass with zero Redis calls during scoring.

### 3.2 Channel-Scoped Deduplication & Dry-Run Safety
- `AlertHistory` provides:
  - Confirmed delivery deduplication key: `alert:dedup:<channel>:<wallet>:<market>`
  - Ambiguous delivery key: `alert:ambiguous:<channel>:<wallet>:<market>` with 60s TTL
- `AlertDispatcher`:
  - `dry_run=True`: Records zero Redis keys and makes zero channel delivery calls; disposition is `"dry_run"`.
  - Multi-channel delivery: Partial failure keeps failed channel eligible for retry while successful channel deduplicates.
  - Ambiguous timeout: On `TimeoutError`, suppresses for 60s before retry.
- **Test Evidence**:
  - `tests/alerter/test_deduplication.py`: All 13 tests pass covering dry-run, multi-channel partial failures, ambiguous timeouts, and circuit breaker.

### 3.3 Storage Schema Migration
- Alembic migration `alembic/versions/20260910_0000_safe_observable_operation.py` adds 8 columns to `risk_assessments`:
  - `delivery_disposition` (VARCHAR(32), indexed)
  - `delivery_channels` (TEXT)
  - `dry_run` (BOOLEAN)
  - `volume_available` (BOOLEAN)
  - `market_daily_volume` (NUMERIC(20, 2))
  - `book_depth_available` (BOOLEAN)
  - `wallet_tx_count` (INTEGER)
  - `wallet_age_known` (BOOLEAN)
- Up/down/up verified on live PostgreSQL via `scripts/runtime_services.py`.
- **Test Evidence**:
  - `tests/storage/test_risk_assessments_schema.py`: All 3 tests pass.

---

## 4. User Story 3: Deterministic End-to-End Integration

### 4.1 End-to-End Test Suite
`tests/integration/test_end_to_end.py` executes full end-to-end journeys using deterministic fakes (`FakeTradesServer`, `FakeEth`, `FakeAlertChannel`, `FakeAsyncRedis`, real SQLite/PostgreSQL engines):

1. `test_end_to_end_dry_run_pipeline`:
   - Ingests trade -> Profiles wallet -> Detects signals -> Scores -> Dry run suppression -> Persists assessment (`disposition="dry_run"`, `dry_run=True`).
   - Verifies 0 channel calls and 0 Redis dedup keys.
2. `test_end_to_end_live_delivery_and_deduplication`:
   - First trade delivered to both Discord and Telegram channels.
   - Dedup keys written to Redis for each channel.
   - Second trade for same wallet/market suppressed with `disposition="duplicate"`.
3. `test_end_to_end_persistence_failure_does_not_block_delivery`:
   - Database connection failure during persistence does not prevent alert dispatch.
4. `test_end_to_end_worker_crash_causes_pipeline_error_and_unready`:
   - Worker 401 terminal failure transitions pipeline to `ERROR` and flips readiness to `False`.

---

## 5. Architectural Invariants Verified

1. **Zero `unittest.mock` Usage**: Zero occurrences of `unittest.mock`, `MagicMock`, `patch`, or `Mock` in tests or implementation.
2. **Cognitive Complexity Budget**: Max complexity per function <= 5 across all files in repository (`complexipy_gate.py`).
3. **No External Side Effects in Tests**: Tests communicate exclusively with local fakes and disposable loopback services.

---

## 6. Phase 2 Independent Review Corrections (2026-09-11, Claude Fable 5)

Phase 2 independently reproduced every phase-1 gate at commit `cbf2d2b` (all profiles
passed; 1,145 tests, 92% line+branch coverage), then found and fixed the following
defects. Every fix began with a failing regression test reproduced at the unfixed head.

| # | Severity | Finding | Fix |
|---|---|---|---|
| 1 | High | Discord/Telegram channels swallowed `httpx` read timeouts, re-posted the same payload up to `max_retries` times, and returned confirmed failure, so the dispatcher's ambiguous path (FR-018) was unreachable for production channels and duplicates were possible. | Channels now treat connect/pool timeouts as retryable confirmed failures and raise `TimeoutError` on read/response timeouts without internal re-post; the dispatcher applies the 60s ambiguity window. |
| 2 | High | Health HTTP server bound with `reuse_port=True`, letting a second process bind an occupied health port silently instead of failing actionably (spec edge case). | Removed `reuse_port`; kept `reuse_address`. A busy port now raises `OSError`. |
| 3 | Medium | Zero-configured-channels dispatch returned the `DispatchResult` default disposition `"delivered"`, persisting an untruthful assessment. | Explicit `no_channels` disposition; documented in data-model and delivery contract. |
| 4 | Medium | Delivery dedup TTL used `max(1, dedup_window_seconds // 3600)` hours, distorting the configured window in both directions (e.g. 1800s → 3600s, 5400s → 3600s). | `AlertHistory` accepts `dedup_window_seconds` and the pipeline passes the configured value through unrounded. |
| 5 | Medium | `_exit_code_for_pipeline` returned exit 1 on graceful shutdown whenever `stats.errors > 0`, misclassifying recoverable per-trade/metadata errors; a worker crash during `STARTING` was overwritten to `RUNNING` by `start()`. | Exit code 1 now derives solely from `PipelineState.ERROR`; `_handle_worker_failure` covers `STARTING`, and `start()` no longer overwrites `ERROR`. |
| 6 | Medium | Readiness/health component checks had no timeout, so a hung dependency could hang `/ready` (FR-005 requires bounded checks). | `HealthMonitor` bounds every checker with `asyncio.wait_for` at 1.0s (`COMPONENT_CHECK_TIMEOUT_SECONDS`); timeouts report `down`. |
| 7 | Medium | `delivery_channels` was `VARCHAR(255)` in the migration and model; the assessment-storage contract requires `TEXT`. | Migration `003_safe_observable_operation` and `RiskAssessmentModel` now use `TEXT`; migration cycle re-verified on PostgreSQL. |
| 8 | Medium | A Redis outage during dispatch raised through `asyncio.gather`, blocking delivery and skipping assessment persistence (FR-012/FR-013). | Dedup-state read/write failures now degrade with explicit possible-duplicate warnings and never block the delivery attempt or persistence. |
| 9 | Low | FR-011 marking: `AlertHistory.should_send`/`record_sent` legacy hour-bucket dedup was neither removed nor marked; `RiskScorer` docstrings still claimed dedup enforcement. | Legacy methods and class docstring explicitly marked non-operational for delivery dedup; scorer docstrings corrected (pure computation, params retained for API compatibility). |
| 10 | Low | README listed nonexistent `polymarket_ingest_*` metrics; research.md claimed a default Polygon fallback URL the code does not set; data-model disposition enumeration omitted `ambiguous`. | Docs corrected to the actual metric names, the actual fallback source (`.env.example`), and the full disposition set including `ambiguous`/`no_channels`. |
| 11 | Low | `_wait_for_stop_or_shutdown` cancelled pending wait tasks without awaiting them. | Cancelled tasks are now awaited (`gather(..., return_exceptions=True)`). |

Correction to §3.3 above: `market_daily_volume` is `NUMERIC(20, 6)` (as in the migration
and contract), not `NUMERIC(20, 2)`.

### Phase 2 Re-Verification (post-fix)

- `uv run --env-file .env.example python scripts/verify.py --profile all`: **PASSED**
  (all 12 gates green, 39.83s; migration cycle `003 → 002 → 003` on disposable PostgreSQL).
- `uv run python scripts/verify.py --profile compatibility`: **PASSED** (1,156 tests).
- `uv run pytest --cov=polymarket_insider_tracker --cov-branch`: **1,156 passed, 2 skipped**, 92% line+branch coverage.
- Isolated per-minor suites (`uv run --isolated --locked --all-extras --python 3.11|3.12 pytest`): recorded in `PHASE2_RESULT.md`.
- 11 new regression tests added (channel timeout semantics, port conflict, bounded health
  checks, no-channel disposition, dedup TTL, Redis-outage resilience, startup-crash state,
  graceful-stop exit code, `TEXT` column type); all failed at `cbf2d2b` and pass after the fixes.

### Known Limitations (unchanged scope)

- SC-006's negative half (no route responds on the superseded default port) is not asserted
  by an automated test because binding/asserting on the shared default port 8080 would be
  flaky on developer hosts; the override-port behavior and busy-port failure are tested.
- After the 60s ambiguity key expires it leaves no marker, so the possible-duplicate
  warning is logged (and the `ambiguous` disposition persisted) at ambiguity time rather
  than at the later retry; README and the delivery contract document that a duplicate
  remains possible after an ambiguous acceptance.
