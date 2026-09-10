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
