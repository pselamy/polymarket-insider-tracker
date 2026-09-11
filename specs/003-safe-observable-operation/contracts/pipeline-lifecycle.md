# Interface Contract: CLI & Pipeline Lifecycle

**Feature**: `specs/003-safe-observable-operation`
**Date**: 2026-09-10

This contract defines CLI options, configuration diagnostics, health port overriding, and process termination semantics.

---

## 1. CLI Arguments & Precedence
- `--config-check`: Run offline syntax and structure validation, display sanitized summary, and exit 0 (success) or 2 (config error).
- `--health-port <PORT>`: Overrides `settings.health_port`. Takes precedence over `HEALTH_PORT` environment variable and default (8080).
- `--dry-run`: Overrides `settings.dry_run` to True.
- `--log-level <LEVEL>`: Overrides `settings.log_level`.

---

## 2. Exit Code Contract
- `0` (`EXIT_SUCCESS`): Graceful shutdown on SIGINT/SIGTERM, or `--config-check` passed.
- `1` (`EXIT_ERROR`): Unhandled exception or terminal background worker failure.
- `2` (`EXIT_CONFIG_ERROR`): Configuration parsing or validation failure.
- `130` (`EXIT_INTERRUPTED`): Immediate user interrupt before event loop startup.

---

## 3. Worker Error Propagation Contract
When the background `TradePoller` or required worker raises an unhandled exception:
1. Pipeline state transitions to `PipelineState.ERROR`.
2. Pipeline `_stats.last_error` records the error string.
3. `/ready` returns HTTP 503 with `"reason": "ingestion_worker_failed"`.
4. The pipeline stop event is set, triggering orderly teardown of database, redis, and HTTP server.
5. The CLI process exits with code 1 within 5 seconds of failure.

---

## 4. Shutdown Timeout Contract
Graceful shutdown is bounded by the shutdown timeout (default 30 seconds):
- `pipeline.stop()` runs under `asyncio.wait_for`; exceeding the bound is logged, the hung
  stop is abandoned, and the process exits with code 1 instead of hanging.
- Every registered coroutine cleanup callback is individually bounded by the same timeout;
  an overrunning callback is logged and abandoned while later callbacks still run
  (synchronous callbacks cannot be interrupted and are expected to be fast).
- A second SIGINT/SIGTERM still forces immediate exit (`128 + signal`).

---

## 5. Per-Trade Processing Failure Semantics
A failure while processing one delivered observation (profiling, detection, scoring,
persistence, or dispatch) is recoverable, not terminal:
- It increments `PipelineStats.errors` (and the poller's `callback_errors` counter) and
  becomes the `/health` top-level `last_error`; `/ready` does not fail for it.
- The observation identity remains recorded at the durable ingestion boundary per the
  slice-001 observation-boundary contract, so downstream processing is at-most-once: a
  failed observation is not redelivered after a restart. The durable risk-assessment row
  for that observation may therefore be missing; the failure is observable in health,
  logs, and counters rather than by replay.
