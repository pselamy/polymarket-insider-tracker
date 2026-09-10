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
