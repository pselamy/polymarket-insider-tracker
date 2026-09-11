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
Graceful shutdown is bounded by **one** shutdown timeout (default 30 seconds), not one per path:
- `pipeline.stop()` is attempted exactly once across the explicit shutdown path and the
  registered cleanup callback (a single-stop guard consumes the attempt for whichever path
  runs first, including after a timed-out, abandoned attempt). Total shutdown therefore
  consumes at most one timeout, never a doubled one.
- The deadline is enforced independently of coroutine cooperation (`wait_bounded`): at the
  bound the attempt is cancelled best-effort and abandoned even if it suppresses
  cancellation, the timeout is logged, and the process exits with code 1 instead of
  hanging. A stop that later completes on its own is still reported as a timeout, never
  retroactively as success.
- Every other registered coroutine cleanup callback is individually bounded the same way;
  an overrunning or cancellation-resisting callback is logged and abandoned while later
  callbacks still run (synchronous callbacks cannot be interrupted and are expected to be
  fast).
- A second SIGINT/SIGTERM still forces immediate exit (`128 + signal`).

---

## 5. Per-Trade Processing Failure Semantics
A failure while processing one delivered observation (profiling, detection, scoring,
persistence, or dispatch) is recoverable, not terminal:
- It increments `PipelineStats.errors` (and the poller's `callback_errors` counter) and
  becomes the `/health` top-level `last_error`; `/ready` does not fail for it.
- Detector and profiling failures are counted per failed detector (a wallet-profiling
  failure propagates out of the fresh-wallet detector instead of masquerading as "wallet
  not fresh"). When a trade still yields a signal, the assessment proceeds with the
  available evidence and its NULL evidence columns show what was absent. When detector
  failure leaves no signal at all, a skip row with
  `delivery_disposition='detector_failure'` durably explains the absent evidence (US3
  scenario 3); it is never dispatched.
- Assessment persistence failures are counted into `PipelineStats.errors` and surface as
  the `/health` top-level `last_error` (FR-013) without blocking an otherwise authorized
  delivery attempt.
- Above-threshold ordering: the qualifying assessment is persisted as a pending row
  (`delivery_disposition='unrecorded'`) **before** formatting or any channel contact, and
  the same row is updated in place with the final disposition after dispatch (inserted
  afresh if the pending write failed). A formatter/dispatch failure or termination after an
  external delivery therefore cannot lose the durable research record; at worst the row
  truthfully keeps `unrecorded`.
- The observation identity remains recorded at the durable ingestion boundary per the
  slice-001 observation-boundary contract, so downstream processing is at-most-once: a
  failed observation is not redelivered after a restart. Because the qualifying assessment
  is durable before delivery, the only rows that can be missing after a crash are
  below-threshold or skip rows whose single write failed; every such failure is observable
  in health, logs, and counters.
