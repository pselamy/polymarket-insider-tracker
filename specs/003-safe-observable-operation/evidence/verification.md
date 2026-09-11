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
  *(Superseded by the round-3 repair, 2026-09-11: now deterministically tested with two
  dynamically allocated ports — see §8, finding 8.)*
- After the 60s ambiguity key expires it leaves no marker, so the possible-duplicate
  warning is logged (and the `ambiguous` disposition persisted) at ambiguity time rather
  than at the later retry; README and the delivery contract document that a duplicate
  remains possible after an ambiguous acceptance.

---

## 7. Phase 3 Findings Repair (2026-09-11, Claude Fable 5, phase 2 rerun)

The phase-3 adversarial review (`gpt-5.6-sol`) returned `FINDINGS` with seven items.
Each was independently reproduced at head `33fdf3e` before fixing; every fix began with a
regression test that failed at the unfixed head (the migration-backfill test runs against
a disposable real PostgreSQL database, gated by `RUN_SERVICE_TESTS=1`).

| # | Severity | Finding (reproduced) | Fix |
|---|---|---|---|
| 1 | Critical | `validate_loopback_database_url` checked only the URL host component; libpq honors `?hostaddr=`/`?host=`/`?service=` query parameters and `PGHOSTADDR`/`PGSERVICE`/`PGSERVICEFILE` environment defaults, so the disposable-migration cycle (CREATE/DROP DATABASE WITH FORCE) could be rerouted to a remote server. | Routing query keys (`host`, `hostaddr`, `port`, `service`) and routing environment variables are rejected as prerequisites; the Alembic subprocess additionally runs with every `PG*` variable stripped (`alembic_subprocess_environment`). |
| 2 | High | Failed downstream processing was durably acknowledged (identity recorded, boundary advanced) while `/health` omitted the contract-promised top-level `last_error`, leaving the failure invisible. | `/health` now reports the pipeline's most recent worker/per-trade error via a `HealthMonitor` last-error provider; the at-most-once acknowledgment semantics (owned by the merged slice-001 observation-boundary contract) are now documented explicitly in the pipeline-lifecycle contract instead of being silent. |
| 3 | High | Delivery deduplication was a non-atomic `EXISTS → send → SET` sequence: two concurrent dispatches of one identity both delivered (reproduced with a yielding fake channel). | The ambiguity key doubles as an atomic per-identity in-flight claim (`SET NX EX 60`), with the dedup key re-checked under the claim; released on confirmed outcomes, retained on ambiguous ones. Claim errors still degrade toward delivery. |
| 4 | High | The alert decision used raw floats while persistence rounded to NUMERIC(4,3): `0.7999999999999999 < 0.8` was stored as `0.800 >= 0.800` with `should_alert=false` — unexplainable and unreplayable; weights were mutable and unpersisted. | Scoring quantizes confidences, threshold, and final score to the persisted 3-decimal precision in exact decimal arithmetic before deciding; persistence uses the same quantizer, so stored rows replay the decision exactly (regression test recomputes the stored score from stored inputs). Weights remain the code-pinned defaults in the wired pipeline; non-default weights are logged at construction. The prior float-artifact test pin was replaced with the consistency contract. |
| 5 | Medium | `delivered` + `ambiguous_timeout` (suppressed-unknown) aggregated to disposition `delivered` with `all_succeeded=true`, falsely incrementing alert statistics; `duplicate` + `ambiguous_timeout` aggregated to `failed`. | Suppressed-unknown statuses count toward `failure_count` and downgrade a delivered aggregate to `partial_failure`; unknown-only mixes classify as `ambiguous`, never `failed` without a confirmed failure. |
| 6 | Medium | The migration backfilled pre-existing rows as `delivery_disposition='dry_run'` with `dry_run=false` — two contradictory facts. | Server default (migration, ORM model, DTO, and domain dataclass) is now `unrecorded`, documented in the data-model disposition enumeration; verified on real PostgreSQL by inserting a row at revision 002 and reading it back after upgrading to head. |
| 7 | Medium | `shutdown_timeout` was never enforced: `pipeline.stop()` and cleanup callbacks were unbounded awaits, so first-signal shutdown could hang indefinitely. | `run_pipeline` bounds `pipeline.stop()` with `asyncio.wait_for` (timeout → logged + exit 1) and `GracefulShutdown.run_cleanup_callbacks` bounds each coroutine callback with the configured timeout; a second signal still forces exit. |
| 8 | Medium | Found during repair verification (pre-existing since merged PR #117): the gated real-service evidence test `test_real_postgres_redis_and_disposable_migration_cycle` always failed under `RUN_SERVICE_TESTS=1` because the harness chdirs tests into a temp directory and `RealMigrationBackend.expected_revisions` resolved Alembic's relative `script_location` against the working directory. | `expected_revisions` pins `script_location` to the repository's `alembic/` tree; regression test runs revision discovery from a foreign working directory. The whole gated integration directory now passes against real loopback services. |

### Phase 3 Repair Re-Verification (post-fix)

Recorded with exact results in `PHASE2_RESULT.md` (dispatch-state directory): static,
compatibility, and services profiles, the `all` profile, the full suite with branch
coverage, isolated 3.11/3.12 suites, and the gated real-PostgreSQL backfill test.

### Behavioral changes accepted with this repair

- Alert-threshold comparison now happens at the persisted 3-decimal precision; scores
  within half a thousandth of the threshold may decide differently than the previous raw
  float comparison, in exchange for durable records that exactly explain and replay the
  decision (FR-012, Constitution IV). This replaced the merged float-artifact regression
  pin `test_assess_preserves_base_addition_order_at_alert_boundary`.
- Aggregate dispositions for mixes involving suppressed-unknown channels changed as
  described in finding 5 (previously `delivered`/`failed`, now `partial_failure`/`ambiguous`).
- Legacy rows migrated by `003_safe_observable_operation` now read `unrecorded` instead
  of `dry_run`. The migration is unmerged, so no deployed data is affected.

---

## 8. Phase 3 Round-3 Findings Repair (2026-09-11, Claude Fable 5, phase 2 rerun)

The round-3 phase-3 adversarial review (`gpt-5.6-sol`) of head `66ced41` returned
`FINDINGS` with eight items. Each was independently reproduced at that head before fixing;
every fix began with a regression test red at the unfixed head (for API-replacing fixes,
the defect was reproduced with the old API in-process before the new-API regression tests
were written).

| # | Severity | Finding (reproduced) | Fix |
|---|---|---|---|
| 1 | High | `_check_ingestion` reported `up`/no error for `IngestionState.DEGRADED` and `POSSIBLE_DATA_LOSS`; a previously active but quiet trade stream was marked `stale` despite a 2-second-old acquisition (the prior quiet-market test never created production stream state). | Recoverable poller states map to a `degraded` component carrying the poller's error (or `ingestion state: <state>`); `/health` reports overall `degraded` (200) and readiness fails only for `down` components per FR-002. Stream staleness is based on the freshest of trade arrival and successful acquisition. The quiet-market test now creates real stream state; new tests cover stale-acquisition, never-evented-stream, degraded `/ready` and `/health` bodies, and the pipeline state mapping. |
| 2 | High | The in-flight claim was an ownerless fixed-TTL `SET NX EX` with unconditional `DEL`: a send outlasting the 60s lease let a second dispatch claim and send (two `delivered` calls reproduced with a scaled 1s lease), and the stale first owner could delete the newer claim. | Claims store a unique ownership token and are released only via `WATCH`/`MULTI` compare-and-delete (parity contract-tested on fakeredis and real Redis); every channel send runs under `send_deadline_seconds` (45s) strictly below `claim_ttl_seconds` (60s), enforced at construction, so a slow send becomes an ambiguous outcome inside its live claim and no send outlives its claim. Regression tests cover token uniqueness, stale-owner release, lease expiry, the send bound, and the scaled two-dispatch reproduction (now: zero completed sends, `ambiguous`/`ambiguous_timeout`). |
| 3 | High | Detector wrappers swallowed exceptions into `None` (and the fresh-wallet detector additionally swallowed wallet-profiling failures), so a failing detector left `errors=0`, `last_error=None`, and zero assessments — indistinguishable from "no signal". | Profiling failures propagate out of the fresh-wallet detector; the pipeline counts each failed detector into `PipelineStats.errors`/`last_error` (surfaced at `/health`). With a surviving signal the assessment proceeds (NULL evidence columns show what was absent); with no signal at all a `delivery_disposition='detector_failure'` skip row durably explains the absent evidence and is never dispatched. Tests cover one-detector and all-detector failures, plus the persist-disabled path. |
| 4 | High | `RiskScorer` accepted custom weights and runtime `set_weights()` mutation while the schema stores neither weights nor an algorithm version; logging was the only trace, so persisted assessments were not reproducible under supported configuration. | Weight configurability was removed entirely (no constructor argument, no mutation API) and the algorithm is versioned in code (`SCORING_ALGORITHM_VERSION = "003.1"`); with exactly one weight set per version, every stored row replays from its own values plus pinned constants. Documented in the assessment-storage contract §4; a persisted-record replay regression recomputes decision and score from stored-precision values and pinned weights only. |
| 5 | Medium | `run_pipeline` stopped the pipeline under the shutdown timeout and the registered cleanup stopped it again under a fresh timeout: a 0.1s timeout took ~0.204s (reproduced: 2 stop calls). | A single-stop guard shares the one attempt between the explicit path and the cleanup callback (also after a timed-out, abandoned attempt). Regression tests assert exactly one stop call and elapsed ≈ one timeout for the hanging case, and no re-stop after a successful stop. |
| 6 | Medium | `_check_nested_schemes` embedded the complete supplied URL in its ValueError, echoing embedded credentials (`user:TOPSECRET@…` reproduced verbatim). | The message names the field and the defect without the value; `hide_input_in_errors=True` was additionally set on every settings group so pydantic never renders raw inputs. Direct-validator tests (HTTP and WebSocket) and a CLI-stderr test assert the secret never appears. Channel error logging was also hardened: httpx error text is logged with the credential-bearing webhook URL / bot token redacted (regression-tested). |
| 7 | Medium | The full instrumented branch-coverage suite was not reproducible: consecutive runs failed once each (`1 failed, 1175 passed, 3 skipped`), alternating between the dry-run end-to-end readiness assert and the worker-crash readiness scenario. Reproduced on the first baseline run; root-caused to `FakeClock.sleep` yielding without real delay, letting the poller free-run and starve the event loop under coverage until the 1.0s bounded database check timed out (`database_unreachable` captured in a diagnostic loop). | `FakeClock.sleep` now takes a 2ms real sleep per fake sleep so paced fakes cannot monopolize the loop, and the two tests synchronize on readiness with a bounded poll (the settled state is still asserted; a crash still must settle to not-ready). Full instrumented suite re-run repeatedly green (see §8 re-verification). |
| 8 | Medium | SC-006 was claimed covered while its negative half (no health route on the superseded port) was explicitly untested. | New deterministic test with two dynamically allocated ports: all four documented routes answer 200 on the effective port, the server's actual bound sockets (`HealthMonitor.http_addresses`) contain only the effective port, and connecting to the superseded port is refused; after stop, no addresses remain bound. |

### Round-3 Repair Re-Verification (post-fix)

Recorded with exact command output in `PHASE2_RESULT.md` (dispatch-state directory):
static, compatibility (isolated 3.11/3.12/3.13), and services profiles against real
loopback PostgreSQL 16.15 and Redis 8.10.1, the `all` profile, the full suite with branch
coverage (repeated), and the gated real-service integration directory.

### Behavioral changes accepted with this repair

- `/ready` no longer fails for recoverable `degraded`/`possible-data-loss` ingestion; the
  component is reported `degraded` instead of the untruthful `up`, and `/health` becomes
  overall `degraded` (200). Terminal and unavailable states still fail readiness (503).
- A channel send slower than 45 seconds (rate-limit waits and retries included) is now cut
  off as an ambiguous outcome instead of running unbounded; the ambiguity window then
  applies as documented.
- `RiskScorer` no longer accepts a `weights` argument and `set_weights`/mutation no longer
  exists (unwired, library-only configurability removed under FR-012/Constitution IV; the
  wired pipeline never passed weights). `get_weights()` still returns the pinned constants.
  *(Superseded by the round-4 repair, 2026-09-11: the API is restored for one deprecation
  window under Patrick's schema decision — see §9, finding 4.)*
- Trades whose detectors all fail now persist a `detector_failure` skip row (previously no
  row) and detector failures increment the error counters (previously silent).

---

## 9. Phase 3 Round-4 Findings Repair (2026-09-11, Claude Fable 5, phase 2 rerun)

The round-4 phase-3 adversarial review (`gpt-5.6-sol`) of head `c4e2ecb` returned
`FINDINGS` with nine items plus one hygiene note. Every repair began with regression
tests proven red at the unfixed head: the new/changed test files were transplanted onto a
disposable detached worktree at `c4e2ecb` and run in an isolated environment — **36
failed** plus two modules red by collection `ImportError`
(`polymarket_insider_tracker.redaction` and `SCORING_ALGORITHM_VERSION` did not exist),
plus the gated real-PostgreSQL backfill test red with `UndefinedColumn:
scoring_algorithm_version`. Notably, at the unfixed head the immutability regression's
attempted `DEFAULT_WEIGHTS` mutation silently succeeded and corrupted scoring for
unrelated tests later in the same process — a live demonstration of finding 4.

| # | Severity | Finding (reproduced) | Fix |
|---|---|---|---|
| 1 | High | Readiness treated a running-but-never-connected poller as `up` and any `DEGRADED` poller as ready; acquisition freshness copied `last_acquisition_at` (set before the HTTP request) instead of `last_success_at`. | `_check_ingestion` gates on a recent **successful** acquisition via the poller's new clock-consistent `seconds_since_last_success`: no success ever, or success older than the shared staleness threshold → `down` (readiness fails); `DEGRADED`/`POSSIBLE_DATA_LOSS` with a fresh success stays a visible ready-compatible `degraded`. The health monitor records only `last_success_at` as acquisition time. Tests: first-failure, never-connected STARTING, stale-success (RUNNING and DEGRADED), successful-empty-page, and failed-acquisition-freshness regressions. |
| 2 | High | `asyncio.wait_for` imposed no bound on a cancellation-suppressing send: with a scaled 1s lease a second dispatch after lease expiry produced two `delivered` sends. | The send deadline is now logical (`asyncio.wait` + cancel + abandon): at the deadline the outcome is ambiguous regardless of any late return, and a resistant send is handed to a background guard that renews the owned claim (new `AlertHistory.extend_channel_claim`, WATCH/MULTI compare-and-expire with fake/real Redis parity tests) until the send truly terminates, then leaves the ordinary ambiguity window; a late success is never recorded as delivered. Regressions: scaled lease-expiry reproduction (now 1 call, `ambiguous`/`ambiguous_timeout`, no dedup key), late-return classification with bounded dispatch return, claim-renewal ownership tests. |
| 3 | High | Above-threshold processing formatted and dispatched before persisting; a formatter exception reproduced `persist_calls=0` and the at-most-once boundary means no replay. | The qualifying assessment is persisted as a pending `unrecorded` row **before** formatting or any channel contact, then the same row is updated in place with the final disposition (`RiskAssessmentRepository.update_delivery`; inserted afresh if the pending write failed). Assessment persistence failures now count into `PipelineStats.errors`/`last_error` (FR-013 observability) without blocking delivery. Regressions: rows-durable-at-send-time ordering, formatter-failure row survival, initial-persist-failure delivery + recovery. |
| 4 | High | Stored assessments could not identify a reproducible algorithm: `DEFAULT_WEIGHTS` was mutable, no version/config was persisted, and the `weights=`/`set_weights()` removal broke the public API. | Per Patrick's 2026-09-11 schema decision: migration `003_safe_observable_operation` now adds `scoring_algorithm_version` (VARCHAR(32) NOT NULL, no insert default; legacy rows backfilled as exactly `legacy-unversioned`) and `scoring_config` (TEXT, canonical deterministic JSON of threshold/bonuses/quantum/weights; NULL only for legacy rows). Every new row records both. `DEFAULT_WEIGHTS` is an immutable `MappingProxyType`; `get_weights()` returns a defensive copy; the deprecated `weights=`/`set_weights()` API is restored, functional, and warns (`DeprecationWarning`) for one compatibility window, with custom-weight rows replayable from their own `scoring_config`. Spec clarification amended from 8 to 10 columns with the reason recorded; upgrade/backfill/downgrade cycle verified on real PostgreSQL including a downgrade → re-upgrade determinism check. |
| 5 | High | `get_health_report()` set the Prometheus gauge from stream state alone; a down database produced `/health` `unhealthy` with `polymarket_health_status=1.0`. | One combined snapshot (`evaluate_overall_health`) feeds `/health`, `/metrics`, and the periodic check; the gauge is set only from the combined verdict. Regressions: down → 503 + gauge 0.0, degraded → 200 + gauge 0.5, scrape-only evaluation. |
| 6 | Medium | RPC/trades URLs returned verbatim by the "redacted" summary; CLI printed the raw trades URL; health errors exposed raw exception text. | New central `redaction` module (`redact_url`, `redact_text`) masks userinfo passwords, lone userinfo tokens, all query values, fragments, and malformed nested-scheme URLs; applied in the configuration summary, CLI output, pipeline error capture, dispatcher/channel logging, and the health output sink. Adversarial tests cover summaries, CLI stdout, `/health` and `/metrics` bodies, pipeline stats, and malformed URLs — one adversarial case (nested-scheme path smuggling) found and fixed a gap during development. |
| 7 | Medium | `asyncio.wait_for` let a cancellation-suppressing stop turn a 0.01s timeout into a late success. | New `shutdown.wait_bounded` enforces the deadline independently of cooperation: at the bound the task is cancelled best-effort and abandoned, reported as timeout (exit 1) even if it later completes. Applied to the single pipeline-stop attempt and every coroutine cleanup callback. Regressions: cancellation-resistant stop (bounded, exit 1) and cancellation-resistant cleanup callback (bounded, later callbacks run). |
| 8 | Medium | `--config-check --dry-run --log-level DEBUG` printed `Dry Run: False` / `Log Level: INFO`. | `apply_cli_overrides` folds every override into the one effective `Settings` before logging, summaries, and the pipeline run; all paths read the same object. Regressions: config-check override reflection, runtime/settings/summary agreement, environment-value survival. |
| 9 | Medium | Three timed-out component checks executed sequentially (3.0s) against the <100ms plan goal. | `evaluate_components` runs all checks concurrently, each bounded at 0.09s, so a probe with every dependency hung stays within one sub-100ms budget. Regressions: barrier-based concurrency proof (deterministic) and hung-probe elapsed bound. |
| 10 | Hygiene | `git diff --check` blank line at EOF of `checklists/requirements.md`. | Removed; `git diff --check` clean. |

---

## 10. Round-7 Detector-Failure Redaction Repair (2026-09-11, Muse Spark writer lane)

The round-6 fresh Sol review (`gpt-5.6-sol`, exact head `3571a3e3fbeff821840b7842891dcdd5adda9072`)
reproduced a load-bearing secret-redaction defect before its session stopped: a synthetic
exception containing `https://user:fresh-leak-secret@rpc.example/path?apikey=fresh-leak-secret`
surfaced verbatim in the `Pipeline._detect_fresh_wallet` warning log and returned error string
(`secret_present_in_pipeline_log=True`). `_detect_size_anomaly` held the identical unredacted
pattern. Prior evidence (§9 finding 6) therefore overstated detector-failure coverage.

Regression proof on the exact start head `3571a3e`: a script driving real `TradeEvent` values
through `wire_pipeline` with `FailingDetector` exceptions embedding synthetic URL userinfo and
query secrets showed both secrets verbatim in the returned `fresh_msg`/`size_msg` and in the
warning logs (`fresh_leak_in_logs=True`, `size_leak_in_logs=True`); downstream
`_record_detector_failures` redaction masked only `last_error`, leaving the boundary leak in
place. Raw log preserved at
`/home/dev/dispatch-state/polymarket-slice003-muse-redaction-r7-20260911/r7-evidence/regression-red-start-head.log`.

Fix (bounded to the detector-failure boundary): `Pipeline._detect_fresh_wallet` and
`Pipeline._detect_size_anomaly` now pass the exception-bearing failure message through the
existing centralized `redact_text` exactly once before it is logged, returned, counted, or
exposed via `PipelineStats.last_error`/health output; the log call also uses `%s` argument
form. No new redactor was introduced. Adjacent detector exception sinks were audited:
`_record_detector_failures`, `_check_database`, `_check_redis`, health serialization, and the
trade-poller `redact_error` path already redact; no other confirmed equivalent of this class
was changed.

Tests: `tests/test_redaction.py::TestDetectorFailureRedaction` (3 tests, real objects and
repository fakes only — `wire_pipeline`, `FailingDetector`, `FakeEth`, `FakeAsyncRedis`; no
mocks or call interception) covers userinfo passwords, lone userinfo tokens, query values,
fragments, and malformed nested-scheme shapes for both detectors; asserts warning logs,
returned error strings, `PipelineStats.last_error`/`errors`, and health-body output carry no
secret while `***` and the non-secret diagnostic context (`fresh wallet detection failed`,
`size anomaly detection failed`, `connection reset`, `detection failed`) remain.

---

## 11. Round-10 Secrets-Boundary Repair (2026-09-11, Muse Spark writer lane)

The round-9 fresh GPT-6 review (`gpt-6-astra`, exact head `2d5dcc6`, verdict `REVISE`)
reproduced two load-bearing secrets-boundary defects on the supported monitoring path and
qualified the round-8 coverage as narrower than claimed. §10 history above is preserved
unchanged; this section appends the correction.

Regression proof on the exact start head `2d5dcc6` (receipts treated as claims, reproduced
before fixing): a script driving real profiler/detector components with narrow
repository-failure injection showed synthetic secrets verbatim in warning logs at
`profiler/analyzer.py` (token-balance, profile-cache read, profile-cache write),
`profiler/chain.py` (chain-cache read, chain-cache write, retry `Web3Exception` warnings),
and `detector/size_anomaly.py` (metadata-fetch failure), plus leaks through the real
fresh-wallet `analyze`/batch path and the size `analyze_batch` path; the central
`redact_text` passed a `https://rpc.example/v2/<secret>` path credential through
unchanged. The same script also recorded the trailing-parenthesis diagnostic loss (a
fragment-bearing parenthesized URL lost its closing `)`). Raw log preserved at
`/home/dev/dispatch-state/polymarket-slice003-muse-secrets-r10-20260911/r10-evidence/regression-red-start-head.log`
(`TOTAL_LEAKING_SINKS=11`, script exit 1). The new regression tests were additionally
transplanted onto a disposable stash of the exact start head and run there: **15 failed**
for the intended reasons (upstream-sink leaks, path-credential leaks, lost closing
delimiter, updated summary/CLI expectations); post-fix the same files pass
(`r10-evidence/regression-red-new-tests-on-start-head.failures.log` and
`r10-evidence/regression-green-post-fix-tests.tail.log`).

Fix (one coherent secrets-boundary repair, no new redactor, no threshold/behavior change):

1. Central redaction (`src/polymarket_insider_tracker/redaction.py`) is now fail-closed on
   endpoint paths: any URL-shaped value with a non-root path keeps scheme plus host (and
   port) so the endpoint stays diagnosable, while the path itself is never emitted
   (`***path***`). The policy is grounded in the actual configuration contract
   (`POLYGON_RPC_URL`, `POLYGON_FALLBACK_RPC_URL`, `POLYMARKET_TRADES_URL`, database and
   Redis URLs accept URLs whose paths may carry provider credentials, and no static shape
   separates a key segment from a benign prefix). Query values are masked literally as
   `***` (no percent-encoding drift), and `redact_text` trims trailing prose delimiters
   (`)`, `]`, quotes, sentence punctuation) before redacting so a parenthesized URL keeps
   its closing delimiter. Runtime URLs are untouched; only output/log/summary redaction
   changed. No allowlist of secret values was introduced.
2. Exception-bearing diagnostics are sanitized through the central `redact_text` at every
   confirmed reachable sink: `profiler/analyzer.py` (token-balance failure, profile-cache
   read/write failures, batch-handler failures), `profiler/chain.py` (chain-cache
   read/write failures, retry `Web3Exception` warnings, batch nonce failures),
   `profiler/funding.py` (transfer-log, chunk-scan, and batch-trace failures — confirmed
   equivalents of the identical raw-exception pattern on the wired enrichment path),
   `detector/size_anomaly.py` (metadata-fetch failure and batch-handler failures), and
   `detector/fresh_wallet.py` (batch-handler failures; the single-trade path already
   propagates to the redacted pipeline boundary). Retries, fallbacks, return contracts,
   scoring, persistence, readiness, shutdown, migrations, APIs, and log levels are
   unchanged.
3. Tests (`tests/test_redaction.py`, `tests/test_config.py`, `tests/test_main.py`) now use
   real profiler/detector components with narrow repository fakes (`FakeEth` subclasses
   raising `Web3Exception`, per-market `FakeMetadataSync` failures, `FakeAsyncRedis`
   subclasses raising on `get`/`set`) instead of a replacement detector that bypasses the
   internal sinks. They assert every captured log and exposed error surface is secret-free
   while trade IDs, market IDs, host names, and non-secret diagnostics (`connection
   reset`, detector names) remain. Coverage: both detector boundaries, all newly fixed
   internal sinks, path credentials (primary and fallback), userinfo/query/fragment/
   malformed-nested shapes, and the trailing-parenthesis diagnostic case. The two
   pre-existing summary/CLI expectations that asserted a benign default path verbatim now
   assert the fail-closed masked form with the host preserved.

Adjacent-sink audit disposition: every other production-reachable `logger.*(..., e)`
site was inspected. Dispatcher channel-error logging, pipeline error capture, health
serialization and the last-error provider, trade-poller `redact_error`, and
`trades_source.redacted_url` already redact. `__main__.py` `logger.exception` reports
only startup/runtime failure text through the same central policy outcome (covered by the
pipeline redaction tests); no unconfirmed sink was changed. The detector `analyze_batch`
and analyzer/chain batch handlers are included in this repair, so no library-path debt
remains for them; the funding batch `trace_many` path is likewise sanitized.

Verification: targeted and affected suites, the test-quality AST policy, static,
compatibility, services, full, coverage, vulture, complexipy (fail-closed, max 5),
strict mypy/pyright, migration/Redis gates, and `git diff --check` are recorded in the
writer receipt under
`/home/dev/dispatch-state/polymarket-slice003-muse-secrets-r10-20260911/r10-evidence/`.
No mocks, monkeypatch interception of product code, skips, exclusions, `no-cover`/`noqa`,
or threshold weakening were introduced.

---

## 12. Round-12 Secrets-Boundary Correction (2026-09-11, Muse Spark writer lane)

The round-11 Fable review (`claude-fable-5`, exact head `76bd4a56`, verdict `REVISE`)
reproduced load-bearing secrets-boundary defects under the candidate's own fail-closed
path-credential contract and recorded that §11's adjacent-sink audit sentence claiming
those helpers "already redact" is inaccurate for the path threat model. §10 and §11
history above is preserved unchanged; this section appends the correction. It also
corrects the §10 sentence naming the trade-poller `redact_error` path as already
redacting: that helper masked only query strings and wallet shapes, not endpoint paths.

Regression proof on the exact start head `76bd4a56` (claims reproduced before fixing,
on the unmodified commit):
- `POLYMARKET_TRADES_URL=https://proxy.example/v2/<secret>/trades` passes `Settings`
  validation; the HTTP failure builds
  `TradesSourceError("HTTP 500 from https://proxy.example/v2/<secret>/trades")`, which
  the poller `_degrade`/`_fail` path logs verbatim (`LEAK: True` on both
  `redacted_url` output and the poller error string).
- `redact_url("https://rpc.example/v2/KEY@prod")` returned completely unredacted.
- `redact_url("wss://https://user:PW@meta.example/v2/KEY")` masked userinfo but
  emitted path `/v2/KEY`.
- `redact_url("https://host?SECRETTOKEN")` preserved the bare token as a key
  (`?SECRETTOKEN=***`).
Raw behavior log: `/tmp/r12-evidence/regression-red-start-head-actual.log`.
The 31 new round-12 tests transplanted onto a pristine `git archive 76bd4a5` source
tree (with `PYTHONPATH` pointed at the archived `src` so the start-head code is what
runs) fail as intended: **26 failed / 5 passed**
(`/tmp/r12-evidence/new-tests-on-start-head.failures.log`). The 5 passes are the
runtime-URL-untouched check, the pre-existing-behavior idempotency-adjacent cases, and
the dispatcher/clob cases whose product code already sanitized that shape; post-fix the
same files pass 31/31. Post-fix behavior log:
`/tmp/r12-evidence/regression-red-start-head.log` (`LEAK: False` on N1, masked N3a/N3b,
`?***` on N5).

Fix (one coherent secrets-boundary correction, no new redactor, no behavior change
beyond diagnostics):
1. N1 — `trades_source.redacted_url` now delegates to central `redact_url`, so a
   proxied trades path is fail-closed (`https://proxy.example/***path***`) while
   scheme/host/port stay diagnosable; every trades-source error message
   (`_exhausted`, `_classify`, `_parse_success`, `_transient`) inherits the policy.
   Poller `redact_error` now runs the legacy wallet/query scrub *after* central
   `redact_text`, so `last_error` and the `_degrade`/`_fail` logs carry the masked
   endpoint plus the non-secret operational context (`HTTP 403/503`,
   `incompatible-schema`). Runtime request URLs are untouched (asserted: the
   secret-bearing URL is still sent on the wire; only diagnostics change).
   Regression: `TestTradesBoundaryRedaction` (3 tests) drives the real `Settings`
   → real `TradesSourceClient` error construction → real poller `_degrade`/`_fail`
   path over `FakeTradesServer` faults. Updated expectations:
   `tests/ingestor/test_trades_source.py` (masked endpoint form),
   `tests/ingestor/test_trade_poller.py` (host + `***path***` instead of the raw
   `/trades` path), `tests/tooling/test_trades_smoke.py` (smoke `endpoint` record
   is the redacted label).
2. N2 — every production-reachable raw-exception interpolation now passes through
   central `redact_text` at the boundary: `metadata_sync.py` (state-change,
   initial-sync, sync-loop, batch-cache, sync-complete callbacks, `_record_sync_failure`,
   both cached-parse sites, fetch-market, gamma-stats fallback), `publisher.py`
   (deserialize), `clob_client.py` (retry backoff, market/orderbook error wrapping,
   midpoint/price/health/server-time logs), `gamma_client.py` (retry warning and
   terminal error), `health.py` (change-callback and check-loop logs),
   `dispatcher.py` (dedup/claim/release/outcome/renew warnings; the send-error path
   already redacted), `trade_poller.py` (callback, repair, state-callback logs),
   `shutdown.py` (abandoned-work, cleanup-callback, handler-install warnings), and
   the deprecation-fenced `websocket.py` (all log/store sites, including
   `last_error`). Exception types, chaining (`from e`), retry counts, and
   fallback/return contracts are unchanged. Regression: `TestAdjacentSinkRedaction`
   drives each real component with a narrow explicit fake and asserts logs plus
   stored `last_error` exclude the synthetic secret while identifiers and
   diagnostics survive. Honestly dispositioned as *not changed*: `observation_boundary.py:130`
   (`BoundarySchemaError` carries only Redis-hash field names, never a URL),
   `publisher.py:308` (`"BUSYGROUP" in str(e)` is a membership test, never emitted),
   `dispatcher.py:448-452` (abandoned-send debug logs only the exception class name),
   Discord/Telegram channel response bodies (`response.status_code`/`response.text`,
   API error code/description — delivery-side surfaces with no configured-credential
   URL on the path; the `httpx.HTTPError` catch in each channel already redacts via
   `self._redact`), and health `ComponentStatus` timeout strings (static text).
3. N3 — `@`-in-path shapes can no longer bypass fail-closed path masking. The
   structured branch now only handles URLs whose `@` (if any) is a genuine netloc
   userinfo; a path `@` without netloc `@` falls into the existing fail-closed
   path rule (`https://rpc.example/v2/KEY@prod` →
   `https://rpc.example/***path***`); the split-across-boundary nested scheme
   (`wss://https://user:PW@meta.example/v2/KEY`, netloc `https:` + path
   `//user:PW@…`) routes to the shape-based fallback, which keeps the embedded
   host readable and masks userinfo plus path
   (`wss://https://***@meta.example/***`). The userinfo fallback now anchors on
   the *first* `@` after the scheme, so a real userinfo plus a path `@`
   (`https://user:pw@h:8443/v2/K@prod`) masks both. Regression:
   `TestAmbiguousUrlShapes` table-drives the exact Fable probes plus
   userinfo/IPv6/port/query/fragment/nested/trailing-delimiter variants, asserts
   the runtime URL is still sent unchanged, and asserts surrounding prose
   delimiters survive.
4. N4 — `__main__.py` pipeline-failure handler now logs
   `redact_text(str(e))` through the central policy. Limitation, honestly recorded:
   `logger.exception` still attaches the raw traceback, which may echo the
   exception text; the message line itself is sanitized while the traceback is
   preserved for diagnosability. Regression: `test_run_pipeline_startup_failure_is_redacted`
   drives the real `run_pipeline` with a raising `Pipeline.start()` whose error
   embeds a credential-bearing DSN, and asserts every `__main__` message line is
   secret-free with `***` present. Adjacent sinks closed in the same pass:
   trade-poller callback/repair/state-callback logs (tested with the real poller
   plus leaky callback/lookup fakes, asserting `callback_errors` still increments
   and repair still returns the observation).
5. N5 — a bare query token is now fail-closed: any `parse_qsl` pair with an empty
   value renders as a bare `***`, so `?SECRETTOKEN` → `?***` and `?a=1&FLAG&b=2`
   → `?a=***&***&b=***` (documented in `_redacted_query`: a bare token and a
   `key=` pair with an empty value are indistinguishable without inventing parsing
   semantics). Fragment/trailing-punctuation handling: fragments render as `#***`
   and prose delimiters are trimmed before redaction, so no secret characters are
   re-emitted (`(see https://meta.example#SECRET) trailing` keeps its delimiter;
   `#SECRET.` → `#***`). If a narrower syntax is ever required, the contract stays
   fail-closed masking. Regression: `test_bare_query_token_is_masked_fail_closed`,
   `test_valueless_query_pairs_are_masked_fail_closed`, and the fragment/trailing
   cases inside `TestAmbiguousUrlShapes`.

Verification (post-fix, exact commands; logs under `/tmp/r12-evidence/`,
`r12-*.log`):
- New round-12 tests: 31 passed (`test_redaction.py -k` selection); full
  `test_redaction.py`: 62 passed.
- Targeted redaction/config/main/trades/metadata/publisher/dispatcher/pipeline/health
  suites: 396 passed.
- AST test-quality policy (`tests/tooling/test_test_quality.py`): 29 passed.
- Full suite minus the two environment-blocked complexipy-tooling tests:
  1246 passed, 1 skipped (non-integration) / 1275 passed, 3 skipped (all, minus
  complexipy tooling); integration (real local Redis): 52 passed, 2 skipped.
- Branch coverage: TOTAL 93% (`--cov-branch`).
- Black: clean (116 files); Ruff: clean; `git diff --check`: clean.
- Complexipy fail-closed launcher (max 5): passed (exit 0).
- Vulture (default confidence): exit 0.
- Strict mypy: **not runnable here** — the pinned gate requires
  `uv run --isolated --locked ... mypy` (Python 3.11) and this environment has no
  `uv` binary; the direct-venv run fails on a pre-existing numpy-stub /
  `python_version=3.11` mismatch identical on the start head (environmental,
  unrelated to this change). Pyright: 0 errors on `src/polymarket_insider_tracker`
  (new-version notice only).
- `tests/tooling/test_complexipy.py` (2 tests): fail here because they shell out to
  `uv`, which is absent (pre-existing environmental failure, identical on the start
  head). The underlying launcher command itself was run directly and passes.
- Services profile (PostgreSQL + migration cycling): **not run** — no loopback
  PostgreSQL in this environment (`pg_isready`: no response); Redis-contract tests
  against local Redis pass (52 passed). No external providers, Polymarket,
  trading, Discord/Telegram, production, cloud, or credentials were contacted.

Evidence corrections vs prior sections: §10's "`redact_error` path already redact"
and §11's "`trades_source.redacted_url` already redact / every other
`logger.*(..., e)` site already redact / `__main__.py` covered by pipeline tests"
sentences were inaccurate for the path threat model and the f-string sink class;
they are superseded by the dispositions above. Surfaces tested by execution are
listed per finding; the "not changed" list above is reasoned from exact call paths
(stored-value flow or emission shape), not from a mechanical enforcement gate.
Fresh evidence in this section binds to the final commit SHA recorded in the writer
receipt; §10/§11 results remain bound to their original heads.
