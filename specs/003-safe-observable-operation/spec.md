# Feature Specification: Safe Observable Operation

**Feature Branch**: `feat/slice-003-safe-observable-operation`

**Created**: 2026-09-06

**Status**: Authorized by Patrick's 2026-09-10 instruction

**Input**: User description: "Make health, readiness, failure propagation, dry-run behavior, alert deduplication, and end-to-end pipeline evidence accurate and safe."

## Clarifications

### Session 2026-09-10

- Q: How does delivery deduplication separate from risk qualification? → A: RiskScorer qualifies or rejects trades based purely on score and threshold; it never mutates Redis or marks delivery state. Delivery deduplication is handled exclusively at alert dispatch time by AlertHistory per channel using the key format `alert:dedup:{channel}:{wallet}:{market}`. Dry-run mode evaluates scoring and records a `dry_run` disposition without calling external channels or writing dedup keys.
- Q: What is the behavior for multi-channel dispatch when one channel fails or times out? → A: Deduplication keys are written strictly per-channel after confirmed successful delivery. A confirmed failure leaves the channel eligible for immediate retry. An ambiguous outcome (e.g. timeout where delivery status is uncertain) sets a temporary 60-second ambiguity suppression key (`alert:ambiguous:{channel}:{wallet}:{market}`) to prevent rapid retries, after which the channel is eligible for retry while logging an explicit possible-duplicate warning.
- Q: What is the distinction between `--config-check`, `/live`, and `/ready`? → A: `--config-check` is an offline validation tool that verifies syntax, field types, and config shapes without network I/O; its output clarifies that offline syntax passed and runtime readiness requires reachable services. `/live` indicates the HTTP server process is running and responsive. `/ready` performs active, bounded checks against required local dependencies (PostgreSQL, Redis, and trade acquisition); it fails (503) if any required dependency is unreachable or if the ingestion worker has terminally failed.
- Q: How does background worker failure propagate to process exit? → A: The pipeline orchestrator monitors background workers (trade poller and metadata sync). If a required worker (such as the trade poller) terminates with an error or unhandled exception, the pipeline transitions to `PipelineState.ERROR`, updates `/ready` to return 503, signals graceful shutdown of remaining services, and causes `__main__.py` to exit with a nonzero status code (EXIT_ERROR = 1).
- Q: What is the unified risk assessment schema for slice 003 and slice 004? → A: Slice 003 introduces Alembic revision `003_safe_observable_operation` adding columns to `risk_assessments`: `delivery_disposition` (VARCHAR(32)), `delivery_channels` (TEXT/JSON), `dry_run` (BOOLEAN), `volume_available` (BOOLEAN), `market_daily_volume` (NUMERIC(20, 6)), `book_depth_available` (BOOLEAN), `wallet_tx_count` (INTEGER), and `wallet_age_known` (BOOLEAN). This satisfies FR-012, FR-019, and G-033 so that slice 004 requires no conflicting schema migration. *(Amended 2026-09-11: the same revision now adds two further columns — see the Session 2026-09-11 clarification below.)*

### Session 2026-09-11

- Q: How does a stored assessment identify the scoring algorithm and configuration that produced it, given that the round-4 adversarial review showed pre-migration rows and rows scored under the (previously mutable) weight configuration were indistinguishable? → A: Patrick's 2026-09-11 decision expands the `003_safe_observable_operation` schema from 8 to 10 columns. `scoring_algorithm_version` (VARCHAR(32), NOT NULL, no insert default) records the producing algorithm version on every new row; the migration backfills pre-existing rows as exactly `legacy-unversioned` because their configuration is unknowable. `scoring_config` (TEXT, nullable) records the canonical deterministic JSON of the exact active configuration (weights, bonuses, threshold, quantum) used for each new assessment; legacy rows remain NULL. The previously removed `weights=` constructor argument and `set_weights()` API are restored for one compatibility/deprecation window (functional, emitting `DeprecationWarning`); `DEFAULT_WEIGHTS` is immutable and `get_weights()` returns a defensive copy. Breaking removal of the deprecated API is outside this slice.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Know Whether Monitoring Is Actually Working (Priority: P1)

As an operator, I can distinguish a live process from a ready monitor, see source freshness and
dependency degradation, and receive a terminal failure when the tracker can no longer monitor.

**Why this priority**: A process that remains running while its ingestion task is dead creates false
confidence and no useful monitoring.

**Independent Test**: Start the pipeline with controllable dependency and ingestion fakes, exercise
healthy, quiet, stale, degraded, and terminal states, and verify process state, health responses,
metrics, logs, and exit behavior agree.

**Acceptance Scenarios**:

1. **Given** required dependencies are reachable and ingestion is acquiring successfully, **When** the
   health interfaces are queried on the configured port, **Then** liveness and readiness are successful
   and the detailed report identifies the last acquisition and trade times separately.
2. **Given** successful acquisition returns no trades during a quiet interval, **When** event age exceeds
   the trade-staleness threshold but acquisitions remain current, **Then** source reachability stays
   visible and the report does not mislabel an empty success as a disconnected source.
3. **Given** a recoverable dependency or source failure, **When** the health report is queried, **Then**
   readiness becomes degraded or unavailable with the failing component and last error identified.
4. **Given** the ingestion worker terminates unexpectedly, **When** the failure reaches the orchestrator,
   **Then** the pipeline enters an error state, readiness fails, cleanup runs, and the CLI exits nonzero.
5. **Given** a command-line health-port override, **When** the pipeline starts, **Then** all health
   interfaces bind to that effective port and the printed configuration reports the same value.

---

### User Story 2 - Exercise Alerts Without Poisoning Delivery State (Priority: P1)

As a maintainer, I can run the complete pipeline in dry-run mode without contacting real notification
channels or suppressing a future real delivery, while successful real deliveries still prevent spam.

**Why this priority**: Verification must be safe, and delivery deduplication must represent what was
actually sent rather than what merely scored above threshold.

**Independent Test**: Drive threshold-passing fixture trades through dry-run, total delivery failure,
partial delivery, successful retry, and duplicate delivery using fake channels and an inspectable
dedup store.

**Acceptance Scenarios**:

1. **Given** dry-run mode and a threshold-passing assessment, **When** the pipeline reaches delivery,
   **Then** no channel is contacted, no delivery dedup state is written, and the “would deliver” outcome
   remains visible in the persisted research record and logs.
2. **Given** all configured channels fail, **When** dispatch completes, **Then** no successful-delivery
   dedup state is written and a later attempt remains eligible.
3. **Given** one channel succeeds and one fails, **When** the same assessment is retried, **Then** the
   successful channel is not spammed and the failed channel remains eligible for retry.
4. **Given** every configured channel has successfully received an equivalent wallet/market alert within
   the dedup window, **When** another equivalent alert qualifies, **Then** delivery is suppressed and the
   duplicate disposition is recorded.
5. **Given** a channel outcome is ambiguous because the request may have been accepted before timeout,
   **When** automatic retry is considered, **Then** the channel enters an explicit unknown state for a
   bounded ambiguity window instead of being mislabeled success or clean failure.

---

### User Story 3 - Reproduce the Whole Monitoring Path Safely (Priority: P2)

As a reviewer, I can run one deterministic scenario that proves a normalized trade is profiled,
enriched, detected, scored, persisted, and either safely suppressed or delivered to fakes, including
observable behavior when a component fails.

**Why this priority**: Hundreds of isolated tests do not prove that runtime wiring implements the public
workflow or that side effects occur in the correct order.

**Independent Test**: Run the documented end-to-end verification with local deterministic doubles and
inspect the normalized trade, profile, signal inputs, assessment row, delivery disposition, health state,
and shutdown result.

**Acceptance Scenarios**:

1. **Given** a complete fixture trade and healthy deterministic dependencies, **When** the scenario runs
   in dry-run mode, **Then** exactly one assessment is persisted with explainable inputs and zero external
   notification calls occur.
2. **Given** a persistence failure after scoring, **When** delivery is otherwise authorized, **Then** the
   failure is visible and the fake delivery attempt still occurs.
3. **Given** a profiling or detector dependency failure, **When** the trade is processed, **Then** the
   failure is counted and the resulting assessment or skip disposition explains which evidence was absent.
4. **Given** graceful shutdown during acquisition or processing, **When** the timeout is not exceeded,
   **Then** active tasks and local resources close without an orphan worker or corrupted delivery state.

### Edge Cases

- A live process can be unready; liveness must not imply source or dependency readiness.
- A high-volume source may be healthy even when downstream processing is lagging; source freshness and
  processing lag must be distinct.
- A quiet but reachable source can have no new trade while still being healthy.
- Zero notification channels, one channel, and multiple channels have different delivery outcomes.
- A channel can time out after accepting a message, so exactly-once external delivery cannot be promised.
- Persistence may fail before or after a channel attempt; both outcomes must remain visible.
- The health port may already be in use and startup must fail actionably rather than hiding the server error.
- Shutdown can race with a retry, an in-flight trade, or a health request.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The running tracker MUST expose the documented liveness, readiness, detailed health, and
  metrics interfaces on the effective configured port.
- **FR-002**: Liveness MUST indicate only that the process can serve health requests. Readiness MUST fail
  when a required local dependency is unavailable, ingestion has terminally failed, or processing cannot
  make progress.
- **FR-003**: Detailed health MUST distinguish source acquisition freshness, most recent trade freshness,
  processing progress, dependency state, and last error. A successful empty acquisition MUST be represented.
- **FR-004**: Every parsed command-line override, including the health port, MUST affect runtime behavior
  and the redacted configuration summary consistently.
- **FR-005**: Offline configuration validation MUST describe only syntax and configuration validity. Any
  readiness claim MUST perform bounded connectivity checks against each required dependency and source.
- **FR-006**: A terminal background-worker failure MUST propagate to pipeline state, health, cleanup, and
  a nonzero CLI exit; the pipeline MUST NOT remain `RUNNING` after its required ingestion worker exits.
- **FR-007**: The default external-service settings used by the quick start MUST pass the bounded readiness
  check when the documented no-key setup claims they do, or the documentation MUST require an operator value.
- **FR-008**: Risk qualification and delivery deduplication MUST be separate decisions. Calculating or
  persisting a qualifying assessment MUST NOT by itself mark an alert as delivered.
- **FR-009**: Dry-run mode MUST make zero network calls to Discord or Telegram and MUST write zero
  successful-delivery dedup keys.
- **FR-010**: Successful-delivery dedup state MUST be scoped per notification channel and written only after
  that channel reports success. A confirmed-failed channel MUST remain eligible without resending to
  successful channels; an ambiguous attempt uses the separate bounded state defined below.
- **FR-011**: The authoritative delivery lifecycle and dedup implementation MUST be singular and documented;
  dormant competing paths MUST be removed, delegated, or explicitly marked non-operational.
- **FR-012**: Every signal-bearing assessment MUST be persisted when configured, including below-threshold,
  dry-run, duplicate, delivery-success, partial-failure, and total-failure dispositions, with enough inputs
  to explain the score and threshold used.
- **FR-013**: Assessment persistence failure MUST be observable and MUST NOT prevent an otherwise authorized
  fake or real delivery attempt.
- **FR-014**: Deterministic end-to-end verification MUST cover ingestion, profiling/enrichment, detection,
  scoring, assessment persistence, dry-run or fake dispatch, health transitions, failure, restart, and shutdown.
- **FR-015**: Required automated verification MUST never contact a real notification destination or place,
  simulate as real, recommend, or automate a trade.
- **FR-016**: Operational and troubleshooting documentation MUST define liveness, readiness, degraded state,
  dry-run effects, delivery dedup semantics, and nonzero terminal exit behavior consistently.
- **FR-017**: The delivery identity MUST be notification channel plus normalized wallet plus market. Side,
  score changes, and repeated trades within the configured window MUST NOT create a new delivery identity;
  their assessments remain durable even when notification is suppressed.
- **FR-018**: Channel outcomes MUST distinguish confirmed success, confirmed failure, and ambiguous/unknown.
  Unknown outcomes MUST suppress automatic retry for a 60-second ambiguity window, then become eligible;
  documentation MUST state that a duplicate remains possible after an ambiguous acceptance.
- **FR-019**: This slice MUST own the target assessment/delivery schema and migration, including the evidence
  availability and reproducibility fields already required by slice 004. Slice 004 MUST extend behavior
  against that target instead of creating a competing assessment migration.
- **FR-020**: This slice MUST own the reusable deterministic end-to-end harness. Slices 001 and 004 MUST
  contribute source fixtures and evidence assertions to this harness rather than create competing frameworks.

### Key Entities

- **Pipeline Health**: Process liveness, readiness, component states, acquisition and processing freshness,
  counters, lag, last errors, and effective health port.
- **Risk Assessment Record**: The normalized trade reference, detector evidence, score, threshold, duplicate
  decision, dry-run state, and delivery disposition needed to explain an evaluation.
- **Channel Delivery Record**: Per-channel attempt and success state used to suppress only deliveries that
  are known to have succeeded within the configured window.
- **Verification Scenario**: A deterministic trade and controlled dependency outcomes with inspectable
  persistence, delivery, health, and shutdown evidence.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Across deterministic healthy, quiet, stale, degraded, and terminal scenarios, 100% of
  liveness/readiness responses and process exit codes match the defined state.
- **SC-002**: A forced ingestion-worker crash changes the pipeline out of running state and produces a
  nonzero CLI outcome within 5 seconds, followed by verified resource cleanup.
- **SC-003**: Across 100 dry-run threshold-passing fixture assessments, real channel call count and newly
  written successful-delivery dedup-key count are both zero.
- **SC-004**: Across success, total-failure, and partial-failure fake-channel scenarios, 100% of successful
  channels are suppressed on retry and 100% of failed channels remain eligible.
- **SC-005**: The deterministic end-to-end scenario produces exactly one explainable assessment record
  from one distinct fixture trade and sends zero real external notifications.
- **SC-006**: Every documented health route responds on the configured override port, and no health route
  responds on the superseded default port for that run.
- **SC-007**: Deterministic behavioral tests prove that configuration-only validation, process liveness,
  source reachability, and full readiness remain distinct states. Contributor documentation describes
  those tested behaviors without being reparsed by a bespoke operational-contract checker.
- **SC-008**: Confirmed-success, confirmed-failure, and ambiguous-timeout fixture cases produce three distinct
  channel states; an ambiguous state suppresses retry for exactly the configured 60-second window and then
  permits retry while retaining an explicit possible-duplicate disposition.

## Assumptions

- The existing `/live`, `/ready`, `/health`, and `/metrics` route names remain the public health contract.
- PostgreSQL, Redis, and the configured trade source are required for readiness. Polygon degradation may
  permit reduced evidence only if the resulting state is explicit and scoring never treats unknown data as known.
- Delivery deduplication is per channel. This makes partial failure retryable without resending to a channel
  that already succeeded.
- The default dedup identity preserves the existing public wallet-plus-market window and adds the channel.
  It intentionally does not split by side or score; every assessment is still persisted.
- External notification providers cannot guarantee exactly-once delivery after ambiguous network failures.
  The proposed policy suppresses retry for 60 seconds, then favors eventual delivery and records that a
  duplicate is possible.
- Persisting signal-bearing assessments remains enabled by default. This slice records delivery disposition
  but does not create a backtesting or calibration workflow.
- Real Discord and Telegram delivery checks remain out of scope unless Patrick separately authorizes them.
- Slice 004's evidence requirements are inputs to this slice's schema plan even though scoring behavior lands
  later. This avoids back-to-back migrations for one assessment contract.
