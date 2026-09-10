# Tasks: Supported Trade Ingestion

**Input**: Design documents from `specs/001-supported-trade-ingestion/`

**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/*.md`,
`evidence/feasibility.md`, and Patrick's validation of the planning package. The reviewer-owned
`checklists/ingestion.md` markers are Patrick's and are never modified by implementation.

**Tests**: Required by the feature specification (FR-012) and the constitution. Every behavior task is
preceded by a test task whose tests are written and observed failing for the intended reason.
`unittest.mock` is prohibited; boundary fakes, `fakeredis`, real values, and the real pipeline are used.

**Organization**: Tasks are grouped by user story so each story has an independently runnable proof.
Requirement identifiers in brackets trace each task to `spec.md`.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel because it touches different files and has no dependency on incomplete work
- **[Story]**: Maps implementation work to `US1`, `US2`, or `US3` from `spec.md`
- Every task names its exact file target or evidence artifact

## Phase 0: Human Validation Gate (hard stop)

**Purpose**: No task below starts before this phase is complete.

- [x] T001 Patrick validated the 2026-09-09 planning package with “merge and continue” on 2026-09-10,
  approving the three recommended items in
  `specs/001-supported-trade-ingestion/plan.md` (aged-gap behavior, recovery-horizon acceptance,
  `POLYMARKET_WS_URL` deprecation window); record the decision text and date in
  `specs/audit/review-gate.md` [FR-016, FR-019, Constitution §Development Workflow 3]

**Checkpoint**: Implementation authorization is recorded; the product stop gate in
`evidence/feasibility.md` remains binding throughout.

---

## Phase 1: Setup (Provider Fake and Redis Parity)

**Purpose**: Give every later test a faithful provider and prove the Redis operations the boundary needs.

- [ ] T002 [P] Add `FakeTradesServer` behind `httpx.MockTransport` per `contracts/source-acquisition.md`
  (newest-first synthetic ledger, `offset` support, identical body for identical URL, injectable 429
  with `Retry-After`, 5xx, timeout, malformed row, missing `outcome`, exact repeat, row newer than `end`,
  non-list body, request log, synthetic wallets only) in `tests/fakes/trades.py` and export it from
  `tests/fakes/__init__.py` [FR-012, SC-006]
- [ ] T003 [P] Add checkpoint hash, identity sorted-set range/trim/cardinality, loss-event list
  push/trim/range, and combined transactional-pipeline scenarios inside a unique namespace to
  `tests/integration/test_redis_contract.py`; run them against `fakeredis` and, with
  `RUN_SERVICE_TESTS=1`, the loopback Redis [FR-006, Contract §Real and Fake Parity]

**Checkpoint**: The fake provider reproduces every observed provider behavior in
`evidence/feasibility.md`; the Redis contract passes on both implementations.

---

## Phase 2: Foundational (Rows, Source Client, Boundary Store)

**Purpose**: Build the three modules every story consumes, tests first.

**⚠️ CRITICAL**: No user-story behavior starts until these modules have failing tests and implementations.

- [ ] T004 [P] Add failing tests for strict parsing of every identity-bearing field, `side` validation,
  decimal canonicalization, mutually exclusive row dispositions, outcome-resolution counters,
  `deferred:future-cycle` reacquisition, composite identity equality across `0.50`/`0.5`, distinct identities for
  maker and taker rows sharing a transaction hash, exact-repeat identity equality, `outcome` repair from
  a `MarketMetadata` token matching `asset`, `unknown` outcome resolution when metadata is absent,
  `future-timestamp`, and wallet-free diagnostics in `tests/ingestor/test_trade_rows.py` [FR-004, FR-005]
- [ ] T005 Implement `TradeObservation`, `RowDisposition`, `parse_trade_row`, `observation_identity`,
  and the metadata repair helper in `src/polymarket_insider_tracker/ingestor/trade_rows.py` [FR-004, FR-005]
- [ ] T006 [P] Add failing tests for exact request parameters, explicit `takerOnly`, strictly increasing
  `end` (including a clock that has not advanced), `start` derivation, single `offset=10000` recovery
  request, transient-versus-terminal classification by status and transport error, retry count, backoff
  bounds with an injected random source, `Retry-After` cap, timeout, empty-list success, non-list body as
  terminal, and per-attempt request accounting in `tests/ingestor/test_trades_source.py` [FR-001, FR-002, FR-008, FR-012, FR-015, FR-018]
- [ ] T007 Implement `TradesSourceClient`, `TradesPage`, `TradesSourceError`, `TradesTransientError`,
  `TradesTerminalError`, and `Retry-After` parsing over `httpx.AsyncClient` in
  `src/polymarket_insider_tracker/ingestor/trades_source.py` [FR-001, FR-002, FR-008, FR-015, FR-018]
- [ ] T008 [P] Add failing tests with `fakeredis` for the source-id namespace, first-start anchoring,
  reload of the identity window, the proof rule (reach, continuity, equal-second rows, empty page),
  trim relative to the newest accepted timestamp, transactional write atomicity using a closed-port
  `redis.asyncio.Redis` as the failure injector, loss-event list cap of 50, coverage mismatch treated as
  absent, unknown schema version as terminal, and mirror-equals-Redis after every cycle in
  `tests/ingestor/test_observation_boundary.py` [FR-005, FR-006, FR-007, FR-017]
- [ ] T009 Implement `ObservationBoundary` (checkpoint, identity window, loss events, proof evaluation,
  transactional advance) in `src/polymarket_insider_tracker/ingestor/observation_boundary.py` [FR-005, FR-006, FR-007, FR-017]

**Checkpoint**: Rows, requests, and the boundary each have red-then-green coverage; every function and
module scores at most 5 in Complexipy; strict mypy and Pyright are clean.

---

## Phase 3: User Story 1 - Monitor Current Wallet-Bearing Trades (Priority: P1) 🎯 MVP

**Goal**: Current wallet-bearing trades flow from the documented source into the existing detection
pipeline once each, without waiting for the metadata crawl.

**Independent Test**: Feed `FakeTradesServer` pages containing new, overlapping, repeated, and
multi-wallet rows through `wire_pipeline`; every distinct eligible observation reaches the detection
path once with wallet, market, side, outcome, price, size, timestamp, asset, and transaction identity
preserved, and acquisition starts before the metadata sync finishes.

### Tests for User Story 1

- [ ] T010 [P] [US1] Add failing tests for healthy cycles: oldest-first delivery, each distinct
  observation delivered once, exact repeats suppressed, maker and taker rows preserved, overlapping
  pages, cadence timing with an injected clock and sleeper, `IngestionStatus` counters and fields,
  state transitions with `on_state_change`, and metric values in `tests/ingestor/test_trade_poller.py`
  [US1 AC2–AC4, FR-004, FR-005, FR-009, FR-012, SC-001]
- [ ] T011 [P] [US1] Add failing tests proving the poller task starts and delivers before a metadata
  sync blocked behind an `asyncio.Barrier` completes its initial crawl, that observations reach
  detectors, scorer, and persistence through `wire_pipeline` with `FakeAlertChannel`, and that stop
  cancels the poller before the metadata task in `tests/test_pipeline.py` [US1 AC1, FR-003, SC-002]

### Implementation for User Story 1

- [ ] T012 [US1] Implement `TradePoller` (cycle loop, `IngestionState`, `IngestionStatus`, delivery
  order, identity-after-callback, checkpoint-after-page, metrics, clean cancellation) in
  `src/polymarket_insider_tracker/ingestor/trade_poller.py` [FR-003, FR-004, FR-005, FR-009, SC-001]
- [ ] T013 [US1] Construct the poller, start it before the metadata task, track and cancel both tasks,
  and record terminal ingestion errors on `PipelineStats` in
  `src/polymarket_insider_tracker/pipeline.py`; extend `wire_pipeline` with the poller and the fake
  server in `tests/fakes/pipeline.py`; export the new names from
  `src/polymarket_insider_tracker/ingestor/__init__.py` [FR-003, FR-009, SC-002]

**Checkpoint**: User Story 1 runs independently: fixture pages produce exactly-once downstream
delivery while the metadata crawl is still blocked.

---

## Phase 4: User Story 2 - Recover Without Silent Loss or Replay Storms (Priority: P2)

**Goal**: Transient failures, malformed data, saturation, restarts, and aged gaps produce visible
states and durable records instead of silent loss or replay.

**Independent Test**: Drive timeouts, throttling, terminal statuses, malformed rows, out-of-order and
equal-second rows, saturation with and without a successful recovery page, restarts inside and beyond
  the horizon, an inside-horizon checkpoint beyond reachable page depth, and a graceful stop through the poller and boundary with `fakeredis`; verify bounded retry,
explicit states, recorded loss events, and deterministic catch-up.

### Tests for User Story 2

- [ ] T014 [P] [US2] Add failing tests for 429 with `Retry-After`, 5xx, and timeout leading to bounded
  retries then `degraded`, recovery on the next success, 401/404/non-list body leading to `failed` with a
  redacted error, invalid rows counted per field while valid rows in the same page are delivered,
  out-of-order and equal-second rows, and an empty response updating `last_success_at` without
  emission in `tests/ingestor/test_trade_poller.py` [US2 AC1, US2 AC4, FR-008, FR-010, FR-012, SC-003]
- [ ] T015 [P] [US2] Add failing tests for first start with no emission, restart inside the horizon
  delivering only missed identities, restart beyond the horizon writing `restart-beyond-horizon` and
  emitting nothing, saturation proven by the recovery page, saturation unproven entering
  `possible-data-loss` with a frozen complete-through boundary and provisional continuity, a row beyond
  the cycle cutoff deferred then emitted on reacquisition, horizon expiry writing
  `horizon-expired` and re-anchoring, `continuity-mismatch`, graceful stop mid-cycle leaving no partial
  checkpoint, and a callback interrupted after delivery re-delivering at most one observation in
  `tests/ingestor/test_trade_poller.py` and `tests/ingestor/test_observation_boundary.py`
  [US2 AC2–AC3, FR-004, FR-006, FR-007, FR-012, FR-017, SC-004]

### Implementation for User Story 2

- [ ] T016 [US2] Implement degraded and failed transitions, invalid-row accounting, and empty-response
  handling in `src/polymarket_insider_tracker/ingestor/trade_poller.py` [FR-008, FR-009, FR-010, SC-003]
- [ ] T017 [US2] Implement the recovery page, possible-data-loss state, provisional boundary, horizon
  expiry, restart classification, and loss-event writing in
  `src/polymarket_insider_tracker/ingestor/trade_poller.py` and
  `src/polymarket_insider_tracker/ingestor/observation_boundary.py` [FR-006, FR-007, FR-017, SC-004]
- [ ] T018 [US2] Add the restart scenario that persists the boundary in one `fakeredis` instance across
  two poller lifetimes inside the real pipeline and asserts all missed fixture trades once and zero
  pre-boundary replays, as this slice's contribution to the slice 003 harness, in `tests/test_pipeline.py`
  [SC-004, Constitution §III]

**Checkpoint**: Every failure class and restart scenario in FR-012 has deterministic coverage; no
state is silent.

---

## Phase 5: User Story 3 - Migrate Existing Configuration Truthfully (Priority: P3)

**Goal**: New defaults need no WebSocket host; the legacy setting warns actionably; documentation and
runtime agree.

**Independent Test**: Load settings with no WebSocket value, a WebSocket-scheme value, an HTTP-scheme
value, each new trades setting at its bounds, and an invalid coverage; verify the documented result,
warning, or failure and the configuration-check output.

### Tests for User Story 3

- [ ] T019 [P] [US3] Add failing tests for the four trades settings (defaults, ranges, invalid values),
  optional `POLYMARKET_WS_URL` with `WebSocketSettingDeprecationWarning` when set, unchanged rejection of
  a non-WebSocket scheme, and the redacted summary shape in `tests/test_config.py`; failing tests for
  the configuration-check output and warning text in `tests/test_main.py`; a failing test that
  `TradeStreamHandler` construction emits `DeprecationWarning` in `tests/ingestor/test_websocket.py`
  [FR-002, FR-011, FR-012, FR-015, US3 AC1–AC2]

### Implementation for User Story 3

- [ ] T020 [US3] Implement the trades settings, optional deprecated `ws_url`, the warning class, and the
  redacted summary in `src/polymarket_insider_tracker/config.py`; print the settings and disposition in
  `src/polymarket_insider_tracker/__main__.py`; add the construction warning in
  `src/polymarket_insider_tracker/ingestor/websocket.py`; update `make_test_settings` in
  `tests/fakes/pipeline.py` [FR-002, FR-011, FR-015, US3 AC1–AC2]
- [ ] T021 [US3] Align `README.md` (overview, environment table, architecture, troubleshooting,
  near-real-time wording), `.env.example` (remove `POLYMARKET_WS_URL`, add the trades settings),
  `CHANGELOG.md`, and `AGENTS.md` with `contracts/status-and-config.md` [FR-014, FR-015, SC-006, US3 AC3]

**Checkpoint**: A clean `.env.example` starts the tracker without a WebSocket host; every documentation
surface describes the same behavior.

---

## Phase 6: Live-Safe Smoke, Evidence, and Convergence

**Purpose**: Prove the contract against the real provider within bounds, run every gate, close the
owned gaps, and hand off for review.

- [ ] T022 [P] Add failing tests for the seven named smoke cases, wallet-free record serialization,
  `--live` required by default, exit codes, and no Redis or alert construction in
  `tests/tooling/test_trades_smoke.py` [FR-013, SC-005]
- [ ] T023 Implement `run_smoke` and the CLI in `scripts/trades_smoke.py` [FR-013, SC-005]
- [ ] T024 Run the explicit bounded live-safe smoke (`--live --coverage both --window-seconds 5`) plus
  twelve consecutive five-second poller cycles against the real endpoint from a disposable local Redis,
  evaluate every product stop-gate condition, and append the redacted commands, records, and verdict to
  `specs/001-supported-trade-ingestion/evidence/smoke.md`; stop for Patrick if any condition trips
  [FR-013, FR-016, FR-019, SC-005, SC-007]
- [ ] T025 Run `uv run python scripts/verify.py --profile static`, `--profile compatibility` on
  isolated Python 3.11/3.12/3.13, and `--env-file .env --profile services` with local PostgreSQL 15 and
  Redis 7; record results in `specs/001-supported-trade-ingestion/evidence/verification.md`
  [Constitution §V, Contract §Required Gates]
- [ ] T026 Update only G-001, G-002, G-003, G-004, G-005, G-006, G-013b, G-030b, G-031, and G-032 with
  implementation and evidence dispositions in `specs/audit/gap-register.md` [Constitution §Development Workflow 8]
- [ ] T027 Complete the ordered review sequence recorded in `AGENTS.md` (first pass, corrective pass,
  independent adversarial review), resolve or record every finding, and record commit hashes in
  `specs/001-supported-trade-ingestion/evidence/verification.md` [AGENTS.md §Review and delivery]
- [ ] T028 Prepare the pull request only after T024–T027 converge; verify required checks at the exact
  head; do not merge without Patrick's approval [Constitution §Development Workflow 9]

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 0** blocks everything.
- **Phase 1** has no code dependency; T002 and T003 run in parallel.
- **Phase 2** depends on Phase 1; T004/T006/T008 (tests) run in parallel, each before its
  implementation task (T005, T007, T009). T009 depends on T005 for identities.
- **User Story 1** depends on Phase 2; T010 and T011 precede T012 and T013.
- **User Story 2** depends on User Story 1's poller; T014 and T015 precede T016 and T017; T018 follows T017.
- **User Story 3** depends on Phase 2 only for the settings consumed by T007; T019 precedes T020; T021
  follows T020 and T013 so documentation describes wired behavior.
- **Phase 6** depends on all stories; T022 precedes T023; T024 requires T023 and a local Redis; T025
  follows all code; T026–T028 follow T025.

### User Story Dependencies

- **US1 (P1)**: Delivers the MVP; independently testable with `FakeTradesServer` and `fakeredis`.
- **US2 (P2)**: Extends the same poller with failure and recovery behavior; independently testable.
- **US3 (P3)**: Configuration and documentation; independently testable through settings loading.

### Within Each User Story

- Observe each required test fail for the intended reason before its implementation task.
- Rows before source client before boundary before poller before pipeline wiring.
- Keep every function and module at or below Complexipy 5 by naming steps, not by suppressing.

### Parallel Opportunities

- T002 ∥ T003; T004 ∥ T006 ∥ T008; T010 ∥ T011; T014 ∥ T015; T019 ∥ T022.
- Evidence tasks never run concurrently against the same evidence file.
- No feature-writing Spec Kit command runs concurrently in this checkout.

## Parallel Example: Foundational Tests

```text
Task T004: tests/ingestor/test_trade_rows.py
Task T006: tests/ingestor/test_trades_source.py
Task T008: tests/ingestor/test_observation_boundary.py
```

## Implementation Strategy

### MVP First (User Story 1)

1. Complete Phase 0, Phase 1, and Phase 2.
2. Complete T010–T013 in red-green order.
3. Stop and run the User Story 1 independent test before touching failure behavior.

### Incremental Delivery

1. Foundation → rows, requests, and boundary proven in isolation.
2. US1 → exactly-once delivery with startup independent of metadata.
3. US2 → visible failure, recovery, restart, and loss semantics.
4. US3 → truthful configuration and documentation.
5. Phase 6 → live-safe evidence, gates, gap register, review, and pull-request preparation.

## Notes

- Every task remains unchecked until implementation evidence proves it complete.
- `[P]` means file-independent, not permission to run concurrent Spec Kit feature-writing commands.
- Reviewer checklist state is controlled by Patrick and is not modified by `$speckit-implement`.
- Stop for a spec or plan revision, or for the product stop gate, if implementation cannot prove the
  boundary rule, keep the request budget, preserve the public configuration path, or satisfy the
  Complexipy limit without exemptions.
