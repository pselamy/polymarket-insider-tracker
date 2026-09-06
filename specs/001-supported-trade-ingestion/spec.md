# Feature Specification: Supported Trade Ingestion

**Feature Branch**: `codex/spec-kit-brownfield-adoption`

**Created**: 2026-09-06

**Status**: Proposed for Patrick review; implementation is not yet authorized

**Input**: User description: "Restore a current, supported, wallet-bearing public Polymarket trade feed and make startup, replay, and provider failure behavior truthful and testable."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Monitor Current Wallet-Bearing Trades (Priority: P1)

As a research operator, I can start the tracker and have current public trades with a trader wallet
flow into the existing detection pipeline from a provider interface that is currently documented and
supported.

**Why this priority**: Without wallet-bearing trades, none of the wallet profiling or suspicious-flow
monitoring promises can operate.

**Independent Test**: Feed a bounded source containing new and repeated public trades, then verify
that every distinct eligible trade is delivered downstream once with the wallet, market, side,
outcome, price, size, timestamp, asset, and transaction identity preserved.

**Acceptance Scenarios**:

1. **Given** all required local dependencies are ready, **When** the operator starts the tracker,
   **Then** source acquisition begins without waiting for a complete market-metadata crawl.
2. **Given** a new complete trade becomes visible from the supported public source, **When** the next
   acquisition cycle completes, **Then** the trade is handed to the detection pipeline exactly once
   within 10 seconds of that source response.
3. **Given** the provider repeats an already observed trade in overlapping results, **When** the result
   is processed again, **Then** the tracker does not profile, score, persist, or dispatch it again.

---

### User Story 2 - Recover Without Silent Loss or Replay Storms (Priority: P2)

As an operator, I can see when ingestion is delayed or unavailable, and the tracker can resume after
a transient failure without silently skipping recent observations or flooding the pipeline with old
history.

**Why this priority**: A monitor that appears healthy while receiving nothing is worse than a visible
failure, and repeated historical processing can distort research records and alerts.

**Independent Test**: Simulate source timeouts, rate limiting, malformed rows, out-of-order rows, and
a restart from a saved checkpoint; verify bounded retry, explicit degraded state, and deterministic
catch-up behavior.

**Acceptance Scenarios**:

1. **Given** a transient provider or network error, **When** acquisition fails, **Then** the tracker
   reports a degraded source with the last success and error, retries with bounded backoff, and does
   not fabricate events.
2. **Given** a saved observation checkpoint and a restart within the recovery window, **When** the
   tracker resumes, **Then** it processes distinct missed trades and suppresses already processed ones.
3. **Given** no checkpoint on a first-ever start, **When** recent history is used to establish the
   starting boundary, **Then** pre-start rows are not emitted as a burst of new monitoring events.
4. **Given** an invalid trade row among valid rows, **When** the batch is read, **Then** the invalid row
   is counted and explained without discarding valid rows from the same response.

---

### User Story 3 - Migrate Existing Configuration Truthfully (Priority: P3)

As an existing user, I receive a clear compatibility path from the obsolete WebSocket setting and
documentation to the supported ingestion contract.

**Why this priority**: Silently changing or reinterpreting a public environment variable can break
deployments and conceal provider drift.

**Independent Test**: Start with each supported, deprecated, and invalid source configuration and
verify the documented result, warning, or actionable failure.

**Acceptance Scenarios**:

1. **Given** the new default configuration, **When** the operator validates or starts the tracker,
   **Then** no obsolete WebSocket host is required.
2. **Given** the legacy WebSocket variable, **When** configuration is loaded during its deprecation
   window, **Then** the operator receives an actionable warning and the value is never silently treated
   as a different kind of source.
3. **Given** documentation examples and defaults, **When** they are compared with runtime behavior,
   **Then** endpoint type, freshness expectation, required credentials, and limitations agree.

### Edge Cases

- Multiple legitimate trade rows may share a transaction hash; uniqueness must not collapse distinct
  market, asset, side, price, size, wallet, or timestamp observations.
- Several trades may share the same second; a timestamp alone is not a sufficient checkpoint.
- Results may arrive newest-first, out of order, late, repeated, or at a pagination boundary.
- The provider may return an empty list during a quiet interval; empty is not automatically failure.
- The provider may throttle, time out, return a partial page, or return a schema-incompatible row.
- The local clock may differ from provider timestamps; event eligibility must not depend on exact clock equality.
- A metadata record may be absent when a trade arrives; that must not block base trade ingestion.
- A first start and a restart with a durable checkpoint have intentionally different replay behavior.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The tracker MUST acquire public trades from a current, officially documented provider
  interface that supplies the trader wallet and all fields required by the existing trade model.
- **FR-002**: The default source and documented examples MUST work without private Polymarket
  credentials and MUST remain read-only.
- **FR-003**: Source acquisition MUST begin independently of a complete metadata refresh; metadata
  enrichment MAY become more complete after ingestion has started.
- **FR-004**: Every valid source row MUST either produce one normalized trade observation or a recorded
  duplicate decision; it MUST NOT disappear silently.
- **FR-005**: The tracker MUST preserve distinct rows that share a transaction hash and MUST suppress
  exact repeated observations within the active replay horizon.
- **FR-006**: The tracker MUST maintain a durable observation boundary sufficient to resume a bounded
  recent window without a duplicate-processing storm.
- **FR-007**: On a first-ever start, the tracker MUST establish a current boundary without emitting
  pre-start history as new activity. A configured explicit backfill is outside this slice.
- **FR-008**: Provider calls MUST be rate-bounded and MUST retry transient failures with capped backoff
  and jitter; non-retryable schema or configuration failures MUST become actionable errors.
- **FR-009**: Ingestion status MUST expose running, degraded, and terminal-failure states plus last
  successful acquisition time, last trade time, consecutive failures, duplicate count, and invalid-row count.
- **FR-010**: An empty successful response MUST update source reachability without pretending that a
  trade was received.
- **FR-011**: The obsolete WebSocket configuration MUST have a documented compatibility policy. It MUST
  be rejected or deprecated explicitly and MUST NOT be silently reinterpreted.
- **FR-012**: Deterministic tests MUST cover overlap, equal timestamps, out-of-order data, malformed
  rows, throttling, transient recovery, terminal failure, first start, restart, and graceful stop.
- **FR-013**: A live-safe smoke check MUST read only public data, MUST send no alert, and MUST report
  source reachability, schema compatibility, latest observed provider timestamp, and measured lag.
- **FR-014**: README, example configuration, troubleshooting, architecture text, and the tracked skill
  MUST describe the same supported ingestion behavior and limitations.

### Key Entities

- **Trade Observation**: One normalized public trade row, including its wallet, market, asset, side,
  outcome, price, size, provider timestamp, transaction identity, and a stable composite observation identity.
- **Observation Boundary**: Durable state identifying the newest fully considered time region plus the
  recent identities retained to make overlapping acquisition safe.
- **Ingestion Status**: Current lifecycle state and evidence of freshness, failure, retries, invalid data,
  duplicates, and throughput.
- **Source Configuration**: The selected public trade source, acquisition cadence, retry bounds, replay
  horizon, and legacy-setting disposition, with secrets excluded.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In deterministic end-to-end verification, 100% of distinct eligible source rows are
  delivered downstream once and 100% of repeated rows are suppressed.
- **SC-002**: Under healthy deterministic conditions, initial source acquisition starts within 5 seconds
  of local dependency readiness and a newly returned trade reaches the pipeline within 10 seconds.
- **SC-003**: After each simulated transient failure class, acquisition recovers without operator action
  and without duplicates; after a terminal failure, the process reports failure instead of remaining healthy.
- **SC-004**: A restart inside the configured recovery horizon processes all distinct missed fixture
  trades exactly once and emits zero pre-boundary replay events.
- **SC-005**: The live-safe smoke check either confirms a schema-compatible wallet-bearing trade result
  or exits nonzero with an actionable provider/schema diagnosis; it never reports a false pass.
- **SC-006**: A documentation-to-runtime audit finds zero conflicting source hosts, protocols, credential
  claims, or freshness descriptions in tracked user documentation.

## Assumptions

- Current official documentation exposes anonymous wallet-bearing public trades through the public
  trades query, while the public Market WebSocket omits the trader wallet and RTDS no longer supports
  trade activity. The proposed default is therefore near-real-time polling of the supported public
  trade query, not an undocumented WebSocket topic.
- “Real-time” will be replaced with “near-real-time” where users could otherwise infer push delivery.
  Internal processing is bounded after a source response; provider publication lag is measured and
  reported rather than falsely guaranteed.
- The default recovery horizon is 10 minutes. Full historical import and user-selected backfill are out
  of scope for this slice.
- Existing normalized trade consumers remain compatible even if the acquisition mechanism changes.
- Market metadata is enrichment. Its absence may reduce detector confidence but cannot hold ingestion
  hostage.
- No authenticated user-trade feed, trading action, private data, or real notification delivery is in scope.
