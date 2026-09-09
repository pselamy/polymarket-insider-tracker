# Feature Specification: Supported Trade Ingestion

**Feature Branch**: `codex/spec-kit-brownfield-adoption`

**Created**: 2026-09-06

**Status**: Decision A1 approved by Patrick on 2026-09-06; clarified and planned on 2026-09-09 with the
feasibility record in [evidence/feasibility.md](evidence/feasibility.md); implementation is not yet authorized

**Input**: User description: "Restore a current, supported, wallet-bearing public Polymarket trade feed and make startup, replay, and provider failure behavior truthful and testable."

## Clarifications

### Session 2026-09-09

- Q: What proves that a full result page reached the prior durable boundary instead of merely padding
  with older history? → A: After client-side ordering, the page's oldest timestamp is strictly older
  than the boundary time, and every retained identity newer than that oldest row reappears in the
  page. The only recovery is the provider's documented second page (offset 10,000). If proof still
  fails, the tracker enters the visible possible-data-loss state and freezes the durable boundary;
  page length alone never proves absence of loss.
- Q: How is a row that lacks `outcome` handled? → A: `outcome`/`outcomeIndex` are repairable from
  cached asset metadata; when metadata is absent the observation is still emitted with an unknown
  outcome and counted as unrepaired. A row missing any identity-bearing field (`transactionHash`,
  `proxyWallet`, `conditionId`, `asset`, `side`, `price`, `size`, `timestamp`) is invalid, counted per
  field, quarantined as aggregate diagnostics, and never repaired by invention.
- Q: Which provider time bounds are trusted? → A: None. The requested upper bound is a strictly
  increasing per-request value that also defeats shared response caching; rows newer than it are
  accepted as the newest data. A row more than 60 seconds ahead of the local clock is invalid
  (`future-timestamp`). The lower bound is the client-enforced recovery horizon.
- Q: What happens when an unresolved gap, or a restart, exceeds the recovery horizon? → A: The
  interval is recorded durably as a loss event with its reason, exposed in status and logs, and the
  boundary re-anchors at the newest proven page without emitting pre-anchor history. Nothing is
  recovered silently and nothing pre-anchor is emitted as new activity.
- Q: What is the compatibility disposition of the obsolete WebSocket setting and Python surface? → A:
  A deprecation window. `POLYMARKET_WS_URL` no longer defaults to a host, is accepted only with a
  `ws://`/`wss://` scheme, produces an actionable warning at configuration load and configuration
  check, and is never used for acquisition. The WebSocket handler stays importable but warns on
  construction. Rejection or removal requires a later approved change with its own migration note.

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
4. **Given** the default all-participant coverage mode, **When** maker and taker observations share a
   transaction, **Then** distinct wallet-bearing rows remain distinct observations while exact repeats
   of either row are suppressed.

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
2. **Given** a saved observation checkpoint and a restart within the recovery horizon, **When** the
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
- A requested time window may be ignored or applied imprecisely by the provider; returned timestamps
  cannot be assumed to remain inside request bounds.
- A full result page can mean either normal history padding or that newer observations exceeded the
  reachable page depth; the tracker must distinguish and surface possible loss.
- The provider may return an empty list during a quiet interval; empty is not automatically failure.
- The provider may throttle, time out, return a partial page, or return a schema-incompatible row.
- The local clock may differ from provider timestamps; event eligibility must not depend on exact clock equality.
- A metadata record may be absent when a trade arrives; that must not block base trade ingestion.
- A first start and a restart with a durable checkpoint have intentionally different replay behavior.
- A row may carry a timestamp newer than the requested upper bound; that is legitimate newest data,
  while a timestamp far ahead of the local clock is invalid.
- A row may lack `outcome` while every identity-bearing field is present; it is repairable from cached
  asset metadata and must not be discarded or invented.
- The provider may serve an identical request from a shared cache; each request must be distinguishable
  so that a stale cached page is never mistaken for a fresh acquisition.
- An unresolved gap or an outage may outlast the recovery horizon; the lost interval must be recorded
  and visible rather than silently skipped.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The tracker MUST acquire public trades from a current, officially documented provider
  interface that supplies the trader wallet and all fields required by the existing trade model.
- **FR-002**: The default source and documented examples MUST work without private Polymarket
  credentials and MUST remain read-only.
- **FR-003**: Source acquisition MUST begin independently of a complete metadata refresh; metadata
  enrichment MAY become more complete after ingestion has started.
- **FR-004**: Every valid source row MUST either produce one normalized trade observation or a recorded
  duplicate decision; it MUST NOT disappear silently. A row missing only `outcome`/`outcomeIndex` is
  valid: it is repaired from cached asset metadata when available and otherwise emitted with an unknown
  outcome and counted as unrepaired. A row missing any identity-bearing field is invalid, counted per
  missing field, and quarantined as aggregate diagnostics without wallet identifiers.
- **FR-005**: The tracker MUST preserve distinct rows that share a transaction hash and MUST suppress
  exact repeated observations within the recovery horizon.
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
  be rejected or deprecated explicitly and MUST NOT be silently reinterpreted. In this slice the policy
  is a deprecation window: the setting no longer defaults to a host, is accepted only with a WebSocket
  scheme, produces an actionable warning at load and configuration check, and is never used for
  acquisition; the obsolete WebSocket Python surface stays importable but warns on construction.
- **FR-012**: Deterministic tests MUST cover overlap, equal timestamps, out-of-order data, malformed
  rows, throttling, transient recovery, terminal failure, first start, restart, and graceful stop.
- **FR-013**: A live-safe smoke check MUST read only public data, MUST send no alert, and MUST report
  source reachability, schema compatibility, latest observed provider timestamp, and measured lag.
- **FR-014**: README, example configuration, troubleshooting, architecture text, changelog, and root
  agent guidance MUST describe the same supported ingestion behavior and limitations. The former tracked
  documentation skill was removed on 2026-09-09 and is no longer a documentation surface.
- **FR-015**: The default coverage contract MUST include all publicly returned participant observations,
  not silently inherit a taker-only provider default. A taker-only mode MAY exist only as an explicit,
  visible operator choice.
- **FR-016**: Before implementation planning is approved, a bounded feasibility record MUST measure
  current all-market row rate, provider publication lag, ordering, time-window behavior, page saturation,
  cache behavior, and throttling against the official request limit without retaining wallet identities.
  The 2026-09-09 record in [evidence/feasibility.md](evidence/feasibility.md) satisfies this requirement
  for planning; implementation entry MUST re-run the bounded live-safe smoke check before convergence.
- **FR-017**: Acquisition MUST detect when a full page does not reach the last durable observation boundary.
  Reaching the boundary means the client-ordered page's oldest timestamp is strictly older than the
  boundary time and every retained identity newer than that oldest row reappears in the page. It MUST
  attempt the bounded documented recovery (the provider's single additional page) or enter a visible
  possible-data-loss state; it MUST NOT advance the durable boundary past an unresolved gap. A gap that
  ages beyond the recovery horizon MUST become a durably recorded, visible loss event before the boundary
  re-anchors.
- **FR-018**: The default request cadence MUST remain at or below 5% of the provider's published endpoint
  limit under normal operation. Retry and catch-up bursts MUST remain within the published limit.
- **FR-019**: If the feasibility gate cannot demonstrate bounded loss detection and sustainable coverage,
  work MUST stop for a product-scope decision. The tracker MUST NOT fall back to an undocumented interface
  or silently claim comprehensive monitoring.

### Key Entities

- **Trade Observation**: One normalized public trade row, including its wallet, market, asset, side,
  outcome, price, size, provider timestamp, transaction identity, and a stable composite observation identity.
- **Observation Boundary**: Durable state identifying the newest fully considered time region plus the
  recent identities retained to make overlapping acquisition safe, and the recorded loss events that
  explain any interval the tracker could not prove it covered.
- **Ingestion Status**: Current lifecycle state and evidence of freshness, failure, retries, invalid data,
  duplicates, and throughput.
- **Source Configuration**: The selected public trade source, coverage mode, acquisition cadence, retry
  bounds, recovery horizon, and legacy-setting disposition, with secrets excluded.

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
- **SC-005**: The live-safe smoke check reports the correct pass/fail result for seven named cases: a
  valid wallet-bearing response, a valid empty response, throttling, timeout, malformed row, incompatible
  top-level schema, and possible page saturation. It retains no wallet identifiers in its evidence output.
- **SC-006**: Deterministic ingestion and live-safe smoke tests establish source hosts, protocols,
  credential behavior, coverage modes, and freshness semantics. Contributor documentation describes
  those tested behaviors without being reparsed by a bespoke contract checker.
- **SC-007**: A timestamped feasibility report records at least three bounded live samples and reports
  row rate, in-window ratio, oldest/newest timestamps, page saturation, ordering, provider lag, response
  caching, and distance from the published rate limit; any unresolved loss condition blocks approval.

## Assumptions

- Current official documentation exposes anonymous wallet-bearing public trades through the public
  trades query, while the public Market WebSocket omits the trader wallet and RTDS no longer supports
  trade activity. The proposed default is therefore near-real-time polling of the supported public
  trade query, not an undocumented WebSocket topic.
- The default requests all publicly exposed participant observations rather than accepting the provider's
  taker-only default. Multiple wallet rows for one transaction can be legitimate and are not duplicates
  unless their complete stable observation identity matches.
- The provider currently publishes a `/trades` limit of 200 requests per 10 seconds. Normal acquisition
  will use at most 10 requests per 10 seconds unless Patrick approves a revised evidence-backed bound.
- A 2026-09-06 five-second aggregate probe returned a full 10,000-row page even though only 83 taker-only
  or 259 all-participant rows had timestamps inside the requested window; some rows were outside the
  documented bounds. Client-side boundary and saturation evidence is therefore mandatory.
- The 2026-09-09 feasibility record confirmed those semantics with six five-second samples, one
  ten-second sample, and a cache probe: every response was a full 10,000-row page ordered newest-first,
  in-window all-participant rates were roughly 76–109 rows per second, identical URLs were served from a
  shared cache for up to 300 seconds while a moving upper bound was not, rows newer than the requested
  upper bound appeared, every all-participant transaction carried several wallet observations, and a small
  share of rows lacked `outcome`. A five-second cadence uses about 1% of the published request limit and
  leaves an order of magnitude of page headroom at observed rates.
- The recovery horizon is a time bound, but reachable depth is rate-dependent: one page covered roughly
  160 seconds of all-participant history at observed rates, and the documented second page doubles it. A
  restart or outage longer than that reachable depth produces a recorded loss event even inside the
  horizon.
- “Real-time” will be replaced with “near-real-time” where users could otherwise infer push delivery.
  Internal processing is bounded after a source response; provider publication lag is measured and
  reported rather than falsely guaranteed.
- The default recovery horizon is 10 minutes. Full historical import and user-selected backfill are out
  of scope for this slice.
- Existing normalized trade consumers remain compatible even if the acquisition mechanism changes.
- Market metadata is enrichment. Its absence may reduce detector confidence but cannot hold ingestion
  hostage.
- No authenticated user-trade feed, trading action, private data, or real notification delivery is in scope.
- Slice 003 owns the reusable end-to-end harness. This slice owns source/replay fixtures and ingestion
  assertions contributed to that harness, avoiding a second competing full-pipeline test framework.
