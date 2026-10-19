# Feature Specification: Truthful Detection Contract

**Feature Branch**: `codex/spec-kit-brownfield-adoption`

**Created**: 2026-09-06

**Status**: Proposed for Patrick review; revised 2026-10-19 after the codex/agy second opinions (not approved; open decisions D1–D3 in [decision-packet.md](decision-packet.md)); implementation is not yet authorized

**Input**: User description: "Make live detector inputs, signal semantics, scoring, persisted evidence, and public capability claims match the monitoring behavior that is actually wired and supportable."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Receive an Explainable Core Assessment (Priority: P1)

As a research operator, I can inspect a signal-bearing assessment and understand which observed trade,
wallet evidence, market data, thresholds, weights, and unavailable inputs produced its score and disposition.

**Why this priority**: A research alert is useful only when its score is traceable to real inputs rather
than dormant modules, missing data represented as zero, or contradictory thresholds.

**Independent Test**: Evaluate a table of fixture trades spanning known and unknown wallet age, high and
low nonce, known and unknown daily volume, niche and non-niche markets, and threshold boundaries; verify
the assessment and documentation explain the exact result.

**Acceptance Scenarios**:

1. **Given** current daily volume is available for a market, **When** a trade is analyzed, **Then** the
   size evidence uses the trade's notional value and that volume, records both inputs, and reports the
   calculated impact.
2. **Given** daily volume or book depth is unavailable, **When** a trade is analyzed, **Then** the missing
   input is represented as unavailable rather than as a measured zero.
3. **Given** wallet age is unknown but transaction count is low, **When** the wallet is assessed, **Then**
   the result is identified as a low-activity, age-unknown candidate and never claims it is under 48 hours old.
4. **Given** a score exactly equal to the configured threshold, **When** no delivery duplicate exists,
   **Then** it qualifies consistently in runtime output, persisted evidence, examples, and documentation.

---

### User Story 2 - Distinguish Operational Signals from Research Modules (Priority: P1)

As a maintainer or user, I can tell which detectors affect live scoring, which data is enrichment only,
and which modules are experimental or deferred.

**Why this priority**: Library presence and unit tests do not make a detector part of the live product.
Claims of “ML” or multiple active signals are misleading when the orchestrator never invokes them.

**Independent Test**: Trace one fixture trade through the running pipeline and compare the invoked modules,
assessment schema, alert output, README capability table, tracked skill, and architecture description.

**Acceptance Scenarios**:

1. **Given** the live core pipeline, **When** its risk score is calculated, **Then** only fresh/low-activity
   wallet evidence, size anomaly evidence, and the niche-market factor contribute.
2. **Given** the sniper-cluster library, **When** public capabilities are listed, **Then** it is identified
   as experimental and non-operational until a separately approved specification wires and validates it.
3. **Given** funding-chain tracing, **When** a fresh-wallet candidate is enriched, **Then** funding records
   may be persisted as research context but do not affect the risk score or alert claims.
4. **Given** no executable backtesting workflow, **When** research and changelog behavior are described,
   **Then** assessment storage is described as future-compatible input, not an existing backtest capability.

---

### User Story 3 - Configure One Coherent Scoring Contract (Priority: P2)

As an operator, I can discover and override the active detector thresholds without contradictory defaults,
unreachable bonuses, or examples that imply unimplemented evidence.

**Why this priority**: Threshold and weight changes alter alert behavior and must be reviewable, reproducible,
and attached to each assessment.

**Independent Test**: Compare default and overridden fixture evaluations against configuration summaries,
persisted assessment fields, sample output, README, example environment, and the tracked skill.

**Acceptance Scenarios**:

1. **Given** no detector override, **When** configuration is loaded, **Then** the active alert threshold is
   0.80 everywhere it is displayed or documented.
2. **Given** a valid threshold override, **When** an assessment is created, **Then** the exact effective
   threshold is recorded and used for qualification.
3. **Given** the two operational signal groups plus a niche factor, **When** the score is calculated, **Then**
   only reachable bonus rules are applied and the reported signal count matches the defined groups.
4. **Given** a sample alert or interpretation guide, **When** a reviewer reconstructs its score from the
   shown evidence and documented weights, **Then** the result agrees within normal rounding tolerance.

### Edge Cases

- A zero daily volume is different from unavailable volume and makes ratio interpretation undefined.
- Market metadata can be stale; the assessment must include observation or refresh time where available.
- Wallet transaction count can change between profiling and alert review; evidence needs its observation time.
- An age can be known and older than 48 hours even when transaction count remains low.
- The profiled address is a proxy wallet: a low or zero transaction count on that address is not evidence about
  the participant's history, and the nonce semantics of the address are unverified (FR-007a).
- Transaction count exactly at the configured boundary must use one documented inclusive/exclusive rule.
- A niche market factor is part of size evidence, not a third independently observed detector invocation.
- An override can make a formerly unreachable score qualify; persisted thresholds must preserve that context.
- Funding tracing can fail after a core assessment remains valid; enrichment failure must not be relabeled
  as evidence of suspicious funding.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The supported live core MUST consist of two operational signal groups: fresh/low-activity
  wallet evidence and size-anomaly evidence. Niche-market context MAY modify size evidence but MUST NOT be
  reported as a separately invoked detector.
- **FR-002**: The default alert threshold MUST be 0.80, and every public default, example, configuration
  summary, persisted assessment, and tracked skill MUST agree.
- **FR-003**: Every risk assessment MUST record the effective threshold, score, signal-group count, applicable
  detector factors, data-availability state, and enough source values to reproduce the calculation.
- **FR-004**: A trade's actual notional value and available 24-hour market volume MUST flow into live size
  analysis. The resulting impact MUST be recorded when the volume is positive and current enough to use.
- **FR-004a** *(acquisition freshness)*: The 0–600 s local recency rule MUST hold in steady state under the
  real synchronization cadence, not only in tests. The volume observation is stamped at Gamma response-body
  receipt, stored independently of the slow CLOB crawl, and never overwritten by a CLOB-only path
  (data-model §1.4, research R02a). The spec's arithmetic bound is `S_g + 2·g + w_g <= 600` for the Gamma
  loop; a loop that cannot meet it fails closed (values become `stale`, never wrongly `available`).
- **FR-004b** *(partial acquisition and cache miss)*: A partial Gamma crawl, a failed fetch, a market outside
  the crawl window, a CLOB cache-miss record and a legacy cache record MUST each map to a defined volume state
  and reason (data-model §1.2), keep the stamps of pages that did succeed, and never refresh an older
  observation's time.
- **FR-005**: Unavailable or nonpositive volume and unavailable book depth MUST be represented explicitly;
  they MUST NOT be presented as observed zero impact.
- **FR-005a** *(volume states)*: Every market's volume MUST be in exactly one of eight mutually exclusive states
  (`available`, `unavailable`, `unqualified`, `missing`, `invalid`, `nonpositive`, `invalid_time`, `stale`)
  assigned by the single precedence table in [data-model.md](data-model.md) §1.2. Zero is a `nonpositive`
  value, never `unavailable` and never a ratio. Only `available` yields a ratio. When volume is not
  `available`, niche context may come only from a category recorded from real market metadata (basis
  recorded); a metadata-lookup failure supplies no category evidence (DECISION D3).
- **FR-006**: Book-depth impact MUST NOT contribute to the supported core score until a source and freshness
  contract are separately approved and wired. Existing library support MAY remain non-operational.
- **FR-007**: A newly-created-wallet claim MUST require known age at or below 48 hours, a recorded age basis
  (FR-007b), and transaction count below the configured freshness boundary.
- **FR-007a** *(wallet subject)*: The wallet that is profiled is the trade row's `proxy_wallet` address
  (`TradeEvent.wallet_address`). Evidence MUST record that subject kind (`proxy_wallet`), and every output MUST
  describe the number as the transaction count **of that proxy address**. No output MAY claim it measures the
  participant's total trading history, experience or account age. Whether a proxy address's Polygon nonce tracks
  trading activity at all is **unverified** (no first-party code, test or checked-in evidence); until a
  qualification receipt exists the wallet group MUST NOT be described more strongly than "low transaction
  count on the proxy address".
- **FR-007b** *(age source)*: A known age MUST carry its basis: `polymarket_first_trade` (earliest public
  trade seen for the proxy address), `chain_first_transaction` (qualified indexer), or none. Current main has
  no operational age source (`get_first_transaction` returns `None` for every wallet). Which source, if any,
  the live product uses is DECISION D1 in [decision-packet.md](decision-packet.md); until a source is selected
  and qualified, live age is **always unknown** and the known-age classification is exercised only by injected
  test profiles and is documented as non-operational. Failure to obtain an age is `unknown`, never `fresh`.
- **FR-008**: When age is unknown and transaction count is below the boundary, the detector MAY emit a
  lower-certainty low-activity-wallet candidate, but all outputs MUST label age as unknown and MUST NOT state
  or imply a less-than-48-hour age. Its confidence MUST be **exactly 0.50** (neither lower nor higher,
  independent of nonce value and trade size), its only factor is the low-activity factor, and it MUST NOT
  receive an age-based, brand-new (nonce 0), very-young or large-trade addition. This is the canonical
  unknown-age rule; [data-model.md](data-model.md) §2 restates it as the single normative table.
- **FR-009**: The transaction-count boundary MUST be strictly fewer than 5 prior transactions for the default
  contract, matching public wording and eliminating the current competing inclusive rules.
- **FR-010**: Missing wallet evidence MUST reduce or remove its contribution; failure to retrieve evidence
  MUST NOT be interpreted as a fresh-wallet fact.
- **FR-011**: The 3+ signal bonus MUST be removed from the supported scoring contract while only two signal
  groups are operational. The reachable two-group bonus MAY remain if its exact behavior is documented.
- **FR-012**: The sniper-cluster module MUST be labeled experimental and non-operational in public and
  developer documentation; it MUST NOT contribute to live scores in this slice.
- **FR-013**: Funding-chain data MUST be labeled best-effort enrichment for fresh/low-activity candidates.
  It MAY be persisted separately but MUST NOT affect score, signal count, or alert wording in this slice.
- **FR-014**: Backtesting and calibration MUST be labeled deferred. No document MAY claim an executable
  backtest workflow until a separate approved specification and evidence exist.
- **FR-015**: Table-driven regression tests MUST cover every threshold boundary, missing-data state, signal
  combination, score bonus, and override in the supported contract.
- **FR-016**: A deterministic pipeline-level test MUST prove the same source values and availability flags
  appear in analysis, scoring, persistence, and formatted dry-run output.
- **FR-017**: Alerts and documentation MUST state that heuristic signals support research and are neither
  proof of illegal activity nor financial advice.
- **FR-018**: README, changelog clarification, sample alert, example environment, architecture text, docstrings,
  and tracked skill MUST distinguish operational core, enrichment, experimental modules, and deferred work.
- **FR-019**: Before the scoring change is approved for implementation, a fixed deterministic replay corpus
  MUST define at least 32 cases spanning every operational signal combination, boundary, and missing-data
  state. The old and proposed contracts MUST both be evaluated against it and their score distributions and
  qualification counts recorded; the comparison is behavioral evidence, not a calibration claim.
- **FR-020**: This slice MUST use the assessment schema and reusable end-to-end harness owned by slice 003,
  adding detector evidence assertions without a second migration or competing pipeline harness.
- **FR-021**: Every publicly returned participant observation selected by slice 001 MUST be evaluated as its
  own wallet-bearing trade. Multiple participants in one transaction MUST NOT be collapsed before profiling.
- **FR-022** *(reachability disclosure)*: Every public description of the alert threshold MUST state which
  evidence classes can reach it. The maximum attainable score per evidence class is published in
  [research.md](research.md) R10 / `evidence/reachability.md`, and a regression test MUST recompute those
  ceilings from the real scorer so that a weight, cap, bonus or threshold change cannot silently change
  what is reachable. With unknown age the ceiling is 0.780 for every trade size.

### Key Entities

- **Wallet Evidence**: Subject address kind (`proxy_wallet`), observed transaction count of that address,
  known or unknown first-seen time and age with its basis, balances, observation time, retrieval status, and
  resulting fresh/low-activity classification.
- **Market Evidence**: Market identity and category, positive known or unavailable daily volume, unavailable
  book depth for this slice, data freshness, and resulting niche context.
- **Operational Signal Group**: One live detector result with confidence and factors that may contribute to
  the score; exactly wallet evidence and size anomaly are supported here.
- **Risk Assessment**: The trade, source evidence, availability flags, effective weights and threshold,
  signal-group count, bonus, final score, qualification, and delivery disposition.
- **Capability Status**: Operational, enrichment-only, experimental/non-operational, or deferred, with the
  owning specification and evidence expectations.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: For the complete table of threshold and missing-data fixtures, 100% of calculated scores,
  signal counts, availability states, and qualification decisions match the documented contract.
- **SC-002**: For every deterministic trade with positive current daily volume, the persisted volume impact
  equals trade notional divided by that volume within 0.0001; unknown inputs are never serialized as measured zero.
- **SC-003**: Across all unknown-age fixture and pipeline outputs, zero statements claim or imply that the
  wallet is under 48 hours old.
- **SC-004**: Deterministic capability tests establish which detectors and scoring inputs are operational
  and ensure unsupported capabilities remain absent from runtime output. Contributor documentation states
  those tested capabilities without being reparsed by a bespoke contract checker.
- **SC-005**: Default and overridden threshold examples reconstruct to the persisted result within 0.01 and
  display the exact effective threshold in 100% of cases.
- **SC-006**: The deterministic end-to-end scenario preserves the same trade notional, wallet transaction
  count/age state, daily volume state, detector factors, score, and threshold from evaluation through persistence.
- **SC-007**: Every public interpretation surface retains the research-only, non-accusatory, non-financial-advice boundary.
- **SC-008**: The checked-in 32-or-more-case replay report records old and proposed score distributions,
  threshold-qualification counts, and per-case deltas; every delta is attributable to a named requirement,
  and any unanticipated change blocks implementation approval.
- **SC-009**: The published maximum score of every evidence class equals the maximum the real scorer produces
  over the reachability grid, and the live-realistic replay (unknown age, every volume state) is checked in
  beside the original corpus with a reproducible command.

## Assumptions

- The current 0.80 runtime default is retained as the operational default because it is already the code and
  changelog contract; this slice does not claim the threshold is statistically calibrated.
- “Fresh wallet” becomes a precise known-age classification. Low transaction count with unknown age remains
  useful but is labeled a lower-certainty “low-activity wallet, age unknown” candidate with confidence exactly
  0.50 and no age, brand-new, very-young or large-trade bonus.
  Because no age source is operational (FR-007b), this is the **only** wallet classification the live
  runtime can produce until decision D1 selects and qualifies a source.
- The default transaction-count rule is fewer than 5 prior transactions, following the README wording. This
  intentionally resolves the current mix of `< 5` and `<= 5` behavior.
- Available positive daily volume is part of the core size contract. Book-depth scoring is disabled and
  documented as unavailable until a separately approved source/freshness design exists.
- Funding tracing remains best-effort stored enrichment only. Sniper clustering remains an experimental
  library. Neither affects live risk scores in this slice.
- Backtesting, label collection, outcome evaluation, new detector algorithms, and claims of predictive
  accuracy are outside scope and require separate approved specifications.
- Slice 003 lands the assessment schema and reusable end-to-end harness first. Slice 004 adds its assertions
  to those contracts and does not own a second migration or harness.
- Slice 003 must also resolve the failing default Polygon endpoint before this slice can claim the default
  wallet-evidence workflow is usable.

## Dependencies and Non-Goals

**Dependencies (all landed on main `1bdccbc`; revised 2026-10-19)**

- **Slice 001 Data API V2 ingestion is the live trade contract**, not a pending separate concern: the
  default trades URL is the official V2 endpoint (`d647adf`) and V2 rows decode into `TradeEvent`
  (`ingestor/trade_row_v2.py`). Slice 004 consumes it and does not change acquisition, cadence,
  cursors or loss handling. Its journey suites are therefore in scope as test obligations
  (tasks T021–T026).
- **The wallet subject is the V2 `proxy_wallet`.** `TradeEvent.wallet_address` is the lowercased
  `proxyWallet` of the public trade row (`ingestor/trade_row_v2.py:27`, `ingestor/trade_rows.py:169`),
  and the wallet profiler queries exactly that address (`profiler/analyzer.py:168`). Whether that
  address's Polygon transaction count or age measures the participant's experience is **unverified**
  (no first-party code, test or checked-in evidence); see FR-007a.
- Slice 003 owns the assessment schema, `scoring_config` column and dry-run safety (G-018).

**Open decisions**: FR-005a (lookup failure), FR-007b/FR-008 (live age unknown, exactly 0.50) and the 0.80 default are written for the recommended defaults of D1–D3 in [decision-packet.md](decision-packet.md); they are not settled until Patrick decides and change with the chosen option.

**Non-goals**: trading or execution, financial advice, backtesting or calibration, book-depth scoring,
funding or sniper scoring, changes to the V2 acquisition path or its cadence, a new database migration,
and any Polymarket credential. The V2 migration itself is complete and out of scope; only its effect on
wallet identity and on the journey tests is in scope.
