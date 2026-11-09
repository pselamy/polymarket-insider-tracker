# Implementation-Validation Evidence (planning consistency)

**Date**: 2026-10-17. **Scope**: planning-only diff. No product code changed; no tests run
beyond document/managed checks. Implementation and human validation remain pending.

## What this packet proves

- Prior Phase0 artifacts hydrated byte-identically from `polymarket-004-preparation-c64a73b`
  (7 files: research, plan, clarification, replay.md, corpus, old-proposed.csv, summary).
- Replay file hashes (actual): corpus `8dfddeb95e1c3…9967531a86e`, canonical CSV
  `63ecab8b0116…eac44d`, summary `41177d987640…c29715`. Corpus: 114 cases / 228 scores /
  63 deltas; default-runtime subset 99 cases with 0 → 15 qualification; 9 override cases
  separated. Behavioral comparison only — not calibration or implementation proof.
- Scoring runtime verified unchanged between replay base and this base (no `src/` diff
  `523b0a3..46e4192`), so bounded replay conclusions are reusable within scope.
- R01 unit-evidence blocker resolved for a proposed contract by independent review
  (`outputs/polymarket-gamma-unit-primary-review.md`, SHA256 `0b9555816e6d7…62ceec279`);
  archived-example limits preserved; no unavailable-everywhere workaround.
- Managed sequence followed: clarify (existing scan) → plan → reviewer-owned domain
  checklist (new, unchecked) → tasks → analyze (report below) → human validation pending.
  No managed helper was missing; no fabricated PASS; reviewer/human criteria unmarked.

## Analyze (final-artifact read-only analysis, 2026-10-17 repair)

Managed `speckit-analyze` LLM workflow applied to the repaired final artifacts
(`spec.md`, `plan.md`, `tasks.md`, `data-model.md`, `contracts/`, `research.md`,
`quickstart.md`, checklists). Findings from the frozen-15c independent and domain reviews
(D1/D2/N1/N2, P1/P2 items) are resolved above; the failed first piped prerequisite (missing
tasks error, run before tasks existed) is retained as historical evidence, superseded by the
later direct document-check exit 0 on frozen 15c and by this final-artifact analysis.

### Coverage: 29 requirements × task / test / evidence receipt

“Proposed” = written future obligation, not implementation or a passing test.

| ID | Implementation task(s) | Named test / scenario obligation | Future evidence receipt |
|---|---|---|---|
| FR-001 | T009d, T013 | `tests/detector/` two-group tables; capability tests | replay groups + scorer proof (T019a) |
| FR-002 | T009a, T015a | scorer equality test, 0.80 everywhere | persisted threshold display (SC-005) |
| FR-003 | T011, T015a | evidence-envelope unit + reconstruction test | per-assessment envelope rows |
| FR-004 | T011, T012 | pipeline + ingestor usable-volume tests | R01 qualification receipt (T018) |
| FR-005 | T011, T012, T016 | unavailable-state tables; nullable-companion serialization test | no-zero audit across surfaces |
| FR-006 | T009d, T013 | book-unavailable core test | capability proof (T019a) |
| FR-007 | T009b, T009d | known-age gate tables incl. override counterexamples | replay + new boundary tests |
| FR-008 | T009c | joint unknown-age regression (confidence/factors/JSON/storage/render) | SC-003 zero-claim audit |
| FR-009 | T009a, T009b, T016 | default/equality/override + alias-conflict tests | override reconstruction (SC-005) |
| FR-010 | T009c, T011 | retrieval-failed / not-requested / nonqualifying tables | durable skip-row proof |
| FR-011 | T015a, T015b | two-group-bonus-only test; 3+ branch removal | scorer proof (T019a) |
| FR-012 | T013 | sniper-experimental capability test | SC-004 proof |
| FR-013 | T013 | funding-enrichment-only test | persistence separation proof |
| FR-014 | T013 | no-backtest-claim test | docs audit |
| FR-015 | T009d, T015a | every-boundary table suite | SC-001 fixture agreement |
| FR-016 | T010 | deterministic pipeline test (analysis→score→persist→dry-run) | SC-006 same-evidence proof |
| FR-017 | T014 | wording audit across formatters | SC-007 surface audit |
| FR-018 | T013, T014 | capability-surface tests | SC-004/007 proof |
| FR-019 | T002, T019a | fixed 114-case corpus | corpus/CSV/summary hashes above |
| FR-020 | T006, T017, T020 | envelope persistence + old-row compatibility + parity | 003-harness proof, no 2nd migration |
| FR-021 | T010 | shared-transaction two-participant pipeline assertion | journey scenario 11 |
| FR-022 | T028 | reachability regression recomputing both posture tables from the real scorer | regenerated `evidence/reachability.md` equals the committed one |
| FR-022a | T029 (with T028) | alert-path report over producible classes: default 0.80 pre-T027 `unreachable` at 0.450; 0.45 `reachable`; post-T027 0.780 / 0.972; log record, `Alert Path:` line, `/health` `alert_path` without verdict change | startup log, `--config-check` output and `/health` body captured on the shipped configuration |
| FR-022b | T030 (only if D2 = R5) | `release_posture = assessment_only`: no format/dispatch call at any threshold, dry-run or real; disposition `assessment_only`; FR-022a status `assessment_only` | table-driven suppression test output and the real-Redis journey receipt |
| SC-001 | T009d, T019a | full fixture agreement | 100% match record |
| SC-002 | T011, T012, T016 | ratio ±0.0001 test; null-never-zero | persisted impact audit |
| SC-003 | T009c | unknown-age output audit | zero fresh-claim record |
| SC-004 | T013 | capability tests | operational-only proof |
| SC-005 | T015a | threshold reconstruction ±0.01, 100% display | example audit |
| SC-006 | T010, T017 | end-to-end same-evidence test | persistence proof |
| SC-007 | T013, T014 | public-surface audit | wording proof |
| SC-008 | T002, T019a | 114-case / 63-delta attribution | hashes + attribution above |
| SC-009 | T028 | published ceilings equal the scorer's maxima | reachability tables regenerated by the shared ceiling function |
| SC-010 | T030 (only if D2 = R5) | zero sends, zero dedup keys, `alerts_sent == 0` at 0.80/0.45/0.40/0.000 in both modes | same receipt as FR-022b |

### Story acceptance scenarios (12)

US1-1..4 → T011/T012 (scenario 1), T012/T016 (scenario 2), T009c (scenario 3),
T015a/T010 (scenario 4, journey 5). US2-1..4 → T013 (scenarios 1–2, 4), T013/T014
(scenario 3). US3-1..4 → T009a/T015a (scenarios 1–2), T015a (scenario 3), T015a/T017
(scenario 4). Edge cases (zero-vs-unavailable, stale metadata, nonce-at-boundary,
niche-as-modifier, override-qualifies, funding-failure) → T009d/T010/T012/T015a/T019a.

### Ambiguity / inconsistency / duplication / constitution findings

| ID | Category | Severity | Status |
|---|---|---|---|
| F-D1 | Inconsistency: hardcoded nonce 5 vs effective override | P1 blocking | Resolved: effective `B` everywhere (data-model §2, plan, research R04, contracts) |
| F-D2 | Inconsistency: new classification vs legacy freshness-flag path | P1 blocking | Resolved: canonical rule §2a + legacy exception + joint regression T009c |
| F-N1 | Ambiguity: “stay nullable” vs required-float baseline | P2 | Resolved: exact pins + additive companion migration (contract §3) |
| F-N2 | Ambiguity: raw vs usable volume | P2 | Resolved: `volume24hr_raw` vs `daily_volume` (data-model §1) |
| F-P1a | Coverage: implementation graph a test list | P1 blocking | Resolved: named files/entry points/order (tasks Phases 3–6) |
| F-P1b | Coverage: FR021 nominal reference | P1 blocking | Resolved: T010 pipeline assertion + journey 11 |
| F-P2 | Consistency: Gamma polling claim | P2 | Resolved: 001 trade polling vs 300s/600s metadata split |
| F-LINK | Consistency: `../003-…` resolves inside 004 | P2 | Resolved: `../../003-safe-observable-operation/…` |
| F-MVP | Ambiguity: “MVP = planning packet” | P2 | Resolved: planning-contract vs product-MVP language |
| F-PAR | Inconsistency: T012 self-dependency | LOW | Resolved: corrected dependency/parallel rules |

Unmapped tasks: none — T001–T020 all map above. Metrics: 21 FR + 8 SC + 12 scenarios =
41 obligations, each with task + test + receipt. No CRITICAL findings remain. No
constitution exception. All 23 reviewer-owned human checklist items remain unchecked;
implementation, tests, and human validation remain pending. Planning consistency is not
operational acceptance.

## Human validation still required (explicitly pending)

- Approve/reject the proposed 004.1 implementation contract (this packet's decision).
- Reviewer-owned domain checklist (23 items, all unchecked).
- Future implementation, full test/storage/pipeline evidence, and convergence.
- ~~Existing separate v2 migration validation request (untouched by this packet).~~ Superseded
  2026-10-19: V2 is the live ingestion contract on main, no longer a separate pending request; its
  effect on wallet identity and the journey suites is part of this slice (spec Dependencies, tasks
  T021–T026, `evidence/revision-2026-10-19.md` F24).

## Traceability

Full per-requirement table lives in §Analyze above (each FR/SC: implementation task(s),
named test/scenario obligation, future evidence receipt). Summary: FR-001–011 →
T009a–d/T011/T012/T013/T015a/T016; FR-012–014 → T013/T014; FR-015/016 → T009d/T010;
FR-017/018 → T014; FR-019 → T002/T019a; FR-020 → T006/T017/T020; FR-021 → T010
(shared-transaction pipeline assertion); SC-001–008 → T009d/T010/T013/T015a/T017/T019a;
12 acceptance scenarios → quickstart journeys 1–12. 002→001→003→004/Relentless
obligations preserved (001 observation boundary, 003 schema/harness reuse, no second
migration); this packet is not reduced to a CI or planning-only goal.

## Addendum 2026-10-19 (appended; the text above is historical and was not rewritten)

- Source pins refreshed to main `f2e2167`: see `evidence/source-refresh-f2e2167.md`; the check is
  `uv run python -m scripts.spec004_evidence.check_source_hashes` (reports 3 of 7
  recorded hashes drifting, all refactor-only).
- The "scoring runtime unchanged" statement above still holds for `scorer.py` and `pipeline.py`
  (byte-identical) and holds semantically for the three refactored detector files.
- New V2 journey obligations: tasks T021–T026. The earlier statement that every FR/SC maps to a
  task remains true; FR-016/FR-021/SC-006 now additionally map to T022 and T026.
- This addendum is not approval, and the 23-item reviewer checklist is still entirely unchecked.
- T012 was split into T012a–d (stamp/aggregation, decoupled loop and cache-miss path, serialization/legacy
  records, freshness evidence) and no longer depends on T011; FR-004a/FR-004b → T012a–d; FR-022/SC-009 →
  T028; wallet-subject qualification → T027.
- New requirement mapping (2026-10-19): FR-004a/FR-004b → T012a–d; FR-005a → T009d, T012c; FR-007a/FR-007b →
  T009b, T009c, T027; FR-022 and SC-009 → T028; V2 journey suites → T021–T026. As before, a mapping is a
  written obligation, not a passing test.
- Round 4 (2026-10-21, N2): T015 and T019 are split at the T027 gate. In the coverage table and summary
  above, T015/T019 now read **T015a/T019a** (the pre-T027 release evidence, not gated by T027);
  **T015b/T019b** repeat FR-011 and SC-001–008 in the post-T027 posture (`004.2` rows) in the
  gate-opening change. SC-002 is now computable for every `available` row (data-model §1.1c bound and the
  exact ratio of §3; round-4 N3).

## Pre-T027 Implementation Validation Receipt (T012d, T019a)

**Execution Date**: 2026-11-09. **Post-implementation validation** of the truthful detection contract (Spec 004) in the pre-T027 posture.

- **Gap Register Convergence**: `specs/audit/gap-register.md` rows `G-022`–`G-028` are updated with their Slice 004 pre-T027 dispositions per Constitution §Development Workflow 8.

### Automated Suite Results

1. **Component Test Suites** (`tests/detector/`, `tests/ingestor/`, `tests/alerter/`, `tests/profiler/`):
   - Command: `uv run pytest tests/detector/ tests/ingestor/ tests/alerter/ tests/profiler/ -q`
   - Result: **2,251 passed**, 40 warnings in 45.99s. Exit status: `0`.
2. **Integration & Journey Suites** (`tests/integration/`):
   - Command: `uv run pytest tests/integration/test_end_to_end.py tests/integration/test_v2_journey.py tests/integration/test_v2_default_journey.py tests/integration/test_scoring_config_rows.py -q`
   - Result: **15 passed**, 1 warning in 1.07s. Exit status: `0`.
3. **Spec 004 Evidence Verification Checks**:
   - `uv run python -m scripts.spec004_evidence.check_envelope`: `ok` (exit `0`)
   - `uv run python -m scripts.spec004_evidence.check_source_hashes`: all 7 hashes match (exit `0`)
   - `uv run python -m scripts.spec004_evidence.reachability --check specs/004-truthful-detection-contract/evidence/reachability.md`: clean match (exit `0`)
   - `uv run python -m scripts.spec004_evidence.replay_live --check specs/004-truthful-detection-contract/evidence/replay-live/results.csv`: clean match (exit `0`)

### Quickstart Journey Scenarios 1–12 Mapping

| Scenario | Obligation & Acceptance | Passing Test Reference |
|---|---|---|
| **1. Positive monetary volume** | Usable positive current volume produces exact quotient ratio (±0.0001); inputs and 004.1 envelope persisted. | `tests/integration/test_end_to_end.py::TestEndToEndPipelineHarness::test_end_to_end_dry_run_pipeline`<br>`tests/ingestor/test_gamma_volume_refresh.py::test_slow_clob_fake_clock_steady_state_and_negative_control` |
| **2. Stale volume** | Stamped volume older than 600 s classified as `stale` (`older_than_600s`), no ratio measured, no zero. | `tests/detector/test_volume_state.py::TestPrecedence::test_the_recency_edges_are_inclusive[600-available]`<br>`tests/detector/test_volume_state.py::TestPrecedence::test_the_recency_edges_are_inclusive[601-stale]`<br>`tests/detector/test_size_volume_evidence.py::test_a_stale_or_unqualified_volume_gives_no_size_signal`<br>`tests/ingestor/test_gamma_volume_refresh.py::test_slow_clob_fake_clock_steady_state_and_negative_control` |
| **3. Invalid/missing/nonpositive** | Distinct volume states (`missing`, `invalid`, `nonpositive`, `unqualified`) produce no ratio; raw stored per §1.1a. | `tests/detector/test_volume_state.py::test_the_replay_corpus_is_the_whole_table`<br>`tests/detector/test_size_volume_evidence.py::test_every_replay_live_case_evaluates_and_scores_through_size_detector[live-nonpositive-zero]`<br>`tests/detector/test_size_volume_evidence.py::test_every_replay_live_case_evaluates_and_scores_through_size_detector[live-invalid-string]`<br>`tests/detector/test_size_volume_evidence.py::test_every_replay_live_case_evaluates_and_scores_through_size_detector[live-missing-null]` |
| **4. Source failure** | Failed Gamma fetch with **no prior observation** leaves volume `unavailable` (`gamma_fetch_failed`) without raising; CLOB metadata still usable. Failed fetch **with an existing observation** retains it unchanged: a failure never writes a record or refreshes its `acquired_at`; the observation is `available` while its age is ≤ 600 s and `stale` (`older_than_600s`) after, never a measured zero. | `tests/ingestor/test_gamma_volume_refresh.py::test_a_gamma_failure_records_the_outcome_and_does_not_raise`<br>`tests/ingestor/test_volume_store.py::test_failed_crawl_keeps_the_older_observation`<br>`tests/ingestor/test_volume_store.py::test_failed_crawl_keeps_prior_observation_available_until_600s_then_stale`<br>`tests/ingestor/test_volume_store.py::test_unreadable_volume_store_leaves_the_clob_metadata_usable` |
| **5. Threshold boundaries** | Score 0.80 inclusive qualifies; nonce `B` excluded (`< B`); override boundaries respected; ratio strictly `> 0.02`. | `tests/detector/test_size_volume_evidence.py::test_size_volume_threshold_boundary_override[boundary_500_strict_false]`<br>`tests/detector/test_size_volume_evidence.py::test_size_volume_threshold_boundary_override[boundary_501_strict_true]`<br>`tests/detector/test_size_volume_evidence.py::test_risk_scorer_alert_threshold_boundary_override[threshold_450_inclusive_true]`<br>`tests/detector/test_size_volume_evidence.py::test_risk_scorer_alert_threshold_boundary_override[threshold_451_false]`<br>`tests/detector/test_freshness_closure.py::test_boundary_10_nonce_9_override_mismatch_joint_regression[24.0-fresh-chain_first_transaction]`<br>`tests/detector/test_freshness_closure.py::test_negative_control_transaction_override_3_3`<br>`tests/detector/test_fresh_wallet.py::TestFreshWalletDetectorAnalyze::test_wallet_at_age_threshold` |
| **6. Unknown age** | Low-activity proxy wallet without age classified `LOW_ACTIVITY_UNKNOWN_AGE`, non-scoring in shipped pre-T027 `004.1` envelope (`wallet_group_status = unqualified_pre_T027`, `confidence: null`, `factors: {}`, `fresh_wallet: "0"`, zero age bonus, normalized across `AlertFormatter`/persisted DTOs under `data-model.md` §2a without claiming wallet age; exact-0.50 scoring rule deferred to post-T027 `T009c`/`O6`). | `tests/detector/test_freshness_closure.py::test_unknown_age_low_activity_joint_regression_pre_t027`<br>`tests/detector/test_freshness_closure.py::test_boundary_10_nonce_9_override_mismatch_joint_regression[None-low_activity_unknown_age-none]` |
| **7. Overrides** | Valid threshold and boundary overrides recorded in 004.1 envelope and used for evaluation. | `tests/test_pipeline_components.py::test_build_components_wires_detector_settings`<br>`tests/detector/test_fresh_wallet.py::TestFreshTransactionBoundaryConstructorContract::test_both_given_conflicting_pair_raises_value_error`<br>`tests/detector/test_size_volume_evidence.py::test_size_volume_threshold_boundary_override[boundary_500_strict_false]`<br>`tests/detector/test_size_volume_evidence.py::test_risk_scorer_alert_threshold_boundary_override[threshold_450_inclusive_true]` |
| **8. Restart** | Persisted rows retain evidence envelope; readers preserve version-specific semantics (`004.1`, `003.1`, legacy). | `tests/integration/test_scoring_config_rows.py::test_restart_and_legacy_003_004_row_compatibility_preserve_version_semantics`<br>`tests/integration/test_scoring_config_rows.py::test_a_scored_row_is_a_004_1_document_whose_threshold_reconstructs[0.8-0.800-False]`<br>`tests/integration/test_scoring_config_rows.py::test_a_scored_row_is_a_004_1_document_whose_threshold_reconstructs[0.4-0.400-True]` |
| **9. Schema compatibility** | Legacy `003.1` and unversioned rows read unchanged; new 004.1 envelope additive only. | `tests/integration/test_scoring_config_rows.py::test_restart_and_legacy_003_004_row_compatibility_preserve_version_semantics`<br>`tests/integration/test_scoring_config_rows.py::test_a_003_1_row_reads_back_unchanged`<br>`tests/detector/test_scoring_config.py::TestScorerOutputUnchanged::test_003_1_row_reads_back_and_reserializes_byte_identically`<br>`tests/detector/test_scoring_config.py::TestDispatch::test_legacy_row_has_no_configuration` |
| **10. Dry-run** | Formatted dry-run alert reflects exact score and envelope; returns before delivery-state write (G-018 dedup safety). | `tests/alerter/test_deduplication.py::TestChannelScopedDeduplication::test_dry_run_makes_zero_calls_and_writes_zero_keys`<br>`tests/integration/test_end_to_end.py::TestEndToEndPipelineHarness::test_end_to_end_dry_run_pipeline` |
| **11. Shared transaction** | Two per-participant observations sharing one transaction ID evaluated as separate wallet-bearing trades. | `tests/integration/test_end_to_end.py::TestEndToEndPipelineHarness::test_end_to_end_volume_less_journey_scores_each_participant_below_threshold`<br>`tests/ingestor/test_trade_row_v2.py::TestV2Decoding::test_maker_and_taker_rows_keep_distinct_composite_identities`<br>`tests/integration/test_v2_journey.py::test_end_to_end_dry_run_pipeline_v2` |
| **12. Old-row compatibility** | Pre-004 rows keep legacy freshness/impact semantics; never reinterpreted as current measured classifications. | `tests/integration/test_scoring_config_rows.py::test_restart_and_legacy_003_004_row_compatibility_preserve_version_semantics`<br>`tests/integration/test_scoring_config_rows.py::test_a_003_1_row_reads_back_unchanged`<br>`tests/ingestor/test_volume_store.py::test_legacy_cached_value_upgrades_to_unqualified_then_to_the_stamped_record` |

### Success Criteria SC-001–SC-009 Traceability

- **SC-001** (Fixture & score agreement): all 65 replay-live cases agree with the expected corpus through the real `SizeAnomalyDetector` and `RiskScorer`, on the asserted fields only: volume state and reason, size confidence, signal-group count, and pre-T027 weighted score (`tests/detector/test_size_volume_evidence.py::test_every_replay_live_case_evaluates_and_scores_through_size_detector`, parametrized over all 65 cases). Agreement on other envelope fields is not claimed here.
- **SC-002** (Volume ratio accuracy & bound): Persisted volume impact exact to 4 decimal places for every `available` trade; null companion for unmeasured (`tests/detector/test_size_impact_companions.py::TestSizeAnomalyDetectorEmitsCompanions::test_detector_signal_carries_measured_impact`, `tests/detector/test_volume_state.py::TestImpact::test_ratio_is_the_exact_quotient_rounded_once_half_even`).
- **SC-003 & SC-004** (Zero false fresh-wallet age claims & Operational core vs research modules): Across all unknown-age pipeline and formatter outputs, zero claims of wallet age; strict capability boundary (`tests/detector/test_freshness_closure.py::test_unknown_age_low_activity_joint_regression_pre_t027`, `tests/detector/test_capability_consistency.py::test_operational_core_only_and_book_depth_non_operational`, `tests/detector/test_capability_consistency.py::test_sniper_module_experimental_and_non_operational_in_live_pipeline`, `tests/detector/test_capability_consistency.py::test_funding_tracing_is_best_effort_enrichment_only`).
- **SC-005** (Threshold reconstruction within 0.01): **Reconstruction: proven.** The default (0.80) and override (0.40) thresholds reconstruct exactly from the persisted 004.1 row (`tests/integration/test_scoring_config_rows.py::test_a_scored_row_is_a_004_1_document_whose_threshold_reconstructs[0.8-0.800-False]`, `tests/integration/test_scoring_config_rows.py::test_a_scored_row_is_a_004_1_document_whose_threshold_reconstructs[0.4-0.400-True]`). **Display (100%): INCOMPLETE.** No test proves the effective threshold is rendered on any alert surface, and `AlertFormatter` does not render it. The T019a completion claim therefore excludes the SC-005 display requirement until threshold rendering and its assertions exist.
- **SC-006** (End-to-end evidence consistency): Same trade notional, wallet address, classification, and score preserved across analysis, scoring, persistence, and formatting (`tests/detector/test_freshness_closure.py::test_unknown_age_low_activity_joint_regression_pre_t027`, `tests/integration/test_end_to_end.py::TestEndToEndPipelineHarness::test_end_to_end_dry_run_pipeline`, `tests/integration/test_scoring_config_rows.py::test_the_row_records_the_wallet_as_non_scoring_evidence_and_the_volume_state`).
- **SC-007** (Research-only boundary on public surfaces): All public surfaces maintain non-accusatory, non-financial-advice research framing (`tests/detector/test_capability_consistency.py::test_no_executable_backtest_runtime_claims_and_research_only_alert_wording`).
- **SC-008** (Replay report score distributions): Replay live corpus results match canonical distributions (`specs/004-truthful-detection-contract/evidence/replay-live/results.csv`, `scripts.spec004_evidence.replay_live --check`).
- **SC-009** (Ceiling equality to real scorer): Pre-T027 ceiling is exactly 0.450, matching real scorer maxima across all producible evidence classes (`specs/004-truthful-detection-contract/evidence/reachability.md`, `scripts.spec004_evidence.reachability --check`).
- **Ratchet**: Verified by `tests/detector/test_capability_consistency.py::test_implementation_validation_cited_test_nodes_exist_in_pytest_collection`.


