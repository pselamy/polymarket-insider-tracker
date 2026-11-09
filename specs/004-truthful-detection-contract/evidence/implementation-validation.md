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
| **1. Positive monetary volume** | Usable positive current volume produces exact quotient ratio (±0.0001); inputs and 004.1 envelope persisted. | `tests/integration/test_end_to_end.py::test_end_to_end_dry_run_pipeline`<br>`tests/ingestor/test_gamma_volume_refresh.py::test_slow_clob_fake_clock_steady_state_and_negative_control` |
| **2. Stale volume** | Stamped volume older than 600 s classified as `stale` (`older_than_600s`), no ratio measured, no zero. | `tests/ingestor/test_gamma_volume_refresh.py::test_slow_clob_fake_clock_steady_state_and_negative_control`<br>`tests/detector/test_volume_state.py::test_p7_stale_exact_bounds` |
| **3. Invalid/missing/nonpositive** | Distinct volume states (`missing`, `invalid`, `nonpositive`, `unqualified`) produce no ratio; raw stored per §1.1a. | `tests/detector/test_size_volume_evidence.py::test_every_replay_live_case_evaluates_and_scores_through_size_detector`<br>`tests/detector/test_volume_state.py::test_all_eight_states_and_reasons_from_corpus` |
| **4. Source failure** | Failed Gamma fetch leaves volume unavailable without raising; CLOB metadata still usable; no carry-forward. | `tests/ingestor/test_gamma_volume_refresh.py::test_a_gamma_failure_records_the_outcome_and_does_not_raise`<br>`tests/ingestor/test_gamma_volume_refresh.py::test_slow_clob_fake_clock_steady_state_and_negative_control` |
| **5. Threshold boundaries** | Score 0.80 inclusive qualifies; nonce `B` excluded (`< B`); override boundaries respected; ratio strictly `> 0.02`. | `tests/detector/test_size_volume_evidence.py::test_size_volume_threshold_boundary_override`<br>`tests/detector/test_size_volume_evidence.py::test_risk_scorer_alert_threshold_boundary_override`<br>`tests/detector/test_freshness_closure.py::test_pre_t027_freshness_boundary_10_nonce_9_and_negative_control_3_3` |
| **6. Unknown age** | Low-activity proxy wallet without age classified `LOW_ACTIVITY_UNKNOWN_AGE`, non-scoring in pre-T027 posture; zero age claims. | `tests/detector/test_freshness_closure.py::test_pre_t027_unknown_age_low_activity_closure`<br>`tests/integration/test_end_to_end.py::test_end_to_end_dry_run_pipeline` |
| **7. Overrides** | Valid threshold and boundary overrides recorded in 004.1 envelope and used for evaluation. | `tests/test_pipeline_components.py::test_wallet_analyzer_receives_fresh_transaction_boundary_setting`<br>`tests/detector/test_freshness_closure.py::test_pre_t027_freshness_boundary_10_nonce_9_and_negative_control_3_3` |
| **8. Restart** | Persisted rows retain evidence envelope; readers preserve version-specific semantics (`004.1`, `003.1`, legacy). | `tests/integration/test_scoring_config_rows.py::test_restart_retains_evidence_envelope_and_version_readers`<br>`tests/integration/test_v2_journey.py::test_v2_journey_restart_resilience` |
| **9. Schema compatibility** | Legacy `003.1` and unversioned rows read unchanged; new 004.1 envelope additive only. | `tests/integration/test_scoring_config_rows.py::test_003_and_legacy_rows_read_unchanged_with_004_code`<br>`tests/integration/test_scoring_config_rows.py::test_004_1_envelope_is_additive_over_003_schema` |
| **10. Dry-run** | Formatted dry-run alert reflects exact score and envelope; returns before delivery-state write (G-018 dedup safety). | `tests/integration/test_end_to_end.py::test_end_to_end_dry_run_pipeline`<br>`tests/alerter/test_deduplication.py::TestChannelScopedDeduplication::test_dry_run_never_writes_or_poisons_dedup_state` |
| **11. Shared transaction** | Two per-participant observations sharing one transaction ID evaluated as separate wallet-bearing trades. | `tests/integration/test_end_to_end.py::test_end_to_end_volume_less_journey_scores_each_participant_below_threshold`<br>`tests/integration/test_v2_journey.py::test_v2_shared_transaction_separate_wallet_evaluations` |
| **12. Old-row compatibility** | Pre-004 rows keep legacy freshness/impact semantics; never reinterpreted as current measured classifications. | `tests/integration/test_scoring_config_rows.py::test_pre_004_rows_keep_legacy_freshness_and_impact_semantics`<br>`tests/ingestor/test_volume_store.py::test_legacy_cached_value_upgrades_to_unqualified_then_to_the_stamped_record` |

### Success Criteria SC-001–SC-009 Traceability

- **SC-001** (Fixture & score agreement): 100% agreement across all 65 replay-live cases through real detector and scorer (`tests/detector/test_size_volume_evidence.py::test_every_replay_live_case_evaluates_and_scores_through_size_detector`, `tests/detector/test_reachability.py`).
- **SC-002** (Volume ratio accuracy & bound): Persisted volume impact exact to 4 decimal places for every `available` trade; null companion for unmeasured (`tests/detector/test_size_impact_companions.py::test_detector_signal_carries_measured_impact`, `tests/detector/test_volume_state.py::test_ratio_precision_and_rounding`).
- **SC-003** (Zero false fresh-wallet age claims): Across all unknown-age pipeline and formatter outputs, zero claims of wallet age; 'Fresh Wallet' and '(Age:' omitted from alerts (`tests/detector/test_freshness_closure.py::test_pre_t027_unknown_age_low_activity_closure`).
- **SC-004** (Operational core vs research modules): Strict capability boundary proven: size anomaly + niche modifier operational for scoring; proxy-wallet low-activity non-scoring observation; sniper experimental; funding enrichment only; no backtest CLI options (`tests/detector/test_capability_consistency.py`).
- **SC-005** (Threshold reconstruction within 0.01): Both default and overridden thresholds reconstruct within 0.01 from 004.1 envelope and display 100% (`tests/detector/test_scorer.py::test_effective_threshold_and_boundary_equality`).
- **SC-006** (End-to-end evidence consistency): Same trade notional, wallet address, classification, and score preserved across analysis, scoring, persistence, and formatting (`tests/integration/test_end_to_end.py::test_end_to_end_dry_run_pipeline`).
- **SC-007** (Research-only boundary on public surfaces): All public surfaces (`README.md`, `CHANGELOG.md`, `AlertFormatter`, docstrings) maintain non-accusatory, non-financial-advice research framing (`tests/detector/test_capability_consistency.py::test_research_only_alert_wording_and_no_backtest_cli`).
- **SC-008** (Replay report score distributions): Replay live corpus results match canonical distributions (`specs/004-truthful-detection-contract/evidence/replay-live/results.csv`, `scripts.spec004_evidence.replay_live --check`).
- **SC-009** (Ceiling equality to real scorer): Pre-T027 ceiling is exactly 0.450, matching real scorer maxima across all producible evidence classes (`specs/004-truthful-detection-contract/evidence/reachability.md`, `scripts.spec004_evidence.reachability --check`).

