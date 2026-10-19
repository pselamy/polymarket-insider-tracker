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
| FR-001 | T009d, T013 | `tests/detector/` two-group tables; capability tests | replay groups + scorer proof (T019) |
| FR-002 | T009a, T015 | scorer equality test, 0.80 everywhere | persisted threshold display (SC-005) |
| FR-003 | T011, T015 | evidence-envelope unit + reconstruction test | per-assessment envelope rows |
| FR-004 | T011, T012 | pipeline + ingestor usable-volume tests | R01 qualification receipt (T018) |
| FR-005 | T011, T012, T016 | unavailable-state tables; nullable-companion serialization test | no-zero audit across surfaces |
| FR-006 | T009d, T013 | book-unavailable core test | capability proof (T019) |
| FR-007 | T009b, T009d | known-age gate tables incl. override counterexamples | replay + new boundary tests |
| FR-008 | T009c | joint unknown-age regression (confidence/factors/JSON/storage/render) | SC-003 zero-claim audit |
| FR-009 | T009a, T009b, T016 | default/equality/override + alias-conflict tests | override reconstruction (SC-005) |
| FR-010 | T009c, T011 | retrieval-failed / not-requested / nonqualifying tables | durable skip-row proof |
| FR-011 | T015 | two-group-bonus-only test; 3+ branch removal | scorer proof (T019) |
| FR-012 | T013 | sniper-experimental capability test | SC-004 proof |
| FR-013 | T013 | funding-enrichment-only test | persistence separation proof |
| FR-014 | T013 | no-backtest-claim test | docs audit |
| FR-015 | T009d, T015 | every-boundary table suite | SC-001 fixture agreement |
| FR-016 | T010 | deterministic pipeline test (analysis→score→persist→dry-run) | SC-006 same-evidence proof |
| FR-017 | T014 | wording audit across formatters | SC-007 surface audit |
| FR-018 | T013, T014 | capability-surface tests | SC-004/007 proof |
| FR-019 | T002, T019 | fixed 114-case corpus | corpus/CSV/summary hashes above |
| FR-020 | T006, T017, T020 | envelope persistence + old-row compatibility + parity | 003-harness proof, no 2nd migration |
| FR-021 | T010 | shared-transaction two-participant pipeline assertion | journey scenario 11 |
| SC-001 | T009d, T019 | full fixture agreement | 100% match record |
| SC-002 | T011, T012, T016 | ratio ±0.0001 test; null-never-zero | persisted impact audit |
| SC-003 | T009c | unknown-age output audit | zero fresh-claim record |
| SC-004 | T013 | capability tests | operational-only proof |
| SC-005 | T015 | threshold reconstruction ±0.01, 100% display | example audit |
| SC-006 | T010, T017 | end-to-end same-evidence test | persistence proof |
| SC-007 | T013, T014 | public-surface audit | wording proof |
| SC-008 | T002, T019 | 114-case / 63-delta attribution | hashes + attribution above |

### Story acceptance scenarios (12)

US1-1..4 → T011/T012 (scenario 1), T012/T016 (scenario 2), T009c (scenario 3),
T015/T010 (scenario 4, journey 5). US2-1..4 → T013 (scenarios 1–2, 4), T013/T014
(scenario 3). US3-1..4 → T009a/T015 (scenarios 1–2), T015 (scenario 3), T015/T017
(scenario 4). Edge cases (zero-vs-unavailable, stale metadata, nonce-at-boundary,
niche-as-modifier, override-qualifies, funding-failure) → T009d/T010/T012/T015/T019.

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
T009a–d/T011/T012/T013/T015/T016; FR-012–014 → T013/T014; FR-015/016 → T009d/T010;
FR-017/018 → T014; FR-019 → T002/T019; FR-020 → T006/T017/T020; FR-021 → T010
(shared-transaction pipeline assertion); SC-001–008 → T009d/T010/T013/T015/T017/T019;
12 acceptance scenarios → quickstart journeys 1–12. 002→001→003→004/Relentless
obligations preserved (001 observation boundary, 003 schema/harness reuse, no second
migration); this packet is not reduced to a CI or planning-only goal.

## Addendum 2026-10-19 (appended; the text above is historical and was not rewritten)

- Source pins refreshed to main `1bdccbc`: see `evidence/source-refresh-1bdccbc.md`; the check is
  `python3 specs/004-truthful-detection-contract/evidence/check_source_hashes.py` (reports 3 of 7
  recorded hashes drifting, all refactor-only).
- The "scoring runtime unchanged" statement above still holds for `scorer.py` and `pipeline.py`
  (byte-identical) and holds semantically for the three refactored detector files.
- New V2 journey obligations: tasks T021–T026. The earlier statement that every FR/SC maps to a
  task remains true; FR-016/FR-021/SC-006 now additionally map to T022 and T026.
- This addendum is not approval, and the 23-item reviewer checklist is still entirely unchecked.
