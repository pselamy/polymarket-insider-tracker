# Tasks: Truthful Detection Contract

**Feature**: `specs/004-truthful-detection-contract` | **Date**: 2026-10-17 | **Status**: proposed.
Implementation and tests are future work; these tasks define obligations with evidence mapping.
Every FR/SC/acceptance scenario has named task/test/evidence obligations.

## Phase 1: Setup (planning receipts — done in this packet)

- [ ] T001 Record base/allocations and hydrate prior Phase0 artifacts byte-identically in packet scope
- [ ] T002 Verify scoring runtime unchanged (no `src/` diff) before reusing replay conclusions

## Phase 2: Foundational (contract pins — proposed in this packet)

- [ ] T003 R01 monetary-volume disposition with claim limits in `research.md` (FR-004/005)
- [ ] T004 Acquisition-provenance + operator-surface + wallet/size + evidence + storage-amendment + compatibility decisions in `research.md`/`plan.md` (FR-003–011, FR-020)
- [ ] T005 `data-model.md` availability states, evidence snapshots, envelope amendment (FR-003–008, SC-002)
- [ ] T006 `contracts/scoring-config.md` versioned envelope amendment (FR-003, FR-020, SC-006)
- [ ] T007 `contracts/detector-api.md` exclusive-boundary path + nullable-impact rule (FR-005, FR-009)
- [ ] T008 Reviewer-owned `checklists/domain-requirements.md`, unchecked (all FR/SC)

## Phase 3: US1 Explainable core assessment [US1] (future implementation)

- [ ] T009 [US1] Table-driven detector tests: known/unknown age, nonce boundary, volume states, niche, threshold equality in `tests/detector/` (FR-001–011, FR-015, SC-001–003)
- [ ] T010 [US1] Pipeline test proving same source values + availability flags in analysis, scoring, persistence, dry-run output (FR-016, SC-006)
- [ ] T011 [US1] Additive evaluation entry point sharing one acquisition with `analyze` in `src/polymarket_insider_tracker/detector/` (FR-004/005)
- [ ] T012 [US1] Per-page acquisition stamp + 600s rule in `src/polymarket_insider_tracker/ingestor/` (SC-002)

## Phase 4: US2 Operational vs research modules [US2] (future implementation)

- [ ] T013 [US2] Capability-consistency tests: operational core only; sniper experimental; funding enrichment-only; no backtest claims (FR-012–014, FR-018, SC-004/007)
- [ ] T014 [US2] Documentation/skill updates distinguishing operational/enrichment/experimental/deferred (FR-018, SC-007)

## Phase 5: US3 Coherent scoring contract [US3] (future implementation)

- [ ] T015 [US3] Threshold/bonus/override tests: default 0.80 equality, effective-threshold recording, reachable two-group bonus, override reconstruction (FR-002/003/011, SC-005)
- [ ] T016 [US3] Exclusive-boundary + deprecated-alias migration with nullable-impact serialization (FR-005/009)
- [ ] T017 [US3] Versioned envelope persistence reusing 003 schema/harness; old-row compatibility (FR-020, SC-006)

## Phase 6: Cross-cutting evidence (future implementation)

- [ ] T018 Bounded current Gamma field/schema qualification receipt (R01 limits preserved)
- [ ] T019 Full pipeline/storage/output/failure/restart/integration evidence per `quickstart.md` journey (SC-001–008)
- [ ] T020 Real PostgreSQL/Redis parity + migration downgrade exercise (disposable DB only)

## Dependencies

US1 → US2, US3 (foundational evidence first); Phase 6 after all story phases. MVP = Phases 1–2 (this planning packet).

## Parallel opportunities

T009/T012 (detector vs ingestor), T013/T015 (capability vs scoring) after T011/T012 land.
