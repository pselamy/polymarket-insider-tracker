# Tasks: Truthful Detection Contract

**Feature**: `specs/004-truthful-detection-contract` | **Date**: 2026-10-17, repaired 2026-10-17 | **Status**: proposed.
Implementation and tests are future work; these tasks define obligations with evidence mapping.
The monitor (ingest → enrich → detect/score → persist → deliver/suppress) is the product MVP;
planning phases alone are not an MVP, so “MVP = Phases 1–2” below means the completed
planning contract only, not a shippable increment.

## Phase 1: Setup (planning receipts — done in this packet)

- [ ] T001 Record base/allocations and hydrate prior Phase0 artifacts byte-identically in packet scope
- [ ] T002 Verify scoring runtime unchanged (no `src/` diff) before reusing replay conclusions

## Phase 2: Foundational (contract pins — proposed in this packet)

- [ ] T003 R01 monetary-volume disposition with claim limits in `research.md` (FR-004/005)
- [ ] T004 Acquisition-provenance + operator-surface + wallet/size + evidence + storage-amendment + compatibility decisions in `research.md`/`plan.md` (FR-003–011, FR-020)
- [ ] T005 `data-model.md` raw/usable volume, effective-boundary classification, canonical freshness, envelope amendment (FR-003–010, SC-002/003)
- [ ] T006 `contracts/scoring-config.md` versioned envelope amendment (FR-003, FR-020, SC-006)
- [ ] T007 `contracts/detector-api.md` exclusive-boundary table + additive nullable-impact migration (FR-005, FR-009)
- [ ] T008 Reviewer-owned `checklists/domain-requirements.md`, unchecked (all FR/SC)

## Phase 3: US1 Explainable core assessment [US1] (future implementation)

Named files, entry points, dependency order:

- [ ] T009a [US1] `config.py::DetectorSettings` — add `DETECTOR_FRESH_TRANSACTION_BOUNDARY`
  (default 5, positive int) and `DETECTOR_SIZE_VOLUME_THRESHOLD` (default 0.02); validate in
  `pipeline.py::_initialize_components`; no other new knobs (FR-002/009, R03)
- [ ] T009b [US1] `detector/fresh_wallet.py` — `fresh_transaction_boundary` keyword +
  deprecated inclusive `max_nonce` alias per contract table; effective `B` captured per
  evaluation; classification uses `nonce < B` with notional ≥ 1000 and age ≤ 48h gates
  (FR-007/009, D1)
- [ ] T009c [US1] Freshness-closure regression in `tests/detector/test_fresh_wallet.py` —
  unknown-age candidate asserts confidence ≤ 0.50, no age/new-wallet/large-trade bonus,
  low-activity factor, JSON `wallet_is_fresh` (`detector/models.py:78`), persisted
  `WalletProfileDTO.is_fresh` (`pipeline.py:671`), rendered label, PLUS the boundary-10 /
  nonce-9 override mismatch (FR-008/010, SC-003/006, D2). Joint assertion, not separate
  label checks.
- [ ] T009d [US1] Table-driven detector tests in `tests/detector/` (extend
  `test_fresh_wallet.py`, `test_size_anomaly.py`, `test_scorer.py`): default/override/equality
  boundary fixtures (nonce exactly `B`, age exactly 48h, score exactly 0.80, ratio exactly
  0.02), transaction-override-3-3 / transaction-override-10-9 counterexamples, all volume
  states, niche modifier, threshold equality (FR-001–011, FR-015, SC-001–003)
- [ ] T010 [US1] Deterministic pipeline test (extend 003 harness in
  `tests/integration/test_end_to_end.py`) proving the same source values + availability
  flags in analysis, scoring, persistence, and formatted dry-run output; per-participant
  observations sharing one transaction evaluated as separate wallet-bearing trades, never
  collapsed before profiling (FR-016/021, SC-006)
- [ ] T011 [US1] Additive evaluation entry point in `src/polymarket_insider_tracker/detector/`
  (new `evaluation.py`; shared helper with `analyze` — no second acquisition) returning
  immutable wallet/market evidence including known-nonqualifying inputs; evidence flows
  `SignalBundle` → `RiskAssessment` → `_persist_assessment` → `alerter/formatter.py`
  (FR-003–005)
- [ ] T012 [US1] Per-page acquisition stamp + 600s rule in
  `src/polymarket_insider_tracker/ingestor/` (`gamma_client.py` stamp at body acquisition,
  `metadata_sync.py::_enrich_and_cache_market` carry-through, `models.py::MarketMetadata`
  serialization); raw `volume24hr_raw` preserved verbatim, usable `daily_volume` separate
  (FR-004/005, SC-002). Depends on T011 for the shared acquisition helper.

## Phase 4: US2 Operational vs research modules [US2] (future implementation)

- [ ] T013 [US2] Capability-consistency tests (`tests/detector/test_sniper.py` scope +
  scorer/formatter surfaces): operational core only (fresh/low-activity + size + niche
  modifier); sniper experimental; funding enrichment-only; no backtest claims (FR-001/006/012–014, FR-018, SC-004/007)
- [ ] T014 [US2] Documentation/skill updates (README, CHANGELOG, sample alert, example env,
  architecture text, docstrings, tracked skill) distinguishing
  operational/enrichment/experimental/deferred (FR-017/018, SC-007). Depends on T013.

## Phase 5: US3 Coherent scoring contract [US3] (future implementation)

- [ ] T015 [US3] Threshold/bonus/override tests in `tests/detector/test_scorer.py`: default
  0.80 equality (inclusive `>=`, 0.001 quantum), effective-threshold recording, reachable
  two-group 1.2 bonus only (3+ branch removed, `multi_signal_bonus_3` neutral key retained),
  override reconstruction within rounding (FR-002/003/011, SC-005)
- [ ] T016 [US3] Exclusive-boundary + deprecated-alias migration (`detector/fresh_wallet.py`,
  `config.py`) with additive nullable-impact serialization (`detector/models.py`,
  `scorer.py::_extract_size_diagnostics`, `alerter/formatter.py`,
  `alerter/channels/*`): `volume_impact_measured`/`book_impact_measured`, never measured
  zero (FR-005/009)
- [ ] T017 [US3] Versioned `004.1` envelope persistence reusing 003 schema/harness
  (`storage/models.py`, `storage/repos.py`, `pipeline.py::_persist_assessment`); old `003.1`
  / legacy-unversioned rows read unchanged with version-specific semantics (FR-020, SC-006).
  Depends on T011.

## Phase 6: Cross-cutting evidence (future implementation)

- [ ] T018 Bounded current Gamma field/schema qualification receipt (read-only; R01 limits preserved)
- [ ] T019 Full pipeline/storage/output/failure/restart/integration evidence per
  `quickstart.md` journey scenarios 1–10 (SC-001–008). Depends on T010, T015–T017.
- [ ] T020 Real PostgreSQL/Redis parity + migration downgrade exercise (disposable DB only)

## Dependencies

T009a → T009b/T009c/T009d; T011 → T010/T012/T017; T013 → T014; T015–T017 → T019;
foundational contract (T003–T008) precedes all implementation. Tests that pin a behavior
(T009c/T009d/T015) land with or before the implementation change they cover — never after
as a separate unblocking layer. Parallel-safe pairs after their prerequisites:
T009d with T012 (detector vs ingestor tables), T013 with T015 (capability vs scoring).

## Traceability (every FR/SC + 12 story acceptance scenarios)

Full per-requirement table with implementation task(s), named test/scenario obligation, and
future evidence receipt lives in `evidence/implementation-validation.md` §Traceability.
Summary: FR-001–011 → T009a–d/T011/T012/T013/T015/T016; FR-012–014 → T013/T014;
FR-015/016 → T009d/T010; FR-017/018 → T014; FR-019 → T002/T019; FR-020 → T006/T017/T020;
FR-021 → T010; SC-001–008 → T009d/T010/T013/T015/T017/T019; 12 acceptance scenarios
(US1-1..4, US2-1..4, US3-1..4) → T009d/T010/T013/T014/T015/T017/T019 with journey
scenarios 1–10 in `quickstart.md`. No FR/SC/scenario is covered by nominal reference —
each names its task, test file, and evidence receipt.
