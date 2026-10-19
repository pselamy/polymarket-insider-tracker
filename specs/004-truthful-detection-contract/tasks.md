# Tasks: Truthful Detection Contract

**Feature**: `specs/004-truthful-detection-contract` | **Date**: 2026-10-17, repaired 2026-10-17 | **Status**: proposed.
Implementation and tests are future work; these tasks define obligations with evidence mapping.
The monitor (ingest → enrich → detect/score → persist → deliver/suppress) is the product MVP;
planning phases alone are not an MVP, so “MVP = Phases 1–2” below means the completed
planning contract only, not a shippable increment.

## Phase 1: Setup (planning receipts — done in this packet)

- [ ] T001 Record base/allocations and hydrate prior Phase0 artifacts byte-identically in packet scope
- [ ] T002 Verify scoring runtime unchanged (no `src/` diff) before reusing replay conclusions.
  Re-run 2026-10-19 against `f2e2167`: `scorer.py`/`pipeline.py` byte-identical; three detector files
  changed by refactors only (`evidence/source-refresh-f2e2167.md`, `scripts/spec004_evidence/`check_source_hashes`.py`)

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
  unknown-age candidate asserts confidence exactly 0.50 (never lower or higher), no age/new-wallet/large-trade bonus,
  low-activity factor, JSON `wallet_is_fresh` (`detector/models.py:124`), persisted
  `WalletProfileDTO.is_fresh` (`pipeline.py:671`), rendered label, PLUS the boundary-10 /
  nonce-9 override mismatch (FR-008/010, SC-003/006, D2). Joint assertion, not separate
  label checks.
- [ ] T009d [US1] Table-driven detector tests in `tests/detector/` (extend
  `test_fresh_wallet.py`, `test_size_anomaly.py`, `test_scorer.py`): default/override/equality
  boundary fixtures (nonce exactly `B`, age exactly 48h, score exactly 0.80, ratio exactly
  0.02), transaction-override-3-3 / transaction-override-10-9 counterexamples, **all eight volume
  states and every reason** (FR-005a; the 45 cases of `evidence/replay-live/cases.json` are the table),
  niche modifier incl. the D3 lookup-failure case, threshold equality (FR-001–011, FR-015, SC-001–003)
- [ ] T010 [US1] Deterministic pipeline test (extend 003 harness in
  `tests/integration/test_end_to_end.py`) proving the same source values + availability
  flags in analysis, scoring, persistence, and formatted dry-run output; per-participant
  observations sharing one transaction evaluated as separate wallet-bearing trades, never
  collapsed before profiling (FR-016/021, SC-006)
- [ ] T011 [US1] Additive evaluation entry point in `src/polymarket_insider_tracker/detector/`
  (new `evaluation.py`; shared helper with `analyze` — no second acquisition) returning
  immutable wallet/market evidence including known-nonqualifying inputs; evidence flows
  `SignalBundle` → `RiskAssessment` → `_persist_assessment` → `alerter/formatter.py`
  (FR-003–005). Signature and clock per contracts/detector-api.md §2 (`evaluate(..., evaluated_at)`,
  injectable `clock`), explicit `daily_volume` rule §3, `Decimal | None` measured companions §4
- [ ] T012a [US1] `ingestor/gamma_client.py`: injectable clock; `_fetch_page_payload` returns items +
  `acquired_at` at response-body receipt; page tuples survive `_fetch_all_pages`/`_aggregate_pages`;
  `GammaMarketStats` gains `acquired_at`, `volume24hr_present`, `volume24hr_raw` (decoded JSON value, semantic
  preservation), `daily_volume_source`; duplicate-id rule; new `crawl_active_markets()` with
  `pages_ok/pages_failed/truncated/started/finished`; `get_active_market_stats()` unchanged for callers
  (FR-004/004b/005, F09, F12). Tests: table over page-failure positions, duplicate ids across pages,
  every raw JSON type, plus the §1.1a cases: `NaN`/`-Infinity` tokens (`$nonfinite` tag), a nested non-finite
  float, a > 256-byte value and a reserved-key object (`$truncated` marker, each reason), all stored as
  validated in `contracts/scoring-config-004.1.{nonfinite,truncated}.example.json`.
- [ ] T012b [US1] `ingestor/metadata_sync.py`: separate Gamma refresh loop writing a per-market volume record
  (TTL 1,200 s) on its own cadence (default `S_g = 180 s`); `_fetch_gamma_stats` reports
  `gamma_fetch_failed` instead of `{}`; `get_market` miss path writes the CLOB record set-if-absent and
  no volume claim; the read path merges the volume record into `MarketMetadata` (FR-004a/004b, F07, F08, F10).
- [ ] T012c [US1] `ingestor/models.py::MarketMetadata.to_dict/from_dict`: nullable provenance fields,
  legacy records (value without provenance) read as `unqualified/no_provenance`; the generic
  `last_updated` is never a stamp source (FR-004b, F11).
- [ ] T012d [US1] Freshness evidence tests: (1) a fake-clock steady-state test that runs the real loops for
  several cycles with a slow CLOB fake (D = 204 s) and asserts no `stale` volume while the Gamma loop is
  healthy; (2) the bound `S_g + 2g + w_g <= 600` asserted from measured fake durations; (3) cache-miss
  race: miss-path write after a sync write leaves the stamped record intact; (4) real-Redis TTL/expiry
  parity covering `stale` then `unavailable/no_volume_record`; (5) legacy-record upgrade. Lands with
  T012a–c. T012 has **no** dependency on T011 (the detector entry point consumes this, not the reverse).

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
  zero; formatter gains the single `Size vs 24h volume` line; persistence writes the measured companion
  (None ⇒ NULL) instead of `round(float,4)` (`pipeline.py:904-906`); alias table incl. negative `max_nonce`
  `ValueError` (FR-005/009, F18–F21)
- [ ] T017 [US3] Versioned `004.1` envelope persistence reusing 003 schema/harness
  (`storage/models.py`, `storage/repos.py`, `pipeline.py::_persist_assessment`); old `003.1`
  / legacy-unversioned rows read unchanged with version-specific semantics (FR-020, SC-006).
  Depends on T011. Includes: the version-dispatched reader of `contracts/scoring-config.md` §3
  (unknown version raises, column/`evidence_schema` mismatch is a corrupt row), the exact
  `scoring-config-004.1.schema.json` as conformance fixture (a test validates real persisted rows against
  it, as `scripts/spec004_evidence/`check_envelope`.py` does for the examples), detector-failure rows with
  `evaluation_evidence: null` + `detector_failures`. The contract text of slice 003
  `assessment-storage.md` §4 is already amended (versioned union); T017 only implements it and
  must not reopen that text.

## Phase 6: Cross-cutting evidence (future implementation)

- [ ] T018 Bounded current Gamma field/schema qualification receipt (read-only; R01 limits preserved)
- [ ] T019 Full pipeline/storage/output/failure/restart/integration evidence per
  `quickstart.md` journey scenarios 1–10 (SC-001–008). Depends on T010, T015–T017.
- [ ] T020 Real PostgreSQL/Redis parity + migration downgrade exercise (disposable DB only)
- [ ] T027 **ENTRY GATE for the wallet group** — wallet-subject qualification receipt (needs explicit
  authorization for read-only anonymous probes; none were run in this revision). No implementation task
  that lets the wallet group contribute to a score (T009d wallet rules, T015, T019) may merge until this
  receipt exists and records what the proxy-address nonce measures; until then the wallet group is
  non-scoring (spec FR-007c). Probe plan: bounded sample of proxy addresses, compare
  `eth_getTransactionCount` with public trade counts and record whether the nonce moves with trading;
  if DECISION D1 selects O2, also qualify the per-user trades query (ordering, `takerOnly`,
  10,000-row offset cap, maker visibility). Until it exists the wallet group stays described as
  "low transaction count on the proxy address" (spec FR-007a/b). If the receipt shows the nonce does not
  track trading, the wallet group stays non-scoring and D1 must be re-decided. (F01, F02)
- [ ] T029 [US3] Default-alert-path disclosure (FR-022a): when the effective configuration makes every
  evidence class unable to reach `DETECTOR_ALERT_THRESHOLD` (ceilings from the T028 table), startup and
  the operator status surface MUST report `default alert path unreachable` with the highest reachable
  ceiling and the configured threshold; test asserts the report for the 0.80/unknown-age default and
  its absence when the threshold is lowered to a reachable value. Lands with T028.
- [ ] T028 [US3] Reachability regression test (`tests/detector/test_scorer.py` or a new
  `tests/detector/test_reachability.py`): recompute the ceilings in `evidence/reachability.md`
  (unknown-age 0.780, not-niche 0.450/0.642, etc.) from the real `FreshWalletDetector`,
  `SizeAnomalyDetector` and `RiskScorer`, so any weight/cap/bonus/threshold change fails loudly (FR-022,
  SC-009). Also add the 45 `evidence/replay-live/cases.json` cases to the table-driven suite.

## Phase 5b: V2 journey suites (added 2026-10-19; future implementation)

Data API V2 is the live ingestion contract on main (`d647adf` default, `bfc1a10` real-service
acceptance), so these five suites plus the shared harness are 004 obligations, not a separate
pending concern. Their fixtures all use `FakeEth(transaction_count=0)` (unknown age), a market
with `daily_volume=None` in category `science`, a $3,250 trade (5000 × 0.65,
`tests/integration/test_end_to_end.py:78-79`) and `alert_threshold=0.4`. Today that scores
`(0.700·0.40 + 0.200·0.35 + 0.200·0.25)·1.2 = 0.480` ≥ 0.4. Under the 004 unknown-age rule
(confidence exactly 0.50, no brand-new bonus) the same fixture scores
`(0.500·0.40 + 0.200·0.35 + 0.200·0.25)·1.2 = 0.384 < 0.4` (replay `core-w2-m1`), so
`should_alert` becomes `False` and `delivery_disposition` becomes `not_alertable`, which fails
`dry_run_assessment` (`tests/integration/_journey_harness.py:126-127`). The change must be made
deliberately, not by loosening the assertion:

- [ ] T021 [US1] `tests/integration/_journey_harness.py` + `tests/integration/test_end_to_end.py::_market`:
  add a fixture builder for a market whose Gamma volume carries a fresh acquisition stamp
  (state `available`, e.g. $10,000 so a $3,250 trade has ratio 0.325, saturating the 3× cap:
  volume confidence 0.5 × niche 1.5 = 0.75, score `(0.5·0.40+0.75·0.35+0.75·0.25)·1.2 = 0.780`).
  The dry-run journeys use it so they keep proving "one qualifying assessment, zero deliveries,
  zero dedup keys" at `alert_threshold=0.4`. Keep `dry_run_assessment` strict.
- [ ] T022 [US1] `tests/integration/test_end_to_end.py`: replace the pinned `"003.1"` (`:222`)
  with the 004.1 identity, keep one deliberate **negative** journey on the volume-less fixture
  asserting score 0.384, `should_alert is False`, `delivery_disposition == "not_alertable"` and
  that the persisted evidence says `volume_state=unavailable` / age unknown; add the FR-016
  same-evidence assertions (analysis → score → row → dry-run render) and the FR-021
  two-participant assertion (T010 lives here).
- [ ] T023 [US1] `tests/integration/test_v2_journey.py` (explicit V2 URL, threshold 0.4):
  switch to the T021 stamped-volume market for the qualifying path; assert the row's
  `scoring_algorithm_version`, `threshold_at_eval == 0.400` and recomputable score; keep
  `test_journey_harness_characterization` (`:61-70`) in sync with the new fixture builder.
- [ ] T024 [US1] `tests/integration/test_v2_default_journey.py` (default URL, no env): same
  fixture change; replace `"003.1"` at `:55` with the 004.1 identity and additionally assert the
  evidence envelope parses under the contract schema (`contracts/scoring-config.md`).
- [ ] T025 [US1] `tests/integration/test_v2_default_journey_services.py` (real Redis/services):
  replace `"003.1"` at `:137`; the stamped-volume market must be served through the real Redis
  metadata cache so the 0-600 s rule is exercised with real expiry, not only with a fake clock.
- [ ] T026 [US3] `tests/integration/test_postgres_journey.py` (real PostgreSQL): replace the
  hard-coded expectations `_EXPECTED_FRESH_WALLET_CONFIDENCE = 0.700`,
  `_EXPECTED_SIZE_ANOMALY_CONFIDENCE = 0.200`, `_EXPECTED_WEIGHTED_SCORE = 0.480`
  (`:66-80`), `_EXPECTED_SCORING_CONFIG` (exact dict equality, incl. the
  `multi_signal_bonus_3` key) and `"003.1"` (`:196`) with literals recomputed from the final
  contract; the stored `scoring_config` becomes the 004.1 envelope and the assertion must parse
  it with the schema's reader dispatch and also read a seeded `003.1` and a
  `legacy-unversioned` row unchanged (no migration, disposable DB only).

Each of T021–T026 lands with the implementation change that alters its expectation, never after
it (same rule as T009c/T009d/T015).

## Dependencies

T009a → T009b/T009c/T009d; T011 → T010/T017; T012a → T012b/T012c → T012d; T012a–c → T011 (market side); T013 → T014; T015–T017 → T019; T021 → T022–T026; T027 gates T009d(wallet rules)/T015/T019; T022–T026 → T019;
foundational contract (T003–T008) precedes all implementation. Tests that pin a behavior
(T009c/T009d/T015) land with or before the implementation change they cover — never after
as a separate unblocking layer. Parallel-safe pairs after their prerequisites:
T009d with T012a–d (detector vs ingestor tables), T013 with T015 (capability vs scoring).

## Traceability (every FR/SC + 12 story acceptance scenarios)

Full per-requirement table with implementation task(s), named test/scenario obligation, and
future evidence receipt lives in `evidence/implementation-validation.md` §Traceability.
Summary: FR-001–011 → T009a–d/T011/T012a–d/T013/T015/T016; FR-012–014 → T013/T014;
FR-015/016 → T009d/T010; FR-017/018 → T014; FR-019 → T002/T019; FR-020 → T006/T017/T020;
FR-021 → T010; FR-007c → T027; FR-022/FR-022a/SC-009 → T028/T029; SC-001–008 → T009d/T010/T013/T015/T017/T019; 12 acceptance scenarios
(US1-1..4, US2-1..4, US3-1..4) → T009d/T010/T013/T014/T015/T017/T019 with journey
scenarios 1–10 in `quickstart.md`. No FR/SC/scenario is covered by nominal reference —
each names its task, test file, and evidence receipt.
