# Source refresh: 46e4192 → 1bdccbc → f2e2167 (2026-10-19)

**Purpose**: the 2026-10-17 packet pinned source hashes and line numbers to main `46e4192`
(replay execution checkout `752a918`). Main moved to `1bdccbc`. This file is an appended
correction: it does not rewrite `replay/summary.json` or the historical receipts, it records
what changed since and whether any conclusion depends on it.

## Second refresh: 1bdccbc → f2e2167

Main moved again (#163, #164, #166: test-scaffold deduplication only). `git diff 1bdccbc f2e2167 -- src`
is empty, so no hash, line pin or scoring conclusion changed. The check tool now reads
`evidence/source-hashes.json` (current-base hashes, regenerated on `f2e2167`); the original
`replay/summary.json` stays the immutable receipt of the replay checkout and is not edited. The
"On 1bdccbc" column below is equally the `f2e2167` value. Tool outputs (`reachability`, `replay_live`,
`check_envelope`) re-ran unchanged.

## Third refresh: f2e2167 → 112a733 (2026-10-19, review round 2)

`git diff f2e2167 112a733 -- src` touches `detector/fresh_wallet.py`, `detector/size_anomaly.py`
and `pipeline.py` only: structural `Protocol` types (`WalletProfiler`, `MarketMetadataSource`,
`SignalDetector`) replace concrete collaborator annotations. No weight, constant, threshold,
comparison, serialized key or runtime behavior changed. Consequences:

- `evidence/source-hashes.json` regenerated for the three drifted files (`base_commit` 112a733);
  `check_source_hashes` exits 0 again. `replay/summary.json` stays the immutable receipt.
- Line pins in `contracts/`, `data-model.md`, `tasks.md`, `decision-packet.md` and the revision
  log moved by the exact diff offsets: `fresh_wallet.py` and `size_anomaly.py` +6 from the class
  definitions down; `pipeline.py` +9 below the `SignalDetector` insertion (e.g. `is_fresh` 671→680,
  persistence `volume_impact` 904-906→913-915, `detector_failure` row 628-647→637-656).
  Constants (`fresh_wallet.py:24-28`, `size_anomaly.py:29`) did not move.
- The immutable "Line pins refreshed" table above describes `f2e2167` and is not rewritten.
- `reachability`, `replay_live`, `check_envelope` re-run unchanged.

## Fourth refresh: 112a733 → 0ddee0b (2026-10-20, review round 3)

`git diff 112a733 0ddee0b -- src` touches `pipeline.py` (the trade callback `Pipeline._on_trade` is
renamed `Pipeline.process_trade`, `pipeline.py:431,581`; no line count changes) and
`storage/database.py` (a new helper outside the scored path). No weight, constant, threshold,
comparison, serialized key or scoring behavior changed, and no line pin cited by this slice moved.
`evidence/source-hashes.json` is regenerated for `pipeline.py` (`base_commit` 0ddee0b);
`check_source_hashes` exits 0. No 004 artifact referred to `_on_trade`. `replay/summary.json` stays
the immutable receipt.

## Fifth refresh: 0ddee0b → f3871cb (2026-10-21, review round 4)

`git diff 0ddee0b f3871cb -- src` (#179) marks Protocol stubs `@abstractmethod` with docstring
bodies in `detector/fresh_wallet.py` (+3 lines from the `WalletProfiler` Protocol on) and
`profiler/analyzer.py` / `profiler/rpc_handle.py`, and removes unreachable wallet-link checks in
`alerter/formatter.py`; `alerter/_channel_delivery.py` gains typing only. `scorer.py` is
byte-identical. No weight, constant, threshold, comparison, serialized key or scoring behavior
changed. `evidence/source-hashes.json` is regenerated for `detector/fresh_wallet.py`
(`base_commit` f3871cb); `check_source_hashes` exits 0. Line pins refreshed:
`detector/fresh_wallet.py` constructor 62→65, `_max_nonce` store 74→83, comparison 153/159→162;
`profiler/analyzer.py` `DEFAULT_FRESH_THRESHOLD` 24→25, address use 168→171,181, `first_seen`
`None` path 180-196→193-201, `_is_wallet_fresh` 210-229→223-242, `nonce >= threshold` 225→238
(some older pins had already drifted; these are re-read against f3871cb). `adf809b` (#178) changes
tests only; test citations in tasks T021–T026 are re-read against f3871cb (round-4 N4).

## Sixth refresh: f3871cb → 3d5ab59 (2026-10-21, review round 5)

`git diff f3871cb 3d5ab59 -- src` (#182, #183) adds one read-only `AlertHistory.retention_ttl`
property in `alerter/history.py`; no hashed file, weight, constant, threshold, comparison,
serialized key or scoring behavior changed, so `evidence/source-hashes.json` keeps every hash and
only its `base_commit` moves to 3d5ab59; `check_source_hashes` exits 0. Tests changed in
`tests/alerter/test_deduplication.py` (`FakeChannel` 34→35, cited by T030) and
`tests/integration/test_pipeline_wiring_characterization.py` (not cited). The journey citations
in T021–T026 (`_journey_harness.py:61,166`, `test_end_to_end.py:183`, `test_v2_journey.py:53`,
`test_v2_default_journey.py:55`, `test_v2_default_journey_services.py:139`,
`test_postgres_journey.py:65,75,175`) were re-read on 3d5ab59 and are unchanged.

## Seventh refresh: 3d5ab59 → 3e2fd42 plus layer 2 (2026-10-21)

Slice 004 layer 2 changes three hashed files, all additively, and `evidence/source-hashes.json` records
the new hashes with `base_commit` 3e2fd42 (layer 1, merged). `scorer.py`, `pipeline.py`,
`detector/fresh_wallet.py` and `profiler/models.py` stay byte-identical, so the scoring arithmetic and
the T002 premise hold.
`ingestor/models.py`: `MarketMetadata` gains the nullable `volume` provenance field and the
`volume_unavailable_reason` field; `to_dict`/`from_dict` carry `volume`, and a legacy bare
`daily_volume` reads as an unqualified observation (T012c). Two `Orderbook`/`Market` expressions were
compacted without changing a value, to keep the file from growing.
`detector/models.py`: `SizeAnomalySignal` gains the optional `volume_evidence` field (default `None`);
`to_dict` is unchanged.
`detector/size_anomaly.py`: a keyword-only `clock`, `_fetch_metadata` also returns the category source,
and `analyze` attaches the volume evidence to the signal. Weights, thresholds, comparisons, niche rule
and confidence arithmetic are untouched (layer 3 owns the scoring change).

## Hash comparison

Command (stdlib only, no `src/` import; exit 1 when any recorded hash drifts):

```bash
uv run python -m scripts.spec004_evidence.check_source_hashes
```

| File (`src/polymarket_insider_tracker/`) | Recorded (`summary.json`) | On 1bdccbc | Disposition |
|---|---|---|---|
| `detector/scorer.py` | `2fd151ee…3904` | same | unchanged |
| `ingestor/models.py` | `28bdadd9…1c11` | same | unchanged |
| `profiler/models.py` | `a4a4a0e4…5be` | same | unchanged |
| `pipeline.py` | `2911f000…0c7` | same | unchanged |
| `detector/fresh_wallet.py` | `edfa1984…1f10` | `030df0ec…2018` | refactor only: batch gather/zip moved to `detector/_batch.py` (#158, `6be456a`) |
| `detector/size_anomaly.py` | `ae189bd0…2918` | `c28253da…0714` | refactor only: same extraction (#158) |
| `detector/models.py` | `854bbd30…38cd` | `94901405…7bd` | refactor only: shared property/serialization helpers (#159, `43099eb`); same keys, values and thresholds (0.7/0.85 inclusive) |

`git diff 46e4192 HEAD -- src/polymarket_insider_tracker/detector/{fresh_wallet,size_anomaly,models}.py`
shows no change to a weight, confidence constant, threshold, comparison, serialized key or
dataclass field. The scoring runtime is therefore unchanged for replay purposes; the replay
corpus/CSV/summary stay valid as the **old-vs-proposed behavioural comparison** they always were.
What changed materially on main is *around* the scoring runtime (see below).

## What did change on main and why it matters to 004

| Change | Commits | Consequence for the contract |
|---|---|---|
| Data API V2 trade acquisition is the default live ingestion (`ingestor/acquisition.py`, `ingestor/v2_*.py`, `ingestor/trade_row_v2.py`; tests assert the unset `POLYMARKET_TRADES_URL` resolves to `OFFICIAL_V2_TRADES_URL`) | `33b8f2a` (#144) decode, `b337309` (#151) opt-in wiring, `d647adf` (#152) default to V2, `bfc1a10` (#156) real-service acceptance | V2 is the live contract, not a pending separate concern. `proxy_wallet` is the participant identity (`trade_row_v2.py:27` → `trade_rows.py:169,251`). See spec Assumptions and research R09. |
| Shared journey harness and V2 journey suites | `b337309` (#151), `d647adf` (#152), `6ac3cd0` (#155 harness extraction), `bfc1a10` (#156); the Postgres journey is older (`0b3b904`, #142) | Their thresholds, expected scores and `"003.1"` assertions change deliberately with 004: tasks T021–T026. |
| Batch/serialization single homes | #158, #159 | Line pins in contracts refreshed; no semantic effect. |
| Everything else in `git diff --stat 46e4192 HEAD -- src` (alerter, profiler `_funding_rpc`, endpoints, retry/attempt modules) | various | Not in the detection score path; `scorer.py` and `pipeline.py` are byte-identical to the recorded hashes, so the persistence/evidence projection described in tasks T017 still maps to the same lines. |

## Line pins refreshed (old → new)

`detector/models.py` `wallet_is_fresh` 78→124; signal float fields 106-107→152-153; direct emission
148-149→172-173; docstring 96-97→142-143. `detector/size_anomaly.py` constructor 60→61.
`config.py` `DetectorSettings` 334→335. `storage/repos.py` 573,617→574,618. Unchanged and
re-verified: `detector/fresh_wallet.py:56`, `pipeline.py:671`, `profiler/analyzer.py:24,210-229`,
`detector/scorer.py:217-220`, `storage/models.py:168`, `ingestor/metadata_sync.py:41-42`.
Pins to line numbers are advisory; the stable references are the `file::symbol` names.

## Staleness check (command and result)

```bash
cd specs/004-truthful-detection-contract
grep -rnE 'models\.py:(78|106-107|148-149)|size_anomaly\.py:60|config\.py:334|repos\.py:573|46e4192|523b0a3|752a918' \
  --include=*.md --include=*.json . | grep -v 'revision-2026\|source-refresh\|corpus.json'
```

Every remaining hit is intentionally historical: it names the original base (`46e4192`, `523b0a3`,
`752a918`) in a sentence that also gives the refreshed base, or is the immutable replay receipt in
`evidence/replay/summary.json` / `evidence/replay.md` / `evidence/implementation-validation.md`
(historical sections), none of which claim to describe current main.

## Eighth refresh: 3aafd0b plus layer 3 (2026-10-21)

Slice 004 layer 3 changes five hashed files and, unlike the earlier refreshes, **changes scoring
behaviour on purpose**; `evidence/source-hashes.json` records the new hashes with `base_commit` 3aafd0b.
`detector/fresh_wallet.py` and `profiler/models.py` stay byte-identical.
`detector/scorer.py`: the wallet group no longer scores (DECISION D1b G1, FR-007c), the 3+ bonus branch
is removed (`multi_signal_bonus_3` stays as the neutral `"1"` key, FR-011), the combination arithmetic
moves into the module function `size_group_score` that the alert-path report also calls, and the
size diagnostics read the classified volume evidence. The numeric literals T028 checks
(`0.40/0.35/0.25`, `MULTI_SIGNAL_BONUS_2 = 1.2`, `0.80`) are unchanged.
`detector/size_anomaly.py`: the volume ratio and the niche basis come from the classified volume state
(D3 (d)); the confidence literals became the named constants `VOLUME_RATIO_CAP`, `VOLUME_SCORE_CAP` and
`NICHE_MULTIPLIER` with the same values; `reachability.py` now requires those names.
`detector/models.py`: `RiskAssessment.wallet_group_status`. `ingestor/models.py`: the lossy
`daily_volume`/`weekly_volume`/`liquidity` fields are retired. `pipeline.py`: the alert-path report
(T029) builds the alert-path health monitor and logs the report at startup.
The T002 premise (replay conclusions about the 003.1 contract) is unaffected because the replay corpus
evaluates the old and proposed contracts side by side; the shipped contract is now the proposed one.

## Ninth refresh: layer 3 review round 1 (2026-10-21)

Review round 1 of the layer-3 PR implements T017 (rows written as `004.1`) with the minimal part of
T011 it needs; `base_commit` stays 3aafd0b. No scoring rule, weight, bonus, threshold or confidence
literal changes, so the scores of the eighth refresh stand.
`detector/fresh_wallet.py` and `detector/size_anomaly.py`: additive `evaluate(trade, *, evaluated_at)`
methods returning the frozen evaluation snapshots of `detector/evaluation.py`; `analyze` reads the
injectable clock once and returns the evaluation's signal; read-only configuration properties (the
fresh detector's exclusive boundary is `max_nonce + 1`); the size detector's market lookup moves
unchanged to `detector/market_lookup.py`. `detector/models.py`: `SCORING_ALGORITHM_VERSION` is `004.1`
and `RiskAssessment` carries the evaluation evidence. `detector/scorer.py`: the pure configuration is
serialized through `scoring_config.serialize_pure_config` (byte-identical output) and the bundle's
evidence is passed to the assessment. `pipeline.py`: one evaluation instant per trade for both
detectors, detector failures keep their exception class, and the row projection moves to
`assessment_row.py`, which writes the composed 004.1 document.

## Tenth refresh: alert-path report from the effective scorer configuration (2026-10-21)

Review round 1 requires the FR-022a report to use the same effective weights and threshold as the
running `RiskScorer`; `base_commit` stays 3aafd0b and no scoring rule or literal changes.
`detector/scorer.py`: a read-only `alert_threshold` property. `pipeline.py`: the startup call binds the
alert-path health monitor to the scorer it built (`bind_scorer`), so `/health` and the startup record
are computed from that scorer's current weights and threshold.
