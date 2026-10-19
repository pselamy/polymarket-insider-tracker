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
