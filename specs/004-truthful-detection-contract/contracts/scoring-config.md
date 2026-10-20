# Contract: Scoring Configuration Amendment (004.1)

**Feature**: `specs/004-truthful-detection-contract` | **Date**: 2026-10-17, rewritten 2026-10-19 | **Status**: proposed.
Amends the shared [003 assessment-storage contract](../../003-safe-observable-operation/contracts/assessment-storage.md)
(§4) without a competing migration. Implementation is not authorized.

Machine-readable contract = the JSON Schema **plus** the cross-field rules of
`scripts/spec004_evidence/check_envelope.py` (run `uv run python -m scripts.spec004_evidence.check_envelope`,
exit 0). The schema alone is not the whole contract: it cannot express the raw-value rule for
`size.volume24hr_raw` (data-model §1.1a; enforced by `check_envelope.stored_raw_errors`), score and ratio
reconstruction, or the state/age consistency rules listed under Reconstruction below.
[`scoring-config-004.1.schema.json`](scoring-config-004.1.schema.json),
[`scoring-config-004.1.example.json`](scoring-config-004.1.example.json) (default pre-T027 posture),
[`scoring-config-004.2.post-t027.example.json`](scoring-config-004.2.post-t027.example.json) (wallet group
qualified; draft version 004.2, §2a),
[`scoring-config-004.1.failure-row.json`](scoring-config-004.1.failure-row.json).

## 1. What this amends (stated plainly)

Existing behavior on main `f2e2167` (refreshed 2026-10-19; originally `46e4192`): `scoring_config` is
the canonical pure-configuration JSON recorded on every new assessment (`detector/scorer.py:217-220`);
rows carry scalar columns plus a nullable `scoring_config` TEXT (`storage/models.py:168`,
`storage/repos.py:574,618`). Slice 003 §4 defines the field as "the canonical deterministic JSON of the
exact active configuration".

**This is a semantic amendment, not a preservation.** 004 redefines `scoring_config` as a
*version-dispatched document* whose meaning depends on the row's `scoring_algorithm_version`:

| `scoring_algorithm_version` | `scoring_config` content | Reader |
|---|---|---|
| `legacy-unversioned` | `NULL` (configuration unknowable) | none; never reinterpreted |
| `003.1` | pure configuration, exactly the five keys below | 003 reader, unchanged |
| `004.1` | the 004.1 document of §2: the **same five 003 keys with the same types**, plus `evidence_schema`, `detector_config`, `detector_failures`, `evaluation_evidence` | 004.1 reader; a 003 pure-config reader that ignores unknown keys keeps working |
| `004.2` (draft; written only after the gate-opening change, §2a) | the 004.1 document with `evidence_schema = "004.2"` and `wallet.group_status = "qualified"` | added by the gate-opening change; until then `UnsupportedScoringVersion` |

Because the 004.1 document is a strict superset of the 003.1 document, a consumer that only reads the
five configuration keys is unaffected; a consumer that rejects unknown keys breaks and must be updated
(this is the named compatibility risk). Slice 003's `assessment-storage.md` §4 has been amended in this
revision to state the same versioned union and point here, so the two normative contracts agree; T017 remains
the code task that implements the writer and readers.

No new DDL: the column is `TEXT` (`storage/models.py:168`). One versioned amendment only.

## 2. The 004.1 document

Canonical JSON: `json.dumps(doc, sort_keys=True, separators=(",", ":"))` (the 003.1 serialization
rule). All decimals are JSON **strings** in plain notation (no exponent, no float); a float anywhere is a
contract violation. Timestamps are ISO-8601 with an explicit `+00:00` offset (aware UTC). Integers and
booleans are JSON numbers/booleans. Authoritative field list = the JSON Schema file; this table is its
readable summary.

| Path | Type / encoding | Null | Meaning |
|---|---|---|---|
| `alert_threshold` | string, exactly 3 decimals (`"0.800"`) | no | Effective threshold; **the only home of the threshold** (not repeated in `detector_config`). Must equal the row's `threshold_at_eval`. |
| `multi_signal_bonus_2` | const `"1.2"` | no | Two-group multiplier. |
| `multi_signal_bonus_3` | const `"1"` | no | Retained structural key, inert (neutral multiplier); the 3+ branch is removed. (003.1 rows carry `"1.3"`; they are not rewritten.) |
| `score_quantum` | const `"0.001"` | no | Quantization step, `ROUND_HALF_EVEN`. |
| `weights` | object name → decimal string, sorted keys; must contain `fresh_wallet`, `niche_market`, `size_anomaly` | no | `str(Decimal(str(float)))` encoding as in 003.1 (`"0.4"`, `"0.25"`, `"0.35"`). |
| `evidence_schema` | const `"004.1"` (`"004.2"` in the qualified posture, §2a) | no | **Root-level sibling** of `evaluation_evidence` (not inside it). Must equal the row's `scoring_algorithm_version`. |
| `detector_config` | object | no | Static effective values: `fresh_transaction_boundary` (int ≥ 1, the captured `B`), `size_volume_threshold` (decimal), `fresh_min_notional`, `known_age_ceiling_hours`, `niche_volume_threshold`, `niche_min_notional` (decimals), `volume_recency_seconds` (const 600). |
| `detector_failures` | array of `{detector, error_class, message}` | empty array when none | `detector` ∈ `fresh_wallet`/`size_anomaly`; `error_class` ≤ 128 chars; `message` = the already-redacted failure text (`pipeline.py:732-737`) truncated to 200 chars. Never a traceback or URL. |
| `evaluation_evidence` | object or `null` | **null** for rows where no evaluation ran (detector-failure skip rows) | Per-assessment observations; below. |

`evaluation_evidence` (all keys required when the object is present):

| Path | Encoding | Null | Meaning |
|---|---|---|---|
| `evaluated_at` | UTC timestamp | no | The single evaluation instant injected into both detectors (detector-api.md §Evaluation clock). |
| `notional` | decimal string | no | Trade notional as evaluated. |
| `wallet.subject_kind` | const `"proxy_wallet"` | no | FR-007a. |
| `wallet.group_status` | const `unqualified_pre_T027` in 004.1; const `qualified` in 004.2 (§2a) | no | FR-007c, the default posture. `unqualified_pre_T027`: the wallet group is **non-scoring**: `wallet.confidence` null, `wallet.factors` `{}`, `contributions.fresh_wallet` `"0"`, the group is not counted in `result.signals_triggered`, and `contributions.bonus` is `"1"` (the 1.2 bonus needs two scoring groups); `classification`, `tx_count` and age fields are still recorded as observations. `qualified`: written only after the reviewed change citing a T027 receipt; the wallet rules of data-model §2 score. Example of each posture: `scoring-config-004.1.example.json` (pre-T027) and `scoring-config-004.2.post-t027.example.json`. |
| `wallet.classification` | enum `fresh`, `low_activity_unknown_age`, `known_nonqualifying`, `not_requested`, `retrieval_failed` | no | data-model §2. |
| `wallet.boundary` | int ≥ 1 | no | The captured effective `B` used for this classification. |
| `wallet.tx_count` | int ≥ 0 | yes (retrieval failed / not requested) | Nonce of the proxy address. |
| `wallet.age_state` / `wallet.age_basis` / `wallet.age_hours` | `known`/`unknown`; `none`/`polymarket_first_trade`/`chain_first_transaction`; decimal string | `age_hours` null when unknown | Known age requires a basis other than `none`. |
| `wallet.observation_time` | UTC timestamp | yes | The profile's `analyzed_at`. |
| `wallet.retrieval_error` | string ≤ 128 | yes | Error class name only. |
| `wallet.confidence`, `wallet.factors` | 3-decimal string; object name → decimal string | confidence null when no signal **or** `group_status = unqualified_pre_T027` | With `qualified`, unknown age ⇒ `"0.500"`, factors `{"base":"0.5"}`. |
| `size.volume_state`, `size.volume_reason` | enum (8 states); enum of the §1.2 reasons | never null (every state has a reason) | data-model §1.2. |
| `size.volume24hr_present`, `size.volume24hr_raw` | boolean; **any JSON value** | raw null when absent/null | Decoded JSON value, stored by the one rule of data-model §1.1a: verbatim if finite, free of the reserved keys and ≤ 256 canonical UTF-8 bytes; otherwise exactly `{"$nonfinite":"NaN"\|"Infinity"\|"-Infinity"}` (bare non-finite float) or `{"$truncated":true,"type":"string\|number\|array\|object","bytes":<n>,"sha256":"<hex>","reason":"oversize\|reserved_key\|nested_nonfinite"}`. These are the only exceptions to verbatim preservation; the state is decided before bounding (a `$nonfinite` tag is always `invalid`). Examples: `scoring-config-004.1.nonfinite.example.json`, `…truncated.example.json`. |
| `size.daily_volume` | decimal string | null unless `available` | The only denominator. |
| `size.daily_volume_acquired_at`, `size.daily_volume_age_seconds`, `size.daily_volume_source` | UTC timestamp; **signed** decimal string (`-?[0-9]+(\.[0-9]+)?`); string | `acquired_at` and `age` null when no observation exists **or the stamp is unusable** (`absent`, `naive`, `unparseable`, `nonpositive_epoch`; the unusable stamp itself is not stored); `source` null when no observation | Age = `evaluated_at − acquired_at`, exactly, whenever a stamp is stored; `acquired_at` and `age` are null together. A stamp later than `evaluated_at` is stored with its real timestamp and a **negative** age (e.g. `"-1"`) **in whatever state wins** (`invalid_time`/`future`, or `unqualified`, `missing`, `invalid`, `nonpositive` when an earlier precedence row matches first), so the evidence shows how far ahead it was. `invalid_time`/`future` always has a negative age; `stale` and `available` never do; `available` is `0`–`600`. The age is exact to the microsecond with trailing zeros removed (`"599.999999"`, `"-0.000001"`), never a float. An explicit caller volume records `acquired_at = evaluated_at`, age `"0"`. |
| `size.volume_impact` | 4-decimal string (`ROUND_HALF_EVEN`) | **null unless `available`** | `notional / daily_volume` as an exact rational rounded once (data-model §3; no fixed-precision context), so it is computable for every `available` row given the §1.1c bound; never `"0.0000"` for a non-measurement (SC-002). |
| `size.book_state` / `size.book_impact` | const `"unavailable"` / `null` | impact always null | FR-006. |
| `size.niche`, `size.niche_basis`, `size.category`, `size.category_source` | bool; `numeric`/`category`/`none`; string; `metadata`/`minimal` | category null if unknown | data-model §3. |
| `size.signal_emitted`, `size.confidence`, `size.factors` | bool; 3-decimal string; object | confidence null when no signal | |
| `contributions.{fresh_wallet,size_anomaly,niche_market}` | exact decimal strings (confidence × weight, ≤ 5 decimals), `"0"` when absent | no | Pre-bonus terms. |
| `contributions.bonus`, `contributions.capped_score` | `"1"` or `"1.2"`; exact decimal | no | `capped_score = min(sum × bonus, 1)` before quantization. |
| `result.weighted_score`, `result.signals_triggered`, `result.should_alert` | 3-decimal string; int 0–2; bool | no | `weighted_score = quantize(capped_score)`; `should_alert = weighted_score >= alert_threshold`; `signals_triggered` = number of **scoring** groups (wallet counts only with non-null confidence; size counts when `signal_emitted`); bonus `"1.2"` exactly when it is 2. |

Reconstruction (CHK014): every `result` field is recomputable from the rest of the document with
decimal arithmetic alone; `scripts/spec004_evidence/check_envelope.py` performs exactly that and also verifies the volume
cross-field rules (`daily_volume`/`volume_impact` non-null exactly when `available`; age within
0–600 s when `available`; every non-null age equals `evaluated_at − acquired_at` (so a future stamp is negative in any state, and `invalid_time`/`future` must be negative), checked against wrong-magnitude, wrong-sign and dropped-stamp mutations; a numeric string must match the §1.1b grammar; one envelope is built and validated for every replay-live case in **both** wallet postures (004.1 pre-T027, 004.2 qualified; §2a); the pre-T027 wallet carries no confidence, factors or contribution; `signals_triggered` counts scoring groups and the bonus is 1.2 exactly at two; with `qualified`, unknown age ⇒ confidence exactly `0.500`). SC-005 (±0.01) is therefore met
with zero tolerance for stored 004.1 rows.

## 2a. Version identity when the T027 gate opens (round-4 N1)

Opening the gate makes the wallet score and the 1.2 two-group bonus reachable: that changes a
combination rule, and both this contract (§3: "any change to a scoring rule … bumps
`scoring_algorithm_version`") and slice 003 `assessment-storage.md` §4 require a version bump for it.
No exception is made for `group_status`. Therefore:

- **004.1 is exactly the pre-T027 posture.** Its schema fixes `wallet.group_status` to
  `unqualified_pre_T027`; a 004.1 document claiming `qualified` is a schema violation (corrupt row).
- **004.2 is the qualified posture**, and nothing else: the 004.1 schema with `evidence_schema` const
  `"004.2"` and `wallet.group_status` const `"qualified"`. `check_envelope.schema_for` derives it from the
  committed 004.1 schema, validates `scoring-config-004.2.post-t027.example.json` and the 64 post-T027 replay
  envelopes against it, and rejects a 004.2 document carrying the pre-T027 posture or the 004.1 version.
- The 004 implementation writes only 004.1. The reviewed change that cites a T027 PASS receipt and opens
  the gate (tasks T015b/T019b) is the change that starts writing 004.2 and adds its reader. If that change
  also alters any other rule (for example a D1a age source), 004.2 is re-cut to include it before anything
  is written, so one version never names two rule sets.
- Rows already written as 004.1 stay 004.1; nothing is rewritten.

## 3. Reader dispatch

```
read_scoring_config(row):
    version = row.scoring_algorithm_version            # authoritative discriminator
    "legacy-unversioned" -> None                       # column is NULL; never inferred
    "003.1"              -> PureConfig(exactly 5 keys)  # unchanged 003 reader
    "004.1"              -> Document004_1               # schema above; evidence_schema must == "004.1"
    "004.2"              -> Document004_2               # §2a; reader added only by the gate-opening change
    anything else        -> raise UnsupportedScoringVersion   # surfaced, never guessed
```

A mismatch between the column and `evidence_schema`, a schema violation, or a 003.1 row carrying
004.1 keys is a corrupt row: it is reported, not coerced. Within `004.x` readers ignore unknown
keys; any change to a scoring rule, weight, bonus or classification bumps `scoring_algorithm_version`
(and therefore `evidence_schema`, which is defined to equal it in this slice).

## 4. Detector-failure rows

`pipeline.py:637-656` writes `RiskAssessment(delivery_disposition="detector_failure", ...)` with only
`scoring_config`. Under 004.1 that row carries the full document with `evaluation_evidence: null`,
the populated `detector_failures` array (example: `scoring-config-004.1.failure-row.json`) and the same
captured `detector_config`, so a failure row is self-describing without inventing observations.

## 5. Unchanged from 003

Every other 003 §4 rule stays: scalar columns and indexes, downgrade path, `legacy-unversioned`
backfill, weight-compatibility window for the deprecated weights API (custom weights are recorded in
`weights`), quantization of confidences/threshold/score before the decision, and
`RiskScorer.scoring_config` as the pure effective configuration (the per-assessment document is
composed at persist time from the captured assessment; the threshold is captured at assessment
creation, never reread from mutable settings).

## 6. Compatibility risks named

- Direct library consumers that reject unknown keys in `scoring_config` break on 004.1 rows and must
  tolerate the additive keys (pure-config readers keep working; evidence readers opt in).
- `multi_signal_bonus_3` changes value between versions (`"1.3"` in 003.1 rows, `"1"` in 004.1 rows) while
  staying inert; consumers must not compare it across versions.
- The deprecated custom-weight API/window is preserved per 003.
