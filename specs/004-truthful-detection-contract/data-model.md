# Data Model: Truthful Detection Contract

**Feature**: `specs/004-truthful-detection-contract` | **Date**: 2026-10-17, revised 2026-10-19 | **Status**: proposed.
Extends the [003 data model](../003-safe-observable-operation/data-model.md); no competing
migration or harness. Implementation is not authorized.

## 1. Market Volume Observation (new provenance; revised 2026-10-19)

One **observation** is one market object received in one successful Gamma page response. It is the
only thing that can make a volume usable, and it is stored on its own (see §1.4), never inside a
record that a CLOB-only path can rewrite.

### 1.1 Fields

| Field | Type | Rule |
|---|---|---|
| `volume24hr_present` | bool | True when the received market object had a `volume24hr` key. Distinguishes an absent key from JSON `null`. |
| `volume24hr_raw` | JSON value (`null`, bool, number, string, array or object) | The **decoded** JSON value as it arrived, kept **semantically**: the original bytes (whitespace, number spelling such as `1e3` vs `1000`) are not recoverable because the client decodes with `response.json()` (`ingestor/gamma_client.py:141`), and no artifact may claim otherwise. Stored so that `5`, `5.0` and `"5"` stay distinct; never coerced or substituted; malformed values are kept as evidence of `invalid`. `null` when absent or null. **One storage rule (§1.1a) bounds it; the two exceptions below are the only ways it is not verbatim.** |
| `daily_volume` | `Decimal \| null` | The usable denominator: set only in state `available`, equal to the finite positive parse of `volume24hr_raw`. This is the only value allowed in `notional / daily_volume`. |
| `daily_volume_acquired_at` | aware UTC datetime \| null | The instant the page's response body was received by `GammaClient` (read from an injectable clock immediately after the HTTP response completes, before JSON decoding). Carried unchanged through the crawl result, the volume record and Redis; a cache read never restamps it. Null when no observation exists or the stamp is unusable. The envelope stores the **signed** age `evaluated_at - acquired_at` whenever a stamp is stored: a stamp later than `evaluated_at` keeps its real timestamp and a **negative** age **in every state** (it is the reason `future` only in P6; in P2–P5 the earlier row wins and the negative age is still recorded as evidence); the other unusable stamps (`absent`, `naive`, `unparseable`, `nonpositive_epoch`) store null for both (contracts/scoring-config.md). |
| `daily_volume_source` | string \| null | Source identifier, e.g. `gamma-markets-v1` (endpoint + first-party revision note `49ec9991b7f3e95197a4d53910f6086bf3ff2294`); `caller` for an explicit argument (§1.3). Null for legacy records. A source is **qualified** iff it is in the code-defined qualified set, initially `{gamma-markets-v1, caller}` (set membership is the T018 receipt). |
| `volume_state` | enum (8 values) | Exactly one of `available`, `unavailable`, `unqualified`, `missing`, `invalid`, `nonpositive`, `invalid_time`, `stale`; assigned by the precedence table in §1.2. |
| `volume_reason` | enum | Sub-reason, recorded for diagnosis, never a separate state (§1.2 last column). |

### 1.1a Raw-value storage rule (the only exceptions to verbatim preservation)

A decoded `volume24hr` value is stored **verbatim** when all of these hold: it contains no non-finite
float, it is not an object with a top-level `$truncated` or `$nonfinite` key, and its canonical JSON
(`sort_keys`, `(",", ":")` separators, UTF-8) is at most **256 bytes**. Otherwise the stored value is
exactly one of two typed markers, applied in this order:

1. **Non-finite scalar** (Python's `json` accepts bare `NaN`, `Infinity`, `-Infinity` on decode, and
   canonical JSON cannot represent them): `{"$nonfinite": "NaN" | "Infinity" | "-Infinity"}`. The
   *string* `"NaN"` is an ordinary string and stays verbatim.
2. **Truncation marker** `{"$truncated": true, "type": "string|number|array|object", "bytes": <canonical length>,
   "sha256": <hex of the canonical bytes>, "reason": "nested_nonfinite|reserved_key|oversize"}`:
   `nested_nonfinite` (a non-finite float inside an array/object; the canonical text then uses Python's
   `NaN`/`Infinity` tokens for the hash), `reserved_key` (so a stored marker is never ambiguous with a
   genuine object), `oversize` (> 256 bytes; this includes a JSON integer of more than 256 digits, whose
`type` is `number`). `type`, `bytes` and `sha256` are measured on the original.

The state is decided **before** bounding, from the decoded value, so a marker never changes a state:
a `$nonfinite` tag is always `invalid` (reason `not_a_finite_number`); a truncated array/object is
`invalid`; a truncated string or number follows the table (a long non-numeric string is `invalid`; a
300-digit positive integer is a finite positive number, so it reaches §1.1c and is `invalid`/`out_of_range`
under the D4 default bound, not because it was truncated). The marker
loses the original content by design; the sha256 and length let an operator confirm a candidate value
without storing it. Schema: `#/$defs/raw_nonfinite`, `#/$defs/raw_truncated`; executable form:
`scripts/spec004_evidence/replay_envelopes.py::bound_raw`; validated examples:
`contracts/scoring-config-004.1.nonfinite.example.json`, `…truncated.example.json`.

### 1.1b Numeric-string grammar

A JSON **number** is numeric by type (Python's `json` has already decoded it; only finiteness and sign
matter). A JSON **string** is a numeric string iff the *whole* string matches the RFC 8259 §6 number
token, ASCII only:

```text
-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?
```

So `"30000"`, `"0.5"`, `"-5"`, `"1e3"` are numeric; `"1_000"`, `" 30000"` (any surrounding or inner
whitespace), `"+5"`, `".5"`, `"5."`, `"007"`, `"0x10"`, `"١٢"` (non-ASCII digits), `"NaN"`, `"Infinity"`
and `""` are **not**, and the observation is `invalid` (`not_a_finite_number`). A string that matches is
converted with `Decimal(text)` and then follows P5–P8. This is deliberately narrower than current
main, whose `_to_decimal` (`ingestor/gamma_client.py:53-59`) passes the text to `Decimal(str(value))`,
which accepts `_` separators, surrounding whitespace, a leading `+` and `NaN`/`Infinity` spellings (it
returns `None` only for `None`, `""` or a conversion error). Executable form:
`scripts/spec004_evidence/replay_live.py::NUMERIC_STRING`; replay cases `live-invalid-underscore-string`,
`live-invalid-whitespace-string`.

**Overflow (number vs string).** A JSON *number* token whose value exceeds the float range, e.g.
`1e400`, is finite as text but Python's `json` decodes it to `inf`; the decoded value is what this
contract classifies, so it is stored as `{"$nonfinite":"Infinity"}` and is `invalid`/
`not_a_finite_number` (§1.1a rule 1 applies to the decoded float, whatever token produced it). The
*string* `"1e400"` matches the grammar and converts to a finite `Decimal`, so it reaches §1.1c and is
`invalid`/`out_of_range`. Replay cases `live-invalid-overflow-number-1e400`,
`live-invalid-out-of-range-1e400-string`.

### 1.1c Usable-magnitude bound (DECISION D4; this is the packet's recommended default)

A finite **positive** value is usable as a denominator only when `10^-6 <= value < 10^15` **and** its
plain-notation form (`format(value, "f")`) is at most 32 characters. Otherwise it is `invalid` with reason
`out_of_range` (P4): it is never formatted, never stored as `daily_volume` and never divided. The bound
keeps every stored decimal short and every ratio finite: without it, the grammar-valid string
`"1e999999999"` would be `available` and its plain-notation `daily_volume` would be a billion digits, and
`"1e-30"` would give a ratio of 3.25 × 10^33 that a 28-digit decimal context cannot quantize (round-4 N3).
The bound applies equally to an explicit caller argument (§1.3). It is a positivity-side rule only:
zero and negative values stay `nonpositive` (P5), so the two rows are disjoint. Replay cases:
`live-invalid-out-of-range-*`, `live-invalid-bigint-300-digits`, `live-explicit-arg-out-of-range`,
`live-available-volume-at-minimum`. Executable form: `replay_live.py::in_range`.

`evaluated_at` is the single evaluation instant injected into the evaluation (contracts/detector-api.md
§Evaluation clock). Age is `evaluated_at - daily_volume_acquired_at`, computed exactly from the two aware
datetimes (microsecond resolution, no float) and stored as a plain signed decimal with trailing zeros
removed (`"30"`, `"599.999999"`, `"-0.000001"`). The P7/P8 comparisons use that exact value, so 600 s is
`available` and 600.000001 s is `stale` (replay cases `live-available-age-0.5`,
`live-available-age-599.999999`, `live-stale-age-600.000001`, `live-stale-age-600.5`,
`live-invalid-time-future-microsecond`).

### 1.2 Precedence table (mutually exclusive, exhaustive; first matching row wins)

| # | State | Condition (evaluated top to bottom) | `volume_reason` values | Representation |
|---|---|---|---|---|
| P1 | `unavailable` | No observation exists for this market at evaluation time, or an explicit `daily_volume` argument was not given and no volume record can be read | `gamma_fetch_failed` (whole Gamma fetch failed), `absent_from_complete_crawl` (crawl completed, market not returned — outside the top-5,000 window or not active), `crawl_truncated` (a page failed, so absence cannot be told from truncation), `clob_miss_path` (metadata came from the CLOB cache-miss path, no Gamma data), `no_volume_record` (volume record expired or never written), `metadata_lookup_failed` (minimal metadata, no volume lookup possible) | `present=false`, raw null, `acquired_at` null, `source` null, `daily_volume` null |
| P2 | `unqualified` | An observation exists but its `daily_volume_source` is not in the qualified set | `unknown_source` (id not qualified), `no_provenance` (legacy pre-004 record: value possibly present, no source/stamp) | fields as stored; value is **not interpreted** (not parsed, not called invalid) |
| P3 | `missing` | Qualified observation; `volume24hr_present` is false or the raw value is JSON `null` | `null_or_absent` | `present` false/true, raw null |
| P4 | `invalid` | Raw present and non-null but not a finite number: a string that is not a numeric string (§1.1b; including the empty string and the strings `"NaN"`/`"Infinity"`), boolean, array, object, or a bare non-finite float (`NaN`, `±Infinity` tokens, or a number token that overflows to `inf` on decode); **or** a finite positive value outside the §1.1c bound | `not_a_finite_number`, `out_of_range` (§1.1c) | raw stored by the §1.1a rule: verbatim when it qualifies; a bare non-finite float as the `$nonfinite` tag; an oversize, reserved-key or nested-non-finite value as the truncation marker |
| P5 | `nonpositive` | Raw parses to a finite value `<= 0`. **Zero is a nonpositive value, not a separate state and never `unavailable`**; the raw value distinguishes `0` from a negative | `zero`, `negative` | raw kept; no ratio |
| P6 | `invalid_time` | Finite positive value but the stamp is unusable: absent, naive (no UTC offset), unparseable, epoch `<= 0`, or later than `evaluated_at` | `absent`, `naive`, `unparseable`, `nonpositive_epoch`, `future` | raw kept; no ratio |
| P7 | `stale` | Finite positive value, valid stamp, `evaluated_at - acquired_at > 600 s` | `older_than_600s` | raw kept; no ratio |
| P8 | `available` | Finite positive value, valid stamp, `0 <= evaluated_at - acquired_at <= 600 s` (both ends inclusive) | `gamma` or `caller` | `daily_volume` set; ratio computed |

Properties: every input maps to exactly one row (P1–P8 partition the space); the state is a pure function
of (observation, qualified set, `evaluated_at`); the fixed 600 s matches the cache TTL and is a local
recency rule, not a provider measurement-time guarantee; the stamp is never synthesized from cache
reads, TTL, CLOB refresh or generic `last_updated`
(`ingestor/models.py::MarketMetadata.from_dict` falls back to *now*). Executable form and one
replay case per state and per reason: `scripts/spec004_evidence/replay_live.py`, `evidence/replay-live/cases.json`
(64 cases; every state and every reason, the §1.1a raw-value markers, the §1.1b grammar, the §1.1c bound, fractional ages at the 0 s / 600 s edges, and a future
stamp under P3, P4 and P5).

### 1.3 Explicit `daily_volume` argument

`SizeAnomalyDetector.analyze(trade, daily_volume=...)` and `analyze_batch(volume_data=...)` keep their
signatures (`detector/size_anomaly.py:93-99,369-389`). A caller-supplied value is a caller assertion
without provenance: a positive `Decimal` inside the §1.1c bound is `available` with
`daily_volume_source = "caller"`, `volume_reason = "caller"` and `acquired_at = evaluated_at`; a positive
value outside the bound is `invalid`/`out_of_range` (same source and stamp); zero or negative is `nonpositive`
(reason `zero`/`negative`); `None` means "no argument" and falls through to the stored observation.
An explicit value overrides the stored observation. Persisted evidence records `source = "caller"` so it
can never be mistaken for Gamma data. The live pipeline never passes it.

### 1.4 Storage and freshness (what makes the 0-600 s rule hold)

- The volume observation is stored under its **own** Redis key per market (volume record), written
  only by the Gamma refresh path, with a TTL of 1,200 s (2 x the recency rule) so an old value remains
  readable as `stale` instead of vanishing. A CLOB-only path (`get_market` miss) never writes a volume
  claim and never overwrites a volume record; it writes the CLOB metadata record with set-if-absent so it
  cannot clobber a record a concurrent sync just wrote.
- The Gamma refresh runs on its own cadence, decoupled from the CLOB crawl. See research R02a for the
  arithmetic: the current coupled loop makes the worst-case entry age `2D + w + S - g`
  (D = CLOB crawl duration, w = batch write, S = sleep 300 s, g = Gamma stamp offset), which exceeds 600 s
  whenever `2D + w - g > 300`; the decoupled loop makes it `S_g + 2g + w_g`.
- `GammaClient` page results carry `(page_index, acquired_at, items)` through aggregation; a duplicate
  condition id keeps the observation with the later `acquired_at` (ties: lower page index). The crawl
  result also reports `pages_ok`, `pages_failed`, `truncated` and start/finish times so P1 reasons can be
  assigned. A failed or partial fetch never refreshes an older observation's time.

## 2. Wallet Evaluation Evidence (immutable snapshot)

| Field | Type | Rule |
|---|---|---|
| `subject_kind` | enum `proxy_wallet` | The profiled address is the V2 `proxy_wallet` (`TradeEvent.wallet_address`). Always `proxy_wallet` in this slice; any future EOA resolution needs its own spec (FR-007a). |
| `tx_count` (nonce) | int \| null | Observed prior-transaction count **of the proxy address**; null when retrieval failed. Never described as the participant's history. |
| `age_hours` | Decimal \| null | Known age; null = unknown. Null whenever `age_basis` is `none`. |
| `age_basis` | enum | `none` \| `polymarket_first_trade` \| `chain_first_transaction` (FR-007b). `none` is the only value current main can produce (`profiler/chain.py:485-532` returns no first transaction for any wallet). A known age without a basis is invalid evidence. |
| `age_state` | enum | `known` (basis ≠ `none`) \| `unknown`. |
| `classification` | enum | `fresh` (known age ≤ 48h, `nonce < B`, notional ≥ 1000, where `B` is the captured effective exclusive boundary, default 5) \| `low_activity_unknown_age` (unknown age, `nonce < B`, notional ≥ 1000; confidence exactly 0.50, see the unknown-age table below) \| `known_nonqualifying` (immutable evidence of a non-qualifying input, distinct from missing) \| `not_requested` \| `retrieval_failed`. The `5` is the default of `DETECTOR_FRESH_TRANSACTION_BOUNDARY`, never a hardcoded constant: boundary 3 excludes nonce 3 (`transaction-override-3-3` emits no wallet signal), boundary 10 includes nonce 9 (`transaction-override-10-9` emits a wallet signal). Equality rule: nonce exactly `B` never qualifies. |
| `group_status` | enum `unqualified_pre_T027` \| `qualified` | Spec FR-007c, the contract's default posture. `unqualified_pre_T027` (every assessment until the T027 receipt exists): `classification`, `tx_count` and the age fields are still recorded as observations, but the wallet group is **non-scoring**: `confidence` null, `factors` empty, wallet contribution `0`, not counted in `signals_triggered`, and the 1.2 two-group bonus never applies (ceiling 0.450). `qualified`: set only by the reviewed change that cites a T027 receipt showing the nonce tracks trading; the unknown-age table below then scores. Not an environment knob. |
| `observation_time` | aware UTC | The wallet profile's `analyzed_at` (`profiler/models.py:96`): when the nonce/first-seen values were obtained, **not** when the detector read them. A cached profile keeps its original `analyzed_at`; a cache read never restamps it. Evidence records both `observation_time` and the trade-evaluation time so a reviewer can see the gap. |

### Unknown-age rule (single normative table; spec FR-008)

The confidences below are the scoring rule once `group_status = qualified`. With
`group_status = unqualified_pre_T027` the same classification is recorded but carries no confidence and
no contribution (row above).

| Input | Result |
|---|---|
| `age_state = unknown`, `nonce < B`, notional ≥ 1000 | class `low_activity_unknown_age`; confidence **exactly 0.50**; factors `{base: 0.5}` only |
| …and nonce = 0 | still exactly 0.50 (no brand-new addition) |
| …and notional > 10000 | still exactly 0.50 (no large-trade addition) |
| `age_state = unknown`, `nonce >= B` or notional < 1000 | `known_nonqualifying` (the class name means "evaluated, input did not qualify", whatever the age state) / no signal; never promoted |
| profile retrieval failed | `retrieval_failed`; no freshness fact; no signal |
| required label | “low-activity wallet, age unknown” on the proxy address; never “new”, “fresh”, “under 48 hours” |

"Capped at 0.50" is retired wording: no artifact may permit a value other than 0.50. The
reviewer-owned CHK005 text still says "cap 0.50"; it is left untouched for the reviewer and is read as
this table.

### 2a. Canonical freshness rule (governs all surfaces)

The proposed classification in §2 is the single authoritative meaning of “fresh” for
signal JSON (`FreshWalletSignal.to_dict()['wallet_is_fresh']` at `detector/models.py:124`),
assessment evidence, persisted wallet rows (`pipeline.py:680`
`WalletProfileDTO.is_fresh`), cached profiles, and rendered output. Effective configuration
provenance: the captured `DETECTOR_FRESH_TRANSACTION_BOUNDARY` value stored per assessment in
the root `detector_config` member of the serialized per-assessment scoring_config envelope
(§4), a sibling of `evaluation_evidence`; classification always evaluates against that
captured `B`, never against a reread mutable setting.

Legacy interpretation (explicit compatibility exception, not current measurement): the
existing library surfaces `profiler/analyzer.py:223–242` (`_is_wallet_fresh`, unknown age →
`True`), `WalletProfile.is_fresh`, and `pipeline.py:680` retain their earlier meaning on old
rows and unamended code paths. They MUST NOT be presented as the new known-age
classification. Old rows keep version-specific reader semantics (§4); unknown-age
`is_fresh=True` flags on those rows are legacy facts, not measured known-age claims.

Configured-boundary-10 mismatch closed: when the effective boundary is 10, the default
analyzer rule would mark nonce 9 non-fresh while the new detector correctly classifies a
known-age nonce-9 candidate as `fresh`. The joint regression (tasks T009c) asserts the
classified signal, JSON flag, persisted row, rendered label, confidence exactly `0.50` for
unknown age, no age bonus, and listed factors together — for both the unknown-age case and
the boundary-10 override case.

## 3. Size Evaluation Evidence (immutable snapshot)

| Field | Type | Rule |
|---|---|---|
| `notional` | Decimal | Trade's actual notional value; always wired into size analysis. |
| `volume_impact` (a.k.a. ratio) | Decimal \| null | `notional / daily_volume` quantised to 0.0001 (`ROUND_HALF_EVEN`, SC-002 ±0.0001) **only in `volume_state = available`**. Computed as the **exact** rational quotient rounded once (e.g. `fractions.Fraction`); a fixed-precision decimal division followed by `quantize` does not conform (Python's default 28-digit context raises `InvalidOperation` once the ratio needs more than 28 digits, and a division rounded before the `quantize` can round twice); with the §1.1c bound every `available` row is computable (executable form `replay_envelopes.py::exact_ratio`, case `live-available-volume-at-minimum`); null in every other state, never serialized as measured zero. Surfaces as `volume_impact_measured: Decimal \| None` (contracts/detector-api.md §4). |
| `book_state` | enum | `unavailable` for this slice (book-depth scoring disabled until separately approved); `book_impact` always null. |
| `niche`, `niche_basis` | bool + enum `numeric` \| `category` \| `none` | Niche is a size modifier, not a third detector invocation. **Numeric basis**: `volume_state = available` and `daily_volume < 50000`. **Category basis** (only when volume is not `available`): category in `science/tech/finance/other` **and** `category_source = metadata`. `none` otherwise. A volume that is `available` and `>= 50000` is not niche regardless of category. |
| `category`, `category_source` | string \| null; enum `metadata` \| `minimal` | `minimal` means the market metadata lookup failed and `SizeAnomalyDetector._create_minimal_metadata` (`detector/size_anomaly.py:211-227`) supplied the placeholder category `"other"`. A placeholder is not evidence: with `minimal` there is no category basis (default of DECISION D3; current main treats it as niche-prone, which the replay shows as 0.480 vs 0.200). |
| `signal_emitted` | bool | Existing rule: confidence `>= 0.1` and, for niche-only, notional `>= 500`. |

Behavior under non-`available` volume: no ratio; the size signal can arise only from the category basis
(confidence 0.2 niche-only); every state in data-model §1.2 other than `available` behaves identically
here (replay: `live-nonpositive-*`, `live-stale-*`, `live-missing-*`, ... all 0.120 for a niche-prone
category in the default pre-T027 posture, 0.384 post-T027).

## 4. Assessment Envelope (versioned amendment of shared 003 contract)

The exact schema, example, decimal encoding, nullability, reader dispatch and the detector-failure row
live in [contracts/scoring-config.md](contracts/scoring-config.md) and its JSON Schema; this section
only records the rules the schema cannot express:

- Identity `004.1`: `scoring_algorithm_version` column = `evidence_schema` (root-level key of the
  `scoring_config` document, a sibling of `evaluation_evidence`, not nested inside it).
- `RiskScorer.scoring_config` stays the pure effective configuration; the per-assessment document adds
  `detector_config` (no threshold: the threshold lives only in root `alert_threshold`),
  `detector_failures` and `evaluation_evidence` at persist time.
- `multi_signal_bonus_3` is retained as an inert key with value `"1"` in 004.1 documents; the
  unreachable operational branch is removed. Two-group 1.2 bonus, weights 0.40/0.35/0.25 and 0.001
  `ROUND_HALF_EVEN` precision are retained.
- Old `003.1` and legacy-unversioned rows are untouched with version-specific reader semantics.
- This is a semantic amendment of the 003 field (a version-dispatched union), stated as such in
  scoring-config.md §1; it is not described as "preserving" the 003 definition.

## 5. Threshold & Score Rules

Default alert threshold 0.80 everywhere (config, docs, persisted rows, skill). Inclusive `>=`
comparison at 0.001 quantum. Effective threshold recorded per assessment. Exactly two
defined signal groups (fresh/low-activity wallet, size anomaly); in the default pre-T027 posture
(FR-007c, `group_status = unqualified_pre_T027`) only the size group scores and the bonus never
applies (ceiling 0.450); reachable two-group bonus only; 3+ bonus removed from the operational contract. Score reconstructs from shown evidence
within rounding tolerance (SC-004/005).
