# Data Model: Truthful Detection Contract

**Feature**: `specs/004-truthful-detection-contract` | **Date**: 2026-10-17 | **Status**: proposed.
Extends the [003 data model](../003-safe-observable-operation/data-model.md); no competing
migration or harness. Implementation is not authorized.

## 1. Market Volume Observation (new provenance)

| Field | Type | Rule |
|---|---|---|
| `volume24hr_raw` | `str \| float \| int \| None` (exact provider value, byte-preserved) | Immutable raw Gamma market `volume24hr` observation as received: missing (`None`), zero, negative, non-numeric/malformed string, or numeric. Never coerced, never substituted, never dropped. Malformed/non-numeric input is retained verbatim as evidence of its `invalid` state. |
| `daily_volume` | `Decimal \| None` (usable denominator) | Separate from raw: set only when raw parses to a finite positive value; otherwise `None` with `volume_state` explaining why. This is the only value that may enter `notional / daily_volume`. |
| `daily_volume_acquired_at` | aware UTC datetime \| null | Stamped once at successful HTTP-page body acquisition; carried through `GammaMarketStats`, enrichment, and Redis unchanged. Null when no successful acquisition produced the raw value. |
| `daily_volume_source` | string | Source identifier (endpoint + first-party revision note `49ec9991b7f3e95197a4d53910f6086bf3ff2294`). |
| `volume_state` | enum | `available` \| `missing` \| `nonpositive` \| `invalid` \| `stale` \| `invalid_time` \| `unqualified`. Zero, missing, malformed, nonpositive, stale, invalid-time remain distinct; never substituted, never measured zero. |
| Usability | rule | `available` only when finite positive volume has valid aware timestamp with `0 <= evaluated_at - acquired_at <= 600s`. Fixed 600s matches cache TTL; not a provider measurement guarantee. Never synthesize from cache reads, TTL, CLOB refresh, or generic `last_updated`. |

## 2. Wallet Evaluation Evidence (immutable snapshot)

| Field | Type | Rule |
|---|---|---|
| `tx_count` (nonce) | int \| null | Observed prior-transaction count; null when retrieval failed. |
| `age_hours` | Decimal \| null | Known age; null = unknown. |
| `age_state` | enum | `known` \| `unknown`. |
| `classification` | enum | `fresh` (known age ≤ 48h, `nonce < B`, notional ≥ 1000, where `B` is the captured effective exclusive boundary, default 5) \| `low_activity_unknown_age` (unknown age, `nonce < B`, notional ≥ 1000; confidence exactly 0.50, no age/new-wallet/large-trade additions) \| `known_nonqualifying` (immutable evidence of a non-qualifying input, distinct from missing) \| `not_requested` \| `retrieval_failed`. The `5` is the default of `DETECTOR_FRESH_TRANSACTION_BOUNDARY`, never a hardcoded constant: boundary 3 excludes nonce 3 (`transaction-override-3-3` emits no wallet signal), boundary 10 includes nonce 9 (`transaction-override-10-9` emits a wallet signal). Equality rule: nonce exactly `B` never qualifies. |
| `observation_time` | aware UTC | Evidence observation timestamp. |

### 2a. Canonical freshness rule (governs all surfaces)

The proposed classification in §2 is the single authoritative meaning of “fresh” for
signal JSON (`FreshWalletSignal.to_dict()['wallet_is_fresh']` at `detector/models.py:124`),
assessment evidence, persisted wallet rows (`pipeline.py:671`
`WalletProfileDTO.is_fresh`), cached profiles, and rendered output. Effective configuration
provenance: the captured `DETECTOR_FRESH_TRANSACTION_BOUNDARY` value stored per assessment in
the root `detector_config` member of the serialized per-assessment scoring_config envelope
(§4), a sibling of `evaluation_evidence`; classification always evaluates against that
captured `B`, never against a reread mutable setting.

Legacy interpretation (explicit compatibility exception, not current measurement): the
existing library surfaces `profiler/analyzer.py:210–229` (`_is_wallet_fresh`, unknown age →
`True`), `WalletProfile.is_fresh`, and `pipeline.py:671` retain their earlier meaning on old
rows and unamended code paths. They MUST NOT be presented as the new known-age
classification. Old rows keep version-specific reader semantics (§4); unknown-age
`is_fresh=True` flags on those rows are legacy facts, not measured known-age claims.

Configured-boundary-10 mismatch closed: when the effective boundary is 10, the default
analyzer rule would mark nonce 9 non-fresh while the new detector correctly classifies a
known-age nonce-9 candidate as `fresh`. The joint regression (tasks T009c) asserts the
classified signal, JSON flag, persisted row, rendered label, confidence `≤ 0.50` for
unknown age, no age bonus, and listed factors together — for both the unknown-age case and
the boundary-10 override case.

## 3. Size Evaluation Evidence (immutable snapshot)

| Field | Type | Rule |
|---|---|---|
| `notional` | Decimal | Trade's actual notional value; always wired into size analysis. |
| `volume_ratio` | Decimal \| null | `notional / daily_volume` (±0.0001, SC-002) when usable; null with `volume_state` otherwise — never serialized as measured zero. |
| `book_state` | enum | `unavailable` for this slice (book-depth scoring disabled until separately approved). |
| `niche` | bool + basis | True when usable volume < 50000 (numeric basis) or via recorded category fallback (`science/tech/finance/other`) when volume unavailable; niche is a size modifier, not a third detector invocation. |

## 4. Assessment Envelope (versioned amendment of shared 003 contract)

Preserve all 003 root keys/types/columns. Proposed algorithm/evidence identity `004.1`:

- `detector_config`: static effective configuration snapshot (threshold, boundary, volume-ratio threshold).
- `evaluation_evidence`: versioned per-assessment envelope — exact source values, acquisition/evaluation times, availability reasons, factors, contributions, threshold.
- Evidence-schema identity recorded alongside `scoring_algorithm_version`.
- `RiskScorer.scoring_config` stays pure effective configuration (threshold, bonuses, quantum, weights as decimal strings, sorted keys); the extended envelope is serialized per assessment.
- Root `multi_signal_bonus_3` retained as documented-neutral structural compatibility key; unreachable operational branch removed. Two-group 1.2 bonus, weights 0.40/0.35/0.25, 0.001 ROUND_HALF_EVEN precision retained.
- Old `003.1` and legacy-unversioned rows untouched with version-specific reader semantics.

## 5. Threshold & Score Rules

Default alert threshold 0.80 everywhere (config, docs, persisted rows, skill). Inclusive `>=`
comparison at 0.001 quantum. Effective threshold recorded per assessment. Exactly two
operational signal groups (fresh/low-activity wallet, size anomaly); reachable two-group bonus
only; 3+ bonus removed from the operational contract. Score reconstructs from shown evidence
within rounding tolerance (SC-004/005).
