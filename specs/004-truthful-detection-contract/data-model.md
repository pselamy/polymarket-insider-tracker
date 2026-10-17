# Data Model: Truthful Detection Contract

**Feature**: `specs/004-truthful-detection-contract` | **Date**: 2026-10-17 | **Status**: proposed.
Extends the [003 data model](../003-safe-observable-operation/data-model.md); no competing
migration or harness. Implementation is not authorized.

## 1. Market Volume Observation (new provenance)

| Field | Type | Rule |
|---|---|---|
| `daily_volume` | Decimal, finite, `> 0` | Gamma market `volume24hr`, interpreted as provider USD-denominated monetary market volume over the provider's 24-hour window (R01 disposition in [research.md](research.md)). |
| `daily_volume_acquired_at` | aware UTC datetime | Stamped once at successful HTTP-page body acquisition; carried through `GammaMarketStats`, enrichment, and Redis unchanged. |
| `daily_volume_source` | string | Source identifier (endpoint + revision note). |
| `volume_state` | enum | `available` \| `missing` \| `nonpositive` \| `invalid` \| `stale` \| `invalid_time` \| `unqualified`. Zero, missing, malformed, nonpositive, stale, invalid-time remain distinct; never substituted, never measured zero. |
| Usability | rule | `available` only when finite positive volume has valid aware timestamp with `0 <= evaluated_at - acquired_at <= 600s`. Fixed 600s matches cache TTL; not a provider measurement guarantee. Never synthesize from cache reads, TTL, CLOB refresh, or generic `last_updated`. |

## 2. Wallet Evaluation Evidence (immutable snapshot)

| Field | Type | Rule |
|---|---|---|
| `tx_count` (nonce) | int \| null | Observed prior-transaction count; null when retrieval failed. |
| `age_hours` | Decimal \| null | Known age; null = unknown. |
| `age_state` | enum | `known` \| `unknown`. |
| `classification` | enum | `fresh` (known age ≤ 48h, nonce < 5, notional ≥ 1000) \| `low_activity_unknown_age` (unknown age, nonce < 5; confidence exactly 0.50, no age/new-wallet/large-trade additions) \| `known_nonqualifying` (immutable evidence of a non-qualifying input, distinct from missing) \| `not_requested` \| `retrieval_failed`. |
| `observation_time` | aware UTC | Evidence observation timestamp. |

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
