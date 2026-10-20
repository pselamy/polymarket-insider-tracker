# Live-realistic replay addendum (unknown age, every volume state)

**Created**: 2026-10-19. **Status**: behavioural comparison only; not calibration, not a prediction
of alert volume, not an approval. The original 114-case replay ([replay.md](replay.md)) is untouched.

**Why**: every default-runtime qualification in the original corpus is a known-age case, but current
main has no age source (research R09/R10), so the live runtime produces only unknown-age wallets.

## Reproduce

From the repository root:

```bash
uv run python -m scripts.spec004_evidence.replay_live --check specs/004-truthful-detection-contract/evidence/replay-live/results.csv   # exit 0 = committed CSV is current
uv run python -m scripts.spec004_evidence.reachability --check specs/004-truthful-detection-contract/evidence/reachability.md         # exit 0 = committed table is current
```

Stdlib only; no `src/` import. `scripts/spec004_evidence/replay_live.py` classifies each case's volume observation with the
precedence table of [data-model.md](../data-model.md) §1 (first match wins) and then scores it with the
arithmetic of `scripts/spec004_evidence/reachability.py`, whose constants are textually verified against the product source.
Columns: `old_score_as_wired` = 003.1 rules with the live wiring (no volume passed);
`groups_pre_T027` / `score_pre_T027` / `alerts_pre_T027_at_{0.80,0.78}` = 004 rules in the **default
pre-T027 posture** (FR-007c: wallet group non-scoring, no two-group bonus);
`groups_post_T027` / `score_post_T027` / `alerts_post_T027_at_{0.80,0.78}` = 004 rules once T027 has
qualified the wallet group; `wallet_confidence_post_T027` is the wallet confidence that would score
after T027 (pre-T027 it is recorded as an observation only).

## Coverage

64 cases: unknown age throughout (nonce 0 unless stated), a $3,250 trade unless stated, category
`science` from real metadata unless stated.

| Volume state | Cases |
|---|---|
| `available` | niche saturated, just above ratio, niche-only, not niche (2), JSON number, age 0 s, 0.5 s, 599.999999 s and 600 s, explicit caller argument, large trade, nonce 4, volume `"0.000001"` (the §1.1c minimum; ratio 3250000000.0000 computed exactly) |
| `stale` | age 600.000001 s, 600.5 s and 601 s |
| `invalid_time` | future (−1 s and −0.000001 s), naive, absent, unparseable, epoch-0 stamps |
| `nonpositive` | zero, negative, explicit caller 0, zero with a future stamp (negative age stored) |
| `missing` | JSON null, key absent, JSON null with a future stamp (negative age stored) |
| `invalid` | non-numeric string, boolean, array, `NaN`, `Infinity`, empty string, bare `NaN`/`-Infinity` tokens (`$nonfinite` tag), nested non-finite, oversize string, reserved key (truncation markers), `"1_000"` and `" 30000"` (fail the data-model §1.1b grammar), non-numeric string with a future stamp (negative age stored); JSON number `1e400` (overflows to `inf` on decode, so the `$nonfinite` tag); `out_of_range` (data-model §1.1c, DECISION D4 default): JSON number `1e-30`, strings `"1e-30"`, `"1e400"`, `"1e999999999"` and a 33-character mantissa, the 300-digit JSON integer (stored as a `number` truncation marker; `available` before round 4), explicit caller `1e-30` |
| `unqualified` | unknown source id, legacy record without provenance |
| `unavailable` | fetch failed, absent from complete crawl, crawl truncated, CLOB miss path, no volume record, tech category, politics category, metadata lookup failed |
| wallet gate | nonce 5 (= B), notional 999.99 |

## Result

- **Default pre-T027 posture**: 0 of 64 qualify at 0.80 and 0 at 0.78; the maximum is 0.450, reached
  by exactly the saturated niche cases. Every non-`available` volume state falls back to the category
  signal and scores 0.120 (niche-prone category) or 0.000 (otherwise).
- **Post-T027 scenario**: 0 of 64 qualify at 0.80; 10 qualify at 0.78 (all `available`, niche, ratio ≥
  0.06). The unknown-age ceiling 0.780 is reached by exactly the saturated niche cases; non-`available`
  states score 0.384 (niche-prone category) or 0.200 (otherwise).
- `live-metadata-lookup-failed`: 003.1 as wired gives 0.480 (category `other` counts as niche);
  the proposed default gives 0.000 pre-T027 (0.200 post-T027) because a lookup failure is not category
  evidence (DECISION D3).
- Ages are exact to the microsecond (an age is `evaluated_at − acquired_at`, both aware datetimes):
  599.999999 s and 600 s are `available`, 600.000001 s is `stale`, −0.000001 s is `invalid_time`/`future`.
- Round-4 correction: `live-available-bigint-300-digits` is renamed `live-invalid-bigint-300-digits`;
  under the §1.1c bound it is `invalid`/`out_of_range` and scores the category fallback (0.120 pre-T027,
  0.384 post-T027) instead of 0.000/0.200. No other existing row changed.
- Deltas against 003.1 are attributable to FR-008 (unknown age 0.50 instead of 0.70 for nonce 0, and no
  large-trade addition), FR-004 (usable volume enables the ratio) and D3 (lookup failure).
