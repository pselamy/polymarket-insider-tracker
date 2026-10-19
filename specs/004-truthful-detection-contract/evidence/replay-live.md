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

51 cases: unknown age throughout (nonce 0 unless stated), a $3,250 trade unless stated, category
`science` from real metadata unless stated.

| Volume state | Cases |
|---|---|
| `available` | niche saturated, just above ratio, niche-only, not niche (2), JSON number, age 0 s, age 600 s, explicit caller argument, large trade, nonce 4, 300-digit JSON integer (stored as a `number` truncation marker; not niche, so no size signal) |
| `stale` | age 601 s |
| `invalid_time` | future, naive, absent, unparseable, epoch-0 stamps |
| `nonpositive` | zero, negative, explicit caller 0, zero with a future stamp (negative age stored) |
| `missing` | JSON null, key absent, JSON null with a future stamp (negative age stored) |
| `invalid` | non-numeric string, boolean, array, `NaN`, `Infinity`, empty string, bare `NaN`/`-Infinity` tokens (`$nonfinite` tag), nested non-finite, oversize string, reserved key (truncation markers), `"1_000"` and `" 30000"` (fail the data-model §1.1b grammar), non-numeric string with a future stamp (negative age stored) |
| `unqualified` | unknown source id, legacy record without provenance |
| `unavailable` | fetch failed, absent from complete crawl, crawl truncated, CLOB miss path, no volume record, tech category, politics category, metadata lookup failed |
| wallet gate | nonce 5 (= B), notional 999.99 |

## Result

- **Default pre-T027 posture**: 0 of 51 qualify at 0.80 and 0 at 0.78; the maximum is 0.450, reached
  by exactly the saturated niche cases. Every non-`available` volume state falls back to the category
  signal and scores 0.120 (niche-prone category) or 0.000 (otherwise).
- **Post-T027 scenario**: 0 of 51 qualify at 0.80; 7 qualify at 0.78 (all `available`, niche, ratio ≥
  0.06). The unknown-age ceiling 0.780 is reached by exactly the saturated niche cases; non-`available`
  states score 0.384 (niche-prone category) or 0.200 (otherwise).
- `live-metadata-lookup-failed`: 003.1 as wired gives 0.480 (category `other` counts as niche);
  the proposed default gives 0.000 pre-T027 (0.200 post-T027) because a lookup failure is not category
  evidence (DECISION D3).
- Deltas against 003.1 are attributable to FR-008 (unknown age 0.50 instead of 0.70 for nonce 0, and no
  large-trade addition), FR-004 (usable volume enables the ratio) and D3 (lookup failure).
