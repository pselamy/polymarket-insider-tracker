# Live-realistic replay addendum (unknown age, every volume state)

**Created**: 2026-10-19. **Status**: behavioural comparison only; not calibration, not a prediction
of alert volume, not an approval. The original 114-case replay ([replay.md](replay.md)) is untouched.

**Why**: every default-runtime qualification in the original corpus is a known-age case, but current
main has no age source (research R09/R10), so the live runtime produces only unknown-age wallets.

## Reproduce

```bash
cd specs/004-truthful-detection-contract/evidence
uv run python -m scripts.spec004_evidence.replay_live --check replay-live/results.csv      # exit 0 = committed CSV is current
uv run python -m scripts.spec004_evidence.reachability --check reachability.md             # exit 0 = committed table is current
```

Stdlib only; no `src/` import. `scripts/spec004_evidence/replay_live.py` classifies each case's volume observation with the
precedence table of [data-model.md](../data-model.md) §1 (first match wins) and then scores it with the
arithmetic of `scripts/spec004_evidence/reachability.py`, whose constants are textually verified against the product source.
Columns: `old_score_as_wired` = 003.1 rules with the live wiring (no volume passed);
`proposed_score` = 004 rules; `alerts_at_0.80` / `alerts_at_0.78`.

## Coverage

45 cases: unknown age throughout (nonce 0 unless stated), a $3,250 trade unless stated, category
`science` from real metadata unless stated.

| Volume state | Cases |
|---|---|
| `available` | niche saturated, just above ratio, niche-only, not niche (2), JSON number, age 0 s, age 600 s, explicit caller argument, large trade, nonce 4 |
| `stale` | age 601 s |
| `invalid_time` | future, naive, absent, unparseable, epoch-0 stamps |
| `nonpositive` | zero, negative, explicit caller 0 |
| `missing` | JSON null, key absent |
| `invalid` | non-numeric string, boolean, array, `NaN`, `Infinity`, empty string |
| `unqualified` | unknown source id, legacy record without provenance |
| `unavailable` | fetch failed, absent from complete crawl, crawl truncated, CLOB miss path, no volume record, tech category, politics category, metadata lookup failed |
| wallet gate | nonce 5 (= B), notional 999.99 |

## Result

- 0 of 40 qualify at the default 0.80; 7 qualify at 0.78 (all `available`, niche, ratio ≥ 0.06).
- The proposed unknown-age ceiling 0.780 is reached by exactly the saturated niche cases; every
  non-`available` volume state falls back to the category signal and scores 0.384 (niche-prone
  category) or 0.200 (otherwise).
- `live-metadata-lookup-failed`: 003.1 as wired gives 0.480 (category `other` counts as niche);
  the proposed default gives 0.200 because a lookup failure is not category evidence (DECISION D3).
- Deltas against 003.1 are attributable to FR-008 (unknown age 0.50 instead of 0.70 for nonce 0, and no
  large-trade addition), FR-004 (usable volume enables the ratio) and D3 (lookup failure).
