---
cluster: 1
status: resolved
resolved-by: PR #164 (partial), dedup cluster 1 card (rest)
watch: tests/alerter/test_formatter.py <-> tests/detector/test_scorer.py, tests/alerter/test_formatter.py <-> tests/alerter/test_history.py, tests/detector/test_scorer.py <-> tests/detector/test_size_anomaly.py, tests/alerter/test_history.py <-> tests/alerter/test_history.py
---

# Cluster 1

- **Files:** test_formatter.py, test_history.py, test_scorer.py, test_size_anomaly.py
- **Knowledge:** Repeated `sample_trade`/`sample_wallet_profile` fixtures and formatter/history case scaffolds (two fixture families share one component)
- **Verdict:** real
- **Counts when first grouped (historical):** 11 clones, 196 duplicated lines. Current counts are generated, see [the README](../README.md).

## 2026-10-19 partial resolution update: cluster 1 (sample_trade/sample_wallet_profile fixtures)

Measured with the pinned jscpd 5.3.3 through the vendored
`quality.duplication.scan(..., minimum=50)` over `quality.source_scope.discover`
(the size check is skipped because `scripts/traceability.py` already exceeds 399
lines), on identical enumeration at both ends. Baseline is `origin/main` at
`370e77b` (after #163): **74 groups, 909 duplicated lines, 1.49%**, 258 sources
tokenized. This card rebased onto it: **72 groups, 858 duplicated lines, 1.40%**,
259 sources. No new group appeared: matched by token count, line count and file
pair, the only differences are the two removed groups below.

Removed groups: the identical `sample_trade`/`sample_wallet_profile` pair that
`test_history.py` and `test_scorer.py` each carried (153 tokens, 39 lines) now has
one home in `tests/conftest.py`, pinned by `tests/test_sample_fixtures.py`; and the
formatter-internal `TradeEvent` field-by-field copy (68 tokens, 12 lines), now
`dataclasses.replace`.

Cluster 1 is **not fully resolved**. Seven cluster-1 groups remain on purpose
(tokens/lines, pair):

| tokens | lines | A | B |
| --- | --- | --- | --- |
| 118 | 31 | test_formatter.py | test_scorer.py |
| 59 | 17 | test_formatter.py | test_scorer.py |
| 55 | 17 | test_formatter.py | test_history.py |
| 50 | 15 | test_formatter.py | test_history.py |
| 57 | 12 | test_scorer.py | test_size_anomaly.py |
| 51 | 6 | test_scorer.py | test_scorer.py |
| 50 | 7 | test_history.py | test_history.py |

They share a shape but encode different test knowledge (different `factors`,
question text, prices, sizes and ages), so sharing them would change behaviour or
need a parameterised factory that merely hides the difference. The formatter and
size-anomaly `sample_trade` fixtures likewise differ in value and stay local.

## 2026-10-21 resolution: remaining groups

Re-measured on `main` at `55f48b2` (after slice-004 layer 3): 16 groups, 214 duplicated
lines; six cluster-1 groups remained (the scorer-internal group was already gone). After
this card: **11 groups, 122 duplicated lines, 0.18%**, none in cluster 1; no new group.

- `fresh_wallet_signal`, `size_anomaly_signal` and `high_risk_assessment` now live once in
  `tests/conftest.py`. The size signal is built on each module's own `sample_metadata`
  (question, price and category stay local). The formatter and history copies of
  `high_risk_assessment` were identical. The history copies of the two signals differed only
  by `factors={}`; history never reads `factors`, so it now uses the shared values.
- `tests/batch_trades.py::indexed_trades` is the one definition of the per-index batch trade
  shared by the scorer and size-anomaly batch tests.
- `test_history.py::_recent_alerts` holds the repeated 24-hour window query.
- Pinned by `tests/test_sample_fixtures.py`.
