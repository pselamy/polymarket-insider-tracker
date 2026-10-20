---
cluster: 1
status: real
resolved-by: PR #164 (partial; clones remain)
files: tests/alerter/test_formatter.py, tests/alerter/test_history.py, tests/detector/test_scorer.py, tests/detector/test_size_anomaly.py
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
