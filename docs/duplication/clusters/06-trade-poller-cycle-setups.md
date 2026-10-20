---
cluster: 6
status: resolved
resolved-by: PR #171
---

# Cluster 6

- **Files:** test_trade_poller.py
- **Knowledge:** **Resolved**: the anchored-poller first-cycle setup (`_anchored` and the `anchored` fixture), the saturated unproven-page setup (`_saturated`), the cached Yes/No repair metadata (`_yes_no`) and the missing-outcome row (`_publish_missing_outcome`) now each have one home in the module
- **Verdict:** resolved (this card; PR #171)
- **Counts when first grouped (historical):** 0 clones, 0 duplicated lines. Current counts are generated, see [the README](../README.md).

## Cluster 6 resolution (trade-poller cycle setups and repair assertions)

Measured with the identical pinned method (jscpd 5.3.3, `SCAN_POLICY` at the 50-token
tier, empty config, no ignores) over the 292 authored Python files enumerated by
`quality.source_scope.discover`: `origin/main` at `b0f37b6` (after #169) reports **52 clone groups,
625 duplicated lines, 1.02%**; this tree reports **46 groups, 561 lines, 0.92%**. The six
`test_trade_poller.py` self-pairs (12, 18, 9, 6, 13 and 6 lines) are absent and the
file-pair set difference is empty, so no new clone group was introduced. Other
clusters' rows above are historical receipts from their own trees.
