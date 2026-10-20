---
cluster: 4
status: resolved
resolved-by: PR #166
---

# Cluster 4

- **Files:** test_dispatcher.py
- **Knowledge:** **Resolved**: channel-to-fake-server wiring (`wired_channel`) and the failed-send redaction assertion (`assert_failure_logged_without`) have one home; the paired success, failure, rate-limit, read-timeout and retry-after cases are parametrized over the channel
- **Verdict:** resolved (PR #166)
- **Counts when first grouped (historical):** 0 clones, 0 duplicated lines. Current counts are generated, see [the README](../README.md).

## Cluster 4 resolution update (2026-10-19, PR #166)

Measured with the pinned jscpd 5.3.3 through the vendored
`quality.duplication.scan(..., minimum=50)` over `quality.source_scope.discover`
(size check skipped, as for cluster 1), whole tree, 295 sources at both ends.
Baseline is `origin/main` at `430c9af` (after #164): **72 groups, 858 duplicated
lines, 1.40%**. This card rebased onto it: **63 groups, 766 duplicated lines,
1.25%**. Compared as a multiset of (file pair, token count), exactly nine
`test_dispatcher.py` self-clones were removed and no group was added or grew.
Redaction cases whose provider response bodies genuinely differ stay as separate
Discord/Telegram functions (they share only the assertion helper). The earlier
74 -> 65 figures covered a narrower file set and are superseded by these.
