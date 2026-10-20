---
cluster: 10
status: resolved
resolved-by: PR #172
---

# Cluster 10

- **Files:** test_terminal_error_durable.py
- **Knowledge:** **Resolved**: pipeline wiring (`_wire_poller_pipeline`), the terminal-401 failure sequence (`_pipeline_with_terminal_401`, `_fail_poller_cycle`, `_report_poller_failure`) and the durable-row assertion (`_assert_poller_401_row`) now have one home each in `tests/test_terminal_error_durable.py`
- **Verdict:** resolved (PR #172)
- **Counts when first grouped (historical):** 0 clones, 0 duplicated lines. Current counts are generated, see [the README](../README.md).

## 2026-10-19 resolution update: cluster 10 (terminal-error durable test wiring)

`_wire_poller_pipeline`, `_pipeline_with_terminal_401`, `_fail_poller_cycle`,
`_report_poller_failure` and `_assert_poller_401_row` in
`tests/test_terminal_error_durable.py` now own the fake-boundary pipeline wiring, the
terminal HTTP 401 poller failure, the hand-off to `_handle_worker_failure`, and the
persisted-row assertion that three tests restated (a fourth restated the wiring); the three cluster-10 clone pairs (68, 62 and 88 tokens) are gone.
Re-measured over the whole tree with the pinned jscpd 5.3.3 and the same flags and 292
files as the cluster-6 update: `origin/main` at `986a7c6` (after #171) reports 46 clone
groups (561 duplicated lines, 0.92%); this branch rebased onto it reports 43 (518 lines,
0.85%). Matched by file pair and line count the only difference is the three removed
`test_terminal_error_durable.py` self-pairs (12, 14 and 17 lines); no group was added or grew.
`test_fresh_pipeline_reads_terminal_reason_from_durable_row` keeps its own state
setup because it seeds a prior row rather than failing the poller. No assertion
changed and the six collected test IDs are unchanged.
