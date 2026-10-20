---
cluster: 3
status: resolved
resolved-by: PR #168
---

# Cluster 3

- **Files:** test_redaction.py
- **Knowledge:** **Resolved**: the primed-poller, idle-pipeline, failing-Redis `run_pipeline`, failed metadata-start and production-console-capture harnesses now each have one helper in `tests/test_redaction.py`
- **Verdict:** resolved (PR #168)
- **Counts when first grouped (historical):** 0 clones, 0 duplicated lines. Current counts are generated, see [the README](../README.md).

## 2026-10-19 resolution update: cluster 3 (redaction-policy test harness setups)

Measured with the pinned jscpd 5.3.3 (same raw command and flags as above, empty
config) over `quality.source_scope.discover` `.py` files, identically enumerated at
both ends. Baseline is `origin/main` at `f2e2167` (after #164 and #166): **63 groups,
766 duplicated lines, 1.25%** over 290 files. This branch rebased onto it: **56
groups, 672 duplicated lines, 1.10%** over 291 files (the added file is
`tests/test_redaction_harness_characterization.py`). Matched by file pair and line
count, the seven removed groups are exactly the seven cluster-3
groups (57, 60, 64, 76, 77, 84 and 120 tokens, all `tests/test_redaction.py` against
itself) and **no group was added**.

Duplicated knowledge and its single homes (all private helpers in
`tests/test_redaction.py`): `_primed_poller` (fake-stack pipeline, one published
trade, priming poll cycle; four call sites), `_idle_pipeline` (empty trades server,
caller-visible Redis; two sites), `_run_pipeline_with_failing_redis` (Redis-factory
failure through `run_pipeline` with the `__main__` logger handler; two sites),
`_failed_metadata_start` (two sites), and the in-class
`_capture_production_record`, which now delegates to the module-level
`_capture_console_output` instead of carrying a second copy of the console-stream
swap. `test_terminal_path_credential_failure_is_redacted_everywhere` keeps its own
setup on purpose: it builds the poller without `poll_clock`, a real difference. No
redaction assertion changed: the `not in` assertion count in the file is 147 before
and after, and the collected test IDs are unchanged.
