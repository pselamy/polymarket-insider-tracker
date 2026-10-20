---
cluster: 11
status: resolved
---

# Cluster 11

- **Files:** profiler/test_models.py
- **Knowledge:** **Resolved**: the `Transaction` construction shared by the equality and hashing tests now has one home (`TestTransactionEquality._transaction`)
- **Verdict:** resolved (this card)
- **Counts when first grouped (historical):** 0 clones, 0 duplicated lines. Current counts are generated, see [the README](../README.md).

## 2026-10-19 resolution update: clusters 11, 14 and 15 (small test-construction duplicates)

Cluster 11: the equality, inequality and hashing tests in `tests/profiler/test_models.py`
restated one `Transaction` literal five times; `TestTransactionEquality._transaction(hash=...)`
now owns it and the "different" case overrides only the hash. Cluster 14: the identical
`sample_trade_event` fixture in `test_publisher.py` and `test_publisher_codec.py` moved to
`tests/ingestor/conftest.py`, and `test_read_events` / `test_read_events_with_bytes` share
`_publish_then_read` (the second test still asserts only the entry id, as before). Cluster 15:
`test_trade_rows.py` and `test_trade_row_v2.py` shared a `_market` builder (now
`tests.fakes.trades.market_metadata`) and the eight malformed price/size/timestamp cases (now
`MALFORMED_NUMERIC_CASES`; the v1 list appends it in the same order, so parametrized IDs are
unchanged). `sample_trade_event` copies in `tests/test_pipeline.py` and
`tests/integration/test_end_to_end.py` were not touched (outside the card scope).

Re-measured with the pinned jscpd 5.3.3 and the same flags over the tracked Python files:
the parent tree (292 files) reports 43 clone groups (518 duplicated lines, 0.85%); this
branch (293 files) reports 36 (430 lines, 0.70%). Matched by file pair and line count, exactly
the seven cluster 11/14/15 groups are gone and none was added. The 715 collected test IDs
under `tests/ingestor` and `tests/profiler` are identical before and after.
