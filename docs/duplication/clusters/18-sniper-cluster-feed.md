---
cluster: 18
status: resolved
---

# Cluster 18

- **Files:** test_sniper.py
- **Knowledge:** **Resolved**: the five-wallet 30-second cluster feed has one home (`clustered_detector`)
- **Verdict:** resolved (this card)
- **Counts when first grouped (historical):** 0 clones, 0 duplicated lines. Current counts are generated, see [the README](../README.md).

## 2026-10-19 resolution update: clusters 18, 20, 23, 24, 26, 27, 29, 30 and 32 (remaining small test-construction duplicates)

Cluster 18: `clustered_detector` in `test_sniper.py` owns the five-wallet, 30-second-after-creation feed
and the detector parameters (the four tests that restated it use it). Cluster 20:
`_fresh_and_old_infos` in `test_analyzer.py` owns the nonce-2 and nonce-100 wallet pair. Cluster 23:
`_open_and_closed_page` (`test_clob_client.py`). Cluster 24: `_abort_after_takeover` and
`_competing_client` in `test_redis_contract.py` (the type-change test also uses `_competing_client`;
the expire test keeps its own TTL assertion). Cluster 26: `_tight_orderbook` (`ingestor/test_models.py`).
Cluster 27: `_assert_rejected_alike` (`test_trade_row_v2_properties.py`). Cluster 29: `build_walk` gained
a `random_source` argument defaulting to the previous `lambda: 1.0`; the CursorWalk defaults it
replaces for `max_pages`/`max_retries` are the same constants. Cluster 30: `_run_v1_cycle` and
`_v2_server_after_v1`. Cluster 32: `_first_page_then` (`test_gamma_client.py`). No behaviour differences
between any pair of copies were found; every existing test is unchanged.

Re-measured with the pinned jscpd 5.3.3 (`quality.duplication.scan`, minimum 50, over
`quality.source_scope.discover`): the parent tree reports 29 clone groups (365 duplicated lines,
0.60%); this branch reports 19 (256 lines, 0.42%). Exactly the ten clone groups of these clusters
(cluster 30 had two) are gone and none was added.

Re-measured after the rebase onto `origin/main` at `f4d2112` (which carries #176 and #180): `origin/main` reports 29 clone groups (365 duplicated lines, 0.60%) and this branch 19 (256 lines, 0.42%), unchanged from the figures above; the whole-tree scan is over 265 sources.
