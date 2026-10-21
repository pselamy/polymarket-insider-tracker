---
cluster: 12
status: resolved
resolved-by: PR #196
watch: tests/profiler/test_funding.py <-> tests/profiler/test_funding_rpc.py
---

# Cluster 12

- **Files:** test_funding.py, test_funding_rpc.py
- **Knowledge:** Parallel method docstrings/signatures describing the same RPC API
- **Verdict:** real
- **Counts when first grouped (historical):** 2 clones, 32 duplicated lines. Current counts are generated, see [the README](../README.md).

- **Resolution:** the real `PolygonClient`-on-`FakeEth` builder, the `Transfer` log builder and the shared addresses/chain head/pruned-history error now live once in `tests/profiler/funding_support.py`; both suites import them.
