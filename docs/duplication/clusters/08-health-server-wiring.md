---
cluster: 8
status: resolved
resolved-by: PR #169
---

# Cluster 8

- **Files:** test_health_server.py
- **Knowledge:** **Resolved**: component checker wiring (`wire_components`) and the serve/GET/stop probe (`probe`) have one home each in `tests/ingestor/test_health_server.py`
- **Verdict:** resolved (PR #169)
- **Counts when first grouped (historical):** 0 clones, 0 duplicated lines. Current counts are generated, see [the README](../README.md).

## 2026-10-19 resolution update: cluster 8 (health-server component wiring)

`wire_components` and `probe` in `tests/ingestor/test_health_server.py` now own the
per-component checker wiring and the serve/GET/stop sequence; the four cluster-8
clone pairs are gone. Re-measured with the pinned jscpd 5.3.3 and the same flags over
the tracked `src tests scripts alembic quality` Python files plus `conftest.py`
(292 files; not the discovered authored scope above, so totals are not comparable to
the figures above): 56 clone groups before, 52 after, none in `test_health_server.py`
and none added. All assertions are unchanged.
