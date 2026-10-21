---
cluster: 33
status: resolved
resolved-by: PR #212
watch: tests/alerter/test_cas_characterization.py <-> tests/alerter/test_cas_characterization.py
---

# Cluster 33

- **Files:** test_cas_characterization.py
- **Knowledge:** **Resolved**: the CAS characterization scenarios shared one setup and three expectations that must change together: seed-and-record a claim, the foreign/missing abort (`WATCH`, `GET`, `UNWATCH`, no transaction) and the owned `MULTI`/terminal/`EXEC` sequence, plus the rival-owner race setup. They now live in `_recorded_claim`, `_assert_foreign_aborts`, `_assert_missing_aborts`, `_owned_sequence`/`_aborted_sequence` and `_raced_history`; `delete` and `expire` supply only their call.
- **Verdict:** resolved (four intra-file clone groups of 113, 61, 57 and 52 tokens)
- **Counts when first grouped (historical):** 4 clones, 46 duplicated lines. Current counts are generated, see [the README](../README.md).

Test names, assertions and the pinned command sequences are unchanged; all ten tests pass.

The other groups the scan reports beside this one were already verdicted incidental by clusters 7, 13, 21 and 31 (declaration blocks, frozen alembic revision scaffolding, parallel wrapper docstrings, SQLAlchemy timestamp column defaults) and are left as they are.
