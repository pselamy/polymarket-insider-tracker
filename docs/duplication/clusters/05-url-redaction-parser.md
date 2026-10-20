---
cluster: 5
status: resolved
resolved-by: PR #153
watch: scripts/_service_redaction.py <-> scripts/_verify_redaction.py
---

# Cluster 5

- **Files:** _url_redaction.py (new)
- **Knowledge:** **Resolved**: identical URL parsing + credential extraction now has one home (`scripts/_url_redaction.py`); both former copies import it
- **Verdict:** resolved (PR #153)
- **Counts when first grouped (historical):** 0 clones, 0 duplicated lines. Current counts are generated, see [the README](../README.md).

### Cluster 5 resolution (2026-10-19)

The byte-identical URL parser duplicated in `scripts/_service_redaction.py` and
`scripts/_verify_redaction.py` now has one home, `scripts/_url_redaction.py`
(`_URL_PATTERN`, `_split_userinfo`, `_extract_query_password`,
`_query_credentials`, `_url_credentials`, `credential_forms`, `redacted_url`,
`redact_url_occurrences`). Both scripts import it and keep only their own
sensitivity policy. `src/polymarket_insider_tracker/redaction.py` was checked
and is a different, broader policy (path masking, query-key preservation,
lone-userinfo masking), so it was not adopted. Before/after on the same tree:
101 -> 99 clone groups, 1347 -> 1271 duplicated lines; the only differences are
the two removed cluster-5 clones. Resolved by PR #153.
