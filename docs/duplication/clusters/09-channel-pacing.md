---
cluster: 9
status: resolved
resolved-by: PR #154
watch: src/polymarket_insider_tracker/alerter/channels/discord.py <-> src/polymarket_insider_tracker/alerter/channels/telegram.py
---

# Cluster 9

- **Files:** discord.py, telegram.py
- **Knowledge:** Duplicated rate-limit wait, `_backoff`, and failure-redaction channel logic
- **Verdict:** real; resolved, see the 2026-10-19 resolution update below
- **Counts when first grouped (historical):** 3 clones, 44 duplicated lines. Current counts are generated, see [the README](../README.md).

## 2026-10-19 resolution update: cluster 9 (Discord/Telegram channel pacing)

Cluster 9 is resolved by the dedup card for cluster 9: draft PR
[#154](https://github.com/pselamy/polymarket-insider-tracker/pull/154) from
branch `ralph/dedup-channels`. The rate-limit wait, the retry backoff, and the failure-redaction
wrapper moved to exactly one home,
`src/polymarket_insider_tracker/alerter/channels/pacing.py`
(`RateLimitWindow`, `sleep_backoff`); both former copies use it, and the
redaction call sites use the pre-existing single home
`polymarket_insider_tracker.redaction.redact_failure_with_secrets` directly.

Re-measurement with the pinned scanner (jscpd 5.3.3 receipt verified via
`mise --yes --locked exec -- jscpd --version`, same SCAN_POLICY at the
50-token tier, no ignore lists or exclusions), enumerating the authored scope
exactly as `verify_sources` does — `quality.source_scope.discover` +
`validate_path`, skipping the 5 rejected `.pyi` stubs, with the 399-line size
hook deliberately not applied — then staging with
`quality.duplication_inputs.stage_inputs` and calling
`quality.duplication.scan(root, config, staged, output, minimum=50, deadline)`:

- Before (baseline commit `233c24f`, same enumeration): **101 clone groups,
  1347 duplicated lines, 2.31%** over 266 files. The three cluster-9 clones
  reproduce there byte-identically (110, 68, and 80 tokens).
- After (this change): **98 clone groups, 1303 duplicated lines, 2.23%** over
  268 files (the two added files are the new pacing module and the
  characterization test module).

Set-difference of the raw clone list between the two scans: exactly the three
cluster-9 clones disappeared and **no new clone group appeared anywhere**.
The totals above differ from the round-1 totals at the top of this document
because that measurement enumerated 258 files; this update re-measures both
trees with one consistent enumeration so the before/after comparison is like
for like. All other recorded round-1 clones are unchanged.

### 2026-10-19 rebase re-measurement (rebased onto main after #153)

After the branch was rebased onto main (which now carries the cluster-5
resolution from #153), both trees were re-measured with the identical pinned
method:

- `origin/main` at `52445f9`: **99 clone groups, 1271 duplicated lines,
  2.16%** over 268 files. The three cluster-9 clones still reproduce there
  (110, 68, and 80 tokens).
- This branch (rebased head): **96 clone groups, 1227 duplicated lines,
  2.09%** over 270 files (the two added files are the new pacing module and
  the characterization test module).

Set-difference against the rebased main scan: exactly the three cluster-9
clones disappeared and **no new clone group appeared anywhere**.
