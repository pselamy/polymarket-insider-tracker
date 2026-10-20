---
cluster: 22
status: resolved
watch: src/polymarket_insider_tracker/detector/fresh_wallet.py <-> src/polymarket_insider_tracker/detector/size_anomaly.py
---

# Cluster 22

- **Files:** fresh_wallet.py, size_anomaly.py
- **Knowledge:** Duplicated batch-result `_handle_batch_result` gather/zip knowledge
- **Verdict:** real; resolved, see the 2026-10-19 cluster 22 resolution update below
- **Counts when first grouped (historical):** 1 clones, 12 duplicated lines. Current counts are generated, see [the README](../README.md).

## 2026-10-19 resolution update: cluster 22 (detector batch-result gather/zip)

Cluster 22's single clone pair had `FreshWalletDetector.analyze_batch` and
`SizeAnomalyDetector.analyze_batch` each re-implementing the same knowledge:
`asyncio.gather(..., return_exceptions=True)`, `zip(trades, results,
strict=True)`, dropping `None`, and a `@staticmethod _handle_batch_result` that
warns on a `BaseException` (message run through `redact_exception_message`) and
returns `None`. The two copies were textually identical apart from the signal type
in their annotations and the module logger each one resolved, so there was no
behavioural difference to adjudicate.

That knowledge now has one home,
`src/polymarket_insider_tracker/detector/_batch.py` (`gather_signals`). Both
`analyze_batch` methods still build their own per-trade tasks (the size detector
keeps `_create_batch_tasks` to attach `daily_volume`/`book_depth`) and delegate
the gather/zip/failure handling to `gather_signals`, passing their own module
logger so a failed-trade warning keeps the same `%(name)s` provenance as before.
Public import paths (`FreshWalletDetector`, `SizeAnomalyDetector`, their
`analyze`/`analyze_batch` methods) are unchanged and every existing test passes
unchanged; the characterization tests added here also pin the warning's logger
name.

Re-measured with the identical pinned method (jscpd 5.3.3, `SCAN_POLICY` at the
50-token tier, empty config, no ignore lists or exclusions), enumerating the
authored scope as `quality.source_scope.discover` + `validate_path` (`.py` only)
and staging with `quality.duplication_inputs.stage_inputs`:

- Before (this card's parent `bfc1a10`, 280 authored Python files): **86 clone
  groups, 1092 duplicated lines, 1.83%**.
- After (this change, 282 authored Python files; the two additions are `_batch.py`
  and `tests/detector/test_batch_characterization.py`): **85 clone groups, 1080
  duplicated lines, 1.80%**.

Set-difference of the raw clone lists: exactly the cluster-22 clone
(`src/.../detector/fresh_wallet.py:223-234 <-> src/.../detector/size_anomaly.py:386-397`,
55 tokens / 12 lines) disappeared and **no new clone group appeared anywhere**.
Resolved by draft PR
[#158](https://github.com/pselamy/polymarket-insider-tracker/pull/158) from branch
`ralph/dedup-batch`.
