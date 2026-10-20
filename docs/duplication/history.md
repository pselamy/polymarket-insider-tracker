# Measurement history

Frozen receipts moved from the former single-file duplication inventory. They are historical, measured on
each card's own tree, and are not edited by later PRs. Current totals are generated; see [README](README.md).

## Original inventory text

Re-measured 2026-10-19 on the round-1 rebase of the "introduced by #151" card,
which now includes #152 (default v2 endpoint), #154 (cluster 9), and this card.
The authored scope is enumerated by `quality.source_scope.discover` +
`validate_path` (the `verify_sources` file set without its 399-line size hook;
see [relentless-ci-adoption.md](relentless-ci-adoption.md) for the coupling):
**279 authored Python files**. Vendored `SCAN_POLICY` at the 50-token tier:
`--mode mild --format python --min-tokens 50 --min-lines 4 --threshold 0
--no-gitignore --workers 1`, empty config, no ignore lists, no exclusions. (The
authored scope later grew to 280 files at this card's parent `bfc1a10`, when #156
added one integration test; that file introduced no clone, so the 86/1092 totals
below are unchanged between 279 and 280 files.)

Totals on main after #158 plus the cluster-25 resolution (#157): **88 clone
groups, 1116 duplicated lines, 1.85% duplication** over **283 authored Python
files** (the scanner's own statistics report 250 sources: files it actually
tokenized after skipping ones smaller than the clone minimums). The
cluster-28 resolution recorded at the end of this document was measured on a
tree that did not yet contain cluster 25 (85 -> **84 groups, 1080 -> 1071
duplicated lines, 1.80% -> 1.78%**, 282 -> 284 authored files, the two additions
being `scripts/_entry_bootstrap.py` and its characterization test); on
top of cluster 25 the rebased tree measures **87 groups, 1107 duplicated lines,
1.83%** over 285 authored files (252 sources tokenized), recorded in that
section. The
pre-resolution tree at cluster 22's parent
`bfc1a10` measured **86 groups, 1092 duplicated lines, 1.83%** over 280 files with
the identical enumeration. For comparison, `origin/main` at `cdaf230` (after #152
and #154) measures **95 groups, 1224 duplicated lines, 2.06%** over 276 files.

(All narrative totals above are historical receipts from each card's review
round, measured on their own then-current trees.)

Current totals (cluster-7 card, round-3 docstring revert rebased onto
`origin/main` at `69c4807`, after #160): **86 clone groups, 1071 duplicated
lines, 1.76%** over **287 authored Python files**; `origin/main` at `69c4807`
measures **87 groups, 1107 lines, 1.83%** over 285 files with the identical
enumeration. The per-clone list below is this tree's scan; see the cluster-7
section at the end for the set-difference.

Totals after the cluster-7 card's round-3 rebase onto `origin/main` at
`382a0f6` (after #157 and #161): **87 clone groups, 1071 duplicated lines,
1.76% duplication** over **285 authored Python files**; `origin/main` at
`382a0f6` measures **88 groups, 1116 duplicated lines, 1.85%** over 283 files
with the identical enumeration, and the raw clone-list difference is exactly the
three cluster-7 clones removed (197, 67 and 52 tokens) plus the two
characterization-module self-pairs added (51 and 68 tokens); no new product
clone group. **Round-3 review correction (supersedes the 87/1071 figures directly above):** those figures were reached partly by token-shuffling edits that hid two clones from jscpd without removing any knowledge (`field(default=...)` wrappers restating plain defaults in `RiskAssessmentDTO`, and a reordered property-binding block in `SizeAnomalySignal`). Both edits are reverted and the characterization module's three copy-pasted threshold tests are now one table-driven test. Re-measured with the identical pinned method: **87 clone groups, 1080 duplicated lines, 1.78%** over 285 files, against `origin/main` at `382a0f6` **88 groups, 1116 lines, 1.85%** over 283 files. Raw clone-list difference: the 197- and 67-token cluster-7 clones removed, one 96-token residual added (see the cluster-7 round-3 correction below); the 52-token DTO/domain mirror is present on both sides, unchanged. The round-1 figures that follow are kept as recorded: after the
card's round-1 rebase onto `6be456a`: **84 clone groups, 1035 duplicated lines, 1.71% duplication** over
**283 authored Python files** (the scanner's own statistics report 250 sources).
The pre-refactor tree of the rebased card chain (US-001 characterization tests
present, before the cluster-7 refactor) measured **87 groups, 1095 duplicated
lines, 1.81%** over the same 283 files; `origin/main` at `6be456a` measures the
85 groups / 1080 lines / 1.80% above over 282 files. Set-differences of the raw
clone lists, both scanners run with the identical enumeration: pre-refactor
rebased chain -> this tree removes exactly the three cluster-7 clones (197, 67
and 52 tokens) and adds **no new clone group**; `origin/main` -> this tree
additionally shows the two intra-file clones of the US-001 characterization
test module (51 and 68 tokens) — mandated evidence tests, not product
duplication.

The first measurement recorded here (92 groups, 1230 lines, 2.16% over 258
files) predated the v1->v2 endpoint merges now on main, so a fresh baseline was
taken on cluster 5's parent commit `233c24f`: **101 groups, 1347 lines, 2.31%
over 266 files**. Cluster 5 then removed exactly its two clones and introduced
none, giving the 99 groups recorded below.

## Original cluster-table commentary

Real knowledge dominates: most clusters are duplicated maintenance surfaces —
fixture blocks, wiring scaffolds, parallel Discord/Telegram knowledge, and
Redis CAS logic. The three incidental clusters are convention-mandated
scaffolding (Alembic revisions, SQLAlchemy column defaults) and parallel API
docstrings. The cluster table above is the original knowledge grouping; the
per-clone list below was regenerated on the cluster-7 card's round-3 tree
(rebased onto `origin/main` at `69c4807`, 287 authored files, 86 groups), so it
reflects the cluster 5, cluster 9, "introduced by #151", cluster 22, cluster 25,
cluster 28 and cluster 7 resolutions; see the totals statement above and the
resolution sections at the end for each card's own measurements.

## Archived per-clone list (2026-10-19, superseded by the generated report)

## Every clone (scanner output, ordered by tokens)

`tokens`/`lines` are the clone size; ranges are 1-based inclusive
(`start-end`). Format: `A:start-end <-> B:start-end`.

| tok | ln | A | B |
| --- | --- | --- | --- |
| 131 | 34 | tests/alerter/test_history.py:24-57 | tests/detector/test_scorer.py:28-61 |
| 120 | 20 | tests/test_redaction.py:1787-1806 | tests/test_redaction.py:1845-1864 |
| 118 | 31 | tests/alerter/test_formatter.py:75-105 | tests/detector/test_scorer.py:74-104 |
| 113 | 18 | tests/alerter/test_cas_characterization.py:186-203 | tests/alerter/test_cas_characterization.py:245-262 |
| 102 | 20 | tests/ingestor/test_publisher.py:23-42 | tests/ingestor/test_publisher_codec.py:50-69 |
| 96 | 20 | tests/detector/test_sniper.py:278-297 | tests/detector/test_sniper.py:314-333 |
| 96 | 16 | src/.../detector/models.py:106-121 | src/.../detector/models.py:155-170 |
| 88 | 17 | tests/test_terminal_error_durable.py:84-100 | tests/test_terminal_error_durable.py:153-169 |
| 84 | 24 | tests/alerter/test_formatter.py:20-43 | tests/alerter/test_history.py:16-39 |
| 84 | 10 | tests/test_redaction.py:1152-1161 | tests/test_redaction.py:1915-1924 |
| 81 | 15 | tests/integration/test_end_to_end.py:71-85 | tests/integration/test_postgres_journey.py:147-161 |
| 80 | 19 | tests/profiler/test_funding.py:37-55 | tests/profiler/test_funding_rpc.py:48-66 |
| 80 | 18 | tests/integration/test_end_to_end.py:72-89 | tests/test_pipeline.py:53-70 |
| 77 | 17 | tests/profiler/test_analyzer.py:336-352 | tests/profiler/test_analyzer.py:382-398 |
| 77 | 10 | tests/test_redaction.py:971-980 | tests/test_redaction.py:1152-1161 |
| 76 | 9 | tests/test_redaction.py:972-980 | tests/test_redaction.py:1054-1062 |
| 75 | 14 | tests/ingestor/test_health_server.py:123-136 | tests/ingestor/test_health_server.py:259-272 |
| 74 | 13 | src/.../profiler/_funding_rpc.py:108-120 | src/.../profiler/funding.py:258-270 |
| 74 | 12 | tests/test_main.py:304-315 | tests/test_main.py:362-373 |
| 71 | 21 | tests/alerter/test_formatter.py:23-43 | tests/detector/test_scorer.py:23-43 |
| 70 | 11 | tests/integration/test_redis_contract.py:487-497 | tests/integration/test_redis_contract.py:532-542 |
| 69 | 15 | tests/integration/test_end_to_end.py:57-71 | tests/test_pipeline.py:82-96 |
| 69 | 11 | tests/ingestor/test_health_server.py:104-114 | tests/ingestor/test_health_server.py:140-150 |
| 68 | 14 | tests/test_terminal_error_durable.py:36-49 | tests/test_terminal_error_durable.py:81-94 |
| 68 | 12 | tests/alerter/test_formatter.py:502-513 | tests/alerter/test_formatter.py:680-691 |
| 67 | 11 | tests/ingestor/test_health_server.py:65-75 | tests/ingestor/test_health_server.py:276-286 |
| 67 | 10 | tests/test_main.py:305-314 | tests/test_main.py:572-581 |
| 66 | 15 | tests/ingestor/test_trade_row_v2.py:50-64 | tests/ingestor/test_trade_rows.py:37-51 |
| 66 | 8 | tests/ingestor/test_endpoint_switch_resume.py:69-76 | tests/ingestor/test_endpoint_switch_resume.py:100-107 |
| 65 | 8 | tests/tooling/test_service_entrypoint.py:148-155 | tests/tooling/test_service_entrypoint.py:176-183 |
| 64 | 17 | tests/test_redaction.py:2024-2040 | tests/test_redaction.py:2077-2093 |
| 62 | 16 | tests/integration/test_end_to_end.py:74-89 | tests/test_pipeline_persistence.py:47-62 |
| 62 | 12 | tests/test_terminal_error_durable.py:38-49 | tests/test_terminal_error_durable.py:152-163 |
| 61 | 13 | tests/profiler/test_funding.py:60-72 | tests/profiler/test_funding_rpc.py:70-82 |
| 61 | 13 | tests/profiler/test_models.py:204-216 | tests/profiler/test_models.py:229-241 |
| 61 | 11 | tests/integration/test_end_to_end.py:320-330 | tests/test_pipeline.py:619-628 |
| 61 | 8 | tests/alerter/test_cas_characterization.py:307-314 | tests/alerter/test_cas_characterization.py:319-326 |
| 61 | 6 | src/.../storage/models.py:51-56 | src/.../storage/models.py:212-217 |
| 60 | 15 | tests/test_redaction.py:2257-2271 | tests/test_redaction.py:2374-2388 |
| 60 | 10 | tests/ingestor/test_models.py:218-227 | tests/ingestor/test_models.py:242-251 |
| 59 | 17 | tests/alerter/test_formatter.py:92-108 | tests/detector/test_scorer.py:91-107 |
| 59 | 11 | tests/ingestor/test_health_server.py:54-64 | tests/ingestor/test_health_server.py:93-103 |
| 58 | 7 | tests/integration/test_end_to_end.py:94-100 | tests/test_pipeline.py:536-542 |
| 58 | 6 | tests/ingestor/test_gamma_client.py:167-172 | tests/ingestor/test_gamma_client.py:225-233 |
| 57 | 13 | tests/test_redaction.py:1547-1559 | tests/test_redaction.py:1886-1897 |
| 57 | 12 | tests/detector/test_scorer.py:620-631 | tests/detector/test_size_anomaly.py:858-869 |
| 57 | 10 | tests/ingestor/test_trade_row_v2_properties.py:95-104 | tests/ingestor/test_trade_row_v2_properties.py:114-123 |
| 57 | 8 | tests/alerter/test_cas_characterization.py:179-186 | tests/alerter/test_cas_characterization.py:238-245 |
| 56 | 9 | tests/ingestor/test_publisher.py:247-255 | tests/ingestor/test_publisher.py:272-280 |
| 55 | 17 | tests/alerter/test_formatter.py:88-104 | tests/alerter/test_history.py:83-99 |
| 55 | 9 | tests/tooling/test_service_entrypoint.py:14-22 | tests/tooling/test_verifier_standalone.py:43-51 |
| 53 | 16 | tests/test_persist_assessment.py:4-19 | tests/test_pipeline_persistence.py:5-20 |
| 53 | 12 | tests/alerter/test_cas_characterization.py:203-214 | tests/alerter/test_cas_characterization.py:262-273 |
| 53 | 10 | tests/profiler/test_models.py:205-214 | tests/profiler/test_models.py:215-224 |
| 53 | 10 | tests/profiler/test_models.py:205-214 | tests/profiler/test_models.py:255-264 |
| 53 | 9 | tests/tooling/test_complexipy.py:104-112 | tests/tooling/test_gate_negatives.py:47-55 |
| 53 | 8 | tests/tooling/test_service_entrypoint.py:148-155 | tests/tooling/test_verifier_standalone.py:165-172 |
| 53 | 7 | tests/ingestor/test_endpoint_switch_resume.py:80-86 | tests/ingestor/test_endpoint_switch_resume.py:107-113 |
| 52 | 15 | alembic/versions/rev_20260522_1130_risk_assessments.py:19-33 | alembic/versions/rev_20260920_0000_terminal_errors.py:19-33 |
| 52 | 11 | tests/ingestor/test_trade_row_v2.py:134-144 | tests/ingestor/test_trade_rows.py:117-127 |
| 52 | 8 | src/.../detector/models.py:265-272 | src/.../storage/repos.py:575-582 |
| 51 | 12 | tests/ingestor/test_clob_client.py:176-187 | tests/ingestor/test_clob_client.py:192-203 |
| 51 | 9 | tests/tooling/test_complexipy.py:105-113 | tests/tooling/test_policy_snapshot.py:77-85 |
| 51 | 6 | tests/detector/test_scorer.py:548-553 | tests/detector/test_scorer.py:796-802 |
| 50 | 15 | alembic/versions/rev_20260522_1130_risk_assessments.py:19-33 | alembic/versions/rev_20260910_0000_safe_observable_operation.py:24-38 |
| 50 | 15 | tests/alerter/test_formatter.py:74-88 | tests/alerter/test_history.py:69-83 |
| 50 | 9 | tests/integration/test_end_to_end.py:253-261 | tests/integration/test_postgres_journey.py:238-246 |
| 50 | 9 | tests/integration/test_end_to_end.py:322-330 | tests/test_pipeline.py:578-585 |
| 50 | 8 | tests/ingestor/_v2_walk_harness.py:52-59 | tests/ingestor/test_v2_cursor_walk.py:301-308 |
| 50 | 7 | tests/alerter/test_history.py:549-555 | tests/alerter/test_history.py:567-573 |
| 50 | 6 | tests/test_persist_assessment.py:96-101 | tests/test_pipeline.py:107-112 |

inventory only; the cluster-5, cluster-9, "introduced by #151", cluster-22,
cluster-25, cluster-28 and cluster-7 resolutions are the product changes reflected
here. Deduplication
decisions for the remaining clusters belong to later, separately authorized cards.

## Rebase re-measurement onto main after #158

After review round 1 the branch was rebased onto main (which now carries the
cluster-22 resolution from #158, so this document's cluster-22 resolution below
is part of this tree), and the loop bookkeeping files (`prd.json`,
`progress.txt`) were removed from the branch's net diff. Both trees were
re-measured with the identical pinned method (same enumeration, staging, and
`quality.duplication.scan(..., minimum=50, ...)` call):

- `origin/main` at `6be456a`: **85 clone groups, 1080 duplicated lines, 1.80%**
  over 282 authored files (249 sources tokenized).
- This branch (rebased head, 283 authored files): **88 clone groups, 1116
  duplicated lines, 1.85%** over 283 files (250 sources tokenized).

Set-difference of the raw pair sets (order-insensitive on the A/B sides):
exactly the cluster-25 pair
(`src/.../alerter/_channel_delivery.py:100-109 <-> 132-141`, 73 tokens / 10
lines) disappeared relative to rebased main and the only additions are the four
self-pairs inside `tests/alerter/test_cas_characterization.py` (53, 57, 61, and
113 tokens) already recorded above — **no new product clone appeared anywhere**.
The pre-rebase figures recorded above (90 -> 89 groups) described the branch
before it carried #158 and are kept as recorded.

This document is inventory only; the cluster-5, cluster-9, "introduced by
#151", cluster-22 (via #158), cluster-25 and cluster-7 resolutions are the only product
changes reflected here. Deduplication decisions for the remaining clusters
belong to later, separately authorized cards.
