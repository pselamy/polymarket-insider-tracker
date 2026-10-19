# Clone inventory (jscpd 5.3.3, pinned reference policy)

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

### Cluster "introduced by #151" resolution (2026-10-19)

PR #151 (bounded v2 cursor acquisition wired into the poller) introduced exactly
**8 clone groups**, all test-side (its production modules `acquisition.py`,
`_http_attempt.py`, `page_analysis.py`, `_cycle_cutoff.py` and
`test_v2_acquisition.py` introduce none; #151's refactor actually removed
duplication in `trades_source.py`). The groups were pinned by scanning
`fcc9cc3` (main before #151: 92 groups) and `233c24f` (main after #151: 101
groups) and diffing the group sets; the only other difference was a pure rename
of the Alembic revisions. The eight:

| tokens | lines | A | B |
| --- | --- | --- | --- |
| 150 | 24 | tests/fakes/trades.py | tests/fakes/trades_v2.py |
| 103 | 21 | tests/integration/test_end_to_end.py | tests/integration/test_v2_journey.py |
| 96 | 18 | tests/integration/test_end_to_end.py | tests/integration/test_v2_journey.py |
| 73 | 8 | tests/ingestor/test_endpoint_switch_resume.py | tests/ingestor/test_v2_poller.py |
| 64 | 16 | tests/integration/test_end_to_end.py | tests/integration/test_v2_journey.py |
| 59 | 10 | tests/integration/test_end_to_end.py | tests/integration/test_v2_journey.py |
| 57 | 6 | tests/integration/test_end_to_end.py | tests/integration/test_v2_journey.py |
| 53 | 7 | tests/integration/test_end_to_end.py | tests/integration/test_v2_journey.py |

They are three families of duplicated knowledge with a single home each now:
request recording in `tests/fakes/trades.py` (`recorded_request`, `ProviderHttp`)
shared by both provider fakes; `RecordingTradeCallback` and the poller wiring
recipe in `tests/ingestor/_poller_harness.py`; and the journey harness —
fixtures/factories re-exported through `tests/integration/conftest.py`, the
existing `test_end_to_end` helpers imported by `test_v2_journey`, and the new
`tests/integration/_journey_harness.py` (`wire_journey_pipeline`,
`running_pipeline`, `dry_run_assessment`). Behaviour is identical; every existing
test passes unchanged and the US-001 characterization tests still pass. Before
the round-1 rebase, on the then-current tree: **99 -> 90 clone groups,
1271 -> 1142 duplicated lines, 2.17% -> 1.95%**; no new clone group (file
pair) was introduced, and the only removed pairs were the three cluster pairs
plus one pre-existing `test_end_to_end` self-scaffold clone that disappeared
once the dry-run journey reused `wire_journey_pipeline`. The rebased totals
below supersede those historical numbers. Resolved by PR #155.

### Rebase re-measurement (round 1 review, after #152 and #154)

The rebased tree was measured with the identical pinned method on both sides:
`origin/main` at `cdaf230` gives 95 groups / 1224 duplicated lines / 2.06% over
276 authored files; this branch gives **86 groups / 1092 duplicated lines /
1.83%** over 279 files. The raw pair diff is **9 pairs removed and 1 pair
added**. Removed: the eight cluster-#151 pairs above (collapsed by the three
single homes), #152's `test_v2_default_journey.py <-> test_v2_journey.py`
harness duplicate, and the `conftest.py` fixture clones that existed only
because #152 kept the journey fixtures there. The one added pair,
`test_end_to_end.py <-> test_pipeline_persistence.py`, is the same
`sample_trade` fixture knowledge that `origin/main` reports as
`conftest.py <-> test_pipeline_persistence.py`; it is re-attributed, not new
knowledge, because the single fixture home is `test_end_to_end.py` and
`conftest.py` re-exports it. `test_v2_default_journey.py` now appears in no
clone pair: it uses the shared `_journey_harness.run_dry_run_journey` exactly
like `test_v2_journey.py`. Cluster 9's three clones are also absent, resolved
by #154.

Reproduce: enumerate the authored scope, stage it, then call the vendored
`quality.duplication.scan(root, config, staged, output, minimum=50, deadline)`
(equivalent raw command, run from the repo root with the pinned tool set:
`mise --yes --locked exec -- jscpd --config <empty-config.json> --mode mild
--format python --min-tokens 50 --min-lines 4 --threshold 0 --max-size
9007199254740991 --no-gitignore --absolute --reporters json --summary
--summary-top <n> --workers 1 --no-colors --no-tips --output <dir> <files...>`).
jscpd exits 1 under the threshold-0 policy whenever any clone exists; the
report it writes is the measurement artifact.

Every clone reported by the scanner is listed below, first grouped into
clusters by the duplicated *knowledge* (same rule, mapping, query,
validation, or fixture — not merely similar text), then listed individually.
A cluster is annotated **real** (the same knowledge is genuinely maintained
in two places and a divergence would be a defect risk) or **incidental**
(structurally similar scaffolding required by convention, not shared
knowledge).

## Clusters ordered by size

| # | Clones | Dup lines | Files | Knowledge | Verdict |
| --- | --- | --- | --- | --- | --- |
| 1 | 11 | 196 | test_formatter.py, test_history.py, test_scorer.py, test_size_anomaly.py | Repeated `sample_trade`/`sample_wallet_profile` fixtures and formatter/history case scaffolds (two fixture families share one component) | real |
| 2 | 0 | 0 | _journey_harness.py | **Resolved**: the sample trade, single-token market, seeded provider history, journey settings, and wire/start/stop scaffolding now have one home (`tests/integration/_journey_harness.py`); the five suites use it | resolved (PR #163) |
| 3 | 0 | 0 | test_redaction.py | **Resolved**: the primed-poller, idle-pipeline, failing-Redis `run_pipeline`, failed metadata-start and production-console-capture harnesses now each have one helper in `tests/test_redaction.py` | resolved (PR #168) |
| 4 | 0 | 0 | test_dispatcher.py | **Resolved**: channel-to-fake-server wiring (`wired_channel`) and the failed-send redaction assertion (`assert_failure_logged_without`) have one home; the paired success, failure, rate-limit, read-timeout and retry-after cases are parametrized over the channel | resolved (PR #166) |
| 5 | 0 | 0 | _url_redaction.py (new) | **Resolved**: identical URL parsing + credential extraction now has one home (`scripts/_url_redaction.py`); both former copies import it | resolved (PR #153) |
| 6 | 0 | 0 | test_trade_poller.py | **Resolved**: the anchored-poller first-cycle setup (`_anchored` and the `anchored` fixture), the saturated unproven-page setup (`_saturated`), the cached Yes/No repair metadata (`_yes_no`) and the missing-outcome row (`_publish_missing_outcome`) now each have one home in the module | resolved (this card; PR #171) |
| 7 | 2 | 24 | detector/models.py, storage/repos.py | **Resolved**: confidence thresholds/classification, trade-identity property accessors and the trade-head serialization now have one home (`serialized_trade_head`, `is_high_confidence_score`, shared property objects, `UNRECORDED_DELIVERY_DISPOSITION`). Two residual groups remain visible on purpose: the `FreshWalletSignal`/`SizeAnomalySignal` declaration block (96 tokens) and the `RiskAssessment`/`RiskAssessmentDTO` field-default mirror (52 tokens, intentional persistence/domain layer boundary) | resolved; residual incidental (see the round-3 review correction below) |
| 8 | 0 | 0 | test_health_server.py | **Resolved**: component checker wiring (`wire_components`) and the serve/GET/stop probe (`probe`) have one home each in `tests/ingestor/test_health_server.py` | resolved (PR #169) |
| 9 | 3 | 44 | discord.py, telegram.py | Duplicated rate-limit wait, `_backoff`, and failure-redaction channel logic | real; resolved, see the 2026-10-19 resolution update below |
| 10 | 0 | 0 | test_terminal_error_durable.py | **Resolved**: pipeline wiring (`_wire_poller_pipeline`), the terminal-401 failure sequence (`_pipeline_with_terminal_401`, `_fail_poller_cycle`, `_report_poller_failure`) and the durable-row assertion (`_assert_poller_401_row`) now have one home each in `tests/test_terminal_error_durable.py` | resolved (PR #172) |
| 11 | 0 | 0 | profiler/test_models.py | **Resolved**: the `Transaction` construction shared by the equality and hashing tests now has one home (`TestTransactionEquality._transaction`) | resolved (this card) |
| 12 | 2 | 32 | test_funding.py, test_funding_rpc.py | Parallel method docstrings/signatures describing the same RPC API | real |
| 13 | 2 | 30 | rev_20260522_1130_risk_assessments.py, rev_20260910_0000_safe_observable_operation.py, rev_20260920_0000_terminal_errors.py | Alembic revision scaffolding (`__all__`, `upgrade()` head) | incidental |
| 14 | 0 | 0 | tests/ingestor/conftest.py | **Resolved**: the `sample_trade_event` fixture has one home (`tests/ingestor/conftest.py`) and the publish-then-read sequence has one helper (`_publish_then_read`) | resolved (this card) |
| 15 | 0 | 0 | tests/fakes/trades.py | **Resolved**: the synthetic `market_metadata` builder and the `MALFORMED_NUMERIC_CASES` table now have one home in `tests/fakes/trades.py`; the v1 and v2 row-decoding tests use them | resolved (this card) |
| 16 | 3 | 25 | test_service_entrypoint.py, test_verifier_standalone.py | Duplicated `_verifier` module-loader helper and entry bootstrap | real |
| 17 | 2 | 22 | test_main.py | Repeated settings/pipeline wiring snippets | real |
| 18 | 1 | 20 | test_sniper.py | Repeated cluster-feeding test construction | real |
| 19 | 2 | 18 | test_complexipy.py, test_gate_negatives.py, test_policy_snapshot.py | Duplicated `spec_from_file_location` verifier loader helper | real |
| 20 | 1 | 17 | test_analyzer.py | Repeated analyzer input construction | real |
| 21 | 1 | 13 | _funding_rpc.py, funding.py | Parallel docstrings for the wrapped vs wrapper transfer-log API | incidental |
| 22 | 1 | 12 | fresh_wallet.py, size_anomaly.py | Duplicated batch-result `_handle_batch_result` gather/zip knowledge | real; resolved, see the 2026-10-19 cluster 22 resolution update below |
| 23 | 1 | 12 | test_clob_client.py | Repeated paged-market fixture | real |
| 24 | 1 | 11 | test_redis_contract.py | Repeated competing-owner CAS contract scenario | real |
| 25 | 0 | 0 | compare_owned_then_execute (new) | **Resolved**: the watch/compare-owner/MULTI/EXEC CAS skeleton now has one home (`_channel_delivery.compare_owned_then_execute`, terminal operation supplied as an explicit callable); `compare_and_delete` and `compare_and_expire` delegate to it | resolved (PR #157) |
| 26 | 1 | 10 | ingestor/test_models.py | Repeated model-construction blocks | real |
| 27 | 1 | 10 | test_trade_row_v2_properties.py | Repeated property-based row assertions | real |
| 28 | 0 | 0 | _entry_bootstrap.py (new) | **Resolved**: the script entry-point sibling-import bootstrap now has one home (`scripts/_entry_bootstrap.py`); both entry points load it | resolved (PR #160) |
| 29 | 1 | 8 | _v2_walk_harness.py, test_v2_cursor_walk.py | `CursorWalk` harness construction recipe duplicated between `build_walk` and an inline jitter-injection test setup | real |
| 30 | 1 | 8 | test_endpoint_switch_resume.py | Repeated v1->v2 endpoint-switch test bootstrap (server/redis/seed/poller wiring block) | real |
| 31 | 1 | 6 | storage/models.py | SQLAlchemy `datetime.now(UTC)` timestamp column defaults | incidental |
| 32 | 1 | 6 | test_gamma_client.py | Repeated paged-response fixture | real |

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

### 2026-10-19 resolution update: cluster 25 (Redis watch/compare-then-execute CAS logic)

Cluster 25 is resolved by the dedup card for cluster 25: draft PR
[#157](https://github.com/pselamy/polymarket-insider-tracker/pull/157) from
branch `ralph/dedup-cas`. The Redis delivery-claim safety rule — open a
transactional pipeline, `WATCH` the claim key, `GET` and decode the stored
ownership token, `UNWATCH` and return `False` on a foreign or missing token
without ever issuing a transaction, otherwise `MULTI`, queue the terminal
operation once, `EXEC`, return `True` — moved to exactly one home,
`src/polymarket_insider_tracker/alerter/_channel_delivery.py`
`compare_owned_then_execute(self, key, token, queue_terminal)`. The terminal
operation is an explicit callable against the tiny `_QueuedPipeline` Protocol
(the `delete`/`expire` surface) — not a boolean/mode flag, per the card. The two
former copies remain the named entry points pinned by
`tests/alerter/test_history_extraction.py` and delegate to the home:
`compare_and_delete` queues `pipe.delete(key)`, `compare_and_expire` queues
`pipe.expire(key, ttl)`. The `release_channel_claim`/`extend_channel_claim`
wrappers and their `WatchError` translation to `False` are untouched, and all
tests pass unchanged (only the US-001 characterization tests were added, before
the refactor).

Re-measurement with the pinned scanner (jscpd 5.3.3 receipt verified via
`mise --yes --locked exec -- jscpd --version`, same SCAN_POLICY at the 50-token
tier, no ignore lists or exclusions), enumerating the authored scope exactly
as `verify_sources` does — `quality.source_scope.discover` + `validate_path`,
skipping the 5 rejected `.pyi` stubs, with the 399-line size hook deliberately
not applied — then staging with `quality.duplication_inputs.stage_inputs` and
calling `quality.duplication.scan(root, config, staged, output, minimum=50,
deadline)`:

- Before (baseline commit `2bdf748`, the pre-refactor head of this branch,
  same enumeration): **90 clone groups, 1138 duplicated lines, 1.89%** over
  281 authored files (248 sources tokenized). The cluster-25 pair
  `src/.../alerter/_channel_delivery.py:100-109 <-> 132-141` (73 tokens,
  10 lines) reproduces there.
- After (this change, commit `7053893`): **89 clone groups, 1128 duplicated
  lines, 1.87%** over 281 authored files (248 sources tokenized).

Set-difference of the raw pair sets between the two scans: exactly the one
cluster-25 pair disappeared and **no new pair appeared anywhere**. Compared
with the round-1 scan above (86 groups), the four added pairs are self-pairs
inside `tests/alerter/test_cas_characterization.py` (53, 57, 61, and 113
tokens), the US-001 characterization module added by this card and present in
both measured trees; they are not attributable to the dedup change. Cluster 24
(the competing-owner scenario clone inside
`tests/integration/test_redis_contract.py`) is a different card's scope and is
untouched. `src/...` abbreviates `src/polymarket_insider_tracker`.

### 2026-10-19 rebase re-measurement (rebased onto main after #158)

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


## 2026-10-19 resolution update: cluster 28 (script entry-point bootstrap)

Cluster 28's single clone pair had `scripts/runtime_services.py` and
`scripts/verify.py` each rebuilding the same entry-point bootstrap: create a
unique synthetic package (`module_from_spec(ModuleSpec(name, loader=None,
is_package=True))`), append the script's own directory to its `__path__`,
register it in `sys.modules`, save `__package__`/`__spec__`, repoint them at the
new package (spec name `f"{helper}.entry"`) so the private sibling modules import
under it, and restore both in a `finally`. The copies differed only in the helper
prefix (`_polymarket_services_` vs `_polymarket_verifier_`) and the random source
(`uuid.uuid4()` plus the module's reusable `uuid` import vs a local `uuid4`
alias), so no behaviour needed adjudicating.

That knowledge now has one home, `scripts/_entry_bootstrap.py`
(`sibling_imports`, a `@contextmanager`). Both entry points load it by file path
with `runpy.run_path(Path(__file__).with_name("_entry_bootstrap.py"))` — the
module cannot be imported, because the package machinery it provides does not
exist until it has run, and `runpy` carries no `sys.path` mutation, so the
isolated (`-I -S`) standalone-verifier contract still holds. Each call site keeps
its own prefix in `with _sibling_imports(globals(), "<prefix>"):` and loses only
the now-unused `importlib`/`uuid4` imports. All three invocation modes stay
correct: direct `python scripts/x.py`, `python -m scripts.x`, and
`spec_from_file_location` loading under an arbitrary name (including a copied
standalone `scripts/` tree). Every existing test passes unchanged; the added
characterization tests pin all of it for both entry points.

Re-measured with the identical pinned method (jscpd 5.3.3, `SCAN_POLICY` at the
50-token tier, empty config, no ignore lists or exclusions), enumerating the
authored scope as `quality.source_scope.discover` + `validate_path` (`.py` only,
size hook not applied) and staging with `quality.duplication_inputs.stage_inputs`:

- Before (branch base `6be456a`, the cluster-22 resolution; 282 authored Python
  files): **85 clone groups, 1080 duplicated lines, 1.80%**.
- After (this change, 284 authored files; the two additions are
  `scripts/_entry_bootstrap.py` and
  `tests/tooling/test_entry_bootstrap_characterization.py`): **84 clone groups,
  1071 duplicated lines, 1.78%**.

Rebased onto main after #157 (cluster 25, 283 files: 88 groups, 1116 lines,
1.85%), the identical method measures **87 clone groups, 1107 duplicated lines,
1.83%** over 285 authored files (252 sources tokenized, 0 clone groups touching
`scripts/_entry_bootstrap.py`, `scripts/runtime_services.py` or
`scripts/verify.py`): −1 group and −9 lines, exactly the cluster-28 clone.

Set-difference of the raw clone lists: exactly the cluster-28 clone
(`scripts/runtime_services.py:46-54 <-> scripts/verify.py:27-35`, 69 tokens / 9
lines) disappeared and **no new clone group appeared anywhere**. The
characterization test was written with a single consolidated loader program so
that it neither self-duplicated nor copied the `_LOAD`/`_LOAD_VERIFIER` loader
text that cluster 16 still owns. Resolved by draft PR
[#160](https://github.com/pselamy/polymarket-insider-tracker/pull/160) from branch
`ralph/dedup-bootstrap`.

## 2026-10-19 resolution update: cluster 7 (signal model property and to_dict serialization)

Three clone groups carried the same knowledge across
`detector/models.py` and `storage/repos.py`: identical confidence-threshold
classification (`is_high_confidence` >= 0.7, `is_very_high_confidence` >= 0.85
on all three signal classes), identical trade-anchored identity accessors
(`wallet_address`, `market_id`, `trade_size_usdc` reading `trade_event`), the
identical six-entry trade-head `to_dict` serialization, and the identical
"unrecorded" delivery default on `RiskAssessment` and `RiskAssessmentDTO`.
Behaviour was characterized first (`tests/detector/test_models.py`,
`tests/storage/test_risk_assessments_schema.py`) and no behavioural difference
existed between the copies; the >= boundary is the only threshold semantics
and the serialization output is byte-identical before and after (field names,
types and ordering unchanged).

One home per knowledge piece: thresholds live in named constants
(`HIGH_CONFIDENCE_THRESHOLD`, `VERY_HIGH_CONFIDENCE_THRESHOLD`) consumed by
`is_high_confidence_score` / `is_very_high_confidence_score`, whose shared
getter functions back each signal class's property objects; trade-head entries
are built once by `serialized_trade_head` and spread into all three `to_dict`s;
`UNRECORDED_DELIVERY_DISPOSITION` lives in the dependency-free
`polymarket_insider_tracker/constants.py`, which both `detector/models.py` and
`storage/repos.py` import (the domain model no longer imports the persistence
layer, and the warning-strict storage-only entrypoints stay free of detector's
dependency tree). No base classes, mixins,
registries or `__module__` rewriting; public import paths unchanged and every
existing test passes unchanged.

Re-measured on the round-1 rebase onto `origin/main` at `6be456a` (this PR is
drafted against that tree), with the identical pinned method (jscpd 5.3.3,
`SCAN_POLICY` at the 50-token tier, empty config, no ignore lists or
exclusions), enumerating the authored scope as
`quality.source_scope.discover` + `validate_path` and staging it manually:

- Before (rebased card chain with the US-001 characterization tests, pre-refactor,
  283 authored Python files): **87 clone groups, 1095 duplicated lines, 1.81%**.
- After (the cluster-7 refactor applied, same 283 files): **84 clone groups,
  1035 duplicated lines, 1.71%**.

Set-difference of the raw clone lists: exactly the three cluster-7 clones
(`src/.../detector/models.py:38-75 <-> src/.../detector/models.py:109-146`,
197 tokens; `src/.../detector/models.py:57-70 <->
src/.../detector/models.py:182-195`, 67 tokens;
`src/.../detector/models.py:244-251 <-> src/.../storage/repos.py:574-581`,
52 tokens) disappeared and **no new clone group appeared anywhere**. Against
`origin/main` directly, the branch additionally contributes only the two
intra-file clones of the characterization test module required by US-001.

Round-3 rebase re-measurement (onto `origin/main` at `382a0f6`, same pinned
method and enumeration): `origin/main` **88 groups, 1116 lines, 1.85%** over 283
files; this branch **87 groups, 1071 lines, 1.76%** over 285 files (the two
added files are `constants.py` and the characterization module `tests/detector/test_models.py`).
Set-difference: the three cluster-7 clones above disappeared; the only additions
are the two intra-file characterization-test clones (51 and 68 tokens, now at
`tests/detector/test_models.py:313-317 <-> 326-330` and `321-330 <-> 334-343`).

### Round-3 review correction: two clones were hidden, not removed

The round-1 and round-3 rebase figures above claimed the 52-token
`RiskAssessment`/`RiskAssessmentDTO` clone was gone and that the 197-token
signal clone left nothing behind. Both claims came from token-shuffling edits,
not from removing knowledge: `RiskAssessmentDTO` restated its plain defaults as
`field(default=...)`, and `SizeAnomalySignal` bound the shared property objects
in a different order from `FreshWalletSignal`. Both edits are reverted.
`RiskAssessmentDTO` uses plain defaults again, with only the disposition
reading `UNRECORDED_DELIVERY_DISPOSITION`, and both signal classes bind the
accessors in the same order. The characterization module's three copy-pasted
threshold tests were also folded into one table-driven test over the three
builders, with the same twelve cases.

Re-measured with the identical pinned method and enumeration: `origin/main` at
`382a0f6` **88 groups, 1116 lines, 1.85%** over 283 files; this branch
**87 groups, 1080 lines, 1.78%** over 285 files. Raw clone-list difference:

- removed: `src/.../detector/models.py:38-75 <-> 109-146` (197 tokens) and
  `src/.../detector/models.py:57-70 <-> 182-195` (67 tokens);
- added: `src/.../detector/models.py:106-121 <-> 155-170` (96 tokens);
- unchanged on both sides: `src/.../detector/models.py:265-272 <->
  src/.../storage/repos.py:569-576` (52 tokens; main `244-251 <-> 574-581`).

Both remaining groups are left visible and are verdicted **incidental**:

- **96-token signal declaration block.** It spans the `confidence`, `factors`
  and `timestamp` field declarations, the five `property(...)` bindings and the
  `to_dict` opening that spreads `serialized_trade_head`. Every behaviour it
  names (thresholds, accessors, trade-head entries) now has one home in the
  module helpers. What remains is each frozen dataclass declaring its own
  public surface. Collapsing it further would need a shared base class or
  mixin, which this card forbids ("no base classes/mixins ... just to share
  lines").
- **52-token DTO/domain field-default mirror.** `RiskAssessmentDTO` is the
  persistence row shape (it mirrors `RiskAssessmentModel` columns), and
  `RiskAssessment` is the domain record. Their Slice 003 optional fields share
  names and `None`/`False` defaults because they describe the same persisted
  facts at two layers. The only shared *value* that could drift, the
  `"unrecorded"` disposition, has one home in the dependency-free
  `constants.py`. Sharing the field list would need dataclass inheritance or a
  generated schema across the persistence/domain boundary. Either way storage
  would depend on the domain shape or the reverse, and slice 004 will change
  the domain side independently. This is an intentional layer boundary and is
  recorded rather than disguised.

### Round-3 docstring revert and rebase onto `69c4807`

The `storage/repos.py` module docstring and the `RiskAssessmentDTO` docstring
had been shortened by this card for no cluster-7 reason; both are restored
byte-for-byte to `origin/main`, so the card's `repos.py` diff is only the
`UNRECORDED_DELIVERY_DISPOSITION` import and the disposition default (656 lines,
main 655). The branch was then rebased onto `origin/main` at `69c4807` (after
#160, cluster 28). Re-measured with the identical pinned method and
enumeration: `origin/main` **87 groups, 1107 lines, 1.83%** over 285 files; this
branch **86 groups, 1071 lines, 1.76%** over 287 files (the two additions are
`constants.py` and `tests/detector/test_models.py`). Raw clone-list difference:

- removed: `src/.../detector/models.py:38-75 <-> 109-146` (197 tokens) and
  `src/.../detector/models.py:57-70 <-> 182-195` (67 tokens);
- added: `src/.../detector/models.py:106-121 <-> 155-170` (96 tokens, the
  incidental signal declaration block verdicted above);
- unchanged on both sides: the 52-token DTO/domain mirror, now
  `src/.../detector/models.py:265-272 <-> src/.../storage/repos.py:575-582`
  (main `244-251 <-> 574-581`; the one-line shift is the constant import).

No other clone group appeared or disappeared. Resolved by draft PR
[#159](https://github.com/pselamy/polymarket-insider-tracker/pull/159) from branch
`ralph/dedup-models`.

### 2026-10-19 resolution update: cluster 2 (integration-test pipeline wiring bootstrap)

Cluster 2 is resolved by the dedup card for cluster 2 on branch `ralph/dedup-wiring`
(draft PR [#163](https://github.com/pselamy/polymarket-insider-tracker/pull/163)). The pipeline-wiring test data and scaffolding that
`test_end_to_end.py`, `test_pipeline.py`, `test_pipeline_persistence.py` and
`test_postgres_journey.py` each copied now has exactly one home, the existing
`tests/integration/_journey_harness.py`: `make_sample_trade_event`, `market_for_trade`,
`seeded_server`, `journey_settings`, plus the already-present `wire_journey_pipeline`,
`running_pipeline`, `pipeline_row`, `persisted_assessments` and `run_until`, which the
suites now import instead of re-defining. Every parameter that varies (settings, market,
database manager, channels, trades server, clock) stays an explicit argument at the call
site; the test-local names other modules imported (`_market`, `_seeded_server`, ...) were
repointed to the harness. One difference was kept on purpose:
`test_persist_assessment.sample_trade` is a different trade (asset `asset_xyz`, $0.42 x
1000) and is not merged. `tests/test_pipeline.py::_run_until` (25000 x 0.5 ms) also differs
from the harness `run_until` (5 s x 2 ms) and stays local.

Re-measured with the identical pinned method (jscpd 5.3.3, `SCAN_POLICY` at the 50-token
tier, empty config, no ignore lists or exclusions; authored scope via
`quality.source_scope.discover` + `validate_path`, `.pyi` skipped, size hook not applied,
staged with `quality.duplication_inputs.stage_inputs`):

- Before (`origin/main` at `1bdccbc`, after #159 and #162, 288 authored files): **84 clone
  groups, 1031 duplicated lines, 1.69%**.
- After (this change rebased onto `1bdccbc`, 289 authored files; the addition is
  `tests/integration/test_pipeline_wiring_characterization.py`): **74 clone groups, 909
  duplicated lines, 1.49%**.

(The pre-rebase measurement against `69c4807` was 87 -> 77 groups and 1107 -> 985 lines; the
rebased figures above supersede it.)

Set-difference of the raw pair multisets (file pair, count): ten pairs disappeared
(`test_end_to_end` <-> `test_pipeline` x5, `test_end_to_end` <-> `test_postgres_journey` x2,
`test_end_to_end` <-> `test_pipeline_persistence`, `test_persist_assessment` <->
`test_pipeline_persistence`, `test_persist_assessment` <-> `test_pipeline`) and **no pair
was added or increased**. The `test_terminal_error_durable.py` (cluster 10) and `test_main.py`
(cluster 17) clones are other cards' scope and are untouched.

## 2026-10-19 partial resolution update: cluster 1 (sample_trade/sample_wallet_profile fixtures)

Measured with the pinned jscpd 5.3.3 through the vendored
`quality.duplication.scan(..., minimum=50)` over `quality.source_scope.discover`
(the size check is skipped because `scripts/traceability.py` already exceeds 399
lines), on identical enumeration at both ends. Baseline is `origin/main` at
`370e77b` (after #163): **74 groups, 909 duplicated lines, 1.49%**, 258 sources
tokenized. This card rebased onto it: **72 groups, 858 duplicated lines, 1.40%**,
259 sources. No new group appeared: matched by token count, line count and file
pair, the only differences are the two removed groups below.

Removed groups: the identical `sample_trade`/`sample_wallet_profile` pair that
`test_history.py` and `test_scorer.py` each carried (153 tokens, 39 lines) now has
one home in `tests/conftest.py`, pinned by `tests/test_sample_fixtures.py`; and the
formatter-internal `TradeEvent` field-by-field copy (68 tokens, 12 lines), now
`dataclasses.replace`.

Cluster 1 is **not fully resolved**. Seven cluster-1 groups remain on purpose
(tokens/lines, pair):

| tokens | lines | A | B |
| --- | --- | --- | --- |
| 118 | 31 | test_formatter.py | test_scorer.py |
| 59 | 17 | test_formatter.py | test_scorer.py |
| 55 | 17 | test_formatter.py | test_history.py |
| 50 | 15 | test_formatter.py | test_history.py |
| 57 | 12 | test_scorer.py | test_size_anomaly.py |
| 51 | 6 | test_scorer.py | test_scorer.py |
| 50 | 7 | test_history.py | test_history.py |

They share a shape but encode different test knowledge (different `factors`,
question text, prices, sizes and ages), so sharing them would change behaviour or
need a parameterised factory that merely hides the difference. The formatter and
size-anomaly `sample_trade` fixtures likewise differ in value and stay local.

## Cluster 4 resolution update (2026-10-19, PR #166)

Measured with the pinned jscpd 5.3.3 through the vendored
`quality.duplication.scan(..., minimum=50)` over `quality.source_scope.discover`
(size check skipped, as for cluster 1), whole tree, 295 sources at both ends.
Baseline is `origin/main` at `430c9af` (after #164): **72 groups, 858 duplicated
lines, 1.40%**. This card rebased onto it: **63 groups, 766 duplicated lines,
1.25%**. Compared as a multiset of (file pair, token count), exactly nine
`test_dispatcher.py` self-clones were removed and no group was added or grew.
Redaction cases whose provider response bodies genuinely differ stay as separate
Discord/Telegram functions (they share only the assertion helper). The earlier
74 -> 65 figures covered a narrower file set and are superseded by these.

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

## 2026-10-19 resolution update: cluster 8 (health-server component wiring)

`wire_components` and `probe` in `tests/ingestor/test_health_server.py` now own the
per-component checker wiring and the serve/GET/stop sequence; the four cluster-8
clone pairs are gone. Re-measured with the pinned jscpd 5.3.3 and the same flags over
the tracked `src tests scripts alembic quality` Python files plus `conftest.py`
(292 files; not the discovered authored scope above, so totals are not comparable to
the figures above): 56 clone groups before, 52 after, none in `test_health_server.py`
and none added. All assertions are unchanged.

## Cluster 6 resolution (trade-poller cycle setups and repair assertions)

Measured with the identical pinned method (jscpd 5.3.3, `SCAN_POLICY` at the 50-token
tier, empty config, no ignores) over the 292 authored Python files enumerated by
`quality.source_scope.discover`: `origin/main` at `b0f37b6` (after #169) reports **52 clone groups,
625 duplicated lines, 1.02%**; this tree reports **46 groups, 561 lines, 0.92%**. The six
`test_trade_poller.py` self-pairs (12, 18, 9, 6, 13 and 6 lines) are absent and the
file-pair set difference is empty, so no new clone group was introduced. Other
clusters' rows above are historical receipts from their own trees.

## 2026-10-19 resolution update: cluster 10 (terminal-error durable test wiring)

`_wire_poller_pipeline`, `_pipeline_with_terminal_401`, `_fail_poller_cycle`,
`_report_poller_failure` and `_assert_poller_401_row` in
`tests/test_terminal_error_durable.py` now own the fake-boundary pipeline wiring, the
terminal HTTP 401 poller failure, the hand-off to `_handle_worker_failure`, and the
persisted-row assertion that three tests restated (a fourth restated the wiring); the three cluster-10 clone pairs (68, 62 and 88 tokens) are gone.
Re-measured over the whole tree with the pinned jscpd 5.3.3 and the same flags and 292
files as the cluster-6 update: `origin/main` at `986a7c6` (after #171) reports 46 clone
groups (561 duplicated lines, 0.92%); this branch rebased onto it reports 43 (518 lines,
0.85%). Matched by file pair and line count the only difference is the three removed
`test_terminal_error_durable.py` self-pairs (12, 14 and 17 lines); no group was added or grew.
`test_fresh_pipeline_reads_terminal_reason_from_durable_row` keeps its own state
setup because it seeds a prior row rather than failing the poller. No assertion
changed and the six collected test IDs are unchanged.

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
