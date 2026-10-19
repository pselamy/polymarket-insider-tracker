# Clone inventory (jscpd 5.3.3, pinned reference policy)

Re-measured 2026-10-19 for dedup cluster 5 (URL credential parsing and
redaction). The authored scope is enumerated by
`quality.source_scope.discover` + `validate_path` (the `verify_sources` file
set without its 399-line size hook; see
[relentless-ci-adoption.md](relentless-ci-adoption.md) for the coupling):
**268 authored Python files** after this card adds `scripts/_url_redaction.py`
and `tests/tooling/test_url_redaction.py`. Vendored `SCAN_POLICY` at the
50-token tier: `--mode mild --format python --min-tokens 50 --min-lines 4
--threshold 0 --no-gitignore --workers 1`, empty config, no ignore lists, no
exclusions.

Totals after the cluster "introduced by #151" card (resolution below):
**90 clone groups, 1142 duplicated lines, 1.95% duplication** over **273
authored Python files** (the scanner's own statistics report 240 sources: files
it actually tokenized after skipping ones smaller than the clone minimums).

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
and after on the same tree: **99 -> 90 clone groups, 1271 -> 1142 duplicated
lines, 2.17% -> 1.95%**; no new clone group (file pair) was introduced, and the
only removed pairs are the three cluster pairs plus one pre-existing
`test_end_to_end` self-scaffold clone that disappeared once the dry-run journey
reused `wire_journey_pipeline`. The scope gained five authored files
(`tests/integration/conftest.py`, `tests/integration/_journey_harness.py`,
`tests/ingestor/_poller_harness.py`, `tests/integration/test_journey_fakes_contract.py`,
`tests/ingestor/test_poller_harness_characterization.py`), so the authored file
count is now 273. Resolved by PR #155.

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
| 2 | 12 | 147 | test_end_to_end.py, test_persist_assessment.py, test_pipeline.py, test_pipeline_persistence.py, test_postgres_journey.py | Repeated `wire_pipeline(...)` bootstrap blocks with the same fake-stack wiring | real |
| 3 | 7 | 94 | test_redaction.py | Repeated harness setups asserting the same redaction policy across inputs | real |
| 4 | 9 | 92 | test_dispatcher.py | Discord/Telegram parallel test cases sharing failure/retry scaffolds | real |
| 5 | 0 | 0 | _url_redaction.py (new) | **Resolved**: identical URL parsing + credential extraction now has one home (`scripts/_url_redaction.py`); both former copies import it | resolved (PR #153) |
| 6 | 6 | 64 | test_trade_poller.py | Repeated poller cycle setups and repair assertions | real |
| 7 | 3 | 60 | detector/models.py, storage/repos.py | Duplicated dataclass property/`to_dict` serialization knowledge across signal models | real |
| 8 | 4 | 47 | test_health_server.py | Repeated `set_component_checker` wiring for up/degraded/down cases | real |
| 9 | 3 | 44 | discord.py, telegram.py | Duplicated rate-limit wait, `_backoff`, and failure-redaction channel logic | real; resolved, see the 2026-10-19 resolution update below |
| 10 | 3 | 43 | test_terminal_error_durable.py | Repeated pipeline wiring + terminal-failure sequencing | real |
| 11 | 3 | 33 | profiler/test_models.py | Repeated `Transaction` construction blocks | real |
| 12 | 2 | 32 | test_funding.py, test_funding_rpc.py | Parallel method docstrings/signatures describing the same RPC API | real |
| 13 | 2 | 30 | rev_20260522_1130_risk_assessments.py, rev_20260910_0000_safe_observable_operation.py, rev_20260920_0000_terminal_errors.py | Alembic revision scaffolding (`__all__`, `upgrade()` head) | incidental |
| 14 | 2 | 29 | test_publisher.py, test_publisher_codec.py | Duplicated codec fixture/assertion pairs | real |
| 15 | 2 | 26 | test_trade_row_v2.py, test_trade_rows.py | Duplicated row-decoding assertions | real |
| 16 | 3 | 25 | test_service_entrypoint.py, test_verifier_standalone.py | Duplicated `_verifier` module-loader helper and entry bootstrap | real |
| 17 | 2 | 22 | test_main.py | Repeated settings/pipeline wiring snippets | real |
| 18 | 1 | 20 | test_sniper.py | Repeated cluster-feeding test construction | real |
| 19 | 2 | 18 | test_complexipy.py, test_gate_negatives.py, test_policy_snapshot.py | Duplicated `spec_from_file_location` verifier loader helper | real |
| 20 | 1 | 17 | test_analyzer.py | Repeated analyzer input construction | real |
| 21 | 1 | 13 | _funding_rpc.py, funding.py | Parallel docstrings for the wrapped vs wrapper transfer-log API | incidental |
| 22 | 1 | 12 | fresh_wallet.py, size_anomaly.py | Duplicated batch-result `_handle_batch_result` gather/zip knowledge | real |
| 23 | 1 | 12 | test_clob_client.py | Repeated paged-market fixture | real |
| 24 | 1 | 11 | test_redis_contract.py | Repeated competing-owner CAS contract scenario | real |
| 25 | 1 | 10 | _channel_delivery.py | `compare_and_delete` vs `compare_and_expire`: near-identical Redis watch/CAS logic | real |
| 26 | 1 | 10 | ingestor/test_models.py | Repeated model-construction blocks | real |
| 27 | 1 | 10 | test_trade_row_v2_properties.py | Repeated property-based row assertions | real |
| 28 | 1 | 9 | runtime_services.py, verify.py | Duplicated script entry-point bootstrap (`_helper_package`/`__spec__` block) | real |
| 29 | 1 | 8 | _v2_walk_harness.py, test_v2_cursor_walk.py | `CursorWalk` harness construction recipe duplicated between `build_walk` and an inline jitter-injection test setup | real |
| 30 | 1 | 8 | test_endpoint_switch_resume.py | Repeated v1->v2 endpoint-switch test bootstrap (server/redis/seed/poller wiring block) | real |
| 31 | 1 | 6 | storage/models.py | SQLAlchemy `datetime.now(UTC)` timestamp column defaults | incidental |
| 32 | 1 | 6 | test_gamma_client.py | Repeated paged-response fixture | real |

Real knowledge dominates: most clusters are duplicated maintenance surfaces —
fixture blocks, wiring scaffolds, parallel Discord/Telegram knowledge, and
Redis CAS logic. The three incidental clusters are convention-mandated
scaffolding (Alembic revisions, SQLAlchemy column defaults) and parallel API
docstrings. The cluster table above is the original knowledge grouping; with
cluster 5 and the "introduced by #151" cluster now resolved, the current total is
the 90 groups regenerated in the per-clone list below from the pinned scan.

## Every clone (scanner output, ordered by tokens)

`tokens`/`lines` are the clone size; ranges are 1-based inclusive
(`start-end`). Format: `A:start-end <-> B:start-end`.

| tok | ln | A | B |
| --- | --- | --- | --- |
| 197 | 38 | src/.../detector/models.py:38-75 | src/.../detector/models.py:109-146 |
| 131 | 34 | tests/alerter/test_history.py:24-57 | tests/detector/test_scorer.py:28-61 |
| 120 | 20 | tests/test_redaction.py:1787-1806 | tests/test_redaction.py:1845-1864 |
| 118 | 31 | tests/alerter/test_formatter.py:75-105 | tests/detector/test_scorer.py:74-104 |
| 110 | 18 | src/.../alerter/channels/discord.py:76-93 | src/.../alerter/channels/telegram.py:96-113 |
| 102 | 20 | tests/ingestor/test_publisher.py:23-42 | tests/ingestor/test_publisher_codec.py:50-69 |
| 96 | 20 | tests/detector/test_sniper.py:278-297 | tests/detector/test_sniper.py:314-333 |
| 88 | 17 | tests/test_terminal_error_durable.py:84-100 | tests/test_terminal_error_durable.py:153-169 |
| 84 | 24 | tests/alerter/test_formatter.py:20-43 | tests/alerter/test_history.py:16-39 |
| 84 | 10 | tests/test_redaction.py:1152-1161 | tests/test_redaction.py:1915-1924 |
| 82 | 18 | tests/ingestor/test_trade_poller.py:288-305 | tests/ingestor/test_trade_poller.py:843-860 |
| 81 | 15 | tests/integration/test_end_to_end.py:66-80 | tests/integration/test_postgres_journey.py:147-161 |
| 80 | 12 | src/.../alerter/channels/discord.py:157-168 | src/.../alerter/channels/telegram.py:181-192 |
| 80 | 18 | tests/integration/test_end_to_end.py:67-84 | tests/test_pipeline.py:53-70 |
| 80 | 19 | tests/profiler/test_funding.py:37-55 | tests/profiler/test_funding_rpc.py:48-66 |
| 78 | 9 | tests/ingestor/test_trade_poller.py:306-314 | tests/ingestor/test_trade_poller.py:862-867 |
| 77 | 17 | tests/profiler/test_analyzer.py:336-352 | tests/profiler/test_analyzer.py:382-398 |
| 77 | 10 | tests/test_redaction.py:971-980 | tests/test_redaction.py:1152-1161 |
| 76 | 9 | tests/test_redaction.py:972-980 | tests/test_redaction.py:1054-1062 |
| 75 | 14 | tests/ingestor/test_health_server.py:123-136 | tests/ingestor/test_health_server.py:259-272 |
| 74 | 13 | src/.../profiler/_funding_rpc.py:108-120 | src/.../profiler/funding.py:258-270 |
| 74 | 12 | tests/test_main.py:304-315 | tests/test_main.py:362-373 |
| 73 | 10 | src/.../alerter/_channel_delivery.py:100-109 | src/.../alerter/_channel_delivery.py:132-141 |
| 73 | 14 | tests/alerter/test_dispatcher.py:421-434 | tests/alerter/test_dispatcher.py:545-559 |
| 72 | 13 | tests/alerter/test_dispatcher.py:403-415 | tests/alerter/test_dispatcher.py:523-535 |
| 71 | 21 | tests/alerter/test_formatter.py:23-43 | tests/detector/test_scorer.py:23-43 |
| 70 | 11 | tests/integration/test_redis_contract.py:481-491 | tests/integration/test_redis_contract.py:526-536 |
| 69 | 9 | scripts/runtime_services.py:46-54 | scripts/verify.py:27-35 |
| 69 | 11 | tests/ingestor/test_health_server.py:104-114 | tests/ingestor/test_health_server.py:140-150 |
| 69 | 15 | tests/integration/test_end_to_end.py:52-66 | tests/test_pipeline.py:82-96 |
| 68 | 14 | src/.../alerter/channels/discord.py:93-106 | src/.../alerter/channels/telegram.py:113-126 |
| 68 | 12 | tests/alerter/test_formatter.py:502-513 | tests/alerter/test_formatter.py:680-691 |
| 68 | 14 | tests/test_terminal_error_durable.py:36-49 | tests/test_terminal_error_durable.py:81-94 |
| 67 | 14 | src/.../detector/models.py:57-70 | src/.../detector/models.py:182-195 |
| 67 | 8 | tests/ingestor/test_endpoint_switch_resume.py:60-67 | tests/ingestor/test_endpoint_switch_resume.py:91-98 |
| 67 | 11 | tests/ingestor/test_health_server.py:65-75 | tests/ingestor/test_health_server.py:276-286 |
| 67 | 10 | tests/test_main.py:305-314 | tests/test_main.py:572-581 |
| 66 | 15 | tests/ingestor/test_trade_row_v2.py:50-64 | tests/ingestor/test_trade_rows.py:37-51 |
| 65 | 8 | tests/tooling/test_service_entrypoint.py:148-155 | tests/tooling/test_service_entrypoint.py:176-183 |
| 64 | 13 | tests/ingestor/test_trade_poller.py:521-533 | tests/ingestor/test_trade_poller.py:583-595 |
| 64 | 17 | tests/test_redaction.py:2024-2040 | tests/test_redaction.py:2077-2093 |
| 62 | 12 | tests/ingestor/test_trade_poller.py:233-244 | tests/ingestor/test_trade_poller.py:261-272 |
| 62 | 16 | tests/integration/test_end_to_end.py:69-84 | tests/test_pipeline_persistence.py:47-62 |
| 62 | 12 | tests/test_terminal_error_durable.py:38-49 | tests/test_terminal_error_durable.py:152-163 |
| 61 | 6 | src/.../storage/models.py:51-56 | src/.../storage/models.py:212-217 |
| 61 | 11 | tests/integration/test_end_to_end.py:342-352 | tests/test_pipeline.py:619-628 |
| 61 | 7 | tests/integration/test_end_to_end.py:94-100 | tests/test_pipeline.py:536-542 |
| 61 | 13 | tests/profiler/test_funding.py:60-72 | tests/profiler/test_funding_rpc.py:70-82 |
| 61 | 13 | tests/profiler/test_models.py:204-216 | tests/profiler/test_models.py:229-241 |
| 60 | 10 | tests/ingestor/test_models.py:218-227 | tests/ingestor/test_models.py:242-251 |
| 60 | 15 | tests/test_redaction.py:2257-2271 | tests/test_redaction.py:2374-2388 |
| 59 | 17 | tests/alerter/test_formatter.py:92-108 | tests/detector/test_scorer.py:91-107 |
| 59 | 11 | tests/ingestor/test_health_server.py:54-64 | tests/ingestor/test_health_server.py:93-103 |
| 58 | 6 | tests/ingestor/test_gamma_client.py:167-172 | tests/ingestor/test_gamma_client.py:225-233 |
| 57 | 12 | tests/detector/test_scorer.py:620-631 | tests/detector/test_size_anomaly.py:858-869 |
| 57 | 6 | tests/ingestor/test_trade_poller.py:912-917 | tests/ingestor/test_trade_poller.py:977-981 |
| 57 | 10 | tests/ingestor/test_trade_row_v2_properties.py:95-104 | tests/ingestor/test_trade_row_v2_properties.py:114-123 |
| 57 | 6 | tests/integration/test_end_to_end.py:85-90 | tests/test_pipeline.py:104-109 |
| 57 | 13 | tests/test_redaction.py:1547-1559 | tests/test_redaction.py:1886-1897 |
| 56 | 9 | tests/ingestor/test_publisher.py:247-255 | tests/ingestor/test_publisher.py:272-280 |
| 55 | 12 | src/.../detector/fresh_wallet.py:223-234 | src/.../detector/size_anomaly.py:386-397 |
| 55 | 11 | tests/alerter/test_dispatcher.py:124-134 | tests/alerter/test_dispatcher.py:273-282 |
| 55 | 10 | tests/alerter/test_dispatcher.py:228-237 | tests/alerter/test_dispatcher.py:309-318 |
| 55 | 10 | tests/alerter/test_dispatcher.py:888-897 | tests/alerter/test_dispatcher.py:915-925 |
| 55 | 17 | tests/alerter/test_formatter.py:88-104 | tests/alerter/test_history.py:83-99 |
| 55 | 9 | tests/tooling/test_service_entrypoint.py:14-22 | tests/tooling/test_verifier_standalone.py:43-51 |
| 53 | 7 | tests/ingestor/test_endpoint_switch_resume.py:71-77 | tests/ingestor/test_endpoint_switch_resume.py:98-104 |
| 53 | 10 | tests/profiler/test_models.py:205-214 | tests/profiler/test_models.py:215-224 |
| 53 | 10 | tests/profiler/test_models.py:205-214 | tests/profiler/test_models.py:255-264 |
| 53 | 16 | tests/test_persist_assessment.py:4-19 | tests/test_pipeline_persistence.py:5-20 |
| 53 | 9 | tests/tooling/test_complexipy.py:104-112 | tests/tooling/test_gate_negatives.py:47-55 |
| 53 | 8 | tests/tooling/test_service_entrypoint.py:148-155 | tests/tooling/test_verifier_standalone.py:165-172 |
| 52 | 15 | alembic/versions/rev_20260522_1130_risk_assessments.py:19-33 | alembic/versions/rev_20260920_0000_terminal_errors.py:19-33 |
| 52 | 8 | src/.../detector/models.py:244-251 | src/.../storage/repos.py:574-581 |
| 52 | 12 | tests/alerter/test_dispatcher.py:157-168 | tests/alerter/test_dispatcher.py:307-318 |
| 52 | 7 | tests/alerter/test_dispatcher.py:439-445 | tests/alerter/test_dispatcher.py:565-571 |
| 52 | 8 | tests/alerter/test_dispatcher.py:935-942 | tests/alerter/test_dispatcher.py:954-961 |
| 52 | 6 | tests/ingestor/test_trade_poller.py:339-344 | tests/ingestor/test_trade_poller.py:417-422 |
| 52 | 11 | tests/ingestor/test_trade_row_v2.py:134-144 | tests/ingestor/test_trade_rows.py:117-127 |
| 51 | 6 | tests/detector/test_scorer.py:548-553 | tests/detector/test_scorer.py:796-802 |
| 51 | 12 | tests/ingestor/test_clob_client.py:176-187 | tests/ingestor/test_clob_client.py:192-203 |
| 51 | 9 | tests/tooling/test_complexipy.py:105-113 | tests/tooling/test_policy_snapshot.py:77-85 |
| 50 | 15 | alembic/versions/rev_20260522_1130_risk_assessments.py:19-33 | alembic/versions/rev_20260910_0000_safe_observable_operation.py:24-38 |
| 50 | 7 | tests/alerter/test_dispatcher.py:384-390 | tests/alerter/test_dispatcher.py:494-500 |
| 50 | 15 | tests/alerter/test_formatter.py:74-88 | tests/alerter/test_history.py:69-83 |
| 50 | 7 | tests/alerter/test_history.py:549-555 | tests/alerter/test_history.py:567-573 |
| 50 | 8 | tests/ingestor/_v2_walk_harness.py:52-59 | tests/ingestor/test_v2_cursor_walk.py:301-308 |
| 50 | 9 | tests/integration/test_end_to_end.py:275-283 | tests/integration/test_postgres_journey.py:238-246 |
| 50 | 9 | tests/integration/test_end_to_end.py:344-352 | tests/test_pipeline.py:578-585 |
| 50 | 6 | tests/test_persist_assessment.py:96-101 | tests/test_pipeline.py:107-112 |

inventory only; the cluster-5 resolution above is the only product change
reflected here so far. Deduplication decisions for the remaining clusters belong
to later, separately authorized cards.

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
`src/...` abbreviates `src/polymarket_insider_tracker`. This document is
inventory only; the cluster-5, cluster-9, and "introduced by #151" resolutions
above are the only product changes reflected here. Deduplication decisions for
the remaining clusters belong to later, separately authorized cards.

