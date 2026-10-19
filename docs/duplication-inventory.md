# Clone inventory (jscpd 5.3.3, pinned reference policy)

Measured 2026-10-19 over the full authored scope (255 authored Python files
discovered by `quality.source_scope`), using the vendored `SCAN_POLICY` at the
50-token tier: `--mode mild --format python --min-tokens 50 --min-lines 4
--threshold 0 --no-gitignore --workers 1`, empty config, no ignore lists, no
exclusions.

Totals: **90 clone groups, 1214 duplicated lines, 2.19% duplication.**
Raw scanner report: `/tmp/duptest/full2/jscpd-report.json` (measurement-time
artifact; re-runnable with `mise --yes --locked exec -- jscpd` using the args
above).

Every clone reported by the scanner is listed below, first grouped into
clusters by the duplicated *knowledge* (same rule, mapping, query, validation,
or fixture — not merely similar text), then listed individually. A cluster is
annotated **real** (the same knowledge is genuinely maintained in two places and
a divergence would be a defect risk) or **incidental** (structurally similar
scaffolding required by convention, not shared knowledge).

## Clusters ordered by size

| # | Clones | Dup lines | Files | Knowledge | Verdict |
| --- | --- | --- | --- | --- | --- |
| 1 | 11 | 196 | test_formatter, test_history, test_scorer, test_size_anomaly | Repeated `sample_trade`/`sample_wallet_profile` fixtures and formatter/history case scaffolds | real |
| 2 | 12 | 147 | test_end_to_end, test_postgres_journey, test_persist_assessment, test_pipeline, test_pipeline_persistence | Repeated `wire_pipeline(...)` bootstrap blocks with the same fake-stack wiring | real |
| 3 | 7 | 94 | test_redaction (self) | Repeated harness setups asserting the same redaction policy across inputs | real |
| 4 | 9 | 92 | test_dispatcher (self) | Discord/Telegram parallel test cases sharing failure/retry scaffolds | real |
| 5 | 2 | 76 | scripts/_service_redaction, scripts/_verify_redaction | **Identical URL parsing + credential-extraction module** (pattern, `_split_userinfo`, `_extract_query_password`, `_query_credentials`, `_url_credentials`, `_credential_forms`, `_redacted_url`) | real |
| 6 | 6 | 64 | test_trade_poller (self) | Repeated poller cycle setups and repair assertions | real |
| 7 | 3 | 60 | detector/models (self), storage/repos | Duplicated dataclass property/`to_dict` serialization knowledge across signal models | real |
| 8 | 4 | 47 | test_health_server (self) | Repeated `set_component_checker` wiring for up/degraded/down cases | real |
| 9 | 3 | 44 | alerter/channels/discord, alerter/channels/telegram | Duplicated rate-limit wait, `_backoff`, and failure-redaction channel logic | real |
| 10 | 3 | 43 | test_terminal_error_durable (self) | Repeated pipeline wiring + terminal-failure sequencing | real |
| 11 | 3 | 25 | test_service_entrypoint, test_verifier_standalone | Duplicated `_verifier` module-loader helper and entry bootstrap | real |
| 12 | 3 | 33 | test_models (profiler, self) | Repeated `Transaction` construction blocks | real |
| 13 | 2 | 29 | test_publisher, test_publisher_codec | Duplicated codec fixture/assertion pairs | real |
| 14 | 2 | 32 | test_funding, test_funding_rpc | Parallel method docstrings/signatures describing the same RPC API | real |
| 15 | 2 | 22 | test_main (self) | Repeated settings/pipeline wiring snippets | real |
| 16 | 2 | 26 | test_trade_row_v2, test_trade_rows | Duplicated row-decoding assertions | real |
| 17 | 2 | 18 | test_complexipy, test_gate_negatives, test_policy_snapshot | Duplicated `spec_from_file_location` verifier loader helper | real |
| 18 | 2 | 30 | alembic revisions (risk_assessments, safe_observable_operation, terminal_errors) | Alembic revision scaffolding (`__all__`, `upgrade()` head) | incidental |
| 19 | 1 | 20 | test_sniper (self) | Repeated cluster-feeding test construction | real |
| 20 | 1 | 17 | test_analyzer (self) | Repeated analyzer input construction | real |
| 21 | 1 | 13 | profiler/_funding_rpc, profiler/funding | Parallel docstrings for the wrapped vs wrapper transfer-log API | incidental |
| 22 | 1 | 10 | _channel_delivery (self) | `compare_and_delete` vs `compare_and_expire`: near-identical Redis watch/CAS logic | real |
| 23 | 1 | 11 | test_redis_contract (self) | Repeated competing-owner CAS contract scenario | real |
| 24 | 1 | 9 | scripts/runtime_services, scripts/verify | Duplicated script entry-point bootstrap (`_helper_package`/`__spec__` block) | real |
| 25 | 1 | 6 | storage/models (self) | SQLAlchemy `datetime.now(UTC)` timestamp column defaults | incidental |
| 26 | 1 | 10 | test_models (ingestor, self) | Repeated model-construction blocks | real |
| 27 | 1 | 6 | test_gamma_client (self) | Repeated paged-response fixture | real |
| 28 | 1 | 10 | test_trade_row_v2_properties (self) | Repeated property-based row assertions | real |
| 29 | 1 | 12 | detector/fresh_wallet, detector/size_anomaly | Duplicated batch-result `_handle_batch_result` gather/zip knowledge | real |
| 30 | 1 | 12 | test_clob_client (self) | Repeated paged-market fixture | real |
| 31 | 1 | 9 | test_scorer (self) | Repeated scorer input construction | real |

Real knowledge dominates: 27 of 31 clusters (81 of 90 clones) are duplicated
maintenance surfaces — fixture blocks, wiring scaffolds, parallel
Discord/Telegram knowledge, URL-credential parsing, and Redis CAS logic. The
three incidental clusters are convention-mandated scaffolding (Alembic
revisions, SQLAlchemy column defaults) and parallel API docstrings.

## Every clone (scanner output, ordered by tokens)

`tokens`/`lines` are the clone size; ranges are 1-based inclusive
(`start-end`). Format: `A:start-end <-> B:start-end`.

| tok | ln | A | B |
| --- | --- | --- | --- |
| 407 | 61 | scripts/_service_redaction.py:8-68 | scripts/_verify_redaction.py:25-85 |
| 197 | 38 | src/.../detector/models.py:38-75 | src/.../detector/models.py:109-146 |
| 131 | 34 | tests/alerter/test_history.py:24-57 | tests/detector/test_scorer.py:28-61 |
| 120 | 20 | tests/test_redaction.py:1787-1806 | tests/test_redaction.py:1845-1864 |
| 118 | 31 | tests/alerter/test_formatter.py:75-105 | tests/detector/test_scorer.py:74-104 |
| 110 | 18 | src/.../alerter/channels/discord.py:76-93 | src/.../alerter/channels/telegram.py:96-113 |
| 102 | 20 | tests/ingestor/test_publisher.py:23-42 | tests/ingestor/test_publisher_codec.py:50-69 |
| 96 | 20 | tests/detector/test_sniper.py:278-297 | tests/detector/test_sniper.py:314-333 |
| 88 | 17 | tests/test_terminal_error_durable.py:84-100 | tests/test_terminal_error_durable.py:153-169 |
| 87 | 19 | tests/integration/test_end_to_end.py:175-193 | tests/integration/test_end_to_end.py:268-286 |
| 84 | 24 | tests/alerter/test_formatter.py:20-43 | tests/alerter/test_history.py:16-39 |
| 84 | 10 | tests/test_redaction.py:1152-1161 | tests/test_redaction.py:1915-1924 |
| 82 | 18 | tests/ingestor/test_trade_poller.py:288-305 | tests/ingestor/test_trade_poller.py:843-860 |
| 81 | 15 | tests/integration/test_end_to_end.py:60-74 | tests/integration/test_postgres_journey.py:147-161 |
| 80 | 12 | src/.../alerter/channels/discord.py:157-168 | src/.../alerter/channels/telegram.py:181-192 |
| 80 | 18 | tests/integration/test_end_to_end.py:61-78 | tests/test_pipeline.py:53-70 |
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
| 69 | 15 | scripts/_service_redaction.py:69-83 | scripts/_verify_redaction.py:90-104 |
| 69 | 9 | scripts/runtime_services.py:46-54 | scripts/verify.py:27-35 |
| 69 | 11 | tests/ingestor/test_health_server.py:104-114 | tests/ingestor/test_health_server.py:140-150 |
| 69 | 15 | tests/integration/test_end_to_end.py:46-60 | tests/test_pipeline.py:82-96 |
| 68 | 14 | src/.../alerter/channels/discord.py:93-106 | src/.../alerter/channels/telegram.py:113-126 |
| 68 | 12 | tests/alerter/test_formatter.py:502-513 | tests/alerter/test_formatter.py:680-691 |
| 68 | 14 | tests/test_terminal_error_durable.py:36-49 | tests/test_terminal_error_durable.py:81-94 |
| 67 | 14 | src/.../detector/models.py:57-70 | src/.../detector/models.py:182-195 |
| 67 | 11 | tests/ingestor/test_health_server.py:65-75 | tests/ingestor/test_health_server.py:276-286 |
| 67 | 10 | tests/test_main.py:305-314 | tests/test_main.py:572-581 |
| 66 | 15 | tests/ingestor/test_trade_row_v2.py:50-64 | tests/ingestor/test_trade_rows.py:37-51 |
| 65 | 8 | tests/tooling/test_service_entrypoint.py:148-155 | tests/tooling/test_service_entrypoint.py:176-183 |
| 64 | 13 | tests/ingestor/test_trade_poller.py:521-533 | tests/ingestor/test_trade_poller.py:583-595 |
| 64 | 17 | tests/test_redaction.py:2024-2040 | tests/test_redaction.py:2077-2093 |
| 62 | 12 | tests/ingestor/test_trade_poller.py:233-244 | tests/ingestor/test_trade_poller.py:261-272 |
| 62 | 16 | tests/integration/test_end_to_end.py:63-78 | tests/test_pipeline_persistence.py:47-62 |
| 62 | 12 | tests/test_terminal_error_durable.py:38-49 | tests/test_terminal_error_durable.py:152-163 |
| 61 | 6 | src/.../storage/models.py:51-56 | src/.../storage/models.py:212-217 |
| 61 | 7 | tests/integration/test_end_to_end.py:88-94 | tests/test_pipeline.py:536-542 |
| 61 | 11 | tests/integration/test_end_to_end.py:345-355 | tests/test_pipeline.py:619-628 |
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
| 57 | 6 | tests/integration/test_end_to_end.py:79-84 | tests/test_pipeline.py:104-109 |
| 57 | 13 | tests/test_redaction.py:1547-1559 | tests/test_redaction.py:1886-1897 |
| 56 | 9 | tests/ingestor/test_publisher.py:247-255 | tests/ingestor/test_publisher.py:272-280 |
| 55 | 12 | src/.../detector/fresh_wallet.py:223-234 | src/.../detector/size_anomaly.py:386-397 |
| 55 | 11 | tests/alerter/test_dispatcher.py:124-134 | tests/alerter/test_dispatcher.py:273-282 |
| 55 | 10 | tests/alerter/test_dispatcher.py:228-237 | tests/alerter/test_dispatcher.py:309-318 |
| 55 | 10 | tests/alerter/test_dispatcher.py:888-897 | tests/alerter/test_dispatcher.py:915-925 |
| 55 | 17 | tests/alerter/test_formatter.py:88-104 | tests/alerter/test_history.py:83-99 |
| 55 | 9 | tests/tooling/test_service_entrypoint.py:14-22 | tests/tooling/test_verifier_standalone.py:43-51 |
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
| 50 | 9 | tests/integration/test_end_to_end.py:185-193 | tests/integration/test_postgres_journey.py:238-246 |
| 50 | 9 | tests/integration/test_end_to_end.py:347-355 | tests/test_pipeline.py:578-585 |
| 50 | 6 | tests/test_persist_assessment.py:96-101 | tests/test_pipeline.py:107-112 |

`src/...` abbreviates `src/polymarket_insider_tracker`. This document is
inventory only: no product code was changed to produce it (US-003 scope).
Deduplication decisions belong to later, separately authorized cards.
