---
cluster: 2
status: resolved
resolved-by: PR #163
---

# Cluster 2

- **Files:** _journey_harness.py
- **Knowledge:** **Resolved**: the sample trade, single-token market, seeded provider history, journey settings, and wire/start/stop scaffolding now have one home (`tests/integration/_journey_harness.py`); the five suites use it
- **Verdict:** resolved (PR #163)
- **Counts when first grouped (historical):** 0 clones, 0 duplicated lines. Current counts are generated, see [the README](../README.md).

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
