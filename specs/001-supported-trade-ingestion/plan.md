# Implementation Plan: Supported Trade Ingestion

**Branch**: `spec/slice-001-plan-remote` (planning) | **Date**: 2026-09-09 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-supported-trade-ingestion/spec.md`, Decision A1
approved by Patrick on 2026-09-06, and the feasibility record in
[evidence/feasibility.md](evidence/feasibility.md). Implementation is not authorized until Patrick
validates this package.

## Summary

Replace the obsolete WebSocket trade path with near-real-time polling of the officially documented
anonymous public trades query, defaulting to all participant observations. Each cycle requests one
full page with a strictly increasing upper bound that defeats shared caching, orders and validates rows
on the client, de-duplicates by a stable composite identity, proves that the page reached the prior
durable boundary before advancing it, and otherwise uses the provider's single documented recovery page
or enters a visible possible-data-loss state. Boundary, identity window, and loss events live in Redis
with fake/real parity. Startup no longer waits for the market metadata crawl, the legacy WebSocket
setting enters a truthful deprecation window, and a bounded live-safe smoke check reports reachability
without retaining wallet identifiers.

## Technical Context

**Language/Version**: CPython 3.11, 3.12, and 3.13; syntax baseline 3.11

**Primary Dependencies**: existing `httpx` async client for acquisition; `redis` (`redis.asyncio`) for
the durable boundary; `pydantic-settings` for configuration; `prometheus-client` for metrics; test
doubles via `fakeredis==2.38.0` and `httpx.MockTransport`. No new dependency.

**Storage**: Redis keys under `polymarket:ingest:<source-id>:` (checkpoint hash, identity sorted set,
loss-event list). No PostgreSQL schema change and no migration.

**Testing**: pytest and pytest-asyncio; boundary fakes in `tests/fakes/`; shared Redis contract in
`tests/integration/test_redis_contract.py` against `fakeredis` and the loopback Redis; the real
pipeline assembled by `tests/fakes/pipeline.py`; `unittest.mock` prohibited by the AST policy.

**Target Platform**: Linux (Ubuntu 24.04 x86_64 blocking reference) and Apple Silicon macOS, unchanged.

**Project Type**: Single Python package providing a long-running CLI monitoring service.

**Performance Goals**: One acquisition every 5 s (1% of the published 200 requests per 10 s); newly
published trades reach the pipeline callback within one poll interval plus request latency after they
appear in a page (SC-002); acquisition starts within 5 s of dependency readiness regardless of the
metadata crawl.

**Constraints**: read-only public data; no credentials; no real notification; identity window bounded
by the recovery horizon (about 4 MB at observed rates); Complexipy maximum 5 for every function and
module; strict mypy and Pyright; Vulture at default confidence; Black and Ruff; ordinary tests and CI
make no external call.

**Scale/Scope**: observed 45–110 all-participant rows per second; 10,000-row pages covering roughly
160–220 s; reachable depth 20,000 rows; four new source modules and four modified ones, one script,
five new and five extended test modules, documentation, and the owned gap-register entries G-001–G-006, G-013b, G-030b, G-031,
and G-032.

## Constitution Check

*GATE: Passed before Phase 0 research; re-checked after Phase 1 design.*

| Rule | Evidence | Result |
|---|---|---|
| Research and monitoring only | Acquisition is anonymous `GET`; no order, position, or private endpoint is touched; the smoke check sends no alert. | Pass |
| Truthful, source-grounded contracts | Source, parameters, limits, and cache behavior come from current official documentation and the 2026-09-09 aggregate probes; the obsolete WebSocket path is deprecated explicitly, not reinterpreted. | Pass |
| End-to-end evidence over isolated coverage | Ingestion fixtures drive the real pipeline through `wire_pipeline`; slice 003 keeps the reusable harness and health wiring; a bounded live-safe smoke check is defined. | Pass |
| Safe effects and durable records | Redis boundary writes are transactional; loss events are durable and visible; dry-run and fake channels are untouched. | Pass |
| Compatibility, reproducibility, reviewability | Public environment names are preserved or deprecated with warnings; exported classes remain importable; no schema change; every existing gate stays required. | Pass |
| Spec Kit workflow | Slice 001 was activated explicitly for every command; clarify preceded plan; the reviewer-owned checklist is unchecked; human validation precedes implementation. | Pass |

No constitution exception is requested.

## Project Structure

### Documentation (this feature)

```text
specs/001-supported-trade-ingestion/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── source-acquisition.md
│   ├── observation-boundary.md
│   ├── status-and-config.md
│   └── live-smoke.md
├── checklists/
│   ├── requirements.md          # author-owned, maintained by clarify
│   └── ingestion.md             # reviewer-owned, all items unchecked
├── evidence/
│   ├── feasibility.md           # 2026-09-09 aggregate probe record and stop gate
│   ├── analysis.md              # read-only Spec Kit analysis output recorded as evidence
│   └── probes/*.json            # aggregate-only probe records (no wallet identifiers)
└── tasks.md
```

### Source Code (repository root)

```text
src/polymarket_insider_tracker/
├── config.py                          # trades settings, WS deprecation warning, redacted summary
├── pipeline.py                        # poller wiring; metadata crawl as background task
└── ingestor/
    ├── __init__.py                    # exports for the new modules; legacy exports retained
    ├── trade_rows.py                  # strict row parsing, dispositions, composite identity
    ├── trades_source.py               # httpx acquisition, retry/backoff/Retry-After, errors
    ├── observation_boundary.py        # Redis checkpoint, identity window, loss events, proof
    ├── trade_poller.py                # cycle loop, states, status, metrics, cancellation
    └── websocket.py                   # unchanged behavior plus a DeprecationWarning on construction
scripts/
└── trades_smoke.py                    # live-safe smoke: deterministic core plus explicit --live
tests/
├── fakes/trades.py                    # FakeTradesServer behind httpx.MockTransport
├── ingestor/test_trade_rows.py
├── ingestor/test_trades_source.py
├── ingestor/test_observation_boundary.py
├── ingestor/test_trade_poller.py
├── integration/test_redis_contract.py # new hash/zset-trim/list/multi scenarios
├── tooling/test_trades_smoke.py
├── test_config.py                     # extended
├── test_pipeline.py                   # extended
└── test_main.py                       # extended for the configuration check output
README.md, .env.example, CHANGELOG.md, AGENTS.md
specs/audit/gap-register.md
```

**Structure Decision**: Keep the single-package `src/` layout. Acquisition, parsing, boundary, and
lifecycle are separate modules so each stays under the Complexipy limit and can be tested with real
values and a single boundary fake. The WebSocket module remains until a later approved removal.

## Design Decisions

The full rationale and alternatives are in [research.md](research.md); this section fixes the
implementation-facing behavior.

### 1. Acquisition request

- `GET <trades_url>?limit=10000&offset=0&takerOnly=<false|true>&start=<boundary-horizon>&end=<n>`
  where `n = max(previous_end + 1, floor(now))`; if the local clock has not advanced past the previous
  `end`, the cycle waits until it has. `start` is `0` on first start.
- Timeout 15 s. Transient failures (transport errors, timeouts, 408/425/429/5xx, invalid JSON) retry up
  to four times with exponential backoff (base 1 s, cap 30 s, full jitter); `Retry-After` is honoured up
  to 60 s. 400/401/403/404/410 or a non-list body are terminal.
- The rate budget is enforced by cadence: a cycle never starts less than the poll interval after the
  previous cycle started, retries count toward `requests_last_10s`, and at most one recovery request
  (`offset=10000`) is made per cycle. Contract: [contracts/source-acquisition.md](contracts/source-acquisition.md).

### 2. Row parsing and dispositions

- A strict parser produces a `TradeObservation` or an invalid disposition per row, never a lenient
  default. Identity-bearing fields are required; `outcome`/`outcomeIndex` are repaired from cached market
  metadata by matching the token id to `asset`, else the observation is emitted with an unknown outcome
  and counted `unrepaired-outcome`.
- Rows more than 60 s ahead of the local clock are `invalid:future-timestamp`. Rows older than the
  horizon behind the newest accepted timestamp are `padding`.
- Diagnostics for invalid rows carry only the row hash and the field name.

### 3. Boundary proof and recovery

- Proof: page oldest timestamp strictly older than the boundary time, and every retained identity newer
  than that oldest row present in the page (data model, "Boundary proof").
- Recovery: one `offset=10000` request merged by identity; overlap between the pages is expected and
  harmless because new rows shift older rows to higher offsets.
- Unproven after recovery: state `possible-data-loss`; durable boundary frozen; gap
  `[boundary_time, oldest)` held in memory; a provisional boundary proves continuity between later pages;
  new observations keep flowing with identity de-duplication.
- Aging: when the frozen boundary is older than the horizon, write the loss event(s), re-anchor the
  boundary at the provisional newest timestamp, and return to `running`. Restart with a checkpoint older
  than the horizon records `restart-beyond-horizon` and re-anchors without emission. Contract:
  [contracts/observation-boundary.md](contracts/observation-boundary.md).
- **Decision for Patrick's confirmation**: the plan records aged gaps as durable loss events and keeps
  monitoring; the stricter alternative is to stay in `possible-data-loss` until an operator restart.

### 4. Delivery and crash semantics

- Observations are delivered oldest-first through the existing `on_trade` callback; a callback
  exception is logged, counted, and does not stop the cycle.
- The identity is written after each callback returns; the checkpoint advances after the whole proven
  page. A crash can re-deliver at most one observation and can never silently lose one.

### 5. Lifecycle, status, and metrics

- States `stopped`, `starting`, `running`, `degraded`, `possible-data-loss`, `failed`; `on_state_change`
  callback; `IngestionStatus` snapshot; Prometheus counters and gauges named in
  [contracts/status-and-config.md](contracts/status-and-config.md).
- `stop()` sets the stop event, cancels the in-flight request, lets the in-progress callback finish,
  and never writes a partial checkpoint. `Pipeline.stop()` cancels the poller task before the metadata
  task.
- Terminal ingestion failure is recorded on `PipelineStats` and exposed in status; pipeline state and CLI
  exit propagation stay with slice 003 (G-016).

### 6. Pipeline startup order

- `_start_background_services` creates the poller task first, then runs `MarketMetadataSync.start()` as
  a tracked background task so the initial crawl cannot delay acquisition (G-004). Detectors already
  tolerate absent metadata.

### 7. Configuration and compatibility

- New `POLYMARKET_TRADES_URL`, `POLYMARKET_TRADES_COVERAGE`, `POLYMARKET_TRADES_POLL_INTERVAL_SECONDS`,
  and `POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS` with the ranges in the data model.
- `POLYMARKET_WS_URL` becomes optional with no default, keeps its scheme validation, warns with
  `WebSocketSettingDeprecationWarning` when set, and is never used; the configuration check prints the
  disposition. `TradeStreamHandler` warns on construction and is no longer constructed by the pipeline.
- `README.md`, `.env.example`, `CHANGELOG.md`, and `AGENTS.md` describe near-real-time polling, the
  coverage default, the request budget, the loss semantics, and the deprecation window consistently.

### 8. Live-safe smoke check

- `scripts/trades_smoke.py` exposes `run_smoke(transport_or_client)` for the seven deterministic cases
  and `--live` for one bounded anonymous request per coverage mode. It writes the aggregate record in
  the data model, asserts `retained_wallet_identifiers: false`, sends nothing, and writes no Redis key.
  Contract: [contracts/live-smoke.md](contracts/live-smoke.md).

### 9. Test strategy

- Tests are written first and observed failing for every behavior task.
- `tests/fakes/trades.py` is a working fake trade server keyed by request parameters: it serves
  newest-first pages, honours `offset`, repeats bodies for identical URLs to model the shared cache,
  injects throttling, timeouts, malformed rows, incompatible schemas, rows newer than `end`, and
  missing `outcome`, and records every request for budget assertions. Wallets are synthetic.
- Redis parity: the boundary store's hash, sorted-set trim, list, and transactional scenarios are
  added to `tests/integration/test_redis_contract.py` and therefore run against the real loopback Redis
  in the `services` profile.
- End-to-end: `wire_pipeline` gains the poller so fixture pages flow through detection, scoring, and
  persistence with `FakeAlertChannel`; slice 003 owns the reusable harness, and this slice contributes
  the source fixtures and ingestion assertions.

### 10. Complexity discipline

- Each module is decomposed into named steps (request building, classification, proof, emission,
  checkpoint) with table-driven dispositions and enum dispatch so every function and module stays at or
  below Complexipy 5 without ignores, baselines, or exclusions.

## Phase Outputs

- [research.md](research.md): sixteen grounded decisions with alternatives
- [data-model.md](data-model.md): observation, disposition, boundary, status, configuration, and smoke
  record definitions
- [contracts/source-acquisition.md](contracts/source-acquisition.md),
  [contracts/observation-boundary.md](contracts/observation-boundary.md),
  [contracts/status-and-config.md](contracts/status-and-config.md),
  [contracts/live-smoke.md](contracts/live-smoke.md)
- [quickstart.md](quickstart.md): post-implementation validation sequence
- [evidence/feasibility.md](evidence/feasibility.md): the FR-016 record and the product stop gate

## Post-Design Constitution Re-check

All gates still pass. Phase 1 adds no trading behavior, no real notification, no schema change, and no
undocumented compatibility break. The only public-surface changes are an optional-by-default deprecated
setting with an actionable warning and new additive settings, which is the migration path the
constitution requires.

## Complexity Tracking

No constitution violation requires justification.

## Items Requiring Patrick's Decision Before Implementation

1. Confirm the aged-gap behavior (record a loss event and keep monitoring) versus stalling until
   restart (Design Decision 3).
2. Confirm the 10-minute recovery horizon knowing the reachable depth is 20,000 rows (about 5–7 minutes
   at observed rates); a restart or outage longer than the reachable depth produces a recorded loss.
3. Confirm the deprecation window for `POLYMARKET_WS_URL` and the retained `TradeStreamHandler`
   export instead of immediate rejection and removal.

## Product Stop Gate

Implementation stops for a product decision, without substituting any undocumented feed, if any
condition in the stop gate of [evidence/feasibility.md](evidence/feasibility.md#product-stop-gate)
is observed during implementation or the pre-convergence live-safe smoke run.
