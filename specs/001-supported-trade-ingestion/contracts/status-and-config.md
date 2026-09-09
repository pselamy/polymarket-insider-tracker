# Contract: Ingestion Status, Metrics, and Configuration Compatibility

## Purpose

Defines what operators, logs, tests, and slice 003's later health wiring can observe about ingestion,
and how existing configuration migrates. Implemented by `ingestor/trade_poller.py` and `config.py`.

## Lifecycle States

| State | Meaning | Entered when | Left when |
|---|---|---|---|
| `stopped` | Not running | Initial, or after `stop()` completes | `start()` |
| `starting` | Loading checkpoint and identity window | `start()` | First cycle begins |
| `running` | Last cycle proven or empty | Successful cycle | Any other transition |
| `degraded` | Last cycle exhausted transient retries | Retry exhaustion | Next successful cycle |
| `possible-data-loss` | Durable boundary frozen over an unresolved gap | Proof failed after the recovery page | Proof against the frozen boundary, or loss event recorded |
| `failed` | Non-retryable error; acquisition stopped | Terminal classification | `stop()` then `start()` |

Every transition invokes `on_state_change(new_state)` (errors in the callback are logged, never raised),
logs at `INFO` (`WARNING` for `degraded`, `ERROR` for `possible-data-loss` and `failed`), and updates
`polymarket_ingest_state`.

## Status Snapshot

`TradePoller.status` returns the `IngestionStatus` value defined in
[data-model.md](../data-model.md#ingestion-status). Guarantees:

- `last_success_at` advances on every HTTP 200 list, including an empty list; `last_trade_at` advances
  only on an accepted observation, so a quiet interval is distinguishable from an outage (FR-010).
- `provider_lag_seconds` is `local_response_time - newest_accepted_timestamp`; it is `null` until the
  first accepted observation and is not clamped, so clock skew is visible as a negative value.
- `processing_lag_seconds` measures time spent inside downstream callbacks in the last cycle.
- `counts` contains every Row Disposition key even when zero, plus `polls`, `recovery_pages`,
  `empty_responses`, and `retries`.
- `last_error` is redacted: no wallet, no credential, no full URL with query string.
- `loss_events` returns the newest five events; the full list of 50 is in Redis.

## Metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `polymarket_ingest_state` | gauge | none | 0 stopped, 1 starting, 2 running, 3 degraded, 4 possible-data-loss, 5 failed |
| `polymarket_ingest_requests_total` | counter | `outcome` (`success`, `transient`, `terminal`) | Every HTTP attempt, retries included |
| `polymarket_ingest_rows_total` | counter | `disposition` | Every parsed row by Row Disposition |
| `polymarket_ingest_boundary_timestamp_seconds` | gauge | none | Durable boundary time |
| `polymarket_ingest_provider_lag_seconds` | gauge | none | Last measured provider lag |
| `polymarket_ingest_page_span_seconds` | gauge | none | Newest minus oldest timestamp in the last page |
| `polymarket_ingest_loss_events_total` | counter | `reason` | Loss events written |
| `polymarket_ingest_request_duration_seconds` | histogram | none | HTTP request latency |

Metric objects are created once at module import, following `ingestor/health.py`.

## Pipeline Integration

- `Pipeline._initialize_components` builds `TradePoller(on_trade=self._on_trade, redis=...,
  settings=settings.polymarket, metadata=self._metadata_sync, on_state_change=...)`.
- `Pipeline._start_background_services` creates the poller task first, then the metadata sync task.
- A `failed` transition records `PipelineStats.last_error` and increments `errors`; propagation to
  `PipelineState`, readiness, and CLI exit is slice 003's contract (G-016) and is not claimed here.
- `Pipeline.stop()` cancels the poller task, awaits it, then stops the metadata sync.

## Configuration Compatibility

| Setting | Before | After | Migration |
|---|---|---|---|
| `POLYMARKET_WS_URL` | Required-with-default WebSocket URL used for acquisition | Optional, no default, WebSocket scheme still enforced, never used | Deprecation warning at load and in `--config-check` naming the replacement settings; removal or rejection needs a later approved change |
| `POLYMARKET_API_KEY` | Optional secret for the CLOB client | Unchanged | None |
| `POLYMARKET_TRADES_URL` | absent | Optional, default `https://data-api.polymarket.com/trades` | Additive |
| `POLYMARKET_TRADES_COVERAGE` | absent | Optional, `all` (default) or `taker-only` | Additive |
| `POLYMARKET_TRADES_POLL_INTERVAL_SECONDS` | absent | Optional, default 5, range 1–60 | Additive |
| `POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS` | absent | Optional, default 600, range 60–3600 | Additive |

Rules:

- `POLYMARKET_WS_URL` with an `http(s)://` value is rejected exactly as today; it is never treated as
  the trades URL.
- `WebSocketSettingDeprecationWarning` is a `UserWarning` subclass emitted once per settings load; the
  message names the deprecated variable, states that it is not used for acquisition, and names the four
  replacement variables. It never prints the value.
- `Settings.redacted_summary()["polymarket"]` becomes
  `{"trades_url", "coverage", "poll_interval_seconds", "recovery_horizon_seconds", "ws_url": "(deprecated, set)" | "(not set)", "api_key": "(set)" | "(not set)"}`.
- `--config-check` prints the four trades settings and the `ws_url` disposition and repeats the
  deprecation warning text when the variable is set.
- `tests/fakes/pipeline.py::make_test_settings` stops passing a WebSocket URL and gains keyword
  arguments for the trades settings with the same defaults.

## Public Python Surface

- `polymarket_insider_tracker.ingestor` additionally exports `TradePoller`, `IngestionState`,
  `IngestionStatus`, `TradeObservation`, `TradesSourceError`, `TradesTransientError`, and
  `TradesTerminalError`.
- `TradeStreamHandler`, `ConnectionState`, `WebSocketStreamStats`, and `TradeStreamError` remain
  exported; `TradeStreamHandler.__init__` emits `DeprecationWarning("TradeStreamHandler is deprecated;
  the tracker acquires trades with TradePoller")`.
- `TradeEvent` is unchanged; `TradeEvent.from_websocket_message` remains for the deprecated handler and
  the strict parser lives in `ingestor/trade_rows.py`.

## Documentation Surfaces

`README.md` (overview, environment table, architecture diagram, troubleshooting), `.env.example`,
`CHANGELOG.md`, and `AGENTS.md` must state: near-real-time polling of the documented public trades
query, all-participant default coverage, the 5-second cadence and its share of the published limit,
possible-data-loss and loss-event semantics, the `POLYMARKET_WS_URL` deprecation window, and that no
Polymarket credential is required. "Real-time" is replaced by "near-real-time" wherever push delivery
could be inferred.
