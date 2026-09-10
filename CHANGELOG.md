# Changelog

All notable changes to this project are documented in this file.
The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed
- **Supported trade ingestion (slice 001)**: trades are now acquired by
  near-real-time polling of the documented anonymous public trades query
  (`https://data-api.polymarket.com/trades`) through the new `TradePoller`,
  replacing the withdrawn WebSocket activity feed. Every cycle requests one
  full page with a strictly increasing `end` cutoff, parses rows strictly,
  de-duplicates by a composite observation identity, proves that the page
  reached the durable complete-through boundary (or uses the single documented
  recovery page), and delivers new observations oldest-first exactly once.
  - Coverage defaults to all public participant observations
    (`takerOnly=false`); `POLYMARKET_TRADES_COVERAGE=taker-only` is an explicit
    operator choice.
  - The default 5-second cadence uses about 1% of the published 200 requests
    per 10 seconds; the configurable minimum of 1 second keeps it at 5%.
  - Acquisition starts before the market-metadata crawl completes; metadata is
    enrichment for outcome repair and detection, never a startup gate.
  - The durable boundary, identity window, and loss events live in Redis under
    `polymarket:ingest:<source-id>:` with `fakeredis`/real-Redis parity.
  - Unproven pages enter a visible `possible-data-loss` state with a frozen
    boundary; gaps that age past the recovery horizon become durable, logged
    loss events (`horizon-expired`, `restart-beyond-horizon`,
    `continuity-mismatch`). The 10-minute horizon is a retention and
    loss-detection bound, not a completeness guarantee.
  - New settings: `POLYMARKET_TRADES_URL`, `POLYMARKET_TRADES_COVERAGE`,
    `POLYMARKET_TRADES_POLL_INTERVAL_SECONDS`,
    `POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS`; new status fields, metrics
    (`polymarket_ingest_*`), and the live-safe `scripts/trades_smoke.py`.

### Deprecated
- `POLYMARKET_WS_URL` no longer defaults to a host, is accepted only with a
  `ws://`/`wss://` scheme, emits `WebSocketSettingDeprecationWarning` at load and
  in `--config-check`, and is never used for acquisition. `TradeStreamHandler`
  stays importable but warns on construction. Removal needs a later approved
  change with its own migration note.

### Added
- **Risk-assessment persistence**: every signal-bearing trade now writes a row
  to the new `risk_assessments` table, regardless of whether the assessment
  meets the alert threshold. This is the ground-truth log future backtests will
  read instead of grepping `alerts.log` / `journalctl`.
  - Pipeline: `Pipeline._score_and_alert` calls `Pipeline._persist_assessment`
    for every assessment; failures are caught and never block alert dispatch.
  - Storage: new `RiskAssessmentModel`, `RiskAssessmentDTO`, and
    `RiskAssessmentRepository` (alembic migration shipped previously).
  - Config: `DETECTOR_PERSIST_ASSESSMENTS` env var (default `true`) controls
    the write path so it can be disabled without code changes.
  - Tests: `tests/test_persist_assessment.py` covers (a) sub-threshold rows are
    persisted with `should_alert=False` and dispatch is skipped, and (b) DB
    failures during persistence do not block dispatching.

### Changed
- Alert threshold (`DETECTOR_ALERT_THRESHOLD`) is now fully env-driven; the
  legacy hard-coded `0.6` default has been raised to `0.80` for production.

### Removed
- Dead code surfaced by the new required Vulture gate. None of these had a caller,
  test, or documented workflow:
  - `DatabaseManager.init_schema()` and `init_schema_async()`. Apply schema with
    `uv run --env-file .env alembic upgrade head`; the module-level `init_db` and
    `init_async_db` helpers remain exported.
  - `GracefulShutdown(exit_on_timeout=...)` and `ShutdownTimeoutError`. The
    parameter was stored but never read; a second signal is the only force-exit
    path, and `wait_with_timeout()` still returns `False` on timeout.
  - `RiskAssessmentRepository.get_by_assessment_id()`, which was never wired to a
    caller or test.
  - Unused constants `AlertHistory.KEY_PREFIX_FEEDBACK`,
    `clob_client.MIN_REQUEST_INTERVAL`, `clob_client.RETRY_STATUS_CODES`,
    `chain.DEFAULT_CONNECTION_POOL_SIZE`, and `chain.DEFAULT_REQUEST_TIMEOUT`,
    plus the private `ClobClient._with_rate_limit` wrapper.

### Notes
- Backtest scripts can now source data from `risk_assessments` directly. The
  `alerts.log` parsing path remains for one release as a fallback.
