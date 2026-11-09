# Changelog

All notable changes to this project are documented in this file.
The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed
- **Truthful detection contract (slice 004)**: establishes calibrated, source-backed
  detection boundaries, reproducible scoring contracts, and truthful capability claims.
  - Versioned `004.1` assessment evidence envelope (`detector_config` + `evaluation_evidence`)
    persisted in `risk_assessments.scoring_config` while preserving `003.1` and `legacy-unversioned`
    reader compatibility.
  - `DETECTOR_FRESH_TRANSACTION_BOUNDARY` (default `5`, exclusive `nonce < B`, deprecating inclusive
    `max_nonce`) and `DETECTOR_SIZE_VOLUME_THRESHOLD` (default `0.02`, strict `>`).
  - Decoupled Gamma 24-hour volume refresh (`0–600 s` recency window), eight-state volume
    classification (`available`, `unavailable`, `unqualified`, `missing`, `invalid`, `nonpositive`,
    `invalid_time`, `stale`), and nullable measured impact companions (`volume_impact_measured`,
    `book_impact_measured` persisted as SQL `NULL` when unmeasured, never fabricated `0.0000`).
  - Pre-T027 non-scoring proxy-wallet posture (`wallet.group_status = unqualified_pre_T027`, live
    `age_state = unknown`, score ceiling `0.450`), removal of the unreachable `3+` signal bonus
    branch (`multi_signal_bonus_3` retained as neutral compatibility key), and `FR-022a` alert-path
    reachability disclosure on startup log, `--config-check`, and `/health`.
  - Explicit capability boundaries: size anomaly + niche modifier operational for scoring;
    proxy-wallet low-activity evaluation recorded as non-scoring observation until T027; funding-chain
    tracing best-effort enrichment only; sniper clustering (`SniperDetector`) experimental and
    non-operational; executable backtesting and calibration deferred.
- `POLYMARKET_API_KEY` is deprecated and ignored. py-clob-client never accepted a bare key and
  every CLOB endpoint the tracker calls is public, so `ClobClient` no longer takes an `api_key`
  or reads the variable, and its startup log no longer reports `api_key_set`. Settings still
  accept the variable for one release, emit `ApiKeySettingDeprecationWarning` at load and in
  `--config-check`, and never print the value.
- **Supported trade ingestion (slice 001)**: trades are now acquired by
  near-real-time polling of the documented anonymous public trades query
  (now the official v2 endpoint, `https://data-api.polymarket.com/v2/trades`)
  through the new `TradePoller`,
  replacing the withdrawn WebSocket activity feed. Every cycle requests one
  full page with a strictly increasing `end` cutoff, parses rows strictly,
  de-duplicates by a composite observation identity, proves that the page
  reached the durable complete-through boundary (or uses the single documented
  recovery page), and delivers new observations oldest-first exactly once.
  - The default `POLYMARKET_TRADES_URL` is now the official Data API **v2**
    endpoint (`https://data-api.polymarket.com/v2/trades`); the environment
    variable name is unchanged. An explicitly configured official v1 URL
    (`https://data-api.polymarket.com/trades`) fails configuration validation
    with a migration message naming the v2 replacement — Polymarket retires
    the v1 endpoint on 2026-10-24. For the official v1/v2 pair with the same
    coverage mode, the saved checkpoint and identity window carry over
    automatically, so changing the URL is the whole migration. Custom HTTP(S)
    proxy URLs remain accepted and must serve the documented v2 envelope. No
    fallback or credentials are added.
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
  meets the alert threshold. This provides future-compatible research storage;
  executable backtesting and statistical calibration workflows are deferred
  until a separate approved specification (FR-014, US2-4).
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
- Persisted `risk_assessments` rows provide future-compatible research storage.
  Executable backtesting and calibration workflows are deferred until a separate
  approved specification (FR-014, US2-4).
