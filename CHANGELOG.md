# Changelog

All notable changes to this project are documented in this file.
The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

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
