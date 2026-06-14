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

### Notes
- Backtest scripts can now source data from `risk_assessments` directly. The
  `alerts.log` parsing path remains for one release as a fallback.
