# Changelog

All notable changes to this project are documented in this file.
The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- **Tail-bet detector** — orthogonal third signal that fires on the *opposite*
  shape from `SizeAnomalyDetector`: low-price BUYs (price ≤ 0.10) with
  `size * (1 - price) ≥ $1,000` of asymmetric upside. The settlement-arbitrage
  shape (BUY at ~0.999 for tens of dollars on a tens-of-thousands notional) is
  hard-filtered out, since its information value is structurally zero.
  - Module: `detector/tail_bet.py` + `TailBetSignal` in `detector/models.py`.
  - Scoring: confidence = `0.5 * price_extremity + 0.5 * payout_to_volume`,
    multiplied by `1.5` for niche markets. Cold-start markets (volume unknown)
    fall back to a flat `0.4` impact baseline so they aren't silently dropped.
  - Weights: `DEFAULT_WEIGHTS["tail_bet"] = 0.40` (highest of the three —
    structural shape is a stronger insider tell than raw notional impact).
    Niche premium also applies, mirroring `SizeAnomalyDetector`.
  - Config: `DETECTOR_TAIL_BET_ENABLED`, `DETECTOR_TAIL_BET_MAX_PRICE`, and
    `DETECTOR_TAIL_BET_MIN_PAYOUT_USDC` env knobs.
  - Pipeline: `_on_trade` now runs all three detectors in parallel via
    `asyncio.gather`; `is_niche_market` on the persisted DTO merges via OR
    across `size_anomaly` and `tail_bet`.
  - Storage: `risk_assessments` gains 4 columns — `tail_bet_confidence`,
    `potential_payout_usdc`, `payout_to_volume_ratio`,
    `payout_to_notional_ratio` (alembic `003_tail_bet_columns`).
  - Alerts: Discord embed shows a dedicated **Tail Bet Upside** field
    (`payout_usdc (~Nx notional)`); Telegram and plain-text get an equivalent
    line with the leverage multiple.
  - Tests: `tests/detector/test_tail_bet.py` covers structural filters,
    confidence math, niche multiplier, volume-unknown baseline, and metadata
    fallback. `tests/detector/test_scorer.py` adds tail-bet-only,
    tail-bet-niche-premium, tail-bet + fresh-wallet 2-signal bonus, and
    all-three-signal 1.3x bonus cases.
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

- `RiskAssessment.tail_bet_signal` is a required field (no default). The frozen
  dataclass cannot have a non-default field follow a default one, and adding
  the new optional signal at the end would have broken constructor positional
  ordering for downstream code; making it required is the cleaner break.
- Alert threshold (`DETECTOR_ALERT_THRESHOLD`) is now fully env-driven; the
  legacy hard-coded `0.6` default has been raised to `0.80` for production.

### Notes

- Backtest scripts can now source data from `risk_assessments` directly. The
  `alerts.log` parsing path remains for one release as a fallback.
- The tail-bet weights (`max_price=0.10`, `min_payout=$1k`,
  `payout_to_volume_ref=5%`) are tuned against the cost-adjusted backtest;
  changing them should go through an explicit sensitivity review.
