"""Composite risk scorer combining all detector signals.

This module provides the RiskScorer class that aggregates signals from
multiple detectors into a unified risk assessment with weighted scoring.
"""

import logging
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal

from redis.asyncio import Redis

from polymarket_insider_tracker.detector.models import (
    FreshWalletSignal,
    RiskAssessment,
    SizeAnomalySignal,
)
from polymarket_insider_tracker.ingestor.models import TradeEvent

logger = logging.getLogger(__name__)

# Default configuration. The threshold lifted from 0.6 to 0.80 after the
# first cost-adjusted backtest showed everything below 0.85 was follower-PnL
# negative under realistic taker fees + half-cent slippage. 0.80 keeps a small
# margin below 0.85+ so we don't drop borderline-high signals on a hard cliff.
# Override at runtime via DETECTOR_ALERT_THRESHOLD env var.
DEFAULT_ALERT_THRESHOLD = 0.80
DEFAULT_DEDUP_WINDOW_SECONDS = 3600  # 1 hour
DEFAULT_REDIS_KEY_PREFIX = "polymarket:dedup:"

# Signal weights are algorithm constants, not runtime configuration. The persisted
# assessment schema stores no weights, so a stored record replays its decision only
# because exactly one weight set exists per algorithm version. Changing any weight,
# bonus, or combination rule requires bumping SCORING_ALGORITHM_VERSION and a schema
# decision on persisting the version before rows produced by both algorithms coexist.
DEFAULT_WEIGHTS = {
    "fresh_wallet": 0.40,
    "size_anomaly": 0.35,
    "niche_market": 0.25,
}

# Version of the scoring algorithm (weights, bonuses, quantization, and combination
# rules) that produced every persisted assessment of this codebase.
SCORING_ALGORITHM_VERSION = "003.1"

# Multi-signal bonuses
MULTI_SIGNAL_BONUS_2 = 1.2  # 20% bonus for 2 signals
MULTI_SIGNAL_BONUS_3 = 1.3  # 30% bonus for 3+ signals

# Confidences, thresholds, and the final score are evaluated at the NUMERIC(4,3)
# precision the assessment schema persists, so a stored record replays the exact
# decision; an unrounded float decision could contradict its own stored values.
SCORE_QUANTUM = Decimal("0.001")


def quantize_score_value(value: float) -> Decimal:
    """Quantize a confidence, score, or threshold to the persisted 3-decimal precision."""
    return Decimal(str(value)).quantize(SCORE_QUANTUM, rounding=ROUND_HALF_EVEN)


@dataclass
class SignalBundle:
    """Bundle of signals for a single trade.

    Collects all available signals for a trade event to pass to the scorer.
    """

    trade_event: TradeEvent
    fresh_wallet_signal: FreshWalletSignal | None = None
    size_anomaly_signal: SizeAnomalySignal | None = None

    @property
    def wallet_address(self) -> str:
        """Return the wallet address from the trade event."""
        return self.trade_event.wallet_address

    @property
    def market_id(self) -> str:
        """Return the market ID from the trade event."""
        return self.trade_event.market_id


class RiskScorer:
    """Composite risk scorer combining signals into unified assessments.

    This scorer:
    - Aggregates signals from multiple detectors for the same trade
    - Applies the algorithm's pinned per-signal weights
    - Calculates multi-signal bonuses for correlated signals
    - Produces RiskAssessment objects for downstream alerting

    Scoring is pure computation with no Redis side effects. Delivery deduplication
    is owned exclusively by the alerter (``AlertHistory`` / ``AlertDispatcher``).

    Scoring Formula (evaluated in exact decimal arithmetic at 3-decimal precision):
        weighted_score = sum(quantize(signal.confidence) * weight[type] for signal in signals)

        # Multi-signal bonus
        if signals >= 2: weighted_score *= 1.2
        if signals >= 3: weighted_score *= 1.3

        # Cap at 1.0, then quantize to the persisted NUMERIC(4,3) precision
        final_score = quantize(min(weighted_score, 1.0))

        should_alert = final_score >= quantize(alert_threshold)

    The quantized inputs, score, and threshold are exactly what the assessment schema
    persists, so a stored record replays the decision. Weights are the algorithm
    constants ``DEFAULT_WEIGHTS`` under ``SCORING_ALGORITHM_VERSION``; no runtime
    weight configuration exists, so every persisted record maps to one algorithm.

    Example:
        ```python
        redis = Redis.from_url("redis://localhost:6379")
        scorer = RiskScorer(redis)

        bundle = SignalBundle(
            trade_event=trade,
            fresh_wallet_signal=fresh_signal,
            size_anomaly_signal=size_signal,
        )

        assessment = await scorer.assess(bundle)
        if assessment.should_alert:
            await send_alert(assessment)
        ```
    """

    def __init__(
        self,
        redis: Redis,
        *,
        alert_threshold: float = DEFAULT_ALERT_THRESHOLD,
        dedup_window_seconds: int = DEFAULT_DEDUP_WINDOW_SECONDS,
        key_prefix: str = DEFAULT_REDIS_KEY_PREFIX,
    ) -> None:
        """Initialize the risk scorer.

        Weights are not configurable: the persisted assessment schema stores no
        weights, so a stored record is replayable only while exactly one weight set
        exists per SCORING_ALGORITHM_VERSION (FR-012, Constitution IV).

        Args:
            redis: Retained for public API compatibility; scoring performs no Redis
                operations since delivery deduplication moved to the alerter (FR-008).
            alert_threshold: Minimum score to trigger alert (default 0.80).
            dedup_window_seconds: Retained for API compatibility; unused by scoring.
            key_prefix: Retained for API compatibility; unused by scoring.
        """
        self._redis = redis
        self._alert_threshold = alert_threshold
        self._dedup_window = dedup_window_seconds
        self._key_prefix = key_prefix
        logger.info(
            "RiskScorer algorithm=%s threshold=%s",
            SCORING_ALGORITHM_VERSION,
            alert_threshold,
        )

    @staticmethod
    def _extract_size_diagnostics(
        signal: SizeAnomalySignal | None,
    ) -> tuple[bool | None, Decimal | None, bool | None]:
        if signal is None:
            return None, None, None
        meta = signal.market_metadata
        vol_avail = meta.daily_volume is not None
        market_vol = meta.daily_volume
        book_avail = signal.book_impact > 0.0 or meta.liquidity is not None
        return vol_avail, market_vol, book_avail

    @staticmethod
    def _extract_fresh_diagnostics(
        signal: FreshWalletSignal | None,
    ) -> tuple[int | None, bool | None]:
        if signal is None:
            return None, None
        profile = signal.wallet_profile
        return profile.nonce, profile.age_hours is not None

    async def assess(self, bundle: SignalBundle) -> RiskAssessment:
        """Assess risk for a trade event with its detected signals.

        This method:
        1. Counts triggered signals
        2. Calculates weighted score with bonuses
        3. Evaluates alert threshold
        4. Creates RiskAssessment with signal diagnostics

        Args:
            bundle: SignalBundle containing trade and all signals.

        Returns:
            RiskAssessment with final scoring and alert decision.
        """
        # Calculate weighted score (already quantized to the persisted precision)
        weighted_score, signals_triggered = self.calculate_weighted_score(bundle)

        # Decide at the same precision the assessment schema persists, so the stored
        # score, threshold, and decision always agree (FR-012).
        should_alert = quantize_score_value(weighted_score) >= quantize_score_value(
            self._alert_threshold
        )

        # Log assessment
        if should_alert:
            logger.info(
                "Risk assessment triggered alert: wallet=%s, market=%s, score=%.2f, signals=%d",
                bundle.wallet_address[:10] + "...",
                bundle.market_id[:10] + "...",
                weighted_score,
                signals_triggered,
            )

        vol_avail, market_vol, book_avail = self._extract_size_diagnostics(
            bundle.size_anomaly_signal
        )
        tx_count, age_known = self._extract_fresh_diagnostics(bundle.fresh_wallet_signal)

        return RiskAssessment(
            trade_event=bundle.trade_event,
            wallet_address=bundle.wallet_address,
            market_id=bundle.market_id,
            fresh_wallet_signal=bundle.fresh_wallet_signal,
            size_anomaly_signal=bundle.size_anomaly_signal,
            signals_triggered=signals_triggered,
            weighted_score=weighted_score,
            should_alert=should_alert,
            volume_available=vol_avail,
            market_daily_volume=market_vol,
            book_depth_available=book_avail,
            wallet_tx_count=tx_count,
            wallet_age_known=age_known,
        )

    @staticmethod
    def _weight(name: str) -> Decimal:
        return Decimal(str(DEFAULT_WEIGHTS[name]))

    def _score_fresh_wallet(self, bundle: SignalBundle) -> tuple[Decimal, int]:
        if bundle.fresh_wallet_signal is None:
            return Decimal(0), 0
        confidence = quantize_score_value(bundle.fresh_wallet_signal.confidence)
        return confidence * self._weight("fresh_wallet"), 1

    def _score_size_anomaly(self, bundle: SignalBundle) -> tuple[Decimal, int]:
        signal = bundle.size_anomaly_signal
        if signal is None:
            return Decimal(0), 0
        return quantize_score_value(signal.confidence) * self._weight("size_anomaly"), 1

    def _score_niche_market(self, bundle: SignalBundle) -> Decimal:
        signal = bundle.size_anomaly_signal
        if signal is None or not signal.is_niche_market:
            return Decimal(0)
        return quantize_score_value(signal.confidence) * self._weight("niche_market")

    @staticmethod
    def _apply_multi_signal_bonus(score: Decimal, count: int) -> Decimal:
        if count >= 3:
            return score * Decimal(str(MULTI_SIGNAL_BONUS_3))
        if count >= 2:
            return score * Decimal(str(MULTI_SIGNAL_BONUS_2))
        return score

    def calculate_weighted_score(self, bundle: SignalBundle) -> tuple[float, int]:
        """Calculate weighted score from all signals.

        Applies per-signal weights and multi-signal bonuses. Confidences and the final
        score are quantized to the persisted 3-decimal precision and combined in exact
        decimal arithmetic, so recomputing from the persisted inputs and the algorithm's
        pinned weights reproduces the returned score exactly.

        Args:
            bundle: SignalBundle with all available signals.

        Returns:
            Tuple of (weighted_score, signals_triggered_count).
        """
        fresh_score, fresh_count = self._score_fresh_wallet(bundle)
        size_score, size_count = self._score_size_anomaly(bundle)
        signals_triggered = fresh_count + size_count

        score = fresh_score + size_score + self._score_niche_market(bundle)
        score = self._apply_multi_signal_bonus(score, signals_triggered)
        quantized = min(score, Decimal(1)).quantize(SCORE_QUANTUM, rounding=ROUND_HALF_EVEN)
        return float(quantized), signals_triggered

    async def assess_batch(self, bundles: list[SignalBundle]) -> list[RiskAssessment]:
        """Assess multiple trade bundles.

        Args:
            bundles: List of SignalBundles to assess.

        Returns:
            List of RiskAssessments.
        """
        import asyncio

        tasks = [self.assess(bundle) for bundle in bundles]
        return await asyncio.gather(*tasks)

    @staticmethod
    def get_weights() -> dict[str, float]:
        """The algorithm's pinned signal weights (a copy; mutating it changes nothing)."""
        return DEFAULT_WEIGHTS.copy()
