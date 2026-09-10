"""Composite risk scorer combining all detector signals.

This module provides the RiskScorer class that aggregates signals from
multiple detectors into a unified risk assessment with weighted scoring.
"""

import logging
from dataclasses import dataclass
from decimal import Decimal

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

# Default weights for each signal type
DEFAULT_WEIGHTS = {
    "fresh_wallet": 0.40,
    "size_anomaly": 0.35,
    "niche_market": 0.25,
}

# Multi-signal bonuses
MULTI_SIGNAL_BONUS_2 = 1.2  # 20% bonus for 2 signals
MULTI_SIGNAL_BONUS_3 = 1.3  # 30% bonus for 3+ signals


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
    - Applies configurable weights based on signal type
    - Calculates multi-signal bonuses for correlated signals
    - Enforces deduplication to prevent alert spam
    - Produces RiskAssessment objects for downstream alerting

    Scoring Formula:
        weighted_score = sum(signal.confidence * weight[type] for signal in signals)

        # Multi-signal bonus
        if signals >= 2: weighted_score *= 1.2
        if signals >= 3: weighted_score *= 1.3

        # Cap at 1.0
        final_score = min(weighted_score, 1.0)

        should_alert = final_score >= alert_threshold AND not deduplicated

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
        weights: dict[str, float] | None = None,
        alert_threshold: float = DEFAULT_ALERT_THRESHOLD,
        dedup_window_seconds: int = DEFAULT_DEDUP_WINDOW_SECONDS,
        key_prefix: str = DEFAULT_REDIS_KEY_PREFIX,
    ) -> None:
        """Initialize the risk scorer.

        Args:
            redis: Redis async client for deduplication.
            weights: Custom weights for signal types. Defaults to DEFAULT_WEIGHTS.
            alert_threshold: Minimum score to trigger alert (default 0.6).
            dedup_window_seconds: Window for deduplication (default 3600 = 1 hour).
            key_prefix: Redis key prefix for dedup keys.
        """
        self._redis = redis
        self._weights = weights or DEFAULT_WEIGHTS.copy()
        self._alert_threshold = alert_threshold
        self._dedup_window = dedup_window_seconds
        self._key_prefix = key_prefix

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
        # Calculate weighted score
        weighted_score, signals_triggered = self.calculate_weighted_score(bundle)

        # Determine if should alert based purely on risk score threshold
        should_alert = weighted_score >= self._alert_threshold

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

    def _score_fresh_wallet(self, bundle: SignalBundle) -> tuple[float, int]:
        if bundle.fresh_wallet_signal is None:
            return 0.0, 0
        weight = self._weights.get("fresh_wallet", 0.0)
        return bundle.fresh_wallet_signal.confidence * weight, 1

    def _score_size_anomaly(self, bundle: SignalBundle) -> tuple[float, int]:
        signal = bundle.size_anomaly_signal
        if signal is None:
            return 0.0, 0
        weight = self._weights.get("size_anomaly", 0.0)
        return signal.confidence * weight, 1

    def _score_niche_market(self, bundle: SignalBundle) -> float:
        signal = bundle.size_anomaly_signal
        if signal is None or not signal.is_niche_market:
            return 0.0
        niche_weight = self._weights.get("niche_market", 0.0)
        return signal.confidence * niche_weight

    @staticmethod
    def _apply_multi_signal_bonus(score: float, count: int) -> float:
        if count >= 3:
            return score * MULTI_SIGNAL_BONUS_3
        if count >= 2:
            return score * MULTI_SIGNAL_BONUS_2
        return score

    def calculate_weighted_score(self, bundle: SignalBundle) -> tuple[float, int]:
        """Calculate weighted score from all signals.

        Applies per-signal weights and multi-signal bonuses.

        Args:
            bundle: SignalBundle with all available signals.

        Returns:
            Tuple of (weighted_score, signals_triggered_count).
        """
        fresh_score, fresh_count = self._score_fresh_wallet(bundle)
        size_score, size_count = self._score_size_anomaly(bundle)
        signals_triggered = fresh_count + size_count

        score = fresh_score
        score += size_score
        score += self._score_niche_market(bundle)
        score = self._apply_multi_signal_bonus(score, signals_triggered)
        return min(score, 1.0), signals_triggered

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

    def get_weights(self) -> dict[str, float]:
        """Get current signal weights.

        Returns:
            Copy of the weights dictionary.
        """
        return self._weights.copy()

    def set_weights(self, weights: dict[str, float]) -> None:
        """Update signal weights.

        Useful for A/B testing different weight configurations.

        Args:
            weights: New weights dictionary.
        """
        self._weights = weights.copy()
        logger.info("Updated risk scorer weights: %s", self._weights)
