"""Composite risk scorer combining all detector signals.

This module provides the RiskScorer class that aggregates signals from
multiple detectors into a unified risk assessment with weighted scoring.
"""

import logging
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal
from types import MappingProxyType

from redis.asyncio import Redis

from polymarket_insider_tracker.detector.evaluation import TradeEvidence
from polymarket_insider_tracker.detector.models import (
    SCORING_ALGORITHM_VERSION,
    FreshWalletSignal,
    RiskAssessment,
    SizeAnomalySignal,
)
from polymarket_insider_tracker.detector.scoring_config import (
    DEFAULT_WALLET_GROUP_STATUS,
    PureConfig,
    serialize_pure_config,
)
from polymarket_insider_tracker.detector.volume_state import VolumeState
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

# The algorithm's default signal weights, immutable so no caller can change scoring
# behind the persisted records' back. Every new assessment row stores its
# scoring_algorithm_version and the exact active configuration (scoring_config), so
# a stored record replays its decision even when the deprecated weights API was
# used. Changing these defaults, a bonus, or a combination rule still requires
# bumping SCORING_ALGORITHM_VERSION.
DEFAULT_WEIGHTS: Mapping[str, float] = MappingProxyType(
    {
        "fresh_wallet": 0.40,
        "size_anomaly": 0.35,
        "niche_market": 0.25,
    }
)

# Multi-signal bonus: only two signal groups exist, so the 3+ branch is removed (FR-011).
# The ``multi_signal_bonus_3`` configuration key is retained as an inert neutral multiplier.
MULTI_SIGNAL_BONUS_2 = 1.2  # 20% bonus for two scoring groups
NEUTRAL_BONUS_3 = 1  # structural key kept for the persisted configuration; never applied

# Confidences, thresholds, and the final score are evaluated at the NUMERIC(4,3)
# precision the assessment schema persists, so a stored record replays the exact
# decision; an unrounded float decision could contradict its own stored values.
SCORE_QUANTUM = Decimal("0.001")


def quantize_score_value(value: float) -> Decimal:
    """Quantize a confidence, score, or threshold to the persisted 3-decimal precision."""
    return Decimal(str(value)).quantize(SCORE_QUANTUM, rounding=ROUND_HALF_EVEN)


def apply_multi_signal_bonus(score: Decimal, count: int) -> Decimal:
    """The two-group bonus; it needs two scoring groups, so it is unreachable while the
    wallet group is non-scoring (FR-007c)."""
    if count >= 2:
        return score * Decimal(str(MULTI_SIGNAL_BONUS_2))
    return score


def size_group_score(
    weights: Mapping[str, float], confidence: float, *, niche: bool
) -> tuple[float, int]:
    """The score and scoring-group count of one size signal under ``weights``.

    The one place the combination (weights, niche term, bonus, cap, quantization) lives, so the
    runtime alert-path ceiling is computed by exactly the arithmetic that scores assessments.
    A signal name missing from a partial deprecated mapping contributes zero, as before slice 003.
    """
    quantized = quantize_score_value(confidence)
    score = quantized * Decimal(str(weights.get("size_anomaly", 0.0)))
    if niche:
        score += quantized * Decimal(str(weights.get("niche_market", 0.0)))
    score = apply_multi_signal_bonus(score, 1)
    capped = min(score, Decimal(1)).quantize(SCORE_QUANTUM, rounding=ROUND_HALF_EVEN)
    return float(capped), 1


def _warn_deprecated_weights(surface: str) -> None:
    warnings.warn(
        f"{surface} is deprecated and will be removed after the slice-003"
        " compatibility window; custom weights are recorded per assessment in"
        " scoring_config so stored records remain replayable",
        DeprecationWarning,
        stacklevel=3,
    )


@dataclass
class SignalBundle:
    """Bundle of signals for a single trade.

    Collects all available signals for a trade event to pass to the scorer.
    """

    trade_event: TradeEvent
    fresh_wallet_signal: FreshWalletSignal | None = None
    size_anomaly_signal: SizeAnomalySignal | None = None
    evidence: TradeEvidence | None = None

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
    - Applies the active per-signal weights (immutable defaults; the deprecated
      weights API remains functional for one compatibility window)
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
    persists, and every new assessment additionally records
    ``scoring_algorithm_version`` plus the canonical ``scoring_config`` JSON of the
    active weights, bonuses, threshold, and quantum, so a stored record replays its
    decision even when the deprecated weight-configuration API was used.

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
            redis: Retained for public API compatibility; scoring performs no Redis
                operations since delivery deduplication moved to the alerter (FR-008).
            weights: Deprecated custom signal weights, kept working for one
                compatibility window with the pre-slice-003 semantics: an empty
                (or omitted) mapping activates the immutable defaults, and a
                signal name missing from a partial mapping contributes zero.
                The effective per-signal weights — implied zeros included — are
                recorded in every new assessment's ``scoring_config`` so records
                stay replayable.
            alert_threshold: Minimum score to trigger alert (default 0.80).
            dedup_window_seconds: Retained for API compatibility; unused by scoring.
            key_prefix: Retained for API compatibility; unused by scoring.
        """
        self._redis = redis
        self._alert_threshold = alert_threshold
        self._dedup_window = dedup_window_seconds
        self._key_prefix = key_prefix
        if weights is not None:
            _warn_deprecated_weights("the RiskScorer weights constructor argument")
        # Pre-slice-003 contract: an empty mapping means "use the defaults"
        # (``weights or DEFAULT_WEIGHTS``), not "score everything as zero".
        self._weights: dict[str, float] = dict(weights) if weights else dict(DEFAULT_WEIGHTS)
        self._scoring_config = self._build_scoring_config()
        logger.info(
            "RiskScorer algorithm=%s threshold=%s config=%s",
            SCORING_ALGORITHM_VERSION,
            alert_threshold,
            self._scoring_config,
        )

    def _build_scoring_config(self) -> str:
        """Canonical deterministic JSON of the exact configuration behind each assessment.

        Values are decimal strings so replaying a stored record never depends on
        float formatting; keys are sorted and separators fixed so equal
        configurations always serialize to the identical byte sequence.
        """
        pure = PureConfig(
            alert_threshold=quantize_score_value(self._alert_threshold),
            multi_signal_bonus_2=Decimal(str(MULTI_SIGNAL_BONUS_2)),
            multi_signal_bonus_3=Decimal(NEUTRAL_BONUS_3),
            score_quantum=SCORE_QUANTUM,
            weights={
                name: Decimal(str(weight)) for name, weight in self._effective_weights().items()
            },
        )
        return serialize_pure_config(pure)

    def _effective_weights(self) -> dict[str, float]:
        """Every weight scoring can consult, with implied zeros made explicit.

        A signal name missing from a partial deprecated mapping contributes
        zero (pre-slice-003 contract); recording that zero keeps each stored
        assessment replayable from its own ``scoring_config`` without knowing
        the missing-key rule.
        """
        names = set(self._weights) | set(DEFAULT_WEIGHTS)
        return {name: self._weights.get(name, 0.0) for name in sorted(names)}

    @property
    def scoring_config(self) -> str:
        """The canonical configuration JSON recorded on every new assessment."""
        return self._scoring_config

    def set_weights(self, weights: dict[str, float]) -> None:
        """Update signal weights (deprecated compatibility window).

        Args:
            weights: New weights dictionary. Replaces the mapping wholesale,
                exactly as before slice 003: a signal name missing from the
                new mapping contributes zero from now on.
        """
        _warn_deprecated_weights("RiskScorer.set_weights")
        self._weights = dict(weights)
        self._scoring_config = self._build_scoring_config()
        logger.info("Updated risk scorer weights: %s", self._weights)

    @staticmethod
    def _extract_size_diagnostics(
        signal: SizeAnomalySignal | None,
    ) -> tuple[bool | None, Decimal | None, bool | None]:
        if signal is None:
            return None, None, None
        # Book depth cannot score and has no source (FR-006): it is recorded as unavailable.
        evidence = signal.volume_evidence
        if evidence is None:
            return None, None, False
        verdict = evidence.verdict
        return verdict.state is VolumeState.AVAILABLE, verdict.daily_volume, False

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
            wallet_group_status=DEFAULT_WALLET_GROUP_STATUS,
            scoring_algorithm_version=SCORING_ALGORITHM_VERSION,
            scoring_config=self._scoring_config,
            evidence=bundle.evidence,
        )

    def calculate_weighted_score(self, bundle: SignalBundle) -> tuple[float, int]:
        """Calculate weighted score from all signals.

        Applies per-signal weights and multi-signal bonuses. Confidences and the final
        score are quantized to the persisted 3-decimal precision and combined in exact
        decimal arithmetic, so recomputing from the persisted inputs and the weights
        recorded in the assessment's scoring_config reproduces the score exactly.

        G1 (FR-007c): the wallet group is recorded evidence only, so it neither adds a
        contribution nor counts as a scoring group until a T027 PASS receipt qualifies it.

        Args:
            bundle: SignalBundle with all available signals.

        Returns:
            Tuple of (weighted_score, signals_triggered_count).
        """
        signal = bundle.size_anomaly_signal
        if signal is None:
            return 0.0, 0
        return size_group_score(self._weights, signal.confidence, niche=signal.is_niche_market)

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
        """The active signal weights as a defensive copy; mutating it changes nothing."""
        return dict(self._weights)
