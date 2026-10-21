"""Fresh wallet detection algorithm.

This module provides the FreshWalletDetector class that identifies trades
from fresh wallets and generates alert signals with confidence scores.
"""

import logging
from abc import abstractmethod
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from polymarket_insider_tracker.detector._batch import gather_signals
from polymarket_insider_tracker.detector.evaluation import WalletClassification, WalletEvaluation
from polymarket_insider_tracker.detector.evaluation_clock import (
    Clock,
    read_evaluation_instant,
    system_clock,
    utc_instant,
)
from polymarket_insider_tracker.detector.models import FreshWalletSignal
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_MIN_TRADE_SIZE = Decimal("1000")  # $1,000 minimum trade size
DEFAULT_MAX_NONCE = 5  # Max nonce to be considered fresh
DEFAULT_MAX_AGE_HOURS = 48.0  # Max age in hours to be considered fresh

# Confidence scoring constants
BASE_CONFIDENCE = 0.5
BRAND_NEW_BONUS = 0.2  # nonce == 0
VERY_YOUNG_BONUS = 0.1  # age < 2 hours
LARGE_TRADE_BONUS = 0.1  # trade size > $10,000
LARGE_TRADE_THRESHOLD = Decimal("10000")


def _qualifying_class(profile: WalletProfile) -> WalletClassification:
    """A qualifying wallet is ``fresh`` only with a known age (data-model section 2)."""
    if profile.age_hours is None:
        return WalletClassification.LOW_ACTIVITY_UNKNOWN_AGE
    return WalletClassification.FRESH


class WalletProfiler(Protocol):
    """Wallet profiling the detector needs (satisfied structurally by ``WalletAnalyzer``)."""

    @abstractmethod
    async def analyze(self, address: str, /) -> WalletProfile:
        """Profile the wallet at ``address``."""


class FreshWalletDetector:
    """Detector for fresh wallet trading patterns.

    This detector analyzes trade events for fresh wallet signals. A trade
    is flagged as suspicious if:
    1. The wallet meets freshness criteria (low nonce, recent activity)
    2. The trade size meets minimum threshold

    The detector produces confidence scores based on multiple factors:
    - Wallet nonce (brand new = higher confidence)
    - Wallet age (very young = higher confidence)
    - Trade size (larger = higher confidence)

    Example:
        ```python
        analyzer = WalletAnalyzer(polygon_client, redis=redis)
        detector = FreshWalletDetector(analyzer)

        # Analyze a single trade
        signal = await detector.analyze(trade_event)
        if signal is not None:
            print(f"Fresh wallet detected! Confidence: {signal.confidence}")
        ```
    """

    def __init__(
        self,
        wallet_analyzer: WalletProfiler,
        *,
        min_trade_size: Decimal = DEFAULT_MIN_TRADE_SIZE,
        max_nonce: int = DEFAULT_MAX_NONCE,
        max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
        clock: Clock = system_clock,
    ) -> None:
        """Initialize the fresh wallet detector.

        Args:
            wallet_analyzer: Wallet profiler (normally a WalletAnalyzer).
            min_trade_size: Minimum trade size to analyze (default $1,000).
            max_nonce: Maximum nonce to consider wallet fresh (default 5).
            max_age_hours: Maximum age in hours to consider fresh (default 48).
            clock: Source of the evaluation instant, read once per analyzed trade.
        """
        self._analyzer = wallet_analyzer
        self._min_trade_size = min_trade_size
        self._max_nonce = max_nonce
        self._max_age_hours = max_age_hours
        self._clock = clock

    @property
    def fresh_transaction_boundary(self) -> int:
        """The effective exclusive boundary ``B``: the inclusive ``max_nonce`` plus one."""
        return self._max_nonce + 1

    @property
    def min_trade_size(self) -> Decimal:
        """The minimum notional a trade needs before its wallet is profiled."""
        return self._min_trade_size

    @property
    def max_age_hours(self) -> float:
        """The known-age ceiling in hours."""
        return self._max_age_hours

    async def analyze(self, trade: TradeEvent) -> FreshWalletSignal | None:
        """Analyze a trade event for fresh wallet signals.

        This method:
        1. Filters out trades below the minimum size threshold
        2. Analyzes the trader's wallet profile
        3. Determines if the wallet is fresh
        4. Calculates confidence score based on multiple factors

        Args:
            trade: TradeEvent to analyze.

        Returns:
            FreshWalletSignal if the trade is from a fresh wallet,
            None otherwise.

        Raises:
            Exception: A wallet profiling failure propagates so the pipeline can count
                it and record why fresh-wallet evidence is absent; swallowing it here
                would be indistinguishable from "wallet is not fresh".
        """
        evaluation = await self.evaluate(trade, evaluated_at=read_evaluation_instant(self._clock))
        return evaluation.signal

    async def evaluate(self, trade: TradeEvent, *, evaluated_at: datetime) -> WalletEvaluation:
        """Classify the trade's wallet at ``evaluated_at``; the signal is set only if it qualifies.

        Raises:
            ValueError: ``evaluated_at`` is naive.
            Exception: A wallet profiling failure propagates, exactly as from ``analyze``.
        """
        instant = utc_instant(evaluated_at)
        # Filter by minimum trade size
        if trade.notional_value < self._min_trade_size:
            logger.debug(
                "Trade %s below minimum size: %s < %s",
                trade.trade_id,
                trade.notional_value,
                self._min_trade_size,
            )
            return self._evaluation(instant, WalletClassification.NOT_REQUESTED, None, None)

        # Get wallet profile
        profile = await self._analyzer.analyze(trade.wallet_address)

        # Check if wallet is fresh
        if not self._is_wallet_fresh(profile):
            logger.debug(
                "Wallet %s is not fresh (nonce=%d, age=%s)",
                trade.wallet_address,
                profile.nonce,
                profile.age_hours,
            )
            return self._evaluation(
                instant, WalletClassification.KNOWN_NONQUALIFYING, profile, None
            )

        # Calculate confidence score
        confidence, factors = self.calculate_confidence(profile, trade)

        logger.info(
            "Fresh wallet signal: wallet=%s, market=%s, size=%s, confidence=%.2f",
            trade.wallet_address[:10] + "...",
            trade.market_id[:10] + "...",
            trade.notional_value,
            confidence,
        )

        signal = FreshWalletSignal(
            trade_event=trade,
            wallet_profile=profile,
            confidence=confidence,
            factors=factors,
        )
        return self._evaluation(instant, _qualifying_class(profile), profile, signal)

    def _evaluation(
        self,
        instant: datetime,
        classification: WalletClassification,
        profile: WalletProfile | None,
        signal: FreshWalletSignal | None,
    ) -> WalletEvaluation:
        boundary = self.fresh_transaction_boundary
        return WalletEvaluation(instant, classification, boundary, profile, signal)

    def _is_wallet_fresh(self, profile: WalletProfile) -> bool:
        """Check if wallet meets freshness criteria.

        A wallet is considered fresh if:
        1. Nonce is at or below max_nonce threshold
        2. Age is unknown OR within max_age_hours

        Args:
            profile: Wallet profile to check.

        Returns:
            True if wallet is fresh.
        """
        # Must have few transactions
        if profile.nonce > self._max_nonce:
            return False

        # If age is known, must be recent
        return not (profile.age_hours is not None and profile.age_hours > self._max_age_hours)

    def calculate_confidence(
        self,
        profile: WalletProfile,
        trade: TradeEvent,
    ) -> tuple[float, dict[str, float]]:
        """Calculate confidence score based on multiple factors.

        Confidence scoring:
        - Base: 0.5 (fresh wallet detected)
        - +0.2 if nonce == 0 (brand new wallet)
        - +0.1 if age < 2 hours (very young)
        - +0.1 if trade size > $10,000 (large trade)

        Final confidence is clamped to [0.0, 1.0].

        Args:
            profile: Wallet profile with nonce and age data.
            trade: Trade event with size data.

        Returns:
            Tuple of (confidence_score, factors_dict).
        """
        factors: dict[str, float] = {"base": BASE_CONFIDENCE}
        confidence = BASE_CONFIDENCE

        # Brand new wallet bonus
        if profile.nonce == 0:
            factors["brand_new"] = BRAND_NEW_BONUS
            confidence += BRAND_NEW_BONUS

        # Very young wallet bonus
        if profile.age_hours is not None and profile.age_hours < 2.0:
            factors["very_young"] = VERY_YOUNG_BONUS
            confidence += VERY_YOUNG_BONUS

        # Large trade bonus
        if trade.notional_value > LARGE_TRADE_THRESHOLD:
            factors["large_trade"] = LARGE_TRADE_BONUS
            confidence += LARGE_TRADE_BONUS

        # Clamp to valid range
        confidence = max(0.0, min(1.0, confidence))

        return confidence, factors

    async def analyze_batch(
        self,
        trades: list[TradeEvent],
    ) -> list[FreshWalletSignal]:
        """Analyze multiple trades for fresh wallet signals.

        Processes trades in parallel for efficiency.

        Args:
            trades: List of trades to analyze.

        Returns:
            List of FreshWalletSignal for trades from fresh wallets.
        """
        tasks = [self.analyze(trade) for trade in trades]
        return await gather_signals(trades, tasks, logger)
