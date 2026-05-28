"""Position size anomaly detection algorithm.

This module provides the SizeAnomalyDetector class that identifies trades
with unusually large position sizes relative to market liquidity.
"""

import logging
from decimal import Decimal

from polymarket_insider_tracker.detector.models import SizeAnomalySignal
from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_VOLUME_THRESHOLD = 0.02  # 2% of daily volume
DEFAULT_BOOK_THRESHOLD = 0.05  # 5% of order book depth
DEFAULT_NICHE_VOLUME_THRESHOLD = Decimal("50000")  # $50k daily volume

# Niche market categories - markets in these categories with low specificity
# are more likely to have insider information value
NICHE_PRONE_CATEGORIES = frozenset({"science", "tech", "finance", "other"})


class SizeAnomalyDetector:
    """Detector for unusually large trade sizes.

    This detector analyzes trade events for size anomalies by comparing
    the trade size against market liquidity metrics:
    - Volume impact: trade size / 24h volume
    - Book impact: trade size / order book depth

    When volume data is unavailable, the detector uses category-based
    heuristics to identify niche markets where large trades are more
    significant.

    Confidence scoring:
    - Volume impact > threshold: base score from impact ratio
    - Book impact > threshold: additional score from impact ratio
    - Niche market multiplier: 1.5x for low-volume markets

    Example:
        ```python
        sync = MarketMetadataSync(redis, clob_client)
        detector = SizeAnomalyDetector(sync)

        # Analyze a trade
        signal = await detector.analyze(trade_event)
        if signal is not None:
            print(f"Size anomaly detected! Confidence: {signal.confidence}")
        ```
    """

    def __init__(
        self,
        metadata_sync: MarketMetadataSync,
        *,
        volume_threshold: float = DEFAULT_VOLUME_THRESHOLD,
        book_threshold: float = DEFAULT_BOOK_THRESHOLD,
        niche_volume_threshold: Decimal = DEFAULT_NICHE_VOLUME_THRESHOLD,
    ) -> None:
        """Initialize the size anomaly detector.

        Args:
            metadata_sync: MarketMetadataSync for fetching market metadata.
            volume_threshold: Threshold for volume impact (default 0.02 = 2%).
            book_threshold: Threshold for book impact (default 0.05 = 5%).
            niche_volume_threshold: Volume below which market is niche ($50k).
        """
        self._metadata_sync = metadata_sync
        self._volume_threshold = volume_threshold
        self._book_threshold = book_threshold
        self._niche_volume_threshold = niche_volume_threshold

    async def analyze(
        self,
        trade: TradeEvent,
        *,
        daily_volume: Decimal | None = None,
        book_depth: Decimal | None = None,
    ) -> SizeAnomalySignal | None:
        """Analyze a trade event for size anomalies.

        This method:
        1. Fetches market metadata
        2. Calculates volume and book impact (if data available)
        3. Determines if market is niche
        4. Calculates confidence score

        Args:
            trade: TradeEvent to analyze.
            daily_volume: Optional 24h volume in USDC. If provided, enables
                volume impact calculation.
            book_depth: Optional order book depth in USDC. If provided,
                enables book impact calculation.

        Returns:
            SizeAnomalySignal if the trade triggers anomaly detection,
            None otherwise.
        """
        # Get market metadata
        try:
            metadata = await self._metadata_sync.get_market(trade.market_id)
            if metadata is None:
                logger.warning(
                    "No metadata found for market %s, creating minimal metadata",
                    trade.market_id,
                )
                metadata = self._create_minimal_metadata(trade)
        except Exception as e:
            logger.warning(
                "Failed to get metadata for market %s: %s",
                trade.market_id,
                e,
            )
            metadata = self._create_minimal_metadata(trade)

        trade_size = trade.notional_value

        # Fall back to gamma-enriched volume on the metadata when the caller
        # didn't supply one — pipeline.py invokes analyze(trade) with no
        # explicit volume, so without this the volume_impact branch is dead
        # and every niche trade scores the flat 0.20 niche_base.
        if daily_volume is None:
            daily_volume = metadata.daily_volume

        # Calculate impacts
        volume_impact = self._calculate_volume_impact(trade_size, daily_volume)
        book_impact = self._calculate_book_impact(trade_size, book_depth)

        # Determine if niche market
        is_niche = self._is_niche_market(metadata, daily_volume)

        # Check if any threshold exceeded
        exceeds_volume = volume_impact > self._volume_threshold
        exceeds_book = book_impact > self._book_threshold

        if not exceeds_volume and not exceeds_book and not is_niche:
            logger.debug(
                "Trade %s does not exceed thresholds: volume=%.4f, book=%.4f",
                trade.trade_id,
                volume_impact,
                book_impact,
            )
            return None

        # Calculate confidence score
        confidence, factors = self.calculate_confidence(
            volume_impact=volume_impact,
            book_impact=book_impact,
            is_niche=is_niche,
        )

        # Only emit signal if confidence is meaningful
        if confidence < 0.1:
            return None

        logger.info(
            "Size anomaly signal: market=%s, size=%s, volume_impact=%.4f, "
            "book_impact=%.4f, niche=%s, confidence=%.2f",
            trade.market_id[:10] + "...",
            trade_size,
            volume_impact,
            book_impact,
            is_niche,
            confidence,
        )

        return SizeAnomalySignal(
            trade_event=trade,
            market_metadata=metadata,
            volume_impact=volume_impact,
            book_impact=book_impact,
            is_niche_market=is_niche,
            confidence=confidence,
            factors=factors,
        )

    def _create_minimal_metadata(self, trade: TradeEvent) -> MarketMetadata:
        """Create minimal metadata from trade event."""
        from polymarket_insider_tracker.ingestor.models import Token

        return MarketMetadata(
            condition_id=trade.market_id,
            question=trade.event_title or "Unknown Market",
            description="",
            tokens=(
                Token(
                    token_id=trade.asset_id,
                    outcome=trade.outcome,
                    price=trade.price,
                ),
            ),
            category="other",
        )

    def _calculate_volume_impact(
        self,
        trade_size: Decimal,
        daily_volume: Decimal | None,
    ) -> float:
        """Calculate trade size as fraction of daily volume.

        Args:
            trade_size: Trade notional value in USDC.
            daily_volume: 24h trading volume in USDC.

        Returns:
            Volume impact ratio, or 0.0 if volume unknown.
        """
        if daily_volume is None or daily_volume <= 0:
            return 0.0
        return float(trade_size / daily_volume)

    def _calculate_book_impact(
        self,
        trade_size: Decimal,
        book_depth: Decimal | None,
    ) -> float:
        """Calculate trade size as fraction of order book depth.

        Args:
            trade_size: Trade notional value in USDC.
            book_depth: Visible order book depth in USDC.

        Returns:
            Book impact ratio, or 0.0 if depth unknown.
        """
        if book_depth is None or book_depth <= 0:
            return 0.0
        return float(trade_size / book_depth)

    def _is_niche_market(
        self,
        metadata: MarketMetadata,
        daily_volume: Decimal | None,
    ) -> bool:
        """Determine if market is considered niche.

        A market is niche if:
        - Volume is below threshold ($50k), OR
        - Category is prone to insider info AND volume is unknown

        Args:
            metadata: Market metadata with category.
            daily_volume: Optional 24h volume.

        Returns:
            True if market is considered niche.
        """
        # If volume known and below threshold, it's niche
        if daily_volume is not None and daily_volume < self._niche_volume_threshold:
            return True

        # If volume unknown, use category heuristics
        return daily_volume is None and metadata.category in NICHE_PRONE_CATEGORIES

    def calculate_confidence(
        self,
        *,
        volume_impact: float,
        book_impact: float,
        is_niche: bool,
    ) -> tuple[float, dict[str, float]]:
        """Calculate confidence score based on impact metrics.

        Confidence scoring:
        - Volume impact: min(impact/threshold, 3) / 3 * 0.5
        - Book impact: min(impact/threshold, 3) / 3 * 0.3
        - Niche multiplier: 1.5x final score

        Final confidence clamped to [0.0, 1.0].

        Args:
            volume_impact: Trade size / daily volume ratio.
            book_impact: Trade size / book depth ratio.
            is_niche: Whether market is niche.

        Returns:
            Tuple of (confidence_score, factors_dict).
        """
        factors: dict[str, float] = {}
        confidence = 0.0

        # Volume impact component
        if volume_impact > self._volume_threshold:
            ratio = min(volume_impact / self._volume_threshold, 3.0)
            volume_score = ratio / 3.0 * 0.5
            factors["volume_impact"] = volume_score
            confidence += volume_score

        # Book impact component
        if book_impact > self._book_threshold:
            ratio = min(book_impact / self._book_threshold, 3.0)
            book_score = ratio / 3.0 * 0.3
            factors["book_impact"] = book_score
            confidence += book_score

        # Niche market multiplier
        if is_niche and confidence > 0:
            factors["niche_multiplier"] = 1.5
            confidence *= 1.5

        # If niche but no other signals, give small base confidence
        if is_niche and confidence == 0:
            factors["niche_base"] = 0.2
            confidence = 0.2

        # Clamp to valid range
        confidence = max(0.0, min(1.0, confidence))

        return confidence, factors

    async def analyze_batch(
        self,
        trades: list[TradeEvent],
        *,
        volume_data: dict[str, Decimal] | None = None,
        book_data: dict[str, Decimal] | None = None,
    ) -> list[SizeAnomalySignal]:
        """Analyze multiple trades for size anomalies.

        Processes trades in parallel for efficiency.

        Args:
            trades: List of trades to analyze.
            volume_data: Optional dict mapping market_id to 24h volume.
            book_data: Optional dict mapping market_id to book depth.

        Returns:
            List of SizeAnomalySignal for trades with anomalies.
        """
        import asyncio

        volume_data = volume_data or {}
        book_data = book_data or {}

        tasks = [
            self.analyze(
                trade,
                daily_volume=volume_data.get(trade.market_id),
                book_depth=book_data.get(trade.market_id),
            )
            for trade in trades
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        signals: list[SizeAnomalySignal] = []
        for trade, result in zip(trades, results, strict=True):
            if isinstance(result, BaseException):
                logger.warning(
                    "Failed to analyze trade %s: %s",
                    trade.trade_id,
                    result,
                )
                continue
            if result is not None:
                signals.append(result)

        return signals
