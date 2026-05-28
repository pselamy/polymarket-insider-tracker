"""Tests for tail-bet detection."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from polymarket_insider_tracker.detector.models import TailBetSignal
from polymarket_insider_tracker.detector.tail_bet import (
    DEFAULT_MAX_PRICE,
    DEFAULT_MIN_PAYOUT_USDC,
    DEFAULT_VOLUME_UNKNOWN_BASELINE,
    NICHE_MULTIPLIER,
    TailBetDetector,
)
from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_metadata_sync() -> AsyncMock:
    sync = AsyncMock(spec=MarketMetadataSync)
    sync.get_market.return_value = MarketMetadata(
        condition_id="market_abc",
        question="Will X happen?",
        description="",
        tokens=(Token(token_id="tok_1", outcome="Yes", price=Decimal("0.05")),),
        category="other",
    )
    return sync


def _trade(
    *,
    side: str = "BUY",
    price: str = "0.05",
    size: str = "30000",
    market_id: str = "market_abc",
) -> TradeEvent:
    return TradeEvent(
        market_id=market_id,
        trade_id="tx_tail",
        wallet_address="0xabc",
        side=side,
        outcome="Yes",
        outcome_index=0,
        price=Decimal(price),
        size=Decimal(size),
        timestamp=datetime.now(UTC),
        asset_id="tok_1",
        event_title="Tail Bet Market",
    )


# ============================================================================
# Structural filters
# ============================================================================


class TestStructuralFilters:
    """The detector must hard-skip trades that aren't a tail bet by definition."""

    @pytest.mark.asyncio
    async def test_sell_side_skipped(self, mock_metadata_sync: AsyncMock) -> None:
        det = TailBetDetector(mock_metadata_sync)
        result = await det.analyze(_trade(side="SELL", price="0.05", size="30000"))
        assert result is None

    @pytest.mark.asyncio
    async def test_price_above_max_skipped(self, mock_metadata_sync: AsyncMock) -> None:
        det = TailBetDetector(mock_metadata_sync)
        # 0.20 is above the 0.10 default cap — settlement-arb-ish, not a tail bet.
        result = await det.analyze(_trade(price="0.20", size="20000"))
        assert result is None

    @pytest.mark.asyncio
    async def test_price_at_max_admitted(self, mock_metadata_sync: AsyncMock) -> None:
        # Boundary: price == max_price is admitted (extremity hits 0 but the
        # impact axis can still drive confidence above the floor).
        det = TailBetDetector(mock_metadata_sync)
        result = await det.analyze(
            _trade(price="0.10", size="50000"),
            daily_volume=Decimal("100000"),
        )
        assert result is not None

    @pytest.mark.asyncio
    async def test_zero_price_skipped(self, mock_metadata_sync: AsyncMock) -> None:
        det = TailBetDetector(mock_metadata_sync)
        result = await det.analyze(_trade(price="0", size="50000"))
        assert result is None

    @pytest.mark.asyncio
    async def test_payout_below_floor_skipped(self, mock_metadata_sync: AsyncMock) -> None:
        # size=500 at price=0.05 -> payout=475, below the $1000 floor.
        det = TailBetDetector(mock_metadata_sync)
        result = await det.analyze(_trade(price="0.05", size="500"))
        assert result is None


# ============================================================================
# Confidence math
# ============================================================================


class TestConfidence:
    @pytest.mark.asyncio
    async def test_extreme_low_price_high_confidence(
        self, mock_metadata_sync: AsyncMock
    ) -> None:
        # price near 0 -> extremity ~= 1.0; impact reaches 1.0 at 5% of volume.
        det = TailBetDetector(mock_metadata_sync)
        result = await det.analyze(
            _trade(price="0.001", size="200000"),  # payout ~= $199.8k
            daily_volume=Decimal("4000000"),  # 5% of vol
        )
        assert result is not None
        assert isinstance(result, TailBetSignal)
        assert result.confidence == pytest.approx(1.0, abs=0.05)
        assert result.is_niche_market is False

    @pytest.mark.asyncio
    async def test_niche_multiplier_applied(self, mock_metadata_sync: AsyncMock) -> None:
        det = TailBetDetector(mock_metadata_sync)
        # daily_volume below 50k threshold -> is_niche=True -> *1.5x.
        # Use a moderate setup where the unmultiplied score is well below 1.0
        # so the multiplier is observable rather than clipped.
        result = await det.analyze(
            _trade(price="0.05", size="30000"),
            daily_volume=Decimal("40000"),
        )
        assert result is not None
        assert result.is_niche_market is True
        assert result.factors.get("niche_multiplier") == NICHE_MULTIPLIER

    @pytest.mark.asyncio
    async def test_volume_unknown_baseline(self, mock_metadata_sync: AsyncMock) -> None:
        # No daily_volume -> impact axis falls back to flat baseline.
        det = TailBetDetector(mock_metadata_sync)
        result = await det.analyze(_trade(price="0.05", size="30000"), daily_volume=None)
        assert result is not None
        assert result.factors.get("volume_unknown_baseline") == DEFAULT_VOLUME_UNKNOWN_BASELINE
        # extremity = 1 - 0.05/0.10 = 0.5; confidence = 0.5*0.5 + 0.5*0.4 = 0.45.
        assert result.confidence == pytest.approx(0.45, abs=0.001)

    @pytest.mark.asyncio
    async def test_payout_to_notional_matches_leverage(
        self, mock_metadata_sync: AsyncMock
    ) -> None:
        # price=0.05 -> payout/notional = 0.95/0.05 = 19.
        det = TailBetDetector(mock_metadata_sync)
        result = await det.analyze(
            _trade(price="0.05", size="30000"),
            daily_volume=Decimal("100000"),
        )
        assert result is not None
        assert result.payout_to_notional_ratio == pytest.approx(19.0, abs=0.01)


# ============================================================================
# Metadata fallback
# ============================================================================


class TestMetadataFallback:
    @pytest.mark.asyncio
    async def test_metadata_failure_does_not_drop_signal(
        self, mock_metadata_sync: AsyncMock
    ) -> None:
        mock_metadata_sync.get_market.side_effect = RuntimeError("boom")
        det = TailBetDetector(mock_metadata_sync)
        result = await det.analyze(
            _trade(price="0.02", size="100000"),
            daily_volume=Decimal("500000"),
        )
        assert result is not None

    @pytest.mark.asyncio
    async def test_metadata_none_does_not_drop_signal(
        self, mock_metadata_sync: AsyncMock
    ) -> None:
        mock_metadata_sync.get_market.return_value = None
        det = TailBetDetector(mock_metadata_sync)
        result = await det.analyze(
            _trade(price="0.02", size="100000"),
            daily_volume=Decimal("500000"),
        )
        assert result is not None


# ============================================================================
# Defaults
# ============================================================================


class TestDefaults:
    def test_default_constants(self) -> None:
        # Guard against accidental tweaks — the thresholds are tuned
        # against the cost-adjusted backtest, so changes need explicit review.
        assert DEFAULT_MAX_PRICE == Decimal("0.10")
        assert DEFAULT_MIN_PAYOUT_USDC == Decimal("1000")
        assert NICHE_MULTIPLIER == 1.5
