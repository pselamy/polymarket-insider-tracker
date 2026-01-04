"""Tests for position size anomaly detection."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from polymarket_insider_tracker.detector.models import SizeAnomalySignal
from polymarket_insider_tracker.detector.size_anomaly import (
    DEFAULT_BOOK_THRESHOLD,
    DEFAULT_NICHE_VOLUME_THRESHOLD,
    DEFAULT_VOLUME_THRESHOLD,
    NICHE_PRONE_CATEGORIES,
    SizeAnomalyDetector,
)
from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_metadata_sync() -> AsyncMock:
    """Create a mock MarketMetadataSync."""
    return AsyncMock(spec=MarketMetadataSync)


@pytest.fixture
def sample_token() -> Token:
    """Create a sample token."""
    return Token(
        token_id="token_123",
        outcome="Yes",
        price=Decimal("0.65"),
    )


@pytest.fixture
def sample_metadata(sample_token: Token) -> MarketMetadata:
    """Create sample market metadata."""
    return MarketMetadata(
        condition_id="market_abc123",
        question="Will it rain tomorrow?",
        description="Weather prediction market",
        tokens=(sample_token,),
        category="science",
    )


@pytest.fixture
def sample_trade() -> TradeEvent:
    """Create a sample trade event."""
    return TradeEvent(
        market_id="market_abc123",
        trade_id="tx_001",
        wallet_address="0x1234567890abcdef",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("0.65"),
        size=Decimal("10000"),  # $6,500 notional
        timestamp=datetime.now(UTC),
        asset_id="token_123",
        event_title="Weather Market",
    )


@pytest.fixture
def large_trade() -> TradeEvent:
    """Create a large trade event."""
    return TradeEvent(
        market_id="market_abc123",
        trade_id="tx_002",
        wallet_address="0xlargewallet",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("0.50"),
        size=Decimal("100000"),  # $50,000 notional
        timestamp=datetime.now(UTC),
        asset_id="token_123",
        event_title="Big Market",
    )


# ============================================================================
# SizeAnomalySignal Tests
# ============================================================================


class TestSizeAnomalySignal:
    """Tests for the SizeAnomalySignal dataclass."""

    def test_signal_creation(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        """Test basic signal creation."""
        signal = SizeAnomalySignal(
            trade_event=sample_trade,
            market_metadata=sample_metadata,
            volume_impact=0.05,
            book_impact=0.10,
            is_niche_market=True,
            confidence=0.75,
            factors={"volume_impact": 0.4, "niche_multiplier": 1.5},
        )

        assert signal.trade_event == sample_trade
        assert signal.market_metadata == sample_metadata
        assert signal.volume_impact == 0.05
        assert signal.book_impact == 0.10
        assert signal.is_niche_market is True
        assert signal.confidence == 0.75
        assert "volume_impact" in signal.factors

    def test_wallet_address_property(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        """Test wallet_address property."""
        signal = SizeAnomalySignal(
            trade_event=sample_trade,
            market_metadata=sample_metadata,
            volume_impact=0.05,
            book_impact=0.10,
            is_niche_market=False,
            confidence=0.5,
            factors={},
        )

        assert signal.wallet_address == sample_trade.wallet_address

    def test_market_id_property(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        """Test market_id property."""
        signal = SizeAnomalySignal(
            trade_event=sample_trade,
            market_metadata=sample_metadata,
            volume_impact=0.05,
            book_impact=0.10,
            is_niche_market=False,
            confidence=0.5,
            factors={},
        )

        assert signal.market_id == sample_trade.market_id

    def test_trade_size_usdc_property(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        """Test trade_size_usdc property returns notional value."""
        signal = SizeAnomalySignal(
            trade_event=sample_trade,
            market_metadata=sample_metadata,
            volume_impact=0.05,
            book_impact=0.10,
            is_niche_market=False,
            confidence=0.5,
            factors={},
        )

        # notional = price * size = 0.65 * 10000 = 6500
        assert signal.trade_size_usdc == Decimal("6500.00")

    def test_is_high_confidence(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        """Test is_high_confidence threshold."""
        high_signal = SizeAnomalySignal(
            trade_event=sample_trade,
            market_metadata=sample_metadata,
            volume_impact=0.05,
            book_impact=0.10,
            is_niche_market=False,
            confidence=0.70,
            factors={},
        )
        low_signal = SizeAnomalySignal(
            trade_event=sample_trade,
            market_metadata=sample_metadata,
            volume_impact=0.05,
            book_impact=0.10,
            is_niche_market=False,
            confidence=0.69,
            factors={},
        )

        assert high_signal.is_high_confidence is True
        assert low_signal.is_high_confidence is False

    def test_is_very_high_confidence(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        """Test is_very_high_confidence threshold."""
        very_high = SizeAnomalySignal(
            trade_event=sample_trade,
            market_metadata=sample_metadata,
            volume_impact=0.05,
            book_impact=0.10,
            is_niche_market=False,
            confidence=0.85,
            factors={},
        )
        high = SizeAnomalySignal(
            trade_event=sample_trade,
            market_metadata=sample_metadata,
            volume_impact=0.05,
            book_impact=0.10,
            is_niche_market=False,
            confidence=0.84,
            factors={},
        )

        assert very_high.is_very_high_confidence is True
        assert high.is_very_high_confidence is False

    def test_to_dict_serialization(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        """Test to_dict produces valid serialization."""
        signal = SizeAnomalySignal(
            trade_event=sample_trade,
            market_metadata=sample_metadata,
            volume_impact=0.05,
            book_impact=0.10,
            is_niche_market=True,
            confidence=0.75,
            factors={"volume_impact": 0.5},
        )

        result = signal.to_dict()

        assert result["wallet_address"] == sample_trade.wallet_address
        assert result["market_id"] == sample_trade.market_id
        assert result["trade_id"] == sample_trade.trade_id
        assert result["trade_size"] == "6500.00"
        assert result["trade_side"] == "BUY"
        assert result["market_category"] == "science"
        assert result["volume_impact"] == 0.05
        assert result["book_impact"] == 0.10
        assert result["is_niche_market"] is True
        assert result["confidence"] == 0.75
        assert result["factors"] == {"volume_impact": 0.5}
        assert "timestamp" in result


# ============================================================================
# SizeAnomalyDetector Initialization Tests
# ============================================================================


class TestSizeAnomalyDetectorInit:
    """Tests for SizeAnomalyDetector initialization."""

    def test_default_initialization(self, mock_metadata_sync: AsyncMock) -> None:
        """Test detector initializes with default values."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        assert detector._volume_threshold == DEFAULT_VOLUME_THRESHOLD
        assert detector._book_threshold == DEFAULT_BOOK_THRESHOLD
        assert detector._niche_volume_threshold == DEFAULT_NICHE_VOLUME_THRESHOLD

    def test_custom_thresholds(self, mock_metadata_sync: AsyncMock) -> None:
        """Test detector with custom thresholds."""
        detector = SizeAnomalyDetector(
            mock_metadata_sync,
            volume_threshold=0.05,
            book_threshold=0.10,
            niche_volume_threshold=Decimal("100000"),
        )

        assert detector._volume_threshold == 0.05
        assert detector._book_threshold == 0.10
        assert detector._niche_volume_threshold == Decimal("100000")


# ============================================================================
# Volume Impact Tests
# ============================================================================


class TestVolumeImpactCalculation:
    """Tests for volume impact calculation."""

    def test_volume_impact_calculation(self, mock_metadata_sync: AsyncMock) -> None:
        """Test correct volume impact calculation."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # Trade size $1000, daily volume $50000 = 2% impact
        impact = detector._calculate_volume_impact(Decimal("1000"), Decimal("50000"))
        assert impact == pytest.approx(0.02)

    def test_volume_impact_none_volume(self, mock_metadata_sync: AsyncMock) -> None:
        """Test volume impact returns 0 when volume is None."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        impact = detector._calculate_volume_impact(Decimal("1000"), None)
        assert impact == 0.0

    def test_volume_impact_zero_volume(self, mock_metadata_sync: AsyncMock) -> None:
        """Test volume impact returns 0 when volume is zero."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        impact = detector._calculate_volume_impact(Decimal("1000"), Decimal("0"))
        assert impact == 0.0

    def test_volume_impact_negative_volume(self, mock_metadata_sync: AsyncMock) -> None:
        """Test volume impact returns 0 when volume is negative."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        impact = detector._calculate_volume_impact(Decimal("1000"), Decimal("-1000"))
        assert impact == 0.0


# ============================================================================
# Book Impact Tests
# ============================================================================


class TestBookImpactCalculation:
    """Tests for order book impact calculation."""

    def test_book_impact_calculation(self, mock_metadata_sync: AsyncMock) -> None:
        """Test correct book impact calculation."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # Trade size $5000, book depth $50000 = 10% impact
        impact = detector._calculate_book_impact(Decimal("5000"), Decimal("50000"))
        assert impact == pytest.approx(0.10)

    def test_book_impact_none_depth(self, mock_metadata_sync: AsyncMock) -> None:
        """Test book impact returns 0 when depth is None."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        impact = detector._calculate_book_impact(Decimal("5000"), None)
        assert impact == 0.0

    def test_book_impact_zero_depth(self, mock_metadata_sync: AsyncMock) -> None:
        """Test book impact returns 0 when depth is zero."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        impact = detector._calculate_book_impact(Decimal("5000"), Decimal("0"))
        assert impact == 0.0


# ============================================================================
# Niche Market Detection Tests
# ============================================================================


class TestNicheMarketDetection:
    """Tests for niche market detection."""

    def test_niche_market_low_volume(
        self, mock_metadata_sync: AsyncMock, sample_metadata: MarketMetadata
    ) -> None:
        """Test market is niche when volume below threshold."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # Volume $40k < $50k threshold
        is_niche = detector._is_niche_market(sample_metadata, Decimal("40000"))
        assert is_niche is True

    def test_not_niche_high_volume(
        self, mock_metadata_sync: AsyncMock, sample_metadata: MarketMetadata
    ) -> None:
        """Test market is not niche when volume above threshold."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # Volume $100k > $50k threshold
        is_niche = detector._is_niche_market(sample_metadata, Decimal("100000"))
        assert is_niche is False

    def test_niche_market_unknown_volume_niche_category(
        self, mock_metadata_sync: AsyncMock, sample_token: Token
    ) -> None:
        """Test market is niche when volume unknown and category is niche-prone."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        for category in NICHE_PRONE_CATEGORIES:
            metadata = MarketMetadata(
                condition_id="test",
                question="Test",
                description="",
                tokens=(sample_token,),
                category=category,
            )
            is_niche = detector._is_niche_market(metadata, None)
            assert is_niche is True, f"Category {category} should be niche"

    def test_not_niche_unknown_volume_mainstream_category(
        self, mock_metadata_sync: AsyncMock, sample_token: Token
    ) -> None:
        """Test market is not niche when volume unknown but category is mainstream."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        mainstream_categories = ["politics", "sports", "crypto", "entertainment"]
        for category in mainstream_categories:
            metadata = MarketMetadata(
                condition_id="test",
                question="Test",
                description="",
                tokens=(sample_token,),
                category=category,
            )
            is_niche = detector._is_niche_market(metadata, None)
            assert is_niche is False, f"Category {category} should not be niche"


# ============================================================================
# Confidence Scoring Tests
# ============================================================================


class TestConfidenceScoring:
    """Tests for confidence score calculation."""

    def test_confidence_volume_impact_only(self, mock_metadata_sync: AsyncMock) -> None:
        """Test confidence with only volume impact."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # Volume impact 3x threshold = max score 0.5
        confidence, factors = detector.calculate_confidence(
            volume_impact=0.06,  # 3x the 0.02 threshold
            book_impact=0.0,
            is_niche=False,
        )

        assert confidence == pytest.approx(0.5)
        assert "volume_impact" in factors
        assert factors["volume_impact"] == pytest.approx(0.5)

    def test_confidence_book_impact_only(self, mock_metadata_sync: AsyncMock) -> None:
        """Test confidence with only book impact."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # Book impact 3x threshold = max score 0.3
        confidence, factors = detector.calculate_confidence(
            volume_impact=0.0,
            book_impact=0.15,  # 3x the 0.05 threshold
            is_niche=False,
        )

        assert confidence == pytest.approx(0.3)
        assert "book_impact" in factors
        assert factors["book_impact"] == pytest.approx(0.3)

    def test_confidence_combined_impacts(self, mock_metadata_sync: AsyncMock) -> None:
        """Test confidence with both volume and book impact."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # Both at 3x threshold = 0.5 + 0.3 = 0.8
        confidence, factors = detector.calculate_confidence(
            volume_impact=0.06,
            book_impact=0.15,
            is_niche=False,
        )

        assert confidence == pytest.approx(0.8)
        assert "volume_impact" in factors
        assert "book_impact" in factors

    def test_confidence_niche_multiplier(self, mock_metadata_sync: AsyncMock) -> None:
        """Test niche multiplier increases confidence."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # Volume impact 2x threshold = 0.33, with 1.5x niche = 0.5
        confidence, factors = detector.calculate_confidence(
            volume_impact=0.04,  # 2x threshold
            book_impact=0.0,
            is_niche=True,
        )

        assert confidence == pytest.approx(0.5, rel=0.01)
        assert "niche_multiplier" in factors
        assert factors["niche_multiplier"] == 1.5

    def test_confidence_niche_only_base(self, mock_metadata_sync: AsyncMock) -> None:
        """Test niche market with no other signals gives base confidence."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # No threshold exceeded, but is niche
        confidence, factors = detector.calculate_confidence(
            volume_impact=0.01,  # Below 0.02 threshold
            book_impact=0.01,  # Below 0.05 threshold
            is_niche=True,
        )

        assert confidence == 0.2
        assert "niche_base" in factors
        assert factors["niche_base"] == 0.2

    def test_confidence_clamped_to_max(self, mock_metadata_sync: AsyncMock) -> None:
        """Test confidence is clamped to 1.0."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # High impacts with niche multiplier would exceed 1.0
        confidence, factors = detector.calculate_confidence(
            volume_impact=0.10,  # 5x threshold (capped at 3x)
            book_impact=0.20,  # 4x threshold (capped at 3x)
            is_niche=True,  # 1.5x multiplier
        )

        assert confidence == 1.0

    def test_confidence_zero_no_signals(self, mock_metadata_sync: AsyncMock) -> None:
        """Test confidence is zero with no signals."""
        detector = SizeAnomalyDetector(mock_metadata_sync)

        confidence, factors = detector.calculate_confidence(
            volume_impact=0.01,  # Below threshold
            book_impact=0.01,  # Below threshold
            is_niche=False,
        )

        assert confidence == 0.0
        assert len(factors) == 0


# ============================================================================
# Analyze Method Tests
# ============================================================================


class TestAnalyzeMethod:
    """Tests for the analyze method."""

    @pytest.mark.asyncio
    async def test_analyze_high_volume_impact(
        self,
        mock_metadata_sync: AsyncMock,
        sample_trade: TradeEvent,
        sample_metadata: MarketMetadata,
    ) -> None:
        """Test analyze detects high volume impact trade."""
        mock_metadata_sync.get_market.return_value = sample_metadata
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # Trade notional = 6500, volume = 65000, impact = 10% > 2% threshold
        signal = await detector.analyze(
            sample_trade,
            daily_volume=Decimal("65000"),
        )

        assert signal is not None
        assert signal.volume_impact == pytest.approx(0.10)
        assert signal.confidence > 0.1

    @pytest.mark.asyncio
    async def test_analyze_high_book_impact(
        self,
        mock_metadata_sync: AsyncMock,
        sample_trade: TradeEvent,
        sample_metadata: MarketMetadata,
    ) -> None:
        """Test analyze detects high book impact trade."""
        mock_metadata_sync.get_market.return_value = sample_metadata
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # Trade notional = 6500, book depth = 32500, impact = 20% > 5% threshold
        signal = await detector.analyze(
            sample_trade,
            book_depth=Decimal("32500"),
        )

        assert signal is not None
        assert signal.book_impact == pytest.approx(0.20)
        assert signal.confidence > 0.1

    @pytest.mark.asyncio
    async def test_analyze_niche_market(
        self,
        mock_metadata_sync: AsyncMock,
        sample_trade: TradeEvent,
        sample_metadata: MarketMetadata,
    ) -> None:
        """Test analyze detects niche market trade."""
        mock_metadata_sync.get_market.return_value = sample_metadata
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # Low volume market (science category with volume unknown)
        signal = await detector.analyze(sample_trade)

        assert signal is not None
        assert signal.is_niche_market is True
        assert signal.confidence == 0.2  # niche_base

    @pytest.mark.asyncio
    async def test_analyze_no_anomaly(
        self,
        mock_metadata_sync: AsyncMock,
        sample_token: Token,
    ) -> None:
        """Test analyze returns None for normal trade."""
        # Politics category is not niche
        metadata = MarketMetadata(
            condition_id="market_politics",
            question="Will Biden win?",
            description="",
            tokens=(sample_token,),
            category="politics",
        )
        mock_metadata_sync.get_market.return_value = metadata

        trade = TradeEvent(
            market_id="market_politics",
            trade_id="tx_normal",
            wallet_address="0xnormal",
            side="BUY",
            outcome="Yes",
            outcome_index=0,
            price=Decimal("0.50"),
            size=Decimal("100"),  # Small trade = $50 notional
            timestamp=datetime.now(UTC),
            asset_id="token_pol",
        )

        detector = SizeAnomalyDetector(mock_metadata_sync)

        # High volume, large book depth = low impact
        signal = await detector.analyze(
            trade,
            daily_volume=Decimal("1000000"),
            book_depth=Decimal("500000"),
        )

        assert signal is None

    @pytest.mark.asyncio
    async def test_analyze_creates_minimal_metadata_on_missing(
        self,
        mock_metadata_sync: AsyncMock,
        sample_trade: TradeEvent,
    ) -> None:
        """Test analyze creates minimal metadata when market not found."""
        mock_metadata_sync.get_market.return_value = None
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # Should still work with minimal metadata (category="other" which is niche)
        signal = await detector.analyze(sample_trade)

        assert signal is not None
        assert signal.market_metadata.condition_id == sample_trade.market_id
        assert signal.market_metadata.category == "other"

    @pytest.mark.asyncio
    async def test_analyze_handles_metadata_exception(
        self,
        mock_metadata_sync: AsyncMock,
        sample_trade: TradeEvent,
    ) -> None:
        """Test analyze handles exception when fetching metadata."""
        mock_metadata_sync.get_market.side_effect = Exception("Redis error")
        detector = SizeAnomalyDetector(mock_metadata_sync)

        # Should still work with minimal metadata
        signal = await detector.analyze(sample_trade)

        assert signal is not None
        assert signal.market_metadata.category == "other"

    @pytest.mark.asyncio
    async def test_analyze_low_confidence_filtered(
        self,
        mock_metadata_sync: AsyncMock,
        sample_token: Token,
    ) -> None:
        """Test analyze returns None when confidence is below 0.1."""
        # Use a mainstream category with below-threshold impacts
        metadata = MarketMetadata(
            condition_id="market_sports",
            question="Super Bowl winner?",
            description="",
            tokens=(sample_token,),
            category="sports",
        )
        mock_metadata_sync.get_market.return_value = metadata

        trade = TradeEvent(
            market_id="market_sports",
            trade_id="tx_small",
            wallet_address="0xsmall",
            side="BUY",
            outcome="Yes",
            outcome_index=0,
            price=Decimal("0.50"),
            size=Decimal("10"),  # Tiny trade
            timestamp=datetime.now(UTC),
            asset_id="token_sports",
        )

        detector = SizeAnomalyDetector(mock_metadata_sync)

        # High volume but below threshold impacts
        signal = await detector.analyze(
            trade,
            daily_volume=Decimal("10000000"),  # $10M volume
            book_depth=Decimal("1000000"),  # $1M depth
        )

        assert signal is None


# ============================================================================
# Batch Analysis Tests
# ============================================================================


class TestBatchAnalysis:
    """Tests for batch analysis."""

    @pytest.mark.asyncio
    async def test_analyze_batch_returns_signals(
        self,
        mock_metadata_sync: AsyncMock,
        sample_metadata: MarketMetadata,
    ) -> None:
        """Test batch analysis returns signals for anomalous trades."""
        mock_metadata_sync.get_market.return_value = sample_metadata

        trades = [
            TradeEvent(
                market_id="market_abc123",
                trade_id=f"tx_{i}",
                wallet_address=f"0xwallet{i}",
                side="BUY",
                outcome="Yes",
                outcome_index=0,
                price=Decimal("0.50"),
                size=Decimal("10000"),  # Large trade
                timestamp=datetime.now(UTC),
                asset_id="token_123",
            )
            for i in range(3)
        ]

        detector = SizeAnomalyDetector(mock_metadata_sync)
        signals = await detector.analyze_batch(trades)

        # All trades are in niche category with unknown volume
        assert len(signals) == 3

    @pytest.mark.asyncio
    async def test_analyze_batch_with_volume_data(
        self,
        mock_metadata_sync: AsyncMock,
        sample_metadata: MarketMetadata,
    ) -> None:
        """Test batch analysis uses provided volume data."""
        mock_metadata_sync.get_market.return_value = sample_metadata

        trades = [
            TradeEvent(
                market_id="market_abc123",
                trade_id="tx_1",
                wallet_address="0xwallet1",
                side="BUY",
                outcome="Yes",
                outcome_index=0,
                price=Decimal("0.50"),
                size=Decimal("10000"),  # $5000 notional
                timestamp=datetime.now(UTC),
                asset_id="token_123",
            )
        ]

        detector = SizeAnomalyDetector(mock_metadata_sync)

        # $5000 trade / $50000 volume = 10% impact
        signals = await detector.analyze_batch(
            trades,
            volume_data={"market_abc123": Decimal("50000")},
        )

        assert len(signals) == 1
        assert signals[0].volume_impact == pytest.approx(0.10)

    @pytest.mark.asyncio
    async def test_analyze_batch_handles_errors(
        self,
        mock_metadata_sync: AsyncMock,
    ) -> None:
        """Test batch analysis handles individual trade errors."""
        # First call succeeds, second fails
        mock_metadata_sync.get_market.side_effect = [
            Exception("Error"),
            None,
        ]

        trades = [
            TradeEvent(
                market_id=f"market_{i}",
                trade_id=f"tx_{i}",
                wallet_address=f"0xwallet{i}",
                side="BUY",
                outcome="Yes",
                outcome_index=0,
                price=Decimal("0.50"),
                size=Decimal("10000"),
                timestamp=datetime.now(UTC),
                asset_id="token_123",
            )
            for i in range(2)
        ]

        detector = SizeAnomalyDetector(mock_metadata_sync)
        signals = await detector.analyze_batch(trades)

        # Both should still produce signals (with minimal metadata fallback)
        assert len(signals) == 2

    @pytest.mark.asyncio
    async def test_analyze_batch_empty_list(self, mock_metadata_sync: AsyncMock) -> None:
        """Test batch analysis with empty list."""
        detector = SizeAnomalyDetector(mock_metadata_sync)
        signals = await detector.analyze_batch([])

        assert signals == []
