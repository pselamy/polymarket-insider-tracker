"""Tests for composite risk scorer."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from polymarket_insider_tracker.detector.models import (
    FreshWalletSignal,
    RiskAssessment,
    SizeAnomalySignal,
)
from polymarket_insider_tracker.detector.scorer import (
    DEFAULT_ALERT_THRESHOLD,
    DEFAULT_WEIGHTS,
    MULTI_SIGNAL_BONUS_2,
    RiskScorer,
    SignalBundle,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_redis() -> AsyncMock:
    """Create a mock Redis client."""
    mock = AsyncMock()
    # Default: key doesn't exist (not a duplicate)
    mock.set.return_value = True
    mock.delete.return_value = 1
    return mock


@pytest.fixture
def sample_trade() -> TradeEvent:
    """Create a sample trade event."""
    return TradeEvent(
        market_id="market_abc123",
        trade_id="tx_001",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("0.65"),
        size=Decimal("10000"),
        timestamp=datetime.now(UTC),
        asset_id="token_123",
        event_title="Test Market",
    )


@pytest.fixture
def sample_wallet_profile() -> WalletProfile:
    """Create a sample wallet profile."""
    return WalletProfile(
        address="0x1234567890abcdef1234567890abcdef12345678",
        nonce=2,
        first_seen=datetime.now(UTC),
        age_hours=1.0,
        is_fresh=True,
        total_tx_count=2,
        matic_balance=Decimal("1000000000000000000"),  # 1 MATIC
        usdc_balance=Decimal("1000000"),  # 1 USDC
    )


@pytest.fixture
def sample_metadata() -> MarketMetadata:
    """Create sample market metadata."""
    return MarketMetadata(
        condition_id="market_abc123",
        question="Will it rain tomorrow?",
        description="Weather prediction market",
        tokens=(Token(token_id="token_123", outcome="Yes", price=Decimal("0.65")),),
        category="science",
    )


@pytest.fixture
def fresh_wallet_signal(
    sample_trade: TradeEvent, sample_wallet_profile: WalletProfile
) -> FreshWalletSignal:
    """Create a sample fresh wallet signal."""
    return FreshWalletSignal(
        trade_event=sample_trade,
        wallet_profile=sample_wallet_profile,
        confidence=0.8,
        factors={"base": 0.5, "brand_new_bonus": 0.2, "large_trade_bonus": 0.1},
    )


@pytest.fixture
def size_anomaly_signal(
    sample_trade: TradeEvent, sample_metadata: MarketMetadata
) -> SizeAnomalySignal:
    """Create a sample size anomaly signal."""
    return SizeAnomalySignal(
        trade_event=sample_trade,
        market_metadata=sample_metadata,
        volume_impact=0.10,
        book_impact=0.15,
        is_niche_market=True,
        confidence=0.7,
        factors={"volume_impact": 0.4, "book_impact": 0.3},
    )


# ============================================================================
# SignalBundle Tests
# ============================================================================


class TestSignalBundle:
    """Tests for the SignalBundle dataclass."""

    def test_bundle_with_no_signals(self, sample_trade: TradeEvent) -> None:
        """Test bundle with only trade, no signals."""
        bundle = SignalBundle(trade_event=sample_trade)

        assert bundle.trade_event == sample_trade
        assert bundle.fresh_wallet_signal is None
        assert bundle.size_anomaly_signal is None
        assert bundle.wallet_address == sample_trade.wallet_address
        assert bundle.market_id == sample_trade.market_id

    def test_bundle_with_fresh_wallet_signal(
        self,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
    ) -> None:
        """Test bundle with fresh wallet signal."""
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
        )

        assert bundle.fresh_wallet_signal == fresh_wallet_signal
        assert bundle.size_anomaly_signal is None

    def test_bundle_with_all_signals(
        self,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        """Test bundle with all signal types."""
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
            size_anomaly_signal=size_anomaly_signal,
        )

        assert bundle.fresh_wallet_signal == fresh_wallet_signal
        assert bundle.size_anomaly_signal == size_anomaly_signal


# ============================================================================
# RiskAssessment Tests
# ============================================================================


class TestRiskAssessment:
    """Tests for the RiskAssessment dataclass."""

    def test_assessment_creation(self, sample_trade: TradeEvent) -> None:
        """Test basic assessment creation."""
        assessment = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=None,
            size_anomaly_signal=None,
            signals_triggered=0,
            weighted_score=0.0,
            should_alert=False,
        )

        assert assessment.trade_event == sample_trade
        assert assessment.signals_triggered == 0
        assert assessment.weighted_score == 0.0
        assert assessment.should_alert is False
        assert assessment.assessment_id is not None
        assert assessment.timestamp is not None

    def test_is_high_risk(self, sample_trade: TradeEvent) -> None:
        """Test is_high_risk property."""
        high_risk = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=None,
            size_anomaly_signal=None,
            signals_triggered=1,
            weighted_score=0.70,
            should_alert=True,
        )
        low_risk = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=None,
            size_anomaly_signal=None,
            signals_triggered=1,
            weighted_score=0.69,
            should_alert=True,
        )

        assert high_risk.is_high_risk is True
        assert low_risk.is_high_risk is False

    def test_is_very_high_risk(self, sample_trade: TradeEvent) -> None:
        """Test is_very_high_risk property."""
        very_high = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=None,
            size_anomaly_signal=None,
            signals_triggered=2,
            weighted_score=0.85,
            should_alert=True,
        )
        high = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=None,
            size_anomaly_signal=None,
            signals_triggered=2,
            weighted_score=0.84,
            should_alert=True,
        )

        assert very_high.is_very_high_risk is True
        assert high.is_very_high_risk is False

    def test_to_dict(
        self,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
    ) -> None:
        """Test to_dict serialization."""
        assessment = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=fresh_wallet_signal,
            size_anomaly_signal=None,
            signals_triggered=1,
            weighted_score=0.65,
            should_alert=True,
        )

        result = assessment.to_dict()

        assert result["wallet_address"] == sample_trade.wallet_address
        assert result["market_id"] == sample_trade.market_id
        assert result["signals_triggered"] == 1
        assert result["weighted_score"] == 0.65
        assert result["should_alert"] is True
        assert result["has_fresh_wallet_signal"] is True
        assert result["has_size_anomaly_signal"] is False
        assert result["fresh_wallet_confidence"] == 0.8
        assert result["size_anomaly_confidence"] is None


# ============================================================================
# RiskScorer Initialization Tests
# ============================================================================


class TestRiskScorerInit:
    """Tests for RiskScorer initialization."""

    def test_default_initialization(self, mock_redis: AsyncMock) -> None:
        """Test scorer initializes with default values."""
        scorer = RiskScorer(mock_redis)

        assert scorer._alert_threshold == DEFAULT_ALERT_THRESHOLD
        assert scorer._weights == DEFAULT_WEIGHTS
        assert scorer._dedup_window == 3600

    def test_custom_configuration(self, mock_redis: AsyncMock) -> None:
        """Test scorer with custom configuration."""
        custom_weights = {"fresh_wallet": 0.5, "size_anomaly": 0.5}
        scorer = RiskScorer(
            mock_redis,
            weights=custom_weights,
            alert_threshold=0.7,
            dedup_window_seconds=1800,
        )

        assert scorer._alert_threshold == 0.7
        assert scorer._weights == custom_weights
        assert scorer._dedup_window == 1800


# ============================================================================
# Weighted Score Calculation Tests
# ============================================================================


class TestWeightedScoreCalculation:
    """Tests for weighted score calculation."""

    def test_no_signals_zero_score(self, mock_redis: AsyncMock, sample_trade: TradeEvent) -> None:
        """Test score is zero when no signals present."""
        scorer = RiskScorer(mock_redis)
        bundle = SignalBundle(trade_event=sample_trade)

        score, count = scorer.calculate_weighted_score(bundle)

        assert score == 0.0
        assert count == 0

    def test_fresh_wallet_only(
        self,
        mock_redis: AsyncMock,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
    ) -> None:
        """Test score with only fresh wallet signal."""
        scorer = RiskScorer(mock_redis)
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
        )

        score, count = scorer.calculate_weighted_score(bundle)

        # 0.8 confidence * 0.4 weight = 0.32
        expected = 0.8 * DEFAULT_WEIGHTS["fresh_wallet"]
        assert score == pytest.approx(expected)
        assert count == 1

    def test_size_anomaly_only(
        self,
        mock_redis: AsyncMock,
        sample_trade: TradeEvent,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        """Test score with only size anomaly signal."""
        scorer = RiskScorer(mock_redis)
        bundle = SignalBundle(
            trade_event=sample_trade,
            size_anomaly_signal=size_anomaly_signal,
        )

        score, count = scorer.calculate_weighted_score(bundle)

        # 0.7 confidence * 0.35 weight + 0.7 * 0.25 niche weight = 0.42
        expected = 0.7 * DEFAULT_WEIGHTS["size_anomaly"] + 0.7 * DEFAULT_WEIGHTS["niche_market"]
        assert score == pytest.approx(expected)
        assert count == 1

    def test_size_anomaly_non_niche(
        self,
        mock_redis: AsyncMock,
        sample_trade: TradeEvent,
        sample_metadata: MarketMetadata,
    ) -> None:
        """Test size anomaly without niche bonus."""
        signal = SizeAnomalySignal(
            trade_event=sample_trade,
            market_metadata=sample_metadata,
            volume_impact=0.10,
            book_impact=0.15,
            is_niche_market=False,
            confidence=0.7,
            factors={},
        )
        scorer = RiskScorer(mock_redis)
        bundle = SignalBundle(
            trade_event=sample_trade,
            size_anomaly_signal=signal,
        )

        score, count = scorer.calculate_weighted_score(bundle)

        # 0.7 * 0.35 = 0.245 (no niche bonus)
        expected = 0.7 * DEFAULT_WEIGHTS["size_anomaly"]
        assert score == pytest.approx(expected)

    def test_multi_signal_bonus_two_signals(
        self,
        mock_redis: AsyncMock,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        """Test 20% bonus for two signals."""
        scorer = RiskScorer(mock_redis)
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
            size_anomaly_signal=size_anomaly_signal,
        )

        score, count = scorer.calculate_weighted_score(bundle)

        # Calculate base score
        base = (
            0.8 * DEFAULT_WEIGHTS["fresh_wallet"]
            + 0.7 * DEFAULT_WEIGHTS["size_anomaly"]
            + 0.7 * DEFAULT_WEIGHTS["niche_market"]
        )
        expected = base * MULTI_SIGNAL_BONUS_2
        assert score == pytest.approx(expected)
        assert count == 2

    def test_score_capped_at_one(
        self,
        mock_redis: AsyncMock,
        sample_trade: TradeEvent,
        sample_wallet_profile: WalletProfile,
        sample_metadata: MarketMetadata,
    ) -> None:
        """Test score is capped at 1.0."""
        # Create high confidence signals
        fresh_signal = FreshWalletSignal(
            trade_event=sample_trade,
            wallet_profile=sample_wallet_profile,
            confidence=1.0,
            factors={},
        )
        size_signal = SizeAnomalySignal(
            trade_event=sample_trade,
            market_metadata=sample_metadata,
            volume_impact=0.10,
            book_impact=0.15,
            is_niche_market=True,
            confidence=1.0,
            factors={},
        )

        scorer = RiskScorer(mock_redis)
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_signal,
            size_anomaly_signal=size_signal,
        )

        score, count = scorer.calculate_weighted_score(bundle)

        assert score == 1.0  # Capped
        assert count == 2


# ============================================================================
# Assess Method Tests
# ============================================================================


class TestAssessMethod:
    """Tests for the assess method."""

    @pytest.mark.asyncio
    async def test_assess_triggers_alert(
        self,
        mock_redis: AsyncMock,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        """Test assess triggers alert for high-risk trades."""
        scorer = RiskScorer(mock_redis)
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
            size_anomaly_signal=size_anomaly_signal,
        )

        assessment = await scorer.assess(bundle)

        assert assessment.should_alert is True
        assert assessment.signals_triggered == 2
        assert assessment.weighted_score >= DEFAULT_ALERT_THRESHOLD

    @pytest.mark.asyncio
    async def test_assess_no_alert_below_threshold(
        self, mock_redis: AsyncMock, sample_trade: TradeEvent
    ) -> None:
        """Test assess does not alert for low-risk trades."""
        scorer = RiskScorer(mock_redis)
        bundle = SignalBundle(trade_event=sample_trade)

        assessment = await scorer.assess(bundle)

        assert assessment.should_alert is False
        assert assessment.signals_triggered == 0
        assert assessment.weighted_score == 0.0

    @pytest.mark.asyncio
    async def test_assess_deduplication(
        self,
        mock_redis: AsyncMock,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        """Test assess deduplicates repeated alerts."""
        # First call: key doesn't exist (returns True)
        # Second call: key exists (returns False/None)
        mock_redis.set.side_effect = [True, False]

        scorer = RiskScorer(mock_redis)
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
            size_anomaly_signal=size_anomaly_signal,
        )

        # First assessment should alert
        assessment1 = await scorer.assess(bundle)
        # Second assessment should be deduplicated
        assessment2 = await scorer.assess(bundle)

        assert assessment1.should_alert is True
        assert assessment2.should_alert is False

    @pytest.mark.asyncio
    async def test_assess_preserves_signals(
        self,
        mock_redis: AsyncMock,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
    ) -> None:
        """Test assess preserves original signals in assessment."""
        scorer = RiskScorer(mock_redis)
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
        )

        assessment = await scorer.assess(bundle)

        assert assessment.fresh_wallet_signal == fresh_wallet_signal
        assert assessment.size_anomaly_signal is None


# ============================================================================
# Deduplication Tests
# ============================================================================


class TestDeduplication:
    """Tests for deduplication functionality."""

    @pytest.mark.asyncio
    async def test_check_and_set_dedup_new_key(self, mock_redis: AsyncMock) -> None:
        """Test dedup returns False for new key."""
        mock_redis.set.return_value = True

        scorer = RiskScorer(mock_redis)
        is_dup = await scorer._check_and_set_dedup("0xwallet", "market123")

        assert is_dup is False
        mock_redis.set.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_and_set_dedup_existing_key(self, mock_redis: AsyncMock) -> None:
        """Test dedup returns True for existing key."""
        mock_redis.set.return_value = False  # Key exists, NX failed

        scorer = RiskScorer(mock_redis)
        is_dup = await scorer._check_and_set_dedup("0xwallet", "market123")

        assert is_dup is True

    @pytest.mark.asyncio
    async def test_clear_dedup(self, mock_redis: AsyncMock) -> None:
        """Test clearing dedup key."""
        mock_redis.delete.return_value = 1

        scorer = RiskScorer(mock_redis)
        cleared = await scorer.clear_dedup("0xwallet", "market123")

        assert cleared is True
        mock_redis.delete.assert_called_once()


# ============================================================================
# Batch Analysis Tests
# ============================================================================


class TestBatchAnalysis:
    """Tests for batch assessment."""

    @pytest.mark.asyncio
    async def test_assess_batch(
        self,
        mock_redis: AsyncMock,
        sample_wallet_profile: WalletProfile,
    ) -> None:
        """Test batch assessment returns assessments for all bundles."""
        scorer = RiskScorer(mock_redis)

        bundles = []
        for i in range(3):
            trade = TradeEvent(
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
            signal = FreshWalletSignal(
                trade_event=trade,
                wallet_profile=sample_wallet_profile,
                confidence=0.8,
                factors={},
            )
            bundles.append(SignalBundle(trade_event=trade, fresh_wallet_signal=signal))

        assessments = await scorer.assess_batch(bundles)

        assert len(assessments) == 3
        assert all(isinstance(a, RiskAssessment) for a in assessments)

    @pytest.mark.asyncio
    async def test_assess_batch_empty(self, mock_redis: AsyncMock) -> None:
        """Test batch assessment with empty list."""
        scorer = RiskScorer(mock_redis)

        assessments = await scorer.assess_batch([])

        assert assessments == []


# ============================================================================
# Weight Management Tests
# ============================================================================


class TestWeightManagement:
    """Tests for weight get/set functionality."""

    def test_get_weights(self, mock_redis: AsyncMock) -> None:
        """Test getting weights returns a copy."""
        scorer = RiskScorer(mock_redis)

        weights = scorer.get_weights()

        assert weights == DEFAULT_WEIGHTS
        # Verify it's a copy, not the original
        weights["fresh_wallet"] = 999
        assert scorer._weights["fresh_wallet"] != 999

    def test_set_weights(self, mock_redis: AsyncMock) -> None:
        """Test setting new weights."""
        scorer = RiskScorer(mock_redis)
        new_weights = {"fresh_wallet": 0.5, "size_anomaly": 0.5}

        scorer.set_weights(new_weights)

        assert scorer._weights == new_weights

    def test_set_weights_makes_copy(self, mock_redis: AsyncMock) -> None:
        """Test set_weights makes a copy of the input."""
        scorer = RiskScorer(mock_redis)
        new_weights = {"fresh_wallet": 0.5, "size_anomaly": 0.5}

        scorer.set_weights(new_weights)
        new_weights["fresh_wallet"] = 999

        assert scorer._weights["fresh_wallet"] == 0.5
