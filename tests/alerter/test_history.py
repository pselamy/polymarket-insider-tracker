"""Tests for alert history and deduplication."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from polymarket_insider_tracker.alerter.history import (
    AlertHistory,
    AlertRecord,
    _generate_dedup_key,
    _get_signals_from_assessment,
)
from polymarket_insider_tracker.detector.models import (
    FreshWalletSignal,
    RiskAssessment,
    SizeAnomalySignal,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile

# ============================================================================
# Fixtures
# ============================================================================


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
        matic_balance=Decimal("1000000000000000000"),
        usdc_balance=Decimal("1000000"),
    )


@pytest.fixture
def sample_metadata() -> MarketMetadata:
    """Create sample market metadata."""
    return MarketMetadata(
        condition_id="market_abc123",
        question="Test market?",
        description="Test",
        tokens=(Token(token_id="token_123", outcome="Yes", price=Decimal("0.65")),),
        category="other",
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
        factors={},
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
        factors={},
    )


@pytest.fixture
def high_risk_assessment(
    sample_trade: TradeEvent,
    fresh_wallet_signal: FreshWalletSignal,
    size_anomaly_signal: SizeAnomalySignal,
) -> RiskAssessment:
    """Create a high-risk assessment."""
    return RiskAssessment(
        trade_event=sample_trade,
        wallet_address=sample_trade.wallet_address,
        market_id=sample_trade.market_id,
        fresh_wallet_signal=fresh_wallet_signal,
        size_anomaly_signal=size_anomaly_signal,
        signals_triggered=2,
        weighted_score=0.82,
        should_alert=True,
    )


@pytest.fixture
def mock_redis() -> MagicMock:
    """Create a mock Redis client."""
    redis = MagicMock()

    # Make async methods return AsyncMock
    redis.exists = AsyncMock(return_value=0)  # Key doesn't exist (not duplicate)
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock(return_value=True)
    redis.ttl = AsyncMock(return_value=3600)
    redis.zadd = AsyncMock(return_value=1)
    redis.expire = AsyncMock(return_value=True)
    redis.zrangebyscore = AsyncMock(return_value=[])
    redis.zcount = AsyncMock(return_value=0)
    redis.zremrangebyscore = AsyncMock(return_value=0)

    # Mock pipeline - async context manager
    pipeline = MagicMock()
    pipeline.__aenter__ = AsyncMock(return_value=pipeline)
    pipeline.__aexit__ = AsyncMock(return_value=None)
    pipeline.set.return_value = pipeline
    pipeline.zadd.return_value = pipeline
    pipeline.expire.return_value = pipeline
    pipeline.execute = AsyncMock(return_value=[True, True, True, True, True, True])
    redis.pipeline.return_value = pipeline

    return redis


# ============================================================================
# AlertRecord Tests
# ============================================================================


class TestAlertRecord:
    """Tests for AlertRecord dataclass."""

    def test_to_dict(self) -> None:
        """Test serialization to dict."""
        now = datetime.now(UTC)
        record = AlertRecord(
            alert_id="test-123",
            wallet_address="0x1234",
            market_id="market_abc",
            risk_score=0.75,
            signals_triggered=["fresh_wallet"],
            channels_attempted=["discord", "telegram"],
            channels_succeeded=["discord"],
            dedup_key="0x1234:market_abc:2026010416",
            feedback_useful=True,
            created_at=now,
        )

        data = record.to_dict()

        assert data["alert_id"] == "test-123"
        assert data["risk_score"] == 0.75
        assert data["feedback_useful"] is True
        assert data["created_at"] == now.isoformat()

    def test_from_dict(self) -> None:
        """Test deserialization from dict."""
        data = {
            "alert_id": "test-456",
            "wallet_address": "0x5678",
            "market_id": "market_xyz",
            "risk_score": 0.82,
            "signals_triggered": ["size_anomaly"],
            "channels_attempted": ["discord"],
            "channels_succeeded": ["discord"],
            "dedup_key": "0x5678:market_xyz:2026010416",
            "feedback_useful": None,
            "created_at": "2026-01-04T16:00:00+00:00",
        }

        record = AlertRecord.from_dict(data)

        assert record.alert_id == "test-456"
        assert record.risk_score == 0.82
        assert record.feedback_useful is None

    def test_from_dict_missing_optional(self) -> None:
        """Test deserialization with missing optional fields."""
        data = {
            "alert_id": "test-789",
            "wallet_address": "0x9999",
            "market_id": "market_aaa",
            "risk_score": "0.5",  # Test string conversion
            "dedup_key": "key",
        }

        record = AlertRecord.from_dict(data)

        assert record.signals_triggered == []
        assert record.channels_attempted == []
        assert record.feedback_useful is None


# ============================================================================
# Helper Function Tests
# ============================================================================


class TestGenerateDedupKey:
    """Tests for dedup key generation."""

    def test_basic_key(self) -> None:
        """Test basic dedup key generation."""
        hour = datetime(2026, 1, 4, 16, 30, 0, tzinfo=UTC)
        key = _generate_dedup_key("0x1234", "market_abc", hour)
        assert key == "0x1234:market_abc:2026010416"

    def test_different_hours(self) -> None:
        """Test that different hours produce different keys."""
        hour1 = datetime(2026, 1, 4, 16, 0, 0, tzinfo=UTC)
        hour2 = datetime(2026, 1, 4, 17, 0, 0, tzinfo=UTC)

        key1 = _generate_dedup_key("0x1234", "market_abc", hour1)
        key2 = _generate_dedup_key("0x1234", "market_abc", hour2)

        assert key1 != key2


class TestGetSignalsFromAssessment:
    """Tests for signal extraction."""

    def test_no_signals(self, sample_trade: TradeEvent) -> None:
        """Test extraction with no signals."""
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

        signals = _get_signals_from_assessment(assessment)
        assert signals == []

    def test_fresh_wallet_only(
        self,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
    ) -> None:
        """Test extraction with fresh wallet signal."""
        assessment = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=fresh_wallet_signal,
            size_anomaly_signal=None,
            signals_triggered=1,
            weighted_score=0.5,
            should_alert=True,
        )

        signals = _get_signals_from_assessment(assessment)
        assert "fresh_wallet" in signals
        assert "size_anomaly" not in signals

    def test_all_signals(self, high_risk_assessment: RiskAssessment) -> None:
        """Test extraction with all signals."""
        signals = _get_signals_from_assessment(high_risk_assessment)
        assert "fresh_wallet" in signals
        assert "size_anomaly" in signals
        assert "niche_market" in signals


# ============================================================================
# AlertHistory Tests
# ============================================================================


class TestAlertHistoryInit:
    """Tests for AlertHistory initialization."""

    def test_default_settings(self, mock_redis: AsyncMock) -> None:
        """Test default configuration."""
        history = AlertHistory(mock_redis)

        assert history.dedup_window_hours == 1
        assert history.retention_days == 30
        assert history._dedup_ttl == 3600
        assert history._retention_ttl == 30 * 86400

    def test_custom_settings(self, mock_redis: AsyncMock) -> None:
        """Test custom configuration."""
        history = AlertHistory(
            mock_redis,
            dedup_window_hours=2,
            retention_days=7,
        )

        assert history.dedup_window_hours == 2
        assert history._dedup_ttl == 7200


class TestShouldSend:
    """Tests for should_send method."""

    @pytest.mark.asyncio
    async def test_not_duplicate(
        self,
        mock_redis: AsyncMock,
        high_risk_assessment: RiskAssessment,
    ) -> None:
        """Test that non-duplicate returns True."""
        mock_redis.exists.return_value = 0
        history = AlertHistory(mock_redis)

        result = await history.should_send(high_risk_assessment)

        assert result is True
        mock_redis.exists.assert_called_once()

    @pytest.mark.asyncio
    async def test_is_duplicate(
        self,
        mock_redis: AsyncMock,
        high_risk_assessment: RiskAssessment,
    ) -> None:
        """Test that duplicate returns False."""
        mock_redis.exists.return_value = 1
        history = AlertHistory(mock_redis)

        result = await history.should_send(high_risk_assessment)

        assert result is False


class TestRecordSent:
    """Tests for record_sent method."""

    @pytest.mark.asyncio
    async def test_record_success(
        self,
        mock_redis: AsyncMock,
        high_risk_assessment: RiskAssessment,
    ) -> None:
        """Test recording a sent alert."""
        history = AlertHistory(mock_redis)

        alert_id = await history.record_sent(
            high_risk_assessment,
            channels_attempted=["discord", "telegram"],
            channels_succeeded={"discord": True, "telegram": False},
        )

        assert alert_id is not None
        assert len(alert_id) == 36  # UUID length

        # Verify pipeline was used
        mock_redis.pipeline.assert_called_once()


class TestRecordFeedback:
    """Tests for record_feedback method."""

    @pytest.mark.asyncio
    async def test_feedback_success(self, mock_redis: AsyncMock) -> None:
        """Test recording feedback for existing alert."""
        existing_record = {
            "alert_id": "test-123",
            "wallet_address": "0x1234",
            "market_id": "market_abc",
            "risk_score": 0.75,
            "signals_triggered": [],
            "channels_attempted": [],
            "channels_succeeded": [],
            "dedup_key": "key",
            "feedback_useful": None,
        }
        mock_redis.get.return_value = json.dumps(existing_record)
        mock_redis.ttl.return_value = 3600

        history = AlertHistory(mock_redis)
        result = await history.record_feedback("test-123", useful=True)

        assert result is True
        mock_redis.set.assert_called()

    @pytest.mark.asyncio
    async def test_feedback_not_found(self, mock_redis: AsyncMock) -> None:
        """Test feedback for non-existent alert."""
        mock_redis.get.return_value = None

        history = AlertHistory(mock_redis)
        result = await history.record_feedback("nonexistent", useful=True)

        assert result is False


class TestGetAlert:
    """Tests for get_alert method."""

    @pytest.mark.asyncio
    async def test_get_existing(self, mock_redis: AsyncMock) -> None:
        """Test getting existing alert."""
        existing_record = {
            "alert_id": "test-123",
            "wallet_address": "0x1234",
            "market_id": "market_abc",
            "risk_score": 0.75,
            "signals_triggered": ["fresh_wallet"],
            "channels_attempted": ["discord"],
            "channels_succeeded": ["discord"],
            "dedup_key": "key",
            "created_at": "2026-01-04T16:00:00+00:00",
        }
        mock_redis.get.return_value = json.dumps(existing_record)

        history = AlertHistory(mock_redis)
        record = await history.get_alert("test-123")

        assert record is not None
        assert record.alert_id == "test-123"
        assert record.risk_score == 0.75

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, mock_redis: AsyncMock) -> None:
        """Test getting non-existent alert."""
        mock_redis.get.return_value = None

        history = AlertHistory(mock_redis)
        record = await history.get_alert("nonexistent")

        assert record is None


class TestGetAlerts:
    """Tests for get_alerts query method."""

    @pytest.mark.asyncio
    async def test_empty_results(self, mock_redis: AsyncMock) -> None:
        """Test query with no results."""
        mock_redis.zrangebyscore.return_value = []

        history = AlertHistory(mock_redis)
        results = await history.get_alerts(
            start=datetime.now(UTC) - timedelta(hours=24),
            end=datetime.now(UTC),
        )

        assert results == []

    @pytest.mark.asyncio
    async def test_with_wallet_filter(self, mock_redis: AsyncMock) -> None:
        """Test query with wallet filter uses correct index."""
        mock_redis.zrangebyscore.return_value = []

        history = AlertHistory(mock_redis)
        await history.get_alerts(
            start=datetime.now(UTC) - timedelta(hours=24),
            end=datetime.now(UTC),
            wallet="0x1234",
        )

        # Verify correct index was used
        call_args = mock_redis.zrangebyscore.call_args
        assert "wallet:0x1234" in call_args[0][0]


class TestGetRecentCount:
    """Tests for get_recent_count method."""

    @pytest.mark.asyncio
    async def test_count_all(self, mock_redis: AsyncMock) -> None:
        """Test counting all recent alerts."""
        mock_redis.zcount.return_value = 42

        history = AlertHistory(mock_redis)
        count = await history.get_recent_count(hours=24)

        assert count == 42

    @pytest.mark.asyncio
    async def test_count_by_wallet(self, mock_redis: AsyncMock) -> None:
        """Test counting alerts for specific wallet."""
        mock_redis.zcount.return_value = 5

        history = AlertHistory(mock_redis)
        count = await history.get_recent_count(hours=24, wallet="0x1234")

        assert count == 5


class TestCleanupOldAlerts:
    """Tests for cleanup_old_alerts method."""

    @pytest.mark.asyncio
    async def test_cleanup_empty(self, mock_redis: AsyncMock) -> None:
        """Test cleanup with no old alerts."""
        mock_redis.zrangebyscore.return_value = []

        history = AlertHistory(mock_redis)
        removed = await history.cleanup_old_alerts()

        assert removed == 0

    @pytest.mark.asyncio
    async def test_cleanup_removes_old(self, mock_redis: AsyncMock) -> None:
        """Test cleanup removes old alerts."""
        mock_redis.zrangebyscore.return_value = [b"alert-1", b"alert-2"]
        mock_redis.zremrangebyscore.return_value = 2

        history = AlertHistory(mock_redis)
        removed = await history.cleanup_old_alerts()

        assert removed == 2
        mock_redis.zremrangebyscore.assert_called_once()
