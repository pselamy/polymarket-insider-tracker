"""Tests for the market metadata synchronizer."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from polymarket_insider_tracker.ingestor.clob_client import ClobClient
from polymarket_insider_tracker.ingestor.metadata_sync import (
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_REDIS_KEY_PREFIX,
    DEFAULT_SYNC_INTERVAL_SECONDS,
    MarketMetadataSync,
    MetadataSyncError,
    SyncState,
    SyncStats,
)
from polymarket_insider_tracker.ingestor.models import (
    Market,
    MarketMetadata,
    Token,
    derive_category,
)


# Test fixtures
@pytest.fixture
def sample_token() -> Token:
    """Create a sample token."""
    return Token(token_id="token123", outcome="Yes", price=Decimal("0.65"))


@pytest.fixture
def sample_market(sample_token: Token) -> Market:
    """Create a sample market."""
    return Market(
        condition_id="cond123",
        question="Will Bitcoin exceed $100k in 2026?",
        description="Market on BTC price",
        tokens=(sample_token,),
        end_date=datetime(2026, 12, 31, tzinfo=UTC),
        active=True,
        closed=False,
    )


@pytest.fixture
def sample_metadata(sample_market: Market) -> MarketMetadata:
    """Create sample metadata from market."""
    return MarketMetadata.from_market(sample_market)


@pytest.fixture
def mock_redis() -> AsyncMock:
    """Create a mock Redis client."""
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.setex = AsyncMock()
    redis.delete = AsyncMock(return_value=1)
    redis.scan = AsyncMock(return_value=(0, []))
    return redis


@pytest.fixture
def mock_clob(sample_market: Market) -> MagicMock:
    """Create a mock CLOB client."""
    clob = MagicMock(spec=ClobClient)
    clob.get_markets = MagicMock(return_value=[sample_market])
    clob.get_market = MagicMock(return_value=sample_market)
    return clob


class TestDeriveCategory:
    """Tests for the derive_category function."""

    def test_politics_keywords(self) -> None:
        """Test political category detection."""
        assert derive_category("Will Trump win the 2024 election?") == "politics"
        assert derive_category("Who will be the next president?") == "politics"
        assert derive_category("Senate majority party after midterms?") == "politics"

    def test_crypto_keywords(self) -> None:
        """Test crypto category detection."""
        assert derive_category("Will Bitcoin hit $100k?") == "crypto"
        assert derive_category("Ethereum price by end of year?") == "crypto"
        assert derive_category("Next altcoin to moon?") == "crypto"

    def test_sports_keywords(self) -> None:
        """Test sports category detection."""
        assert derive_category("Who will win the Super Bowl?") == "sports"
        assert derive_category("NBA Finals champion?") == "sports"
        assert derive_category("Next UFC heavyweight champion?") == "sports"

    def test_entertainment_keywords(self) -> None:
        """Test entertainment category detection."""
        assert derive_category("Best Picture Oscar winner?") == "entertainment"
        assert derive_category("Next Grammy Album of the Year?") == "entertainment"
        assert derive_category("Highest box office movie this summer?") == "entertainment"

    def test_finance_keywords(self) -> None:
        """Test finance category detection."""
        assert derive_category("Fed interest rate decision?") == "finance"
        assert derive_category("Will we enter a recession?") == "finance"
        assert derive_category("S&P 500 by year end?") == "finance"

    def test_tech_keywords(self) -> None:
        """Test tech category detection."""
        assert derive_category("Will Apple release a new iPhone?") == "tech"
        assert derive_category("Next major AI breakthrough?") == "tech"
        assert derive_category("Tesla vehicle deliveries?") == "tech"

    def test_science_keywords(self) -> None:
        """Test science category detection."""
        assert derive_category("NASA Mars mission timeline?") == "science"
        assert derive_category("FDA approval for new drug?") == "science"
        assert derive_category("Climate change targets met?") == "science"

    def test_other_category(self) -> None:
        """Test fallback to 'other' category."""
        assert derive_category("Random obscure question?") == "other"
        assert derive_category("Will it be sunny tomorrow?") == "other"

    def test_case_insensitive(self) -> None:
        """Test case insensitivity."""
        assert derive_category("BITCOIN PRICE") == "crypto"
        assert derive_category("bitcoin price") == "crypto"
        assert derive_category("Bitcoin Price") == "crypto"


class TestMarketMetadata:
    """Tests for the MarketMetadata dataclass."""

    def test_from_market(self, sample_market: Market) -> None:
        """Test creating metadata from a market."""
        metadata = MarketMetadata.from_market(sample_market)

        assert metadata.condition_id == sample_market.condition_id
        assert metadata.question == sample_market.question
        assert metadata.description == sample_market.description
        assert metadata.tokens == sample_market.tokens
        assert metadata.end_date == sample_market.end_date
        assert metadata.active == sample_market.active
        assert metadata.closed == sample_market.closed
        assert metadata.category == "crypto"  # "Bitcoin" in question
        assert metadata.last_updated is not None

    def test_to_dict(self, sample_metadata: MarketMetadata) -> None:
        """Test serialization to dict."""
        data = sample_metadata.to_dict()

        assert data["condition_id"] == sample_metadata.condition_id
        assert data["question"] == sample_metadata.question
        assert data["category"] == "crypto"
        assert len(data["tokens"]) == 1
        assert data["tokens"][0]["token_id"] == "token123"

    def test_from_dict(self, sample_metadata: MarketMetadata) -> None:
        """Test deserialization from dict."""
        data = sample_metadata.to_dict()
        restored = MarketMetadata.from_dict(data)

        assert restored.condition_id == sample_metadata.condition_id
        assert restored.question == sample_metadata.question
        assert restored.category == sample_metadata.category
        assert len(restored.tokens) == 1

    def test_roundtrip(self, sample_metadata: MarketMetadata) -> None:
        """Test serialization roundtrip."""
        data = sample_metadata.to_dict()
        json_str = json.dumps(data)
        parsed = json.loads(json_str)
        restored = MarketMetadata.from_dict(parsed)

        assert restored.condition_id == sample_metadata.condition_id
        assert restored.question == sample_metadata.question


class TestSyncStats:
    """Tests for the SyncStats dataclass."""

    def test_defaults(self) -> None:
        """Test default values."""
        stats = SyncStats()

        assert stats.total_syncs == 0
        assert stats.successful_syncs == 0
        assert stats.failed_syncs == 0
        assert stats.markets_cached == 0
        assert stats.last_sync_time is None
        assert stats.last_error is None


class TestMarketMetadataSync:
    """Tests for the MarketMetadataSync class."""

    def test_init(self, mock_redis: AsyncMock, mock_clob: MagicMock) -> None:
        """Test initialization."""
        sync = MarketMetadataSync(redis=mock_redis, clob_client=mock_clob)

        assert sync.state == SyncState.STOPPED
        assert sync.stats.total_syncs == 0
        assert sync._sync_interval == DEFAULT_SYNC_INTERVAL_SECONDS
        assert sync._cache_ttl == DEFAULT_CACHE_TTL_SECONDS
        assert sync._key_prefix == DEFAULT_REDIS_KEY_PREFIX

    def test_init_custom_config(self, mock_redis: AsyncMock, mock_clob: MagicMock) -> None:
        """Test initialization with custom config."""
        sync = MarketMetadataSync(
            redis=mock_redis,
            clob_client=mock_clob,
            sync_interval_seconds=60,
            cache_ttl_seconds=120,
            key_prefix="custom:",
        )

        assert sync._sync_interval == 60
        assert sync._cache_ttl == 120
        assert sync._key_prefix == "custom:"

    @pytest.mark.asyncio
    async def test_start_stop(self, mock_redis: AsyncMock, mock_clob: MagicMock) -> None:
        """Test starting and stopping the sync service."""
        sync = MarketMetadataSync(redis=mock_redis, clob_client=mock_clob)

        # Start
        await sync.start()
        assert sync.state == SyncState.IDLE
        assert sync.stats.total_syncs == 1
        assert sync.stats.successful_syncs == 1

        # Stop
        await sync.stop()
        assert sync.state == SyncState.STOPPED

    @pytest.mark.asyncio
    async def test_start_performs_initial_sync(
        self, mock_redis: AsyncMock, mock_clob: MagicMock
    ) -> None:
        """Test that start performs an initial sync."""
        sync = MarketMetadataSync(redis=mock_redis, clob_client=mock_clob)

        await sync.start()

        # Should have called get_markets
        mock_clob.get_markets.assert_called_once_with(True)

        # Should have cached the market
        mock_redis.setex.assert_called()

        await sync.stop()

    @pytest.mark.asyncio
    async def test_start_failure(self, mock_redis: AsyncMock, mock_clob: MagicMock) -> None:
        """Test start failure handling."""
        mock_clob.get_markets.side_effect = Exception("API error")
        sync = MarketMetadataSync(redis=mock_redis, clob_client=mock_clob)

        with pytest.raises(MetadataSyncError, match="initial sync failed"):
            await sync.start()

        assert sync.state == SyncState.ERROR
        assert sync.stats.last_error == "API error"

    @pytest.mark.asyncio
    async def test_get_market_cache_hit(
        self,
        mock_redis: AsyncMock,
        mock_clob: MagicMock,
        sample_metadata: MarketMetadata,
    ) -> None:
        """Test get_market with cache hit."""
        # Setup cache hit
        cached_data = json.dumps(sample_metadata.to_dict())
        mock_redis.get = AsyncMock(return_value=cached_data)

        sync = MarketMetadataSync(redis=mock_redis, clob_client=mock_clob)
        await sync.start()

        result = await sync.get_market("cond123")

        assert result is not None
        assert result.condition_id == "cond123"
        # Should not have called API
        mock_clob.get_market.assert_not_called()

        await sync.stop()

    @pytest.mark.asyncio
    async def test_get_market_cache_miss(self, mock_redis: AsyncMock, mock_clob: MagicMock) -> None:
        """Test get_market with cache miss."""
        # Setup cache miss
        mock_redis.get = AsyncMock(return_value=None)

        sync = MarketMetadataSync(redis=mock_redis, clob_client=mock_clob)
        await sync.start()

        result = await sync.get_market("cond123")

        assert result is not None
        assert result.condition_id == "cond123"
        # Should have called API
        mock_clob.get_market.assert_called_with("cond123")
        # Should have cached the result
        assert mock_redis.setex.call_count >= 2  # Initial sync + cache miss

        await sync.stop()

    @pytest.mark.asyncio
    async def test_get_market_not_found(self, mock_redis: AsyncMock, mock_clob: MagicMock) -> None:
        """Test get_market when market doesn't exist."""
        mock_redis.get = AsyncMock(return_value=None)
        mock_clob.get_market.return_value = None

        sync = MarketMetadataSync(redis=mock_redis, clob_client=mock_clob)
        await sync.start()

        result = await sync.get_market("nonexistent")

        assert result is None

        await sync.stop()

    @pytest.mark.asyncio
    async def test_invalidate_market(self, mock_redis: AsyncMock, mock_clob: MagicMock) -> None:
        """Test cache invalidation."""
        sync = MarketMetadataSync(redis=mock_redis, clob_client=mock_clob)
        await sync.start()

        result = await sync.invalidate_market("cond123")

        assert result is True
        mock_redis.delete.assert_called_with(f"{DEFAULT_REDIS_KEY_PREFIX}cond123")

        await sync.stop()

    @pytest.mark.asyncio
    async def test_force_sync(self, mock_redis: AsyncMock, mock_clob: MagicMock) -> None:
        """Test forced sync."""
        sync = MarketMetadataSync(redis=mock_redis, clob_client=mock_clob)
        await sync.start()

        # Initial sync
        assert sync.stats.total_syncs == 1

        # Force sync
        await sync.force_sync()

        assert sync.stats.total_syncs == 2
        assert sync.stats.successful_syncs == 2

        await sync.stop()

    @pytest.mark.asyncio
    async def test_state_change_callback(self, mock_redis: AsyncMock, mock_clob: MagicMock) -> None:
        """Test state change callback."""
        states: list[SyncState] = []

        def on_state_change(state: SyncState) -> None:
            states.append(state)

        sync = MarketMetadataSync(
            redis=mock_redis,
            clob_client=mock_clob,
            on_state_change=on_state_change,
        )

        await sync.start()
        await sync.stop()

        assert SyncState.STARTING in states
        assert SyncState.SYNCING in states
        assert SyncState.IDLE in states
        assert SyncState.STOPPING in states
        assert SyncState.STOPPED in states

    @pytest.mark.asyncio
    async def test_sync_complete_callback(
        self, mock_redis: AsyncMock, mock_clob: MagicMock
    ) -> None:
        """Test sync complete callback."""
        sync_stats: list[SyncStats] = []

        def on_sync_complete(stats: SyncStats) -> None:
            sync_stats.append(stats)

        sync = MarketMetadataSync(
            redis=mock_redis,
            clob_client=mock_clob,
            on_sync_complete=on_sync_complete,
        )

        await sync.start()

        assert len(sync_stats) == 1
        assert sync_stats[0].successful_syncs == 1
        assert sync_stats[0].markets_cached == 1

        await sync.stop()

    @pytest.mark.asyncio
    async def test_get_markets_by_category(
        self, mock_redis: AsyncMock, mock_clob: MagicMock, sample_metadata: MarketMetadata
    ) -> None:
        """Test getting markets by category."""
        # Setup scan to return keys
        key = f"{DEFAULT_REDIS_KEY_PREFIX}cond123"
        mock_redis.scan = AsyncMock(return_value=(0, [key]))

        # Setup get to return cached data
        cached_data = json.dumps(sample_metadata.to_dict())
        mock_redis.get = AsyncMock(return_value=cached_data)

        sync = MarketMetadataSync(redis=mock_redis, clob_client=mock_clob)
        # Don't start to avoid initial sync complexity
        sync._state = SyncState.IDLE

        results = await sync.get_markets_by_category("crypto")

        assert len(results) == 1
        assert results[0].category == "crypto"

    @pytest.mark.asyncio
    async def test_cannot_start_twice(self, mock_redis: AsyncMock, mock_clob: MagicMock) -> None:
        """Test that starting twice doesn't double-start."""
        sync = MarketMetadataSync(redis=mock_redis, clob_client=mock_clob)

        await sync.start()
        await sync.start()  # Should be a no-op

        assert sync.stats.total_syncs == 1  # Only one initial sync

        await sync.stop()

    @pytest.mark.asyncio
    async def test_stop_when_stopped(self, mock_redis: AsyncMock, mock_clob: MagicMock) -> None:
        """Test stopping when already stopped."""
        sync = MarketMetadataSync(redis=mock_redis, clob_client=mock_clob)

        await sync.stop()  # Should be a no-op

        assert sync.state == SyncState.STOPPED
