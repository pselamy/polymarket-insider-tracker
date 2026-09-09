"""Tests for the market metadata synchronizer."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import pytest

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
from tests.fakes.redis import FakeRedis


class FakeClobClient:
    """Fake CLOB client providing market lookup."""

    def __init__(
        self, markets: list[Market] | None = None, raise_error: Exception | None = None
    ) -> None:
        self._markets = markets or []
        self._raise_error = raise_error

    def get_markets(self, only_active: bool = True) -> list[Market]:
        _ = only_active
        if self._raise_error is not None:
            raise self._raise_error
        return list(self._markets)

    def get_market(self, condition_id: str) -> Market | None:
        if self._raise_error is not None:
            raise self._raise_error
        return next((m for m in self._markets if m.condition_id == condition_id), None)


class FakeGammaClient:
    """Fake Gamma client providing market volume stats."""

    def __init__(
        self,
        stats: dict[str, Any] | None = None,
        raise_error: Exception | None = None,
    ) -> None:
        self._stats = stats or {}
        self._raise_error = raise_error

    async def get_active_market_stats(self) -> dict[str, Any]:
        if self._raise_error is not None:
            raise self._raise_error
        return dict(self._stats)


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
def fake_redis() -> FakeRedis:
    """Create a working fake Redis client."""
    return FakeRedis()


@pytest.fixture
def fake_clob(sample_market: Market) -> FakeClobClient:
    """Create a fake CLOB client."""
    return FakeClobClient([sample_market])


@pytest.fixture
def fake_gamma() -> FakeGammaClient:
    """Create a fake GammaClient that returns empty volume stats."""
    return FakeGammaClient()


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

    def test_init(
        self, fake_redis: FakeRedis, fake_clob: FakeClobClient, fake_gamma: FakeGammaClient
    ) -> None:
        """Test initialization."""
        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, fake_clob),
            gamma_client=cast(Any, fake_gamma),
        )

        assert sync.state == SyncState.STOPPED
        assert sync.stats.total_syncs == 0
        assert sync._sync_interval == DEFAULT_SYNC_INTERVAL_SECONDS
        assert sync._cache_ttl == DEFAULT_CACHE_TTL_SECONDS
        assert sync._key_prefix == DEFAULT_REDIS_KEY_PREFIX

    def test_init_custom_config(
        self, fake_redis: FakeRedis, fake_clob: FakeClobClient, fake_gamma: FakeGammaClient
    ) -> None:
        """Test initialization with custom config."""
        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, fake_clob),
            gamma_client=cast(Any, fake_gamma),
            sync_interval_seconds=60,
            cache_ttl_seconds=120,
            key_prefix="custom:",
        )

        assert sync._sync_interval == 60
        assert sync._cache_ttl == 120
        assert sync._key_prefix == "custom:"

    @pytest.mark.asyncio
    async def test_start_stop(
        self, fake_redis: FakeRedis, fake_clob: FakeClobClient, fake_gamma: FakeGammaClient
    ) -> None:
        """Test starting and stopping the sync service."""
        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, fake_clob),
            gamma_client=cast(Any, fake_gamma),
        )

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
        self, fake_redis: FakeRedis, fake_clob: FakeClobClient, fake_gamma: FakeGammaClient
    ) -> None:
        """Test that start performs an initial sync."""
        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, fake_clob),
            gamma_client=cast(Any, fake_gamma),
        )

        await sync.start()

        # Should have cached the market
        cached = await fake_redis.get(f"{DEFAULT_REDIS_KEY_PREFIX}cond123")
        assert cached is not None

        await sync.stop()

    @pytest.mark.asyncio
    async def test_start_failure(self, fake_redis: FakeRedis, fake_gamma: FakeGammaClient) -> None:
        """Test start failure handling."""
        failing_clob = FakeClobClient(raise_error=Exception("API error"))
        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, failing_clob),
            gamma_client=cast(Any, fake_gamma),
        )

        with pytest.raises(MetadataSyncError, match="initial sync failed"):
            await sync.start()

        assert sync.state == SyncState.ERROR
        assert sync.stats.last_error == "API error"

    @pytest.mark.asyncio
    async def test_get_market_cache_hit(
        self,
        fake_redis: FakeRedis,
        fake_gamma: FakeGammaClient,
        sample_metadata: MarketMetadata,
    ) -> None:
        """Test get_market with cache hit."""
        # Setup cache hit
        cached_data = json.dumps(sample_metadata.to_dict())
        await fake_redis.set(f"{DEFAULT_REDIS_KEY_PREFIX}cond123", cached_data)

        empty_clob = FakeClobClient([])
        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, empty_clob),
            gamma_client=cast(Any, fake_gamma),
        )
        await sync.start()

        result = await sync.get_market("cond123")

        assert result is not None
        assert result.condition_id == "cond123"

        await sync.stop()

    @pytest.mark.asyncio
    async def test_get_market_cache_miss(
        self,
        fake_redis: FakeRedis,
        sample_market: Market,
        fake_gamma: FakeGammaClient,
    ) -> None:
        """Test get_market with cache miss."""
        fake_clob = FakeClobClient([sample_market])
        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, fake_clob),
            gamma_client=cast(Any, fake_gamma),
        )
        # Clear cache first to ensure cache miss
        await fake_redis.delete(f"{DEFAULT_REDIS_KEY_PREFIX}cond123")

        result = await sync.get_market("cond123")

        assert result is not None
        assert result.condition_id == "cond123"
        # Should have cached the result
        cached = await fake_redis.get(f"{DEFAULT_REDIS_KEY_PREFIX}cond123")
        assert cached is not None

        await sync.stop()

    @pytest.mark.asyncio
    async def test_get_market_not_found(
        self, fake_redis: FakeRedis, fake_gamma: FakeGammaClient
    ) -> None:
        """Test get_market when market doesn't exist."""
        empty_clob = FakeClobClient([])
        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, empty_clob),
            gamma_client=cast(Any, fake_gamma),
        )
        await sync.start()

        result = await sync.get_market("nonexistent")

        assert result is None

        await sync.stop()

    @pytest.mark.asyncio
    async def test_invalidate_market(
        self, fake_redis: FakeRedis, fake_clob: FakeClobClient, fake_gamma: FakeGammaClient
    ) -> None:
        """Test cache invalidation."""
        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, fake_clob),
            gamma_client=cast(Any, fake_gamma),
        )
        await sync.start()

        result = await sync.invalidate_market("cond123")

        assert result is True
        cached = await fake_redis.get(f"{DEFAULT_REDIS_KEY_PREFIX}cond123")
        assert cached is None

        await sync.stop()

    @pytest.mark.asyncio
    async def test_force_sync(
        self, fake_redis: FakeRedis, fake_clob: FakeClobClient, fake_gamma: FakeGammaClient
    ) -> None:
        """Test forced sync."""
        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, fake_clob),
            gamma_client=cast(Any, fake_gamma),
        )
        await sync.start()

        # Initial sync
        assert sync.stats.total_syncs == 1

        # Force sync
        await sync.force_sync()

        assert sync.stats.total_syncs == 2
        assert sync.stats.successful_syncs == 2

        await sync.stop()

    @pytest.mark.asyncio
    async def test_state_change_callback(
        self, fake_redis: FakeRedis, fake_clob: FakeClobClient, fake_gamma: FakeGammaClient
    ) -> None:
        """Test state change callback."""
        states: list[SyncState] = []

        def on_state_change(state: SyncState) -> None:
            states.append(state)

        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, fake_clob),
            gamma_client=cast(Any, fake_gamma),
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
        self, fake_redis: FakeRedis, fake_clob: FakeClobClient, fake_gamma: FakeGammaClient
    ) -> None:
        """Test sync complete callback."""
        sync_stats: list[SyncStats] = []

        def on_sync_complete(stats: SyncStats) -> None:
            sync_stats.append(stats)

        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, fake_clob),
            gamma_client=cast(Any, fake_gamma),
            on_sync_complete=on_sync_complete,
        )

        await sync.start()

        assert len(sync_stats) == 1
        assert sync_stats[0].successful_syncs == 1
        assert sync_stats[0].markets_cached == 1

        await sync.stop()

    @pytest.mark.asyncio
    async def test_get_markets_by_category(
        self,
        fake_redis: FakeRedis,
        fake_clob: FakeClobClient,
        fake_gamma: FakeGammaClient,
        sample_metadata: MarketMetadata,
    ) -> None:
        """Test getting markets by category."""
        key = f"{DEFAULT_REDIS_KEY_PREFIX}cond123"
        await fake_redis.set(key, json.dumps(sample_metadata.to_dict()))

        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, fake_clob),
            gamma_client=cast(Any, fake_gamma),
        )
        sync._state = SyncState.IDLE

        results = await sync.get_markets_by_category("crypto")

        assert len(results) == 1
        assert results[0].category == "crypto"

    @pytest.mark.asyncio
    async def test_cannot_start_twice(
        self, fake_redis: FakeRedis, fake_clob: FakeClobClient, fake_gamma: FakeGammaClient
    ) -> None:
        """Test that starting twice doesn't double-start."""
        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, fake_clob),
            gamma_client=cast(Any, fake_gamma),
        )

        await sync.start()
        await sync.start()  # Should be a no-op

        assert sync.stats.total_syncs == 1  # Only one initial sync

        await sync.stop()

    @pytest.mark.asyncio
    async def test_stop_when_stopped(
        self, fake_redis: FakeRedis, fake_clob: FakeClobClient, fake_gamma: FakeGammaClient
    ) -> None:
        """Test stopping when already stopped."""
        sync = MarketMetadataSync(
            redis=cast(Any, fake_redis),
            clob_client=cast(Any, fake_clob),
            gamma_client=cast(Any, fake_gamma),
        )

        await sync.stop()  # Should be a no-op

        assert sync.state == SyncState.STOPPED
