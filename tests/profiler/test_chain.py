"""Tests for the Polygon blockchain client."""

import asyncio
import inspect
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from redis.asyncio import Redis
from web3 import AsyncWeb3
from web3.eth import AsyncEth
from web3.providers import AsyncBaseProvider

from polymarket_insider_tracker.profiler.chain import (
    DEFAULT_CACHE_TTL_SECONDS,
    PolygonClient,
    RateLimiter,
    RPCError,
)
from tests.fakes.web3 import FakeAsyncWeb3, FakeEth

# Valid Ethereum addresses for testing
VALID_ADDRESS = "0x742d35Cc6634C0532925a3b844Bc9e7595f5eaE2"
VALID_ADDRESS_2 = "0x8ba1f109551bD432803012645Ac136ddd64DBA72"
VALID_ADDRESS_3 = "0x1234567890AbCdEf1234567890ABcDeF12345678"
VALID_TOKEN = "0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0"  # MATIC token


class RecordingRateLimiter(RateLimiter):
    """Recording rate limiter that tracks acquire calls."""

    def __init__(self) -> None:
        super().__init__(max_tokens=10.0, refill_rate=10.0, tokens=10.0, last_refill=0.0)
        self.acquired: int = 0

    async def acquire(self, tokens: float = 1.0) -> None:
        _ = tokens
        self.acquired += 1


class BlockNumberProvider(AsyncBaseProvider):
    """A JSON-RPC boundary that lets real Web3 dispatch and decode block numbers."""

    async def make_request(self, method, params):
        assert method == "eth_blockNumber"
        assert params == () or params == []
        return {"jsonrpc": "2.0", "id": 1, "result": "0x2faf080"}


async def test_health_check_uses_real_web3_rpc_method(fake_redis: FakeAsyncRedis) -> None:
    client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)
    client._w3 = AsyncWeb3(BlockNumberProvider(), middleware=[])

    assert await client.health_check() is True


async def test_block_number_provider_returns_json_rpc_wire_value() -> None:
    response = await BlockNumberProvider().make_request("eth_blockNumber", [])

    assert response == {"jsonrpc": "2.0", "id": 1, "result": "0x2faf080"}


# A real client bound to a closed loopback port: every command fails with a connection error.
UNREACHABLE_REDIS_URL = "redis://127.0.0.1:1"


class TestRateLimiter:
    """Tests for the RateLimiter class."""

    def test_create(self) -> None:
        """Test creating a rate limiter."""
        limiter = RateLimiter.create(10.0)

        assert limiter.max_tokens == 10.0
        assert limiter.refill_rate == 10.0
        assert limiter.tokens == 10.0

    @pytest.mark.asyncio
    async def test_acquire_available(self) -> None:
        """Test acquiring when tokens are available."""
        limiter = RateLimiter.create(10.0)

        await limiter.acquire(1.0)

        assert limiter.tokens < 10.0

    @pytest.mark.asyncio
    async def test_acquire_multiple(self) -> None:
        """Test acquiring multiple tokens."""
        limiter = RateLimiter.create(10.0)

        for _ in range(5):
            await limiter.acquire(1.0)

        assert limiter.tokens < 6.0

    @pytest.mark.asyncio
    async def test_acquire_waits_when_empty(self) -> None:
        """Test that acquire waits when tokens are depleted."""
        limiter = RateLimiter.create(2.0)

        # Deplete tokens
        await limiter.acquire(2.0)

        # This should wait briefly for refill
        start = asyncio.get_event_loop().time()
        await limiter.acquire(0.5)
        elapsed = asyncio.get_event_loop().time() - start

        # Should have waited some time
        assert elapsed >= 0.1


class TestFakeEthFidelity:
    """The RPC fake must expose web3's surface the way web3 does, or it hides product bugs."""

    async def test_block_number_is_an_awaitable_property_and_get_block_number_a_method(
        self,
    ) -> None:
        """web3 exposes ``block_number`` as a property; only ``get_block_number`` is callable."""
        assert isinstance(inspect.getattr_static(AsyncEth, "block_number"), property)
        assert isinstance(inspect.getattr_static(FakeEth, "block_number"), property)
        assert inspect.getattr_static(AsyncEth, "get_block_number") is not None
        eth = FakeEth(block_number=7)

        assert await eth.block_number == 7
        assert await eth.get_block_number() == 7
        assert not callable(eth.block_number)


class TestPolygonClient:
    """Tests for the PolygonClient class."""

    def test_init(self) -> None:
        """Test initialization."""
        client = PolygonClient("https://polygon-rpc.com")

        assert client._rpc_url == "https://polygon-rpc.com"
        assert client._fallback_rpc_url is None
        assert client._cache_ttl == DEFAULT_CACHE_TTL_SECONDS

    def test_init_with_fallback(self) -> None:
        """Test initialization with fallback RPC."""
        client = PolygonClient(
            "https://polygon-rpc.com",
            fallback_rpc_url="https://fallback.com",
        )

        assert client._fallback_rpc_url == "https://fallback.com"
        assert client._w3_fallback is not None

    def test_init_custom_config(self) -> None:
        """Test initialization with custom config."""
        client = PolygonClient(
            "https://polygon-rpc.com",
            cache_ttl_seconds=600,
            max_requests_per_second=50,
            max_retries=5,
        )

        assert client._cache_ttl == 600
        assert client._max_retries == 5
        assert client._rate_limiter.max_tokens == 50

    def test_cache_key(self) -> None:
        """Test cache key generation."""
        client = PolygonClient("https://polygon-rpc.com")

        key = client._cache_key("nonce", "0xAbC123")

        assert key == "polygon:nonce:0xabc123"

    @pytest.mark.asyncio
    async def test_acquire_rate_limit_uses_shared_limiter(self) -> None:
        """Public consumers share the client's existing request limiter."""
        client = PolygonClient("https://polygon-rpc.com")
        limiter = RecordingRateLimiter()
        client._rate_limiter = limiter

        await client.acquire_rate_limit()

        assert limiter.acquired == 1

    def test_select_web3_uses_healthy_client(self) -> None:
        """The public selector follows primary health and fallback availability."""
        client = PolygonClient(
            "https://polygon-rpc.com",
            fallback_rpc_url="https://fallback.com",
        )
        primary = client._w3
        fallback = client._w3_fallback

        assert client.select_web3() is primary
        client._primary_healthy = False
        assert client.select_web3() is fallback

        client_without_fallback = PolygonClient("https://polygon-rpc.com")
        sole_client = client_without_fallback._w3
        client_without_fallback._primary_healthy = False
        assert client_without_fallback.select_web3() is sole_client

    @pytest.mark.asyncio
    async def test_get_cached_miss(self, fake_redis: FakeAsyncRedis) -> None:
        """Test cache miss."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)

        result = await client._get_cached("test:key")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_cached_hit(self, fake_redis: FakeAsyncRedis) -> None:
        """Test cache hit."""
        await fake_redis.set("test:key", b"cached_value")
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)

        result = await client._get_cached("test:key")

        assert result == "cached_value"

    @pytest.mark.asyncio
    async def test_get_cached_error_handling(self) -> None:
        """A Redis connection failure is swallowed and treated as a cache miss."""
        unreachable = Redis.from_url(UNREACHABLE_REDIS_URL)
        client = PolygonClient("https://polygon-rpc.com", redis=unreachable)

        try:
            result = await client._get_cached("test:key")
        finally:
            await unreachable.aclose()

        assert result is None

    @pytest.mark.asyncio
    async def test_set_cached(self, fake_redis: FakeAsyncRedis) -> None:
        """Test setting cache."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)

        await client._set_cached("test:key", "value")

        assert await fake_redis.get("test:key") == b"value"
        assert await fake_redis.ttl("test:key") == DEFAULT_CACHE_TTL_SECONDS

    @pytest.mark.asyncio
    async def test_set_cached_custom_ttl(self, fake_redis: FakeAsyncRedis) -> None:
        """Test setting cache with custom TTL."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)

        await client._set_cached("test:key", "value", ttl=3600)

        assert await fake_redis.get("test:key") == b"value"
        assert await fake_redis.ttl("test:key") == 3600

    @pytest.mark.asyncio
    async def test_get_transaction_count_cached(self, fake_redis: FakeAsyncRedis) -> None:
        """Test getting transaction count from cache."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)
        await fake_redis.set(client._cache_key("nonce", VALID_ADDRESS), b"42")

        count = await client.get_transaction_count(VALID_ADDRESS)

        assert count == 42

    @pytest.mark.asyncio
    async def test_get_transaction_count_uncached(self, fake_redis: FakeAsyncRedis) -> None:
        """Test getting transaction count from blockchain."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)
        client._w3 = FakeAsyncWeb3(FakeEth(transaction_count=42))

        count = await client.get_transaction_count(VALID_ADDRESS)

        assert count == 42
        assert await fake_redis.get(client._cache_key("nonce", VALID_ADDRESS)) == b"42"

    @pytest.mark.asyncio
    async def test_get_transaction_counts_batch(self, fake_redis: FakeAsyncRedis) -> None:
        """Test batch getting transaction counts."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)
        await fake_redis.set(client._cache_key("nonce", VALID_ADDRESS), b"10")
        await fake_redis.set(client._cache_key("nonce", VALID_ADDRESS_3), b"30")
        client._w3 = FakeAsyncWeb3(FakeEth(transaction_count=20))

        addresses = [VALID_ADDRESS, VALID_ADDRESS_2, VALID_ADDRESS_3]
        counts = await client.get_transaction_counts(addresses)

        assert counts[VALID_ADDRESS.lower()] == 10  # From cache
        assert counts[VALID_ADDRESS_2.lower()] == 20  # From blockchain
        assert counts[VALID_ADDRESS_3.lower()] == 30  # From cache

    @pytest.mark.asyncio
    async def test_get_transaction_counts_empty(self, fake_redis: FakeAsyncRedis) -> None:
        """Test batch with empty list."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)

        counts = await client.get_transaction_counts([])

        assert counts == {}

    @pytest.mark.asyncio
    async def test_get_balance_cached(self, fake_redis: FakeAsyncRedis) -> None:
        """Test getting balance from cache."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)
        await fake_redis.set(client._cache_key("balance", VALID_ADDRESS), b"1000000000000000000")

        balance = await client.get_balance(VALID_ADDRESS)

        assert balance == Decimal("1000000000000000000")

    @pytest.mark.asyncio
    async def test_get_balance_uncached(self, fake_redis: FakeAsyncRedis) -> None:
        """Test getting balance from blockchain."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)
        client._w3 = FakeAsyncWeb3(FakeEth(balance_wei=2000000000000000000))

        balance = await client.get_balance(VALID_ADDRESS)

        assert balance == Decimal("2000000000000000000")
        assert (
            await fake_redis.get(client._cache_key("balance", VALID_ADDRESS))
            == b"2000000000000000000"
        )

    @pytest.mark.asyncio
    async def test_get_wallet_info(self, fake_redis: FakeAsyncRedis) -> None:
        """Test getting aggregated wallet info."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)
        client._w3 = FakeAsyncWeb3(
            FakeEth(
                transaction_count=42,
                balance_wei=1000000000000000000,
            ),
        )

        info = await client.get_wallet_info(VALID_ADDRESS)

        assert info.address == VALID_ADDRESS.lower()
        assert info.transaction_count == 42
        assert info.balance_wei == Decimal("1000000000000000000")
        assert info.first_transaction is None

    @pytest.mark.asyncio
    async def test_get_first_transaction_no_transactions(self, fake_redis: FakeAsyncRedis) -> None:
        """Test get_first_transaction when wallet has no transactions."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)
        client._w3 = FakeAsyncWeb3(FakeEth(transaction_count=0))

        tx = await client.get_first_transaction(VALID_ADDRESS)

        assert tx is None

    @pytest.mark.asyncio
    async def test_health_check_success(self, fake_redis: FakeAsyncRedis) -> None:
        """Test successful health check."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)
        client._w3 = FakeAsyncWeb3(FakeEth(block_number=50000000))

        healthy = await client.health_check()

        assert healthy is True

    @pytest.mark.asyncio
    async def test_health_check_failure(self, fake_redis: FakeAsyncRedis) -> None:
        """Test failed health check."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)
        client._w3 = FakeAsyncWeb3(FakeEth(always_fail=True))

        healthy = await client.health_check()

        assert healthy is False


class TestPolygonClientRetryLogic:
    """Tests for retry and failover logic."""

    @pytest.mark.asyncio
    async def test_retry_on_failure(self, fake_redis: FakeAsyncRedis) -> None:
        """Test that client retries on RPC failure."""
        client = PolygonClient(
            "https://polygon-rpc.com",
            redis=fake_redis,
            max_retries=3,
            retry_delay_seconds=0.01,
        )
        eth = FakeEth(transaction_count=42, fail_count=2)
        client._w3 = FakeAsyncWeb3(eth)

        count = await client.get_transaction_count(VALID_ADDRESS)

        assert count == 42
        assert eth.call_count == 3

    @pytest.mark.asyncio
    async def test_failover_to_secondary(self, fake_redis: FakeAsyncRedis) -> None:
        """Test failover to secondary RPC."""
        client = PolygonClient(
            "https://polygon-rpc.com",
            fallback_rpc_url="https://fallback.com",
            redis=fake_redis,
            max_retries=1,
            retry_delay_seconds=0.01,
        )
        primary_eth = FakeEth(always_fail=True)
        fallback_eth = FakeEth(transaction_count=42)
        client._w3 = FakeAsyncWeb3(primary_eth)
        client._w3_fallback = FakeAsyncWeb3(fallback_eth)

        count = await client.get_transaction_count(VALID_ADDRESS)

        assert count == 42
        assert not client._primary_healthy
        assert fallback_eth.call_count == 1

    @pytest.mark.asyncio
    async def test_all_retries_exhausted(self, fake_redis: FakeAsyncRedis) -> None:
        """Test error when all retries are exhausted."""
        client = PolygonClient(
            "https://polygon-rpc.com",
            redis=fake_redis,
            max_retries=2,
            retry_delay_seconds=0.01,
        )
        eth = FakeEth(always_fail=True)
        client._w3 = FakeAsyncWeb3(eth)

        with pytest.raises(RPCError):
            await client.get_transaction_count(VALID_ADDRESS)


class TestPolygonClientRateLimiting:
    """Tests for rate limiting."""

    @pytest.mark.asyncio
    async def test_rate_limiting_enforced(self, fake_redis: FakeAsyncRedis) -> None:
        """Test that rate limiting delays requests."""
        client = PolygonClient(
            "https://polygon-rpc.com",
            redis=fake_redis,
            max_requests_per_second=5.0,
        )

        # Deplete rate limit
        client._rate_limiter.tokens = 0
        client._w3 = FakeAsyncWeb3(FakeEth(transaction_count=42))

        start = asyncio.get_event_loop().time()
        await client.get_transaction_count(VALID_ADDRESS)
        elapsed = asyncio.get_event_loop().time() - start

        # Should have waited for token refill
        assert elapsed >= 0.1


class TestPolygonClientTokenBalance:
    """Tests for ERC20 token balance queries."""

    @pytest.mark.asyncio
    async def test_get_token_balance_cached(self, fake_redis: FakeAsyncRedis) -> None:
        """Test getting token balance from cache."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)
        client._w3 = FakeAsyncWeb3()
        await fake_redis.set(
            client._cache_key(f"token:{VALID_TOKEN.lower()}", VALID_ADDRESS),
            b"1000000",
        )

        balance = await client.get_token_balance(VALID_ADDRESS, VALID_TOKEN)

        assert balance == Decimal("1000000")

    @pytest.mark.asyncio
    async def test_get_token_balance_uncached(self, fake_redis: FakeAsyncRedis) -> None:
        """Test getting token balance from blockchain."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)
        client._w3 = FakeAsyncWeb3(FakeEth(token_balance=5000000))

        balance = await client.get_token_balance(VALID_ADDRESS, VALID_TOKEN)

        assert balance == Decimal("5000000")
        assert (
            await fake_redis.get(client._cache_key(f"token:{VALID_TOKEN.lower()}", VALID_ADDRESS))
            == b"5000000"
        )


class TestPolygonClientBlock:
    """Tests for block queries."""

    @pytest.mark.asyncio
    async def test_get_block_cached(self, fake_redis: FakeAsyncRedis) -> None:
        """Test getting block from cache."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)
        await fake_redis.set("polygon:block:50000000", b'{"timestamp": 1704369600}')

        block = await client.get_block(50000000)

        assert block["timestamp"] == 1704369600

    @pytest.mark.asyncio
    async def test_get_block_uncached(self, fake_redis: FakeAsyncRedis) -> None:
        """Test getting block from blockchain."""
        client = PolygonClient("https://polygon-rpc.com", redis=fake_redis)
        client._w3 = FakeAsyncWeb3(
            FakeEth(
                block_timestamp=1704369600,
                block_number=50000000,
            ),
        )

        block = await client.get_block(50000000)

        assert block["timestamp"] == 1704369600
        # Block cache uses 1 hour TTL
        assert await fake_redis.ttl("polygon:block:50000000") == 3600
