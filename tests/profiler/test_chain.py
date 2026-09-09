"""Tests for the Polygon blockchain client."""

import asyncio
from decimal import Decimal
from typing import Any, cast

import pytest

from polymarket_insider_tracker.profiler.chain import (
    DEFAULT_CACHE_TTL_SECONDS,
    PolygonClient,
    RateLimiter,
    RPCError,
)
from tests.fakes.redis import FakeRedis
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


class ErrorRedis(FakeRedis):
    """Fake Redis that raises on get."""

    async def get(self, name: str) -> Any:
        _ = name
        raise Exception("Redis error")


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
    async def test_get_cached_miss(self) -> None:
        """Test cache miss."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))

        result = await client._get_cached("test:key")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_cached_hit(self) -> None:
        """Test cache hit."""
        redis = FakeRedis()
        await redis.set("test:key", b"cached_value")
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))

        result = await client._get_cached("test:key")

        assert result == "cached_value"

    @pytest.mark.asyncio
    async def test_get_cached_error_handling(self) -> None:
        """Test that cache errors are handled gracefully."""
        redis = ErrorRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))

        result = await client._get_cached("test:key")

        assert result is None  # Should not raise

    @pytest.mark.asyncio
    async def test_set_cached(self) -> None:
        """Test setting cache."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))

        await client._set_cached("test:key", "value")

        assert await redis.get("test:key") == b"value"
        assert await redis.ttl("test:key") == DEFAULT_CACHE_TTL_SECONDS

    @pytest.mark.asyncio
    async def test_set_cached_custom_ttl(self) -> None:
        """Test setting cache with custom TTL."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))

        await client._set_cached("test:key", "value", ttl=3600)

        assert await redis.get("test:key") == b"value"
        assert await redis.ttl("test:key") == 3600

    @pytest.mark.asyncio
    async def test_get_transaction_count_cached(self) -> None:
        """Test getting transaction count from cache."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))
        await redis.set(client._cache_key("nonce", VALID_ADDRESS), b"42")

        count = await client.get_transaction_count(VALID_ADDRESS)

        assert count == 42

    @pytest.mark.asyncio
    async def test_get_transaction_count_uncached(self) -> None:
        """Test getting transaction count from blockchain."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))
        client._w3 = cast(Any, FakeAsyncWeb3(FakeEth(transaction_count=42)))

        count = await client.get_transaction_count(VALID_ADDRESS)

        assert count == 42
        assert await redis.get(client._cache_key("nonce", VALID_ADDRESS)) == b"42"

    @pytest.mark.asyncio
    async def test_get_transaction_counts_batch(self) -> None:
        """Test batch getting transaction counts."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))
        await redis.set(client._cache_key("nonce", VALID_ADDRESS), b"10")
        await redis.set(client._cache_key("nonce", VALID_ADDRESS_3), b"30")
        client._w3 = cast(Any, FakeAsyncWeb3(FakeEth(transaction_count=20)))

        addresses = [VALID_ADDRESS, VALID_ADDRESS_2, VALID_ADDRESS_3]
        counts = await client.get_transaction_counts(addresses)

        assert counts[VALID_ADDRESS.lower()] == 10  # From cache
        assert counts[VALID_ADDRESS_2.lower()] == 20  # From blockchain
        assert counts[VALID_ADDRESS_3.lower()] == 30  # From cache

    @pytest.mark.asyncio
    async def test_get_transaction_counts_empty(self) -> None:
        """Test batch with empty list."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))

        counts = await client.get_transaction_counts([])

        assert counts == {}

    @pytest.mark.asyncio
    async def test_get_balance_cached(self) -> None:
        """Test getting balance from cache."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))
        await redis.set(client._cache_key("balance", VALID_ADDRESS), b"1000000000000000000")

        balance = await client.get_balance(VALID_ADDRESS)

        assert balance == Decimal("1000000000000000000")

    @pytest.mark.asyncio
    async def test_get_balance_uncached(self) -> None:
        """Test getting balance from blockchain."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))
        client._w3 = cast(Any, FakeAsyncWeb3(FakeEth(balance_wei=2000000000000000000)))

        balance = await client.get_balance(VALID_ADDRESS)

        assert balance == Decimal("2000000000000000000")
        assert (
            await redis.get(client._cache_key("balance", VALID_ADDRESS)) == b"2000000000000000000"
        )

    @pytest.mark.asyncio
    async def test_get_wallet_info(self) -> None:
        """Test getting aggregated wallet info."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))
        client._w3 = cast(
            Any,
            FakeAsyncWeb3(
                FakeEth(
                    transaction_count=42,
                    balance_wei=1000000000000000000,
                )
            ),
        )

        info = await client.get_wallet_info(VALID_ADDRESS)

        assert info.address == VALID_ADDRESS.lower()
        assert info.transaction_count == 42
        assert info.balance_wei == Decimal("1000000000000000000")
        assert info.first_transaction is None

    @pytest.mark.asyncio
    async def test_get_first_transaction_no_transactions(self) -> None:
        """Test get_first_transaction when wallet has no transactions."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))
        client._w3 = cast(Any, FakeAsyncWeb3(FakeEth(transaction_count=0)))

        tx = await client.get_first_transaction(VALID_ADDRESS)

        assert tx is None

    @pytest.mark.asyncio
    async def test_health_check_success(self) -> None:
        """Test successful health check."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))
        client._w3 = cast(Any, FakeAsyncWeb3(FakeEth(block_number=50000000)))

        healthy = await client.health_check()

        assert healthy is True

    @pytest.mark.asyncio
    async def test_health_check_failure(self) -> None:
        """Test failed health check."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))
        client._w3 = cast(Any, FakeAsyncWeb3(FakeEth(always_fail=True)))

        healthy = await client.health_check()

        assert healthy is False


class TestPolygonClientRetryLogic:
    """Tests for retry and failover logic."""

    @pytest.mark.asyncio
    async def test_retry_on_failure(self) -> None:
        """Test that client retries on RPC failure."""
        redis = FakeRedis()
        client = PolygonClient(
            "https://polygon-rpc.com",
            redis=cast(Any, redis),
            max_retries=3,
            retry_delay_seconds=0.01,
        )
        eth = FakeEth(transaction_count=42, fail_count=2)
        client._w3 = cast(Any, FakeAsyncWeb3(eth))

        count = await client.get_transaction_count(VALID_ADDRESS)

        assert count == 42
        assert eth.call_count == 3

    @pytest.mark.asyncio
    async def test_failover_to_secondary(self) -> None:
        """Test failover to secondary RPC."""
        redis = FakeRedis()
        client = PolygonClient(
            "https://polygon-rpc.com",
            fallback_rpc_url="https://fallback.com",
            redis=cast(Any, redis),
            max_retries=1,
            retry_delay_seconds=0.01,
        )
        primary_eth = FakeEth(always_fail=True)
        fallback_eth = FakeEth(transaction_count=42)
        client._w3 = cast(Any, FakeAsyncWeb3(primary_eth))
        client._w3_fallback = cast(Any, FakeAsyncWeb3(fallback_eth))

        count = await client.get_transaction_count(VALID_ADDRESS)

        assert count == 42
        assert not client._primary_healthy
        assert fallback_eth.call_count == 1

    @pytest.mark.asyncio
    async def test_all_retries_exhausted(self) -> None:
        """Test error when all retries are exhausted."""
        redis = FakeRedis()
        client = PolygonClient(
            "https://polygon-rpc.com",
            redis=cast(Any, redis),
            max_retries=2,
            retry_delay_seconds=0.01,
        )
        eth = FakeEth(always_fail=True)
        client._w3 = cast(Any, FakeAsyncWeb3(eth))

        with pytest.raises(RPCError):
            await client.get_transaction_count(VALID_ADDRESS)


class TestPolygonClientRateLimiting:
    """Tests for rate limiting."""

    @pytest.mark.asyncio
    async def test_rate_limiting_enforced(self) -> None:
        """Test that rate limiting delays requests."""
        redis = FakeRedis()
        client = PolygonClient(
            "https://polygon-rpc.com",
            redis=cast(Any, redis),
            max_requests_per_second=5.0,
        )

        # Deplete rate limit
        client._rate_limiter.tokens = 0
        client._w3 = cast(Any, FakeAsyncWeb3(FakeEth(transaction_count=42)))

        start = asyncio.get_event_loop().time()
        await client.get_transaction_count(VALID_ADDRESS)
        elapsed = asyncio.get_event_loop().time() - start

        # Should have waited for token refill
        assert elapsed >= 0.1


class TestPolygonClientTokenBalance:
    """Tests for ERC20 token balance queries."""

    @pytest.mark.asyncio
    async def test_get_token_balance_cached(self) -> None:
        """Test getting token balance from cache."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))
        client._w3 = cast(Any, FakeAsyncWeb3())
        await redis.set(
            client._cache_key(f"token:{VALID_TOKEN.lower()}", VALID_ADDRESS),
            b"1000000",
        )

        balance = await client.get_token_balance(VALID_ADDRESS, VALID_TOKEN)

        assert balance == Decimal("1000000")

    @pytest.mark.asyncio
    async def test_get_token_balance_uncached(self) -> None:
        """Test getting token balance from blockchain."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))
        client._w3 = cast(Any, FakeAsyncWeb3(FakeEth(token_balance=5000000)))

        balance = await client.get_token_balance(VALID_ADDRESS, VALID_TOKEN)

        assert balance == Decimal("5000000")
        assert (
            await redis.get(client._cache_key(f"token:{VALID_TOKEN.lower()}", VALID_ADDRESS))
            == b"5000000"
        )


class TestPolygonClientBlock:
    """Tests for block queries."""

    @pytest.mark.asyncio
    async def test_get_block_cached(self) -> None:
        """Test getting block from cache."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))
        await redis.set("polygon:block:50000000", b'{"timestamp": 1704369600}')

        block = await client.get_block(50000000)

        assert block["timestamp"] == 1704369600

    @pytest.mark.asyncio
    async def test_get_block_uncached(self) -> None:
        """Test getting block from blockchain."""
        redis = FakeRedis()
        client = PolygonClient("https://polygon-rpc.com", redis=cast(Any, redis))
        client._w3 = cast(
            Any,
            FakeAsyncWeb3(
                FakeEth(
                    block_timestamp=1704369600,
                    block_number=50000000,
                )
            ),
        )

        block = await client.get_block(50000000)

        assert block["timestamp"] == 1704369600
        # Block cache uses 1 hour TTL
        assert await redis.ttl("polygon:block:50000000") == 3600
