"""Tests for ClobClient wrapper."""

import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest

import polymarket_insider_tracker.ingestor.clob_client as clob_module
from polymarket_insider_tracker.ingestor.clob_client import (
    ClobClient,
    RateLimiter,
    RetryError,
    with_retry,
)
from polymarket_insider_tracker.ingestor.models import Market, Orderbook


class TestRateLimiter:
    """Tests for RateLimiter."""

    def test_acquire_sync_no_wait_first_call(self) -> None:
        """First call should not wait."""
        limiter = RateLimiter(max_requests_per_second=10)
        start = time.monotonic()
        limiter.acquire_sync()
        elapsed = time.monotonic() - start

        # Should be nearly instant
        assert elapsed < 0.05

    def test_acquire_sync_enforces_rate(self) -> None:
        """Subsequent calls should be rate limited."""
        limiter = RateLimiter(max_requests_per_second=10)  # 100ms between calls

        # First call
        limiter.acquire_sync()

        # Second call should wait
        start = time.monotonic()
        limiter.acquire_sync()
        elapsed = time.monotonic() - start

        # Should wait at least 90ms (allowing some tolerance)
        assert elapsed >= 0.08


class TestWithRetry:
    """Tests for retry decorator."""

    def test_success_first_try(self) -> None:
        """Function succeeds on first try."""
        call_count = 0

        @with_retry(max_retries=3)
        def succeed() -> str:
            nonlocal call_count
            call_count += 1
            return "success"

        result = succeed()

        assert result == "success"
        assert call_count == 1

    def test_success_after_retries(self) -> None:
        """Function succeeds after some retries."""
        call_count = 0

        @with_retry(max_retries=3, base_delay=0.01)
        def succeed_eventually() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Not yet")
            return "success"

        result = succeed_eventually()

        assert result == "success"
        assert call_count == 3

    def test_exhausted_retries(self) -> None:
        """Raises RetryError after exhausting retries."""

        @with_retry(max_retries=2, base_delay=0.01)
        def always_fails() -> str:
            raise ValueError("Always fails")

        with pytest.raises(RetryError) as exc_info:
            always_fails()

        assert "3 attempts failed" in str(exc_info.value)
        assert isinstance(exc_info.value.last_exception, ValueError)

    def test_specific_exception_types(self) -> None:
        """Only retries on specified exception types."""
        call_count = 0

        @with_retry(max_retries=3, base_delay=0.01, retry_on=(ValueError,))
        def raise_type_error() -> str:
            nonlocal call_count
            call_count += 1
            raise TypeError("Not retried")

        with pytest.raises(TypeError):
            raise_type_error()

        # Should only be called once since TypeError is not in retry_on
        assert call_count == 1


@dataclass
class FakeClobLevel:
    price: str
    size: str


@dataclass
class FakeClobOrderbook:
    market: str
    asset_id: str
    tick_size: str
    bids: list[FakeClobLevel] | None
    asks: list[FakeClobLevel] | None


class FakeBaseClobClient:
    """Working in-memory fake for py-clob-client BaseClobClient."""

    def __init__(self, host: str | None = None) -> None:
        self.host = host
        self.fail_health: bool = False
        self.fail_market: bool = False
        self.fail_midpoint: bool = False
        self.simplified_markets_pages: list[dict[str, Any]] = []
        self.call_count_simplified: int = 0
        self.health_call_count: int = 0

    def get_ok(self) -> str:
        self.health_call_count += 1
        if self.fail_health:
            raise RuntimeError("Connection failed")
        return "OK"

    def get_server_time(self) -> int:
        return 1704067200000

    def get_simplified_markets(self, cursor: str | None = None) -> dict[str, Any]:
        _ = cursor
        self.call_count_simplified += 1
        if self.simplified_markets_pages:
            idx = min(self.call_count_simplified - 1, len(self.simplified_markets_pages) - 1)
            return self.simplified_markets_pages[idx]
        return {
            "data": [
                {
                    "condition_id": "0x123",
                    "question": "Test market?",
                    "tokens": [],
                    "closed": False,
                }
            ],
            "next_cursor": "LTE=",
        }

    def get_market(self, condition_id: str) -> dict[str, Any]:
        if self.fail_market:
            raise RuntimeError("Not found")
        return {
            "condition_id": condition_id,
            "question": "Will it happen?",
            "tokens": [
                {"token_id": "t1", "outcome": "Yes"},
                {"token_id": "t2", "outcome": "No"},
            ],
        }

    def get_order_book(self, token_id: str) -> FakeClobOrderbook:
        return FakeClobOrderbook(
            market="0xmarket",
            asset_id=token_id,
            tick_size="0.01",
            bids=[FakeClobLevel(price="0.50", size="100")],
            asks=[FakeClobLevel(price="0.52", size="150")],
        )

    def get_order_books(self, token_ids: list[str]) -> list[FakeClobOrderbook]:
        return [
            FakeClobOrderbook(market=f"m{i}", asset_id=t, tick_size="0.01", bids=[], asks=[])
            for i, t in enumerate(token_ids, 1)
        ]

    def get_midpoint(self, token_id: str) -> dict[str, str]:
        _ = token_id
        if self.fail_midpoint:
            raise RuntimeError("API error")
        return {"mid": "0.55"}

    def get_price(self, token_id: str, side: str = "BUY") -> dict[str, str]:
        _ = token_id
        return {"price": "0.53" if side == "BUY" else "0.51"}


class TestClobClient:
    """Tests for ClobClient wrapper."""

    @pytest.fixture
    def fake_base_client(self, monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeBaseClobClient]:
        """Create and monkeypatch working base CLOB client fake."""
        fake = FakeBaseClobClient()
        monkeypatch.setattr(clob_module, "BaseClobClient", lambda _host: fake)
        yield fake

    def test_init_defaults(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test client initialization with defaults."""
        _ = fake_base_client
        client = ClobClient()
        assert client._host == "https://clob.polymarket.com"
        assert client._max_retries == 3

    def test_init_with_env_api_key(
        self, fake_base_client: FakeBaseClobClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test client reads API key from environment."""
        _ = fake_base_client
        monkeypatch.setenv("POLYMARKET_API_KEY", "test-key")
        client = ClobClient()
        assert client._api_key == "test-key"

    def test_init_with_explicit_api_key(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test client uses explicitly provided API key."""
        _ = fake_base_client
        client = ClobClient(api_key="explicit-key")
        assert client._api_key == "explicit-key"

    def test_health_check_success(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test health check returns True when API responds OK."""
        client = ClobClient()
        result = client.health_check()
        assert result is True
        assert fake_base_client.health_call_count == 1

    def test_health_check_failure(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test health check returns False on error."""
        fake_base_client.fail_health = True
        client = ClobClient()
        result = client.health_check()
        assert result is False

    def test_get_server_time(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test getting server time."""
        _ = fake_base_client
        client = ClobClient()
        result = client.get_server_time()
        assert result == 1704067200000

    def test_get_markets(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test fetching markets."""
        _ = fake_base_client
        client = ClobClient()
        markets = client.get_markets()
        assert len(markets) == 1
        assert isinstance(markets[0], Market)
        assert markets[0].condition_id == "0x123"

    def test_get_markets_filters_closed(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test that closed markets are filtered when active_only=True."""
        fake_base_client.simplified_markets_pages = [
            {
                "data": [
                    {"condition_id": "0x1", "closed": False},
                    {"condition_id": "0x2", "closed": True},
                ],
                "next_cursor": "LTE=",
            }
        ]
        client = ClobClient()
        markets = client.get_markets(active_only=True)
        assert len(markets) == 1
        assert markets[0].condition_id == "0x1"

    def test_get_markets_includes_closed(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test that closed markets are included when active_only=False."""
        fake_base_client.simplified_markets_pages = [
            {
                "data": [
                    {"condition_id": "0x1", "closed": False},
                    {"condition_id": "0x2", "closed": True},
                ],
                "next_cursor": "LTE=",
            }
        ]
        client = ClobClient()
        markets = client.get_markets(active_only=False)
        assert len(markets) == 2

    def test_get_markets_pagination(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test that pagination is handled correctly."""
        fake_base_client.simplified_markets_pages = [
            {
                "data": [{"condition_id": "0x1"}],
                "next_cursor": "cursor2",
            },
            {
                "data": [{"condition_id": "0x2"}],
                "next_cursor": "LTE=",
            },
        ]
        client = ClobClient()
        markets = client.get_markets()
        assert len(markets) == 2
        assert fake_base_client.call_count_simplified == 2

    def test_get_market(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test fetching a single market."""
        _ = fake_base_client
        client = ClobClient()
        market = client.get_market("0xabc")
        assert isinstance(market, Market)
        assert market.condition_id == "0xabc"
        assert len(market.tokens) == 2

    def test_get_market_not_found(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test error handling when market not found."""
        fake_base_client.fail_market = True
        client = ClobClient()
        with pytest.raises(RetryError) as exc_info:
            client.get_market("0xnotfound")
        assert "get_market" in str(exc_info.value)
        assert exc_info.value.last_exception is not None

    def test_get_orderbook(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test fetching an orderbook."""
        _ = fake_base_client
        client = ClobClient()
        orderbook = client.get_orderbook("token123")
        assert isinstance(orderbook, Orderbook)
        assert orderbook.asset_id == "token123"
        assert len(orderbook.bids) == 1
        assert len(orderbook.asks) == 1

    def test_get_orderbooks(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test fetching multiple orderbooks."""
        _ = fake_base_client
        client = ClobClient()
        orderbooks = client.get_orderbooks(["t1", "t2"])
        assert len(orderbooks) == 2
        assert all(isinstance(ob, Orderbook) for ob in orderbooks)

    def test_get_midpoint(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test fetching midpoint price."""
        _ = fake_base_client
        client = ClobClient()
        result = client.get_midpoint("token123")
        assert result == "0.55"

    def test_get_midpoint_error(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test midpoint returns None on error."""
        fake_base_client.fail_midpoint = True
        client = ClobClient()
        result = client.get_midpoint("token123")
        assert result is None

    def test_get_price_buy(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test fetching buy price."""
        _ = fake_base_client
        client = ClobClient()
        result = client.get_price("token123", side="BUY")
        assert result == "0.53"

    def test_get_price_sell(self, fake_base_client: FakeBaseClobClient) -> None:
        """Test fetching sell price."""
        _ = fake_base_client
        client = ClobClient()
        result = client.get_price("token123", side="SELL")
        assert result == "0.51"
