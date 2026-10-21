"""ClobClient failure handling: retried orderbook fetches, tolerated price/time failures."""

from __future__ import annotations

import time
from collections.abc import Iterator

import pytest

import polymarket_insider_tracker.ingestor.clob_client as clob_module
from polymarket_insider_tracker.ingestor.clob_client import (
    ClobClient,
    ClobClientError,
    RateLimiter,
    RetryError,
)
from tests.fakes import FakeBaseClobClient


@pytest.fixture
def fake_base_client(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeBaseClobClient]:
    fake = FakeBaseClobClient()

    def build(_host: str) -> FakeBaseClobClient:
        return fake

    monkeypatch.setattr(clob_module, "BaseClobClient", build)
    yield fake


@pytest.fixture
def no_backoff_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retry backoff is wall-clock time; skip it so exhausted retries finish at once."""

    def skip(_seconds: float) -> None:
        return None

    monkeypatch.setattr(time, "sleep", skip)


async def test_async_acquire_spaces_consecutive_requests() -> None:
    limiter = RateLimiter(max_requests_per_second=20)  # 50ms between requests
    await limiter.acquire()

    start = time.monotonic()
    await limiter.acquire()

    assert time.monotonic() - start >= 0.04


def test_orderbook_failure_is_retried_then_reported_with_its_cause(
    fake_base_client: FakeBaseClobClient, no_backoff_sleep: None
) -> None:
    _ = no_backoff_sleep
    fake_base_client.orderbook_error = RuntimeError("book unavailable")

    with pytest.raises(RetryError) as exc_info:
        ClobClient().get_orderbook("token123")

    cause = exc_info.value.last_exception
    assert isinstance(cause, ClobClientError)
    assert "Failed to fetch orderbook for token123: book unavailable" in str(cause)


def test_orderbooks_failure_is_retried_then_reported_with_its_cause(
    fake_base_client: FakeBaseClobClient, no_backoff_sleep: None
) -> None:
    _ = no_backoff_sleep
    fake_base_client.orderbook_error = RuntimeError("books unavailable")

    with pytest.raises(RetryError) as exc_info:
        ClobClient().get_orderbooks(["t1", "t2"])

    cause = exc_info.value.last_exception
    assert isinstance(cause, ClobClientError)
    assert "Failed to fetch orderbooks: books unavailable" in str(cause)


def test_price_failure_yields_none(fake_base_client: FakeBaseClobClient) -> None:
    fake_base_client.price_error = RuntimeError("price down")

    assert ClobClient().get_price("token123", side="SELL") is None


def test_server_time_failure_yields_none(fake_base_client: FakeBaseClobClient) -> None:
    fake_base_client.server_time_error = RuntimeError("clock down")

    assert ClobClient().get_server_time() is None
