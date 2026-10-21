"""Primary/fallback recovery, batch nonce failures and cached first-transaction replay."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from aiohttp import ClientConnectorError
from fakeredis import FakeAsyncRedis

import polymarket_insider_tracker.profiler.chain as chain_module
from polymarket_insider_tracker.profiler.chain import PolygonClient, RPCError
from tests.fakes.web3 import FakeAsyncWeb3, FakeEth

ADDRESS = "0x742d35Cc6634C0532925a3b844Bc9e7595f5eaE2"
OTHER_ADDRESS = "0x8ba1f109551bD432803012645Ac136ddd64DBA72"
RECOVERY_SECONDS = 60.0


class SteppedClock:
    """Stands in for the ``time`` module inside ``chain``: monotonic time moves only on demand."""

    def __init__(self, now: float) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now


def _failover_client(primary: FakeEth, fallback: FakeEth) -> PolygonClient:
    return PolygonClient(
        "https://polygon-rpc.com",
        max_requests_per_second=10_000.0,
        max_retries=1,
        retry_delay_seconds=0.0,
        web3=FakeAsyncWeb3(primary),
        fallback_web3=FakeAsyncWeb3(fallback),
    )


async def test_fallback_rpc_url_is_used_when_the_primary_fails() -> None:
    """A fallback given only as a URL is built into a client that serves failover calls."""
    client = PolygonClient(
        "https://polygon-rpc.com",
        fallback_rpc_url="http://127.0.0.1:1",  # loopback port nothing listens on
        max_retries=1,
        retry_delay_seconds=0.0,
        web3=FakeAsyncWeb3(FakeEth(always_fail=True)),
    )

    with pytest.raises(ClientConnectorError, match=r"127\.0\.0\.1:1"):
        await client.get_transaction_count(ADDRESS)


async def test_failed_primary_is_skipped_until_the_recovery_interval_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = SteppedClock(1_000.0)
    monkeypatch.setattr(chain_module, "time", clock)
    primary = FakeEth(transaction_count=7, fail_count=1)
    fallback = FakeEth(transaction_count=9)
    client = _failover_client(primary, fallback)

    assert await client.get_transaction_count(ADDRESS) == 9  # primary fails, fallback answers
    assert primary.call_count == 1

    assert await client.get_transaction_count(OTHER_ADDRESS) == 9  # primary skipped for now
    assert primary.call_count == 1

    clock.now += RECOVERY_SECONDS + 1
    client_address = "0x1234567890AbCdEf1234567890ABcDeF12345678"
    assert await client.get_transaction_count(client_address) == 7  # primary probed and healthy
    assert primary.call_count == 2


async def test_primary_is_not_retried_before_the_recovery_interval_elapses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = SteppedClock(1_000.0)
    monkeypatch.setattr(chain_module, "time", clock)
    primary = FakeEth(always_fail=True)
    client = _failover_client(primary, FakeEth(transaction_count=9))
    await client.get_transaction_count(ADDRESS)

    clock.now += RECOVERY_SECONDS - 1
    await client.get_transaction_count(OTHER_ADDRESS)

    assert primary.call_count == 1


async def test_failed_nonce_in_a_batch_counts_as_zero(fake_redis: FakeAsyncRedis) -> None:
    client = PolygonClient(
        "https://polygon-rpc.com",
        redis=fake_redis,
        max_retries=1,
        retry_delay_seconds=0.0,
        web3=FakeAsyncWeb3(FakeEth(always_fail=True)),
    )

    counts = await client.get_transaction_counts([ADDRESS, OTHER_ADDRESS])

    assert counts == {ADDRESS.lower(): 0, OTHER_ADDRESS.lower(): 0}


async def test_fully_cached_batch_makes_no_rpc_calls(fake_redis: FakeAsyncRedis) -> None:
    eth = FakeEth(transaction_count=99)
    client = PolygonClient("https://polygon-rpc.com", redis=fake_redis, web3=FakeAsyncWeb3(eth))
    await fake_redis.set(f"polygon:nonce:{ADDRESS.lower()}", b"4")

    counts = await client.get_transaction_counts([ADDRESS])

    assert counts == {ADDRESS.lower(): 4}
    assert eth.call_count == 0


async def test_cached_first_transaction_is_replayed_without_rpc(
    fake_redis: FakeAsyncRedis,
) -> None:
    eth = FakeEth()
    client = PolygonClient("https://polygon-rpc.com", redis=fake_redis, web3=FakeAsyncWeb3(eth))
    await fake_redis.set(
        f"polygon:first_tx:{ADDRESS.lower()}",
        json.dumps(
            {
                "hash": "0xabc",
                "block_number": 123,
                "timestamp": "2024-01-02T03:04:05+00:00",
                "from_address": "0xfrom",
                "to_address": "0xto",
                "value": "1000000000000000000",
                "gas_used": 21000,
                "gas_price": "30000000000",
            }
        ),
    )

    tx = await client.get_first_transaction(ADDRESS)

    assert tx is not None
    assert (tx.hash, tx.block_number, tx.from_address, tx.to_address) == (
        "0xabc",
        123,
        "0xfrom",
        "0xto",
    )
    assert tx.timestamp == datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert (tx.value, tx.gas_used, tx.gas_price) == (
        Decimal("1000000000000000000"),
        21000,
        Decimal("30000000000"),
    )
    assert eth.call_count == 0


async def test_failure_of_primary_and_fallback_raises_rpc_error() -> None:
    primary, fallback = FakeEth(always_fail=True), FakeEth(always_fail=True)
    client = _failover_client(primary, fallback)

    with pytest.raises(RPCError, match="get_transaction_count failed after all retries"):
        await client.get_transaction_count(ADDRESS)

    assert (primary.call_count, fallback.call_count) == (1, 1)


async def test_token_balance_rpc_failure_is_reported_as_rpc_error() -> None:
    client = PolygonClient(
        "https://polygon-rpc.com",
        web3=FakeAsyncWeb3(FakeEth(token_balance_fails=True)),
    )

    with pytest.raises(RPCError, match="Failed to get token balance: token balance call failed"):
        await client.get_token_balance(ADDRESS, "0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0")


async def test_cached_empty_wallet_replays_as_no_first_transaction(
    fake_redis: FakeAsyncRedis,
) -> None:
    eth = FakeEth(transaction_count=5)
    client = PolygonClient("https://polygon-rpc.com", redis=fake_redis, web3=FakeAsyncWeb3(eth))
    await fake_redis.set(f"polygon:first_tx:{ADDRESS.lower()}", "null")

    assert await client.get_first_transaction(ADDRESS) is None
    assert eth.call_count == 0
