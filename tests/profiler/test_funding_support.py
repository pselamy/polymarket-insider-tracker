"""Pin the shared funding-test support used by the tracer and scanner suites."""

from __future__ import annotations

import pytest

from polymarket_insider_tracker.profiler.chain import PolygonClient, RPCError
from tests.fakes import FakeEth
from tests.profiler.funding_support import (
    CHAIN_HEAD,
    TEST_WALLET,
    polygon_client_on,
    transfer_to,
)


def test_transfer_to_builds_one_usdc_log_with_seeded_hash() -> None:
    log = transfer_to("0x" + "bb" * 20, "0x" + "aa" * 20, seed=7)
    assert log["blockNumber"] == CHAIN_HEAD
    assert log["data"] == (1_000_000).to_bytes(32, "big")
    assert log["transactionHash"] == (7).to_bytes(32, "big")


async def test_polygon_client_on_binds_fallback_only_when_given() -> None:
    failing = FakeEth(block_number=CHAIN_HEAD, always_fail=True)
    healthy = FakeEth(block_number=CHAIN_HEAD, transaction_count=4)
    assert isinstance(polygon_client_on(failing), PolygonClient)
    with pytest.raises(RPCError):
        await polygon_client_on(failing).get_transaction_count(TEST_WALLET)
    assert (
        await polygon_client_on(failing, fallback=healthy).get_transaction_count(TEST_WALLET) == 4
    )
