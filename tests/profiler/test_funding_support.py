"""Pin the shared funding-test support used by the tracer and scanner suites."""

from __future__ import annotations

from polymarket_insider_tracker.profiler.chain import PolygonClient
from tests.fakes import FakeEth
from tests.profiler.funding_support import CHAIN_HEAD, polygon_client_on, transfer_to


def test_transfer_to_builds_one_usdc_log_with_seeded_hash() -> None:
    log = transfer_to("0x" + "bb" * 20, "0x" + "aa" * 20, seed=7)
    assert log["blockNumber"] == CHAIN_HEAD
    assert log["data"] == (1_000_000).to_bytes(32, "big")
    assert log["transactionHash"] == (7).to_bytes(32, "big")


def test_polygon_client_on_binds_fallback_only_when_given() -> None:
    eth = FakeEth(block_number=CHAIN_HEAD)
    assert isinstance(polygon_client_on(eth), PolygonClient)
    assert polygon_client_on(eth)._w3_fallback is None
    assert (
        polygon_client_on(eth, fallback=FakeEth(block_number=CHAIN_HEAD))._w3_fallback is not None
    )
