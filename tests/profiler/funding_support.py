"""Funding-test support shared by the tracer and log-scanner suites.

One home for the real ``PolygonClient`` bound to ``FakeEth`` providers, the ``Transfer`` log
builder, and the addresses, chain head and pruned-history error those suites share.
"""

from __future__ import annotations

from typing import Any

from polymarket_insider_tracker.profiler.chain import PolygonClient
from tests.fakes import FakeAsyncWeb3, FakeEth, transfer_log

TEST_WALLET = "0x1234567890abcdef1234567890abcdef12345678"
TEST_SOURCE = "0xabcdef1234567890abcdef1234567890abcdef12"
CHAIN_HEAD = 50_000_000
PRUNED_HISTORY_ERROR = RuntimeError(
    "{'code': -32701, 'message': 'History has been pruned for this block. To remove "
    "restrictions, order a dedicated full node here: https://www.allnodes.com/pol/host'}"
)


def polygon_client_on(eth: FakeEth, *, fallback: FakeEth | None = None) -> PolygonClient:
    """A real PolygonClient with its web3 providers bound to fakes and no retry delays."""
    return PolygonClient(
        "https://polygon-rpc.com",
        fallback_rpc_url="https://fallback.invalid" if fallback else None,
        max_requests_per_second=10_000.0,
        max_retries=1,
        retry_delay_seconds=0.0,
        web3=FakeAsyncWeb3(eth),
        fallback_web3=None if fallback is None else FakeAsyncWeb3(fallback),
    )


def transfer_to(
    to_address: str, from_address: str, *, block_number: int = CHAIN_HEAD, seed: int = 0
) -> dict[str, Any]:
    """A one-USDC ``Transfer`` log into ``to_address`` with a seed-derived transaction hash."""
    return transfer_log(
        from_address=from_address,
        to_address=to_address,
        amount=1_000_000,
        tx_hash=f"0x{seed:064x}",
        block_number=block_number,
    )
