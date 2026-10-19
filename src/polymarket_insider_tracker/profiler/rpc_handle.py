"""Structural type of the web3 handle ``PolygonClient`` drives.

``AsyncWeb3`` satisfies these protocols; so does the in-memory fake used by the tests, which is
what lets the client accept an injected handle without any typing bypass.
"""

from __future__ import annotations

from collections.abc import Awaitable, Mapping, Sequence
from typing import Any, Protocol

from eth_typing import ChecksumAddress
from web3 import AsyncWeb3
from web3.types import FilterParams


class EthRpc(Protocol):
    """The ``eth`` namespace of the Polygon JSON-RPC handle."""

    @property
    def block_number(self) -> Awaitable[int]:
        """The latest block number."""

    def get_logs(self, filter_params: FilterParams) -> Awaitable[Sequence[Mapping[str, Any]]]:
        """Fetch the logs matching ``filter_params``."""

    # The one confined escape: web3 overloads ``contract`` and resolves ``functions`` dynamically,
    # so pyright reports 'No overloaded function matches' for every structural proxy type.
    # ``erc20_balance_of`` is the only caller and returns a typed ``int``.
    def contract(self, address: ChecksumAddress, **kwargs: Any) -> Any:
        """Build a contract proxy at ``address``."""


class Web3Handle(Protocol):
    """A Polygon RPC handle: ``AsyncWeb3`` or an injected equivalent."""

    @property
    def eth(self) -> EthRpc:
        """The ``eth`` namespace."""


async def erc20_balance_of(
    handle: Web3Handle, token: str, holder: str, abi: Sequence[Mapping[str, Any]]
) -> int:
    """Read ``holder``'s balance of the ERC-20 ``token`` through ``handle``."""
    contract = handle.eth.contract(AsyncWeb3.to_checksum_address(token), abi=abi)
    balance = await contract.functions.balanceOf(AsyncWeb3.to_checksum_address(holder)).call()
    return int(balance)
