"""Structural type of the web3 handle ``PolygonClient`` drives.

``AsyncWeb3`` satisfies these protocols; so does the in-memory fake used by the tests, which is
what lets the client accept an injected handle without any typing bypass.
"""

from __future__ import annotations

import json
from abc import abstractmethod
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar

from eth_typing import ABI, ChecksumAddress
from web3 import AsyncWeb3
from web3.types import FilterParams

from polymarket_insider_tracker.ingestor.volume_observation import JsonValue

T = TypeVar("T")


class EthRpc(Protocol):
    """The ``eth`` namespace of the Polygon JSON-RPC handle."""

    @property
    @abstractmethod
    def block_number(self) -> Awaitable[int]:
        """The latest block number."""

    @abstractmethod
    def get_block_number(self) -> Awaitable[int]:
        """The latest block number, as a call."""

    @abstractmethod
    def get_transaction_count(self, account: ChecksumAddress) -> Awaitable[int]:
        """The nonce of ``account``."""

    @abstractmethod
    def get_balance(self, account: ChecksumAddress) -> Awaitable[int]:
        """The native balance of ``account`` in Wei."""

    @abstractmethod
    def get_block(self, block_identifier: int) -> Awaitable[Mapping[str, object]]:
        """The block numbered ``block_identifier``."""

    @abstractmethod
    def get_logs(self, filter_params: FilterParams) -> Awaitable[Sequence[Mapping[str, object]]]:
        """Fetch the logs matching ``filter_params``."""

    # The one confined escape: web3 overloads ``contract`` and resolves ``functions`` dynamically,
    # so pyright reports 'No overloaded function matches' for every structural proxy type.
    # ``erc20_balance_of`` is the only caller and returns a typed ``int``.
    @abstractmethod
    def contract(self, address: ChecksumAddress, **kwargs: Any) -> Any:
        """Build a contract proxy at ``address``."""


RpcCall = Callable[[EthRpc], Awaitable[T]]
"""One typed operation on a handle's ``eth`` namespace, run against each provider in turn."""


class Web3Handle(Protocol):
    """A Polygon RPC handle: ``AsyncWeb3`` or an injected equivalent."""

    @property
    @abstractmethod
    def eth(self) -> EthRpc:
        """The ``eth`` namespace."""


async def erc20_balance_of(handle: Web3Handle, token: str, holder: str, abi: ABI) -> int:
    """Read ``holder``'s balance of the ERC-20 ``token`` through ``handle``."""
    contract = handle.eth.contract(AsyncWeb3.to_checksum_address(token), abi=abi)
    balance = await contract.functions.balanceOf(AsyncWeb3.to_checksum_address(holder)).call()
    return int(balance)


@dataclass(frozen=True)
class RpcFailure:
    """The outcome of an RPC attempt that exhausted its retries (or was not attempted)."""

    error: Exception | None


def decode_cached_block(cached: str | None) -> dict[str, object] | None:
    """The cached block, or ``None`` when absent or not a JSON object (treated as a miss)."""
    if cached is None:
        return None
    decoded: JsonValue = json.loads(cached)
    if not isinstance(decoded, dict):
        return None
    return dict[str, object](decoded)


def block_timestamp(block: Mapping[str, object]) -> int:
    """The block's integer timestamp; any other type is rejected rather than coerced."""
    timestamp = block["timestamp"]
    if not isinstance(timestamp, int):
        raise ValueError(f"Block timestamp is not an integer: {type(timestamp).__name__}")
    return timestamp
