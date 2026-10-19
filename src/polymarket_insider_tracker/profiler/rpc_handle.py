"""Structural type of the web3 handle ``PolygonClient`` drives.

``AsyncWeb3`` satisfies these protocols; so does the in-memory fake used by the tests, which is
what lets the client accept an injected handle without any typing bypass.
"""

from __future__ import annotations

from collections.abc import Awaitable
from typing import Any, Protocol

from eth_typing import ChecksumAddress


class EthRpc(Protocol):
    """The ``eth`` namespace of the Polygon JSON-RPC handle."""

    @property
    def block_number(self) -> Awaitable[int]: ...

    def get_logs(self, filter_params: Any) -> Awaitable[list[Any]]: ...

    # web3 builds contract proxies dynamically (``functions`` is resolved by ``__getattr__``) and
    # overloads ``contract``, so no structural type for the proxy is satisfiable by AsyncEth.
    def contract(self, address: ChecksumAddress, **kwargs: Any) -> Any: ...


class Web3Handle(Protocol):
    """A Polygon RPC handle: ``AsyncWeb3`` or an injected equivalent."""

    @property
    def eth(self) -> EthRpc: ...
