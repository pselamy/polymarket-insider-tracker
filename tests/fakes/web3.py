"""Fakes for the Polygon JSON-RPC boundary exposed through web3's ``AsyncWeb3.eth``."""

from __future__ import annotations

from collections.abc import Awaitable, Generator
from dataclasses import dataclass
from typing import Any

from eth_typing import ChecksumAddress
from web3 import AsyncWeb3
from web3.exceptions import Web3Exception
from web3.types import FilterParams

from polymarket_insider_tracker.profiler.funding import TRANSFER_EVENT_SIGNATURE


def transfer_log(
    *, from_address: str, to_address: str, amount: int, tx_hash: str, block_number: int
) -> dict[str, Any]:
    """Build an ERC-20 ``Transfer`` log entry shaped like web3's ``eth_getLogs`` output."""
    return {
        "topics": [
            TRANSFER_EVENT_SIGNATURE,
            bytes.fromhex(from_address.removeprefix("0x").zfill(64)),
            bytes.fromhex(to_address.removeprefix("0x").zfill(64)),
        ],
        "data": bytes.fromhex(f"{amount:064x}"),
        "transactionHash": bytes.fromhex(tx_hash.removeprefix("0x")),
        "blockNumber": block_number,
    }


def _topic_hex(topic: object) -> str:
    if isinstance(topic, bytes):
        return "0x" + topic.hex()
    assert isinstance(topic, str), f"eth_getLogs topic must be bytes or hex text, got {topic!r}"
    return topic.lower()


def _block(identifier: object) -> int:
    assert isinstance(identifier, int), f"eth_getLogs window must be numeric, got {identifier!r}"
    return identifier


@dataclass(frozen=True)
class LogQuery:
    """One ``eth_getLogs`` request as the provider saw it, with every field narrowed."""

    address: str
    topics: tuple[str | None, ...]
    from_block: int
    to_block: int

    def required_topic(self, index: int) -> str:
        """Return the topic at ``index``, which the request must have filtered on."""
        topic = self.topics[index]
        assert topic is not None, f"eth_getLogs topic {index} was a wildcard"
        return topic


def _narrow_topic(topic: object) -> str | None:
    return None if topic is None else _topic_hex(topic)


def _record(filter_params: FilterParams) -> LogQuery:
    topics = filter_params["topics"]
    return LogQuery(
        address=str(filter_params["address"]),
        topics=tuple(_narrow_topic(topic) for topic in topics),
        from_block=_block(filter_params["fromBlock"]),
        to_block=_block(filter_params["toBlock"]),
    )


class TransferLogIndex:
    """The chain's ERC-20 ``Transfer`` log index as ``eth_getLogs`` exposes it.

    Logs are stored by ``(token contract, recipient topic)`` and answered only when their block
    lies inside the requested window, exactly like a node filters by address, topics, and block
    range. ``fail_chunk`` attaches an error to the window starting at a block so tests can exercise
    partial-history and flaky-provider behaviour. ``queries`` records every window requested; the
    chunking of those windows is the provider-facing contract the tracer must honour.
    """

    def __init__(self) -> None:
        self._transfers: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._faults: dict[int, Exception] = {}
        self.queries: list[LogQuery] = []

    def add_transfer(self, token: str, log: dict[str, Any]) -> None:
        key = (token.lower(), _topic_hex(log["topics"][2]))
        self._transfers.setdefault(key, []).append(log)

    def fail_chunk(self, from_block: int, error: Exception) -> None:
        self._faults[from_block] = error

    async def __call__(self, filter_params: FilterParams) -> list[dict[str, Any]]:
        query = _record(filter_params)
        self.queries.append(query)
        from_block, to_block = query.from_block, query.to_block
        fault = self._faults.get(from_block)
        if fault is not None:
            raise fault
        key = (query.address.lower(), query.required_topic(2))
        return [
            log
            for log in self._transfers.get(key, [])
            if from_block <= log["blockNumber"] <= to_block
        ]


class FakeContractFunction:
    """Bound ERC-20 view call returning a fixed balance."""

    def __init__(self, balance: int) -> None:
        self._balance = balance

    async def call(self) -> int:
        return self._balance


class FakeContractFunctions:
    """The ``functions`` namespace of an ERC-20 contract."""

    def __init__(self, balance: int) -> None:
        self._balance = balance

    def balanceOf(self, address: ChecksumAddress) -> FakeContractFunction:
        _ = address
        return FakeContractFunction(self._balance)


class FakeContract:
    """A web3 contract proxy exposing ``functions.balanceOf``."""

    def __init__(self, balance: int) -> None:
        self.functions = FakeContractFunctions(balance)


class FakeBlockNumber:
    """``eth.block_number`` as web3 exposes it: an awaitable property."""

    def __init__(self, value: int, before_await: Any) -> None:
        self._value = value
        self._before_await = before_await

    def __await__(self) -> Generator[Any, None, int]:
        self._before_await()

        async def resolve() -> int:
            return self._value

        return resolve().__await__()


class FakeEth:
    """The ``AsyncWeb3.eth`` RPC surface the profiler uses, with fault injection.

    ``fail_count`` makes the first N calls raise (transient failures); ``always_fail`` makes every
    call raise. ``call_count`` is the number of RPC calls attempted, the provider-facing contract
    that retry and failover tests assert.
    """

    def __init__(
        self,
        *,
        transaction_count: int = 42,
        balance_wei: int = 1000000000000000000,
        block_timestamp: int = 1704369600,
        block_number: int = 50000000,
        token_balance: int = 5000000,
        fail_count: int = 0,
        always_fail: bool = False,
        logs: TransferLogIndex | None = None,
    ) -> None:
        self._transaction_count = transaction_count
        self._balance_wei = balance_wei
        self._block_timestamp = block_timestamp
        self._block_number = block_number
        self._token_balance = token_balance
        self._fail_count = fail_count
        self._always_fail = always_fail
        self._logs = logs
        self.call_count = 0

    def _count_call(self) -> None:
        self.call_count += 1
        if self._always_fail or self.call_count <= self._fail_count:
            raise Web3Exception("RPC error")

    async def get_transaction_count(self, address: str) -> int:
        _ = address
        self._count_call()
        return self._transaction_count

    async def get_balance(self, address: str) -> int:
        _ = address
        self._count_call()
        return self._balance_wei

    async def get_block(self, block_identifier: Any) -> dict[str, Any]:
        _ = block_identifier
        self._count_call()
        return {"timestamp": self._block_timestamp, "number": self._block_number}

    @property
    def block_number(self) -> Awaitable[int]:
        return FakeBlockNumber(self._block_number, self._count_call)

    async def get_block_number(self) -> int:
        self._count_call()
        return self._block_number

    def contract(self, address: ChecksumAddress, **kwargs: Any) -> FakeContract:
        _ = (address, kwargs)
        return FakeContract(self._token_balance)

    async def get_logs(self, filter_params: FilterParams) -> list[dict[str, Any]]:
        self._count_call()
        if self._logs is None:
            return []
        return await self._logs(filter_params)


class FakeAsyncWeb3:
    """An ``AsyncWeb3`` whose ``eth`` namespace is a ``FakeEth``."""

    def __init__(self, eth: FakeEth | None = None) -> None:
        self.eth = eth or FakeEth()

    @staticmethod
    def to_checksum_address(value: str) -> str:
        return AsyncWeb3.to_checksum_address(value)
