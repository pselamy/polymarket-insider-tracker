"""Concrete Fake implementations of Web3 and Eth interfaces for tests."""

from typing import Any

from web3 import AsyncWeb3
from web3.exceptions import Web3Exception


class FakeContractFunction:
    """Fake contract function providing an awaitable call method."""

    def __init__(self, return_value: int = 5000000) -> None:
        self._return_value = return_value

    async def call(self) -> int:
        return self._return_value


class FakeContractFunctions:
    """Fake contract functions namespace."""

    def __init__(self, balance: int = 5000000) -> None:
        self._balance = balance

    def balanceOf(self, address: str) -> FakeContractFunction:
        _ = address
        return FakeContractFunction(self._balance)


class FakeContract:
    """Fake web3 contract."""

    def __init__(self, balance: int = 5000000) -> None:
        self.functions = FakeContractFunctions(balance)


class FakeBlockNumber:
    """Awaitable and callable block number value."""

    def __init__(self, value: int = 50000000, fail_hook: Any = None) -> None:
        self._value = value
        self._fail_hook = fail_hook

    def __await__(self) -> Any:
        if self._fail_hook is not None:
            self._fail_hook()

        async def _coro() -> int:
            return self._value

        return _coro().__await__()

    def __call__(self, *args: Any, **kwargs: Any) -> "FakeBlockNumber":
        _ = (args, kwargs)
        return self

    def __int__(self) -> int:
        return self._value


class FakeEth:
    """Fake web3 eth namespace."""

    def __init__(
        self,
        transaction_count: int = 42,
        balance_wei: int = 1000000000000000000,
        block_timestamp: int = 1704369600,
        block_number: int = 50000000,
        token_balance: int = 5000000,
        fail_count: int = 0,
        always_fail: bool = False,
        fail_exception: Exception | None = None,
        logs: list[Any] | None = None,
        logs_handler: Any = None,
    ) -> None:
        self._transaction_count = transaction_count
        self._balance_wei = balance_wei
        self._block_timestamp = block_timestamp
        self._block_number = block_number
        self._token_balance = token_balance
        self._fail_count = fail_count
        self._always_fail = always_fail
        self._fail_exception = fail_exception or Web3Exception("RPC error")
        self._logs = logs or []
        self._logs_handler = logs_handler
        self.call_count: int = 0

    def _maybe_fail(self) -> None:
        self.call_count += 1
        if self._always_fail or self.call_count <= self._fail_count:
            raise self._fail_exception

    async def get_transaction_count(self, address: str) -> int:
        _ = address
        self._maybe_fail()
        return self._transaction_count

    async def get_balance(self, address: str) -> int:
        _ = address
        self._maybe_fail()
        return self._balance_wei

    async def get_block(self, block_identifier: Any) -> dict[str, Any]:
        _ = block_identifier
        self._maybe_fail()
        return {"timestamp": self._block_timestamp, "number": self._block_number}

    @property
    def block_number(self) -> FakeBlockNumber:
        return FakeBlockNumber(self._block_number, fail_hook=self._maybe_fail)

    def contract(self, address: str, abi: Any) -> FakeContract:
        _ = (address, abi)
        return FakeContract(self._token_balance)

    async def get_logs(self, filter_params: dict[str, Any]) -> list[Any]:
        self._maybe_fail()
        if self._logs_handler is not None:
            return await self._logs_handler(filter_params)
        return list(self._logs)


class FakeAsyncWeb3:
    """Fake AsyncWeb3 instance."""

    def __init__(self, eth: FakeEth | None = None) -> None:
        self.eth = eth or FakeEth()

    @staticmethod
    def to_checksum_address(value: str) -> str:
        return AsyncWeb3.to_checksum_address(value)
