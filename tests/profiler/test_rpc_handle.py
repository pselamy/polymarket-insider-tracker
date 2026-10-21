"""``erc20_balance_of`` rejects contract and balance shapes it cannot trust."""

import pytest
from eth_typing import ABI, ChecksumAddress

from polymarket_insider_tracker.profiler.rpc_handle import erc20_balance_of
from tests.fakes.web3 import FakeAsyncWeb3, FakeEth

TOKEN = "0x" + "11" * 20
HOLDER = "0x" + "22" * 20
NO_ABI: ABI = []


class _TextBalanceCall:
    async def call(self) -> str:
        return "7"


class _TextBalanceFunctions:
    def balanceOf(self, holder: ChecksumAddress) -> _TextBalanceCall:
        _ = holder
        return _TextBalanceCall()


class _TextBalanceContract:
    functions = _TextBalanceFunctions()


async def test_integer_balance_is_returned() -> None:
    handle = FakeAsyncWeb3(FakeEth(token_balance=42))
    assert await erc20_balance_of(handle, TOKEN, HOLDER, NO_ABI) == 42


async def test_proxy_without_functions_is_rejected() -> None:
    handle = FakeAsyncWeb3(FakeEth(contract_proxy=object()))
    with pytest.raises(TypeError, match="no functions namespace"):
        await erc20_balance_of(handle, TOKEN, HOLDER, NO_ABI)


async def test_non_integer_balance_is_rejected() -> None:
    handle = FakeAsyncWeb3(FakeEth(contract_proxy=_TextBalanceContract()))
    with pytest.raises(TypeError, match="did not return an integer"):
        await erc20_balance_of(handle, TOKEN, HOLDER, NO_ABI)
