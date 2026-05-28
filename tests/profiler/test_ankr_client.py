"""Tests for the Ankr Advanced API client."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from polymarket_insider_tracker.profiler.ankr_client import (
    AnkrClient,
    _parse_transaction,
    _to_int,
)

VALID_ADDRESS = "0x000000000000000000000000000000000000dEaD"


def _make_response(payload: dict[str, Any], status: int = 200) -> MagicMock:
    """Build a minimal response stand-in for the AsyncClient.post mock."""
    resp = MagicMock()
    resp.status_code = status
    resp.json = MagicMock(return_value=payload)
    if status >= 400:
        resp.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError(
                "boom", request=MagicMock(), response=MagicMock(status_code=status)
            )
        )
    else:
        resp.raise_for_status = MagicMock(return_value=None)
    return resp


class TestToInt:
    def test_int_passthrough(self) -> None:
        assert _to_int(42) == 42

    def test_hex_lower(self) -> None:
        assert _to_int("0x10") == 16

    def test_hex_upper(self) -> None:
        assert _to_int("0X10") == 16

    def test_decimal_string(self) -> None:
        assert _to_int("123") == 123


class TestParseTransaction:
    def test_full_payload(self) -> None:
        raw = {
            "hash": "0xabc",
            "blockNumber": "0xcecea0",
            "from": "0xAAA",
            "to": "0xBBB",
            "value": "0x16345785d8a0000",
            "gas": "0x5208",
            "gasUsed": "0x5208",
            "gasPrice": "0x3b9aca00",
            "timestamp": "0x60a1b2c3",
        }
        tx = _parse_transaction(raw)
        assert tx is not None
        assert tx.hash == "0xabc"
        assert tx.block_number == int("0xcecea0", 16)
        assert tx.from_address == "0xaaa"
        assert tx.to_address == "0xbbb"
        assert tx.gas_used == 0x5208
        assert tx.gas_price == Decimal(0x3B9ACA00)
        assert tx.timestamp == datetime.fromtimestamp(int("0x60a1b2c3", 16), tz=UTC)

    def test_decimal_timestamp(self) -> None:
        """Some Ankr responses encode timestamp as a decimal int, not hex."""
        raw = {
            "hash": "0x",
            "blockNumber": "1",
            "from": "0xa",
            "to": "0xb",
            "value": "0",
            "gas": "0",
            "gasPrice": "0",
            "timestamp": 1704067200,
        }
        tx = _parse_transaction(raw)
        assert tx is not None
        assert tx.timestamp.year == 2024

    def test_to_address_null(self) -> None:
        raw = {
            "hash": "0x",
            "blockNumber": "1",
            "from": "0xa",
            "to": None,
            "value": "0",
            "gas": "0",
            "gasPrice": "0",
            "timestamp": "0x1",
        }
        tx = _parse_transaction(raw)
        assert tx is not None
        assert tx.to_address is None

    def test_missing_timestamp_returns_none(self) -> None:
        """Old txs without timestamp can't compute wallet age — soft-miss."""
        raw = {"hash": "0x", "blockNumber": "1", "from": "0xa", "to": "0xb"}
        assert _parse_transaction(raw) is None

    def test_garbage_payload_returns_none(self) -> None:
        assert _parse_transaction({"blockNumber": "not-a-number"}) is None


class TestAnkrClient:
    def test_init_rejects_empty_key(self) -> None:
        with pytest.raises(ValueError):
            AnkrClient(api_key="")

    def test_url_uses_path_encoded_key(self) -> None:
        client = AnkrClient(api_key="abc123", http_client=MagicMock())
        assert client._url.endswith("/abc123")

    @pytest.mark.asyncio
    async def test_get_first_transaction_returns_parsed(self) -> None:
        http = MagicMock()
        http.post = AsyncMock(
            return_value=_make_response(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "transactions": [
                            {
                                "hash": "0xfeed",
                                "blockNumber": "0x10",
                                "from": "0xaaa",
                                "to": "0xbbb",
                                "value": "0x0",
                                "gas": "0x5208",
                                "gasPrice": "0x1",
                                "timestamp": "0x60a1b2c3",
                            }
                        ]
                    },
                }
            )
        )
        client = AnkrClient(api_key="key", http_client=http)
        tx = await client.get_first_transaction(VALID_ADDRESS)
        assert tx is not None
        assert tx.hash == "0xfeed"
        # Verify request shape: pageSize=1, descOrder=False, blockchain=polygon
        sent = http.post.call_args.kwargs["json"]["params"]
        assert sent["pageSize"] == 1
        assert sent["descOrder"] is False
        assert sent["blockchain"] == "polygon"
        assert sent["address"] == VALID_ADDRESS

    @pytest.mark.asyncio
    async def test_empty_transactions_returns_none(self) -> None:
        http = MagicMock()
        http.post = AsyncMock(
            return_value=_make_response(
                {"jsonrpc": "2.0", "id": 1, "result": {"transactions": []}}
            )
        )
        client = AnkrClient(api_key="key", http_client=http)
        assert await client.get_first_transaction(VALID_ADDRESS) is None

    @pytest.mark.asyncio
    async def test_jsonrpc_error_returns_none(self) -> None:
        http = MagicMock()
        http.post = AsyncMock(
            return_value=_make_response(
                {"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "rate limit"}}
            )
        )
        client = AnkrClient(api_key="key", http_client=http)
        assert await client.get_first_transaction(VALID_ADDRESS) is None

    @pytest.mark.asyncio
    async def test_http_error_returns_none(self) -> None:
        http = MagicMock()
        http.post = AsyncMock(side_effect=httpx.ConnectError("network down"))
        client = AnkrClient(api_key="key", http_client=http)
        # Must not raise — caller relies on this for graceful fallback.
        assert await client.get_first_transaction(VALID_ADDRESS) is None

    @pytest.mark.asyncio
    async def test_aclose_only_closes_owned_client(self) -> None:
        http = MagicMock()
        http.aclose = AsyncMock()
        client = AnkrClient(api_key="key", http_client=http)
        await client.aclose()
        # We injected the client; AnkrClient must NOT close it for us.
        http.aclose.assert_not_called()
