"""Ankr Advanced API client for indexed wallet history queries.

Standard Polygon RPC nodes can answer ``eth_getTransactionCount`` and
``eth_getBalance`` cheaply, but they cannot return a wallet's *first*
transaction without scanning the full chain. Ankr's Advanced API exposes
``ankr_getTransactionsByAddress`` which is backed by an indexer and returns
ordered transactions in a single call.

We use this purely to back ``PolygonClient.get_first_transaction`` so the
``wallet_age_hours`` field on ``WalletProfile`` becomes populated. Failure
here is non-fatal: the caller falls back to ``None`` and the freshness
heuristic continues to work off nonce alone.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx

from polymarket_insider_tracker.profiler.models import Transaction

logger = logging.getLogger(__name__)

DEFAULT_ANKR_ENDPOINT = "https://rpc.ankr.com/multichain"
DEFAULT_BLOCKCHAIN = "polygon"
DEFAULT_TIMEOUT_SECONDS = 15.0


class AnkrClientError(Exception):
    """Raised when the Ankr Advanced API returns an unrecoverable error."""


class AnkrClient:
    """Thin async client over Ankr's ``ankr_getTransactionsByAddress``.

    The full Advanced API is much larger; we only need first-tx lookup,
    so this client deliberately stays narrow. Adding more methods is fine
    when a concrete need shows up.
    """

    def __init__(
        self,
        api_key: str,
        *,
        endpoint: str = DEFAULT_ANKR_ENDPOINT,
        blockchain: str = DEFAULT_BLOCKCHAIN,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("AnkrClient requires a non-empty api_key")
        self._api_key = api_key
        self._url = f"{endpoint.rstrip('/')}/{api_key}"
        self._blockchain = blockchain
        self._timeout = timeout_seconds
        self._owned_client = http_client is None
        self._http = http_client or httpx.AsyncClient(timeout=timeout_seconds)

    async def aclose(self) -> None:
        if self._owned_client:
            await self._http.aclose()

    async def __aenter__(self) -> AnkrClient:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.aclose()

    async def get_first_transaction(self, address: str) -> Transaction | None:
        """Return the chronologically first transaction for ``address``.

        Returns ``None`` when the wallet has no transactions, when Ankr
        responds with an empty result, or when the API errors. Errors are
        logged at WARNING — they should not crash the calling pipeline.
        """
        payload = {
            "jsonrpc": "2.0",
            "method": "ankr_getTransactionsByAddress",
            "params": {
                "blockchain": self._blockchain,
                "address": address,
                "pageSize": 1,
                "descOrder": False,
            },
            "id": 1,
        }
        try:
            response = await self._http.post(self._url, json=payload)
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, ValueError) as e:
            logger.warning("Ankr first-tx lookup failed for %s: %s", address, e)
            return None

        if "error" in data:
            logger.warning(
                "Ankr returned error for %s: %s", address, data["error"]
            )
            return None

        transactions = data.get("result", {}).get("transactions") or []
        if not transactions:
            return None

        return _parse_transaction(transactions[0])


def _parse_transaction(raw: dict[str, Any]) -> Transaction | None:
    """Convert an Ankr transaction dict into our internal ``Transaction``.

    Ankr returns hex-encoded ``blockNumber``/``gas``/``gasPrice``/``value``
    plus a top-level ``timestamp`` (decimal seconds, sometimes hex). Some
    fields are missing for very old txs; we treat any parse failure as a
    soft miss rather than crashing.
    """
    try:
        block_number = _to_int(raw["blockNumber"])
        gas_used = _to_int(raw.get("gasUsed") or raw.get("gas") or "0x0")
        gas_price = Decimal(_to_int(raw.get("gasPrice") or "0x0"))
        value = Decimal(_to_int(raw.get("value") or "0x0"))
        timestamp_raw = raw.get("timestamp")
        if timestamp_raw is None:
            return None
        ts = datetime.fromtimestamp(_to_int(timestamp_raw), tz=UTC)
        return Transaction(
            hash=str(raw.get("hash") or ""),
            block_number=block_number,
            timestamp=ts,
            from_address=str(raw.get("from") or "").lower(),
            to_address=(str(raw["to"]).lower() if raw.get("to") else None),
            value=value,
            gas_used=gas_used,
            gas_price=gas_price,
        )
    except (KeyError, ValueError, TypeError) as e:
        logger.warning("Failed to parse Ankr transaction payload: %s", e)
        return None


def _to_int(v: Any) -> int:
    """Coerce hex (``0x...``) or decimal strings/ints into ``int``."""
    if isinstance(v, int):
        return v
    s = str(v)
    if s.startswith("0x") or s.startswith("0X"):
        return int(s, 16)
    return int(s)
