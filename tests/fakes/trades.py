"""A working fake of the documented public trades query, served through ``httpx.MockTransport``.

``FakeTradesServer`` models the provider behavior recorded in
``specs/001-supported-trade-ingestion/evidence/feasibility.md``: it serves pages newest-first from a
synthetic ledger, fills each page to ``limit`` when enough rows exist, honours ``offset`` and
``takerOnly``, ignores ``start``/``end`` as filters, and answers an identical URL with the identical
body until the ledger changes, the way the provider's shared edge cache does. Faults (throttling,
server errors, timeouts, non-list bodies, terminal statuses) are injected as server state for the
next requests. Every wallet is synthetic and every request is recorded.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

import httpx

PAGE_LIMIT = 10_000
MAX_OFFSET = 10_000
DEFAULT_LIMIT = 100
CACHE_CONTROL = "public, max-age=300"

FaultKind = Literal["throttle", "server-error", "timeout", "non-list", "invalid-json", "terminal"]


def synthetic_wallet(index: int) -> str:
    """Return a deterministic synthetic 40-hex wallet address; never a real one."""
    return "0x" + f"{index:040x}"


def synthetic_transaction(index: int) -> str:
    """Return a deterministic synthetic 64-hex transaction hash."""
    return "0x" + f"{index:064x}"


def trade_row(
    *,
    timestamp: int,
    transaction: int = 0,
    wallet: int = 0,
    market: str = "0xmarket",
    asset: str = "asset-yes",
    side: str = "BUY",
    price: str | float = "0.5",
    size: str | float = "10",
    outcome: str | None = "Yes",
    outcome_index: int | None = 0,
) -> dict[str, Any]:
    """Build one provider-shaped trade row with synthetic identities.

    ``outcome=None`` and ``outcome_index=None`` omit the optional fields the way the provider does
    for a small share of rows.
    """
    row: dict[str, Any] = {
        "proxyWallet": synthetic_wallet(wallet),
        "side": side,
        "asset": asset,
        "conditionId": market,
        "size": size,
        "price": price,
        "timestamp": timestamp,
        "title": "Synthetic market?",
        "slug": "synthetic-market",
        "icon": "",
        "eventSlug": "synthetic-event",
        "name": "",
        "pseudonym": "",
        "bio": "",
        "profileImage": "",
        "profileImageOptimized": "",
        "transactionHash": synthetic_transaction(transaction),
    }
    if outcome is not None:
        row["outcome"] = outcome
    if outcome_index is not None:
        row["outcomeIndex"] = outcome_index
    return row


class FakeClock:
    """A manually advanced clock whose ``sleep`` advances time instead of waiting."""

    def __init__(self, now: float) -> None:
        self.now = now
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds
        await asyncio.sleep(0)


@dataclass(frozen=True)
class Fault:
    """What the server does to one upcoming request."""

    kind: FaultKind
    status: int | None = None
    retry_after: str | None = None


def throttled(retry_after: str | None = "1") -> Fault:
    """HTTP 429 with an optional ``Retry-After`` header."""
    return Fault("throttle", status=429, retry_after=retry_after)


def server_error(status: int = 503) -> Fault:
    """HTTP 5xx."""
    return Fault("server-error", status=status)


def timeout() -> Fault:
    """A request that never answers within the client timeout."""
    return Fault("timeout")


def non_list_body() -> Fault:
    """HTTP 200 whose JSON body is an object instead of a list."""
    return Fault("non-list", status=200)


def invalid_json() -> Fault:
    """HTTP 200 whose body is not JSON at all."""
    return Fault("invalid-json", status=200)


def terminal(status: int = 404) -> Fault:
    """A non-retryable HTTP status."""
    return Fault("terminal", status=status)


@dataclass(frozen=True)
class RecordedRequest:
    """One request the server received."""

    url: str
    params: dict[str, str]
    at: float
    headers: dict[str, str]
    timeout: dict[str, float | None] | None


def _sort_timestamp(row: object) -> int:
    """Order malformed rows as if they were oldest, the way a provider would still serve them."""
    value = row.get("timestamp", 0) if isinstance(row, dict) else 0
    return value if isinstance(value, int) else 0


def _transaction_of(row: object) -> object:
    return row.get("transactionHash") if isinstance(row, dict) else None


@dataclass(frozen=True)
class _LedgerEntry:
    row: object
    taker: bool
    sequence: int


@dataclass
class _CachedBody:
    version: int
    body: bytes
    hits: int = 0


@dataclass
class FakeTradesServer:
    """The provider behind ``httpx.MockTransport``; see the module docstring."""

    clock: Callable[[], float] = field(default_factory=lambda: FakeClock(0.0))
    page_limit: int = PAGE_LIMIT
    requests: list[RecordedRequest] = field(default_factory=list[RecordedRequest])
    faults: list[Fault] = field(default_factory=list[Fault])
    _ledger: list[_LedgerEntry] = field(default_factory=list[_LedgerEntry])
    _cache: dict[str, _CachedBody] = field(default_factory=dict[str, _CachedBody])
    _version: int = 0

    def publish(self, row: object, *, taker: bool = True) -> None:
        """Append one raw row (any JSON value) to the ledger; a repeat is served as a repeat."""
        self._ledger.append(_LedgerEntry(row, taker, len(self._ledger)))
        self._version += 1

    def retract(self, transaction_hash: str) -> int:
        """Drop every ledger row of ``transaction_hash``, modelling a provider rewrite."""
        before = len(self._ledger)
        self._ledger = [
            entry for entry in self._ledger if _transaction_of(entry.row) != transaction_hash
        ]
        self._version += 1
        return before - len(self._ledger)

    def fail_next(self, fault: Fault, times: int = 1) -> None:
        """Make the next ``times`` requests produce ``fault`` before pages are served again."""
        self.faults.extend([fault] * times)

    @property
    def row_count(self) -> int:
        return len(self._ledger)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=self.transport())

    def __call__(self, request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params.items())
        self.requests.append(
            RecordedRequest(
                url=str(request.url),
                params=params,
                at=self.clock(),
                headers=dict(request.headers.items()),
                timeout=request.extensions.get("timeout"),
            )
        )
        if self.faults:
            return self._fail(self.faults.pop(0), request)
        return self._serve(request, params)

    @staticmethod
    def _fail(fault: Fault, request: httpx.Request) -> httpx.Response:
        match fault.kind:
            case "timeout":
                raise httpx.ReadTimeout("fake provider did not answer", request=request)
            case "non-list":
                return httpx.Response(200, json={"error": "unexpected shape"}, request=request)
            case "invalid-json":
                return httpx.Response(200, text="<html>not json</html>", request=request)
            case "throttle":
                headers = {} if fault.retry_after is None else {"Retry-After": fault.retry_after}
                return httpx.Response(429, headers=headers, text="throttled", request=request)
            case _:
                return httpx.Response(fault.status or 500, text="failure", request=request)

    def _serve(self, request: httpx.Request, params: dict[str, str]) -> httpx.Response:
        offset = int(params.get("offset", "0"))
        if offset > MAX_OFFSET:
            return httpx.Response(
                400, text="offset exceeds the documented maximum", request=request
            )
        cached = self._cache.get(str(request.url))
        if cached is not None and cached.version == self._version:
            cached.hits += 1
            return self._page_response(request, cached.body, "HIT", cached.hits * 2)
        body = self._page_body(params, offset)
        self._cache[str(request.url)] = _CachedBody(self._version, body)
        return self._page_response(request, body, "MISS", None)

    def _page_body(self, params: dict[str, str], offset: int) -> bytes:
        """Serve the page at ``offset``.

        ``page_limit`` shrinks the provider's 10,000-row page so saturation is testable; the
        documented recovery offset then addresses the second page of that smaller size.
        """
        limit = min(int(params.get("limit", str(DEFAULT_LIMIT))), self.page_limit)
        if offset == MAX_OFFSET:
            offset = self.page_limit
        taker_only = params.get("takerOnly", "true") == "true"
        entries = [entry for entry in self._ledger if entry.taker or not taker_only]
        ordered = sorted(entries, key=lambda entry: (-_sort_timestamp(entry.row), -entry.sequence))
        page = [entry.row for entry in ordered[offset : offset + limit]]
        return json.dumps(page).encode()

    @staticmethod
    def _page_response(
        request: httpx.Request, body: bytes, cache_status: str, age: int | None
    ) -> httpx.Response:
        headers = {
            "content-type": "application/json",
            "cache-control": CACHE_CONTROL,
            "cf-cache-status": cache_status,
        }
        if age is not None:
            headers["age"] = str(age)
        return httpx.Response(200, content=body, headers=headers, request=request)

    def body_sha256(self, params: dict[str, str]) -> str:
        """Digest of the body the server would serve now for ``params`` (offset ``0`` by default)."""
        return hashlib.sha256(self._page_body(params, int(params.get("offset", "0")))).hexdigest()
