"""T027 Part B per-user history walks (research.md R09a, protocol v2) and their recording.

One walk per address: the documented per-user trades query with ``user`` set to the proxy address,
``taker_only=false``, ``start=1`` and a fixed ``end`` on every page; the first page asks
``limit=1000``, every later page sends only the previous response's ``pagination.next_cursor``
(never ``offset``). A walk stops at the terminal page (``next_cursor = null``), on the first error
(no retry: an errored, retried or interrupted walk is incomplete by rule), or when its page cap or
the shared request budget is spent (also incomplete). Requests are paced to at most 2 per second.

Each request becomes one JSON line (``PageRecord`` fields): the exact parameters, HTTP status,
error class, paging envelope, body SHA-256 and, per valid row, its observation identity and
timestamp. The raw body is kept under ``raw_dir`` for audit and is not part of the receipt.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO

import httpx

from polymarket_insider_tracker.endpoints import OFFICIAL_V2_TRADES_URL
from polymarket_insider_tracker.ingestor.trade_row_v2 import parse_v2_trade_row
from polymarket_insider_tracker.ingestor.trade_rows import TradeObservation
from polymarket_insider_tracker.shapes import object_list
from scripts.spec004_evidence.json_shapes import is_object

PAGE_LIMIT = "1000"
MIN_INTERVAL_SECONDS = 0.5
REQUEST_TIMEOUT_SECONDS = 30.0
TOTAL_BUDGET = 1000


@dataclass
class Budget:
    """The shared request budget of the whole Part B run (both reads)."""

    remaining: int

    def take(self) -> bool:
        if self.remaining <= 0:
            return False
        self.remaining -= 1
        return True


@dataclass
class Pacer:
    """At most one request start per ``MIN_INTERVAL_SECONDS`` (<= 2 requests/s)."""

    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    last: float | None = None

    async def wait(self) -> None:
        delay = 0.0 if self.last is None else self.last + MIN_INTERVAL_SECONDS - self.clock()
        if delay > 0:
            await self.sleep(delay)
        self.last = self.clock()


def first_params(address: str, end: int) -> dict[str, str]:
    return {
        "user": address,
        "taker_only": "false",
        "start": "1",
        "end": str(end),
        "limit": PAGE_LIMIT,
    }


def continuation_params(address: str, end: int, cursor: str) -> dict[str, str]:
    params = first_params(address, end)
    del params["limit"]
    return params | {"cursor": cursor}


@dataclass
class Page:
    """One recorded request; ``record`` is the JSON line ``t027_partb.page_record`` reads."""

    address: str
    read: int
    page: int
    requested_at: float
    params: dict[str, str]
    status: int | None = None
    error: str | None = None
    next_cursor: str | None = None
    has_more: bool | None = None
    body_sha256: str | None = None
    rows: list[list[object]] = field(default_factory=list[list[object]])
    invalid_rows: int = 0

    def record(self) -> dict[str, object]:
        return dict(self.__dict__)

    def continues(self) -> bool:
        return self.error is None and self.status == 200 and self.next_cursor is not None


def paging(value: object) -> tuple[str | None, bool] | None:
    """``(next_cursor, has_more)`` when both have their documented types, else ``None``."""
    if not is_object(value):
        return None
    cursor, more = value.get("next_cursor"), value.get("has_more")
    if not isinstance(more, bool) or not (cursor is None or isinstance(cursor, str)):
        return None
    return cursor, more


def pagination_of(body: object) -> tuple[list[object], str | None, bool] | None:
    """The documented ``{data, pagination}`` envelope, or ``None`` when it cannot be trusted."""
    if not is_object(body):
        return None
    rows, envelope = object_list(body.get("data")), paging(body.get("pagination"))
    if rows is None or envelope is None:
        return None
    return rows, *envelope


def own_row(parsed: object, address: str, end: int) -> TradeObservation | None:
    """A valid row of the walked address at or before ``end``; anything else is invalid.

    An invalid, foreign or out-of-range row makes the walk incomplete, so a misread ``end``
    unit or a provider filter that is not applied can never pass as a complete history.
    """
    if not isinstance(parsed, TradeObservation) or parsed.provider_timestamp > end:
        return None
    return parsed if parsed.event.wallet_address.lower() == address else None


def absorb_rows(page: Page, rows: Sequence[object], now: int) -> None:
    end = int(page.params["end"])
    for raw in rows:
        parsed = own_row(parse_v2_trade_row(raw, now=now), page.address, end)
        if parsed is None:
            page.invalid_rows += 1
            continue
        page.rows.append([parsed.identity, parsed.provider_timestamp])


def absorb(page: Page, response: httpx.Response, now: int) -> None:
    page.status = response.status_code
    page.body_sha256 = hashlib.sha256(response.content).hexdigest()
    if response.status_code != 200:
        page.error = f"http-{response.status_code}"
        return
    try:
        envelope = pagination_of(json.loads(response.content))
    except ValueError:
        envelope = None
    if envelope is None:
        page.error = "untrusted-envelope"
        return
    rows, page.next_cursor, page.has_more = envelope
    absorb_rows(page, rows, now)


@dataclass
class Walker:
    """Walks addresses for one read and appends every request to ``out``."""

    client: httpx.AsyncClient
    read: int
    end: int
    budget: Budget
    out: IO[str]
    raw_dir: Path | None = None
    url: str = OFFICIAL_V2_TRADES_URL
    pacer: Pacer = field(default_factory=Pacer)
    wall_clock: Callable[[], float] = time.time

    async def request(self, page: Page) -> None:
        try:
            response = await self.client.get(
                self.url, params=page.params, timeout=REQUEST_TIMEOUT_SECONDS
            )
        except httpx.HTTPError as exc:
            page.error = type(exc).__name__
            return
        absorb(page, response, int(self.wall_clock()))
        self.keep_raw(page, response.content)

    def keep_raw(self, page: Page, content: bytes) -> None:
        if self.raw_dir is None:
            return
        target = self.raw_dir / f"read{self.read}" / page.address / f"{page.page}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)

    def params_for(self, address: str, cursor: str | None) -> dict[str, str]:
        if cursor is None:
            return first_params(address, self.end)
        return continuation_params(address, self.end, cursor)

    async def walk(self, address: str, page_cap: int) -> None:
        """Walk one address until the terminal page, an error, its page cap or the budget."""
        cursor: str | None = None
        for index in range(page_cap):
            if not self.budget.take():
                return
            await self.pacer.wait()
            page = Page(
                address, self.read, index, self.wall_clock(), self.params_for(address, cursor)
            )
            await self.request(page)
            self.out.write(json.dumps(page.record(), sort_keys=True) + "\n")
            self.out.flush()
            if not page.continues():
                return
            cursor = page.next_cursor
