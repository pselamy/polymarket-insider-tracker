"""A working fake of the documented Data API v2 cursor feed behind ``httpx.MockTransport``.

``FakeV2TradesServer`` models the documented v2 provider: a ``{data, pagination}`` envelope with
opaque cursors that carry the page position, newest-first rows from a synthetic ledger, and an exact
``has_more``/``next_cursor`` pair so an empty or short page never ends a walk on its own. The first
page request sends ``limit``; continuation requests send only ``cursor`` plus the same filters, the
way the walk issues them. ``fail_next`` injects one arbitrary JSON body (for example a malformed
pagination object) for the next physical request. Every wallet and transaction is synthetic and
every request is recorded.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import httpx

from tests.fakes.trades import FakeClock, RecordedRequest

V2_PAGE_LIMIT = 1000
_CURSOR_PREFIX = "cursor-"


def _sort_timestamp(row: object) -> int:
    value = row.get("timestamp", 0) if isinstance(row, dict) else 0
    return value if isinstance(value, int) else 0


@dataclass
class FakeV2TradesServer:
    """A cursor-paged v2 provider; ``page_size`` lets a small test span several pages."""

    clock: Callable[[], float] = field(default_factory=lambda: FakeClock(0.0))
    page_size: int = V2_PAGE_LIMIT
    requests: list[RecordedRequest] = field(default_factory=list[RecordedRequest])
    _ledger: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])
    _injected: list[object] = field(default_factory=list[object])
    _raw_injected: list[bytes] = field(default_factory=list[bytes])

    def publish(self, row: Mapping[str, Any]) -> None:
        """Append one already-v2-shaped row (build with ``v2_row(trade_row(...))``)."""
        self._ledger.append(dict(row))

    def fail_next(self, body: object) -> None:
        """Serve ``body`` (any JSON value) for the next physical request instead of a page."""
        self._injected.append(body)

    def fail_next_raw(self, content: bytes) -> None:
        """Serve ``content`` (a non-JSON body) for the next physical request instead of a page."""
        self._raw_injected.append(content)

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
        if self._injected:
            return httpx.Response(200, json=self._injected.pop(0), request=request)
        if self._raw_injected:
            return httpx.Response(200, content=self._raw_injected.pop(0), request=request)
        return httpx.Response(200, json=self._page_envelope(params), request=request)

    def _page_envelope(self, params: Mapping[str, str]) -> dict[str, object]:
        offset = self._offset(params.get("cursor"))
        limit = (
            self.page_size
            if "cursor" in params
            else min(int(params.get("limit", str(self.page_size))), self.page_size)
        )
        ordered = sorted(
            enumerate(self._ledger), key=lambda item: (-_sort_timestamp(item[1]), -item[0])
        )
        page = [row for _, row in ordered[offset : offset + limit]]
        next_offset = offset + limit
        has_more = next_offset < len(ordered)
        next_cursor = f"{_CURSOR_PREFIX}{next_offset}" if has_more else None
        return {
            "data": page,
            "pagination": {"next_cursor": next_cursor, "has_more": has_more},
        }

    @staticmethod
    def _offset(cursor: str | None) -> int:
        if cursor is None:
            return 0
        return int(cursor.removeprefix(_CURSOR_PREFIX))
