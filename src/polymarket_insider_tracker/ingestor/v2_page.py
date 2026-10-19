"""Documented Data API v2 page request and cursor envelope (unwired).

Data API v2 serves ``/v2/trades`` behind a ``{data, pagination}`` envelope with cursor-only
traversal. This module owns the documented first-page request (``limit``, explicit ``taker_only``,
and the cycle's fixed ``start``/``end``), the continuation request (only ``cursor`` plus those same
filters; the cursor carries the page size, so ``limit`` is not resent), and the strict reading of
``pagination.next_cursor``/``has_more`` that decides whether the feed may be trusted to continue.

A missing, malformed, or self-contradictory pagination object is an actionable source failure: it
never proves continuity, and a caller must not treat it as the end of a walk. The transport is
injected, so nothing here opens a connection. See the approved slice-001 v2 amendment, items 2-3.

Field names and cursor semantics are cited from the official documentation:
https://docs.polymarket.com/api-reference/feeds/list-trades and
https://docs.polymarket.com/api-reference/data-api/overview
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Protocol, cast

V2_PAGE_LIMIT: Final[int] = 1000

__all__ = [
    "V2_PAGE_LIMIT",
    "V2AcquisitionError",
    "V2EnvelopeError",
    "V2Page",
    "V2PageRequest",
    "V2PageTransport",
    "V2Pagination",
    "V2PaginationError",
    "V2SourceError",
    "V2TerminalError",
    "V2TransientError",
    "V2TransportError",
    "parse_v2_page",
]


class V2AcquisitionError(Exception):
    """Base for v2 acquisition failures; messages never carry a cursor value."""


class V2SourceError(V2AcquisitionError):
    """The response cannot be trusted to continue; it never proves continuity."""


class V2EnvelopeError(V2SourceError):
    """The response is not the documented ``{data, pagination}`` envelope."""


class V2PaginationError(V2SourceError):
    """``pagination`` is missing, malformed, contradicts itself, or repeats a cursor."""


class V2TransportError(V2AcquisitionError):
    """A physical page request failed."""


class V2TransientError(V2TransportError):
    """A retryable transport failure, with any documented ``Retry-After`` delay."""

    def __init__(self, message: str, *, retry_after_seconds: float = 0.0) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class V2TerminalError(V2TransportError):
    """A non-retryable transport failure."""


class V2PageTransport(Protocol):
    """One physical page fetch: the raw decoded body, or a transport error."""

    async def fetch(self, request: V2PageRequest) -> object: ...


@dataclass(frozen=True)
class V2PageRequest:
    """One documented v2 page request; ``cursor`` is ``None`` only on the first page."""

    taker_only: bool
    start: int
    end: int
    cursor: str | None = None

    @property
    def is_continuation(self) -> bool:
        return self.cursor is not None

    def params(self) -> dict[str, str]:
        """The documented query parameters; ``offset`` is never sent."""
        params = {
            "taker_only": "true" if self.taker_only else "false",
            "start": str(self.start),
            "end": str(self.end),
        }
        if self.cursor is None:
            params["limit"] = str(V2_PAGE_LIMIT)
        else:
            params["cursor"] = self.cursor
        return params


@dataclass(frozen=True)
class V2Pagination:
    """The exact paging envelope: follow ``next_cursor`` until it is ``null``."""

    next_cursor: str | None
    has_more: bool


@dataclass(frozen=True)
class V2Page:
    """One trusted page: the request, its raw rows, and its paging envelope."""

    request: V2PageRequest
    rows: tuple[object, ...]
    pagination: V2Pagination
    received_at: float


def parse_v2_page(request: V2PageRequest, body: object, *, received_at: float) -> V2Page:
    """Read one documented envelope; raise :class:`V2SourceError` when it cannot be trusted."""
    envelope = _require_mapping(body, "response")
    rows = envelope.get("data")
    if not isinstance(rows, list):
        raise V2EnvelopeError("malformed response envelope: data is not a list")
    pagination = _parse_pagination(envelope.get("pagination"))
    return V2Page(
        request=request,
        rows=tuple(cast("list[object]", rows)),
        pagination=pagination,
        received_at=received_at,
    )


def _parse_pagination(value: object) -> V2Pagination:
    pagination = _require_mapping(value, "pagination")
    next_cursor = pagination.get("next_cursor")
    if next_cursor is not None and not isinstance(next_cursor, str):
        raise V2PaginationError("malformed pagination: next_cursor is not a string or null")
    has_more = pagination.get("has_more")
    if not isinstance(has_more, bool):
        raise V2PaginationError("malformed pagination: has_more is not a boolean")
    if has_more != (next_cursor is not None):
        raise V2PaginationError("contradictory pagination: has_more disagrees with next_cursor")
    return V2Pagination(next_cursor=next_cursor, has_more=has_more)


def _require_mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise V2EnvelopeError(f"malformed response envelope: {name} is not an object")
    return cast("dict[str, object]", value)
