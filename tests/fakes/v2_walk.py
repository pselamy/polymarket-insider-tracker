"""A working fake of the documented Data API v2 cursor feed for the bounded walk.

``FakeCursorFeed`` models the provider behavior the approved v2 contract relies on: the
``{data, pagination}`` envelope, opaque cursors that carry the page size, and ``has_more`` that is
exact, so an empty or short page never ends a walk on its own. Pages are scripted by the cursor that
requests them (``None`` is the first page); a scripted ``Exception`` is raised in place of the page
for that physical request, which lets tests inject retryable and terminal transport failures.

``v2_row`` renames a documented v1-shaped row to its documented v2 spelling so tests reuse the
existing ``trade_row`` builder. ``CountingBudget`` wraps the real attempt budget and records how
many physical attempt starts were admitted. Every wallet and transaction is synthetic.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field

from polymarket_insider_tracker.ingestor._attempt_budget import AttemptBudget
from polymarket_insider_tracker.ingestor.v2_page import V2PageRequest, V2TerminalError

Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]

V2_RENAMES: dict[str, str] = {
    "proxyWallet": "proxy_wallet",
    "conditionId": "condition_id",
    "asset": "token_id",
    "transactionHash": "transaction_hash",
    "outcomeIndex": "outcome_index",
    "eventSlug": "event_slug",
    "profileImage": "profile_image",
    "profileImageOptimized": "profile_image_optimized",
}


def v2_row(row: Mapping[str, object]) -> dict[str, object]:
    """Rename a documented v1-shaped row to its documented v2 spelling."""
    return {V2_RENAMES.get(key, key): value for key, value in row.items()}


def v2_envelope(
    rows: Iterable[object],
    *,
    next_cursor: str | None = None,
    has_more: bool | None = None,
) -> dict[str, object]:
    """Build the documented ``{data, pagination}`` envelope."""
    if has_more is None:
        has_more = next_cursor is not None
    return {
        "data": list(rows),
        "pagination": {"next_cursor": next_cursor, "has_more": has_more},
    }


@dataclass
class FakeCursorFeed:
    """Scripted v2 pages keyed by the cursor that requests them (``None`` = first page)."""

    pages: Mapping[str | None, object]
    requests: list[V2PageRequest] = field(default_factory=list)
    script: list[Exception | None] = field(default_factory=list)

    def fail_next(self, error: Exception) -> None:
        """Raise ``error`` for the next physical request instead of serving a page."""
        self.script.append(error)

    async def fetch(self, request: V2PageRequest) -> object:
        self.requests.append(request)
        outcome = self.script.pop(0) if self.script else None
        if outcome is not None:
            raise outcome
        if request.cursor not in self.pages:
            raise V2TerminalError(f"no scripted page for cursor {request.cursor!r}")
        return self.pages[request.cursor]

    def cursors(self) -> list[str | None]:
        """The cursor of every physical request, in order."""
        return [request.cursor for request in self.requests]


class CountingBudget(AttemptBudget):
    """The real attempt budget, plus a count of every admitted physical attempt start."""

    def __init__(self, *, clock: Clock, sleep: Sleep) -> None:
        super().__init__(clock=clock, sleep=sleep)
        self.acquires = 0

    async def acquire(self) -> None:
        self.acquires += 1
        await super().acquire()
