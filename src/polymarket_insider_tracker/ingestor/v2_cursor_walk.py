"""One bounded, truthful cursor walk per acquisition cycle (unwired).

The approved slice-001 v2 amendment (items 2-3) requires one consistent cursor walk per cycle:
request ``limit=1000`` with explicit ``taker_only`` and the cycle's fixed ``start``/``end``, then
follow only the returned cursor, repeating the same filters. Start with one page; if the durable
proof fails, follow up to 19 further pages, checking proof after each, so a cycle never exceeds 20
pages or 20,000 raw observations. A page larger than the documented 1,000-row limit is a provider
contract violation: it raises a source failure instead of silently dropping the fetched rows.

This component is standalone: nothing in the poller or HTTP client imports it, so it changes no
existing behavior. Every physical request, each page and each retry, acquires one slot from the
injected :class:`~polymarket_insider_tracker.ingestor._attempt_budget.AttemptBudget`. The result is
typed and truthful about completeness: a nonempty walk that stops on documented exhaustion or the
depth limit without proof exposes ``possible_data_loss`` so the caller can freeze the boundary. An
empty terminal cycle is a successful quiet cycle, not a new gap.

The optional ``proof`` callback is evaluated after each recorded page with the cumulative
observations of the walk so far, in page order, so a proof that is only satisfiable by combining
pages can succeed. This mirrors the v1 continuity proof in
:mod:`polymarket_insider_tracker.ingestor.trade_poller` (``_prove``), which reasons over everything
observed back to the durable boundary rather than the latest page alone.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final

from polymarket_insider_tracker.ingestor._attempt_budget import AttemptBudget
from polymarket_insider_tracker.ingestor._retry import MAX_RETRIES, RandomSource, RetryPolicy
from polymarket_insider_tracker.ingestor.trade_row_v2 import parse_v2_trade_row
from polymarket_insider_tracker.ingestor.trade_rows import InvalidRow, TradeObservation
from polymarket_insider_tracker.ingestor.v2_page import (
    V2_PAGE_LIMIT,
    V2EnvelopeError,
    V2Page,
    V2PageRequest,
    V2PageTransport,
    V2PaginationError,
    V2TransientError,
    parse_v2_page,
)

MAX_WALK_PAGES: Final[int] = 20
MAX_WALK_ROWS: Final[int] = 20_000

Clock = Callable[[], float]
Sleeper = Callable[[float], Awaitable[None]]
CancellationCheck = Callable[[], bool]
ProofCheck = Callable[[Sequence[TradeObservation]], bool]

__all__ = [
    "MAX_RETRIES",
    "MAX_WALK_PAGES",
    "MAX_WALK_ROWS",
    "CursorWalk",
    "StopReason",
    "WalkResult",
]


class StopReason(StrEnum):
    """Why a bounded walk stopped."""

    PROOF = "proof"
    EXHAUSTED = "exhausted"
    PAGE_LIMIT = "page-limit"
    CANCELLED = "cancelled"


_LOSS_STOP_REASONS: Final[frozenset[StopReason]] = frozenset(
    {StopReason.EXHAUSTED, StopReason.PAGE_LIMIT}
)


@dataclass(frozen=True)
class WalkResult:
    """The typed outcome of one walk: what was read and whether completeness is proven."""

    pages: tuple[V2Page, ...]
    observations: tuple[TradeObservation, ...]
    invalid_rows: tuple[InvalidRow, ...]
    stop_reason: StopReason
    completeness_proven: bool

    @property
    def pages_fetched(self) -> int:
        return len(self.pages)

    @property
    def raw_rows(self) -> int:
        return sum(len(page.rows) for page in self.pages)

    @property
    def possible_data_loss(self) -> bool:
        """A nonempty walk that stopped without proof on exhaustion or the depth limit.

        An empty terminal cycle is a successful quiet cycle, and a cancelled walk is a graceful
        stop the next cycle re-acquires, so neither opens a new gap.
        """
        return (
            not self.completeness_proven
            and self.stop_reason in _LOSS_STOP_REASONS
            and self.raw_rows > 0
        )


class CursorWalk:
    """Walk one cycle's pages over an injected transport and attempt budget."""

    def __init__(
        self,
        transport: V2PageTransport,
        *,
        budget: AttemptBudget | None = None,
        clock: Clock = time.time,
        sleeper: Sleeper = asyncio.sleep,
        random_source: RandomSource = random.random,
        max_pages: int = MAX_WALK_PAGES,
        max_retries: int = MAX_RETRIES,
        is_cancelled: CancellationCheck | None = None,
    ) -> None:
        self._transport = transport
        self._budget = budget if budget is not None else AttemptBudget()
        self._clock = clock
        self._sleeper = sleeper
        self._retry = RetryPolicy(max_retries=max_retries, random_source=random_source)
        self._max_pages = max_pages
        self._is_cancelled = is_cancelled

    async def run(
        self,
        *,
        taker_only: bool,
        start: int,
        end: int,
        proof: ProofCheck | None = None,
    ) -> WalkResult:
        """Fetch bounded pages until proof, exhaustion, cancellation, or the depth limit.

        A cooperative cancellation check stops the walk with :attr:`StopReason.CANCELLED` and a
        partial result. Task cancellation is never swallowed: ``CancelledError`` from the budget
        wait, the transport, or the retry sleep propagates to the caller.
        """
        state = _WalkState(max_pages=self._max_pages, proof=proof)
        while True:
            stop = self._stop_before_page(state)
            if stop is not None:
                return state.finish(stop)
            request = state.next_request(taker_only=taker_only, start=start, end=end)
            body = await self._fetch(request)
            page = parse_v2_page(request, body, received_at=self._clock())
            stop = state.record(page, now=int(self._clock()))
            if stop is not None:
                return state.finish(stop)

    def _stop_before_page(self, state: _WalkState) -> StopReason | None:
        if self._is_cancelled is not None and self._is_cancelled():
            return StopReason.CANCELLED
        if state.at_page_limit():
            return StopReason.PAGE_LIMIT
        return None

    async def _fetch(self, request: V2PageRequest) -> object:
        for attempt in range(self._retry.max_retries):
            try:
                return await self._attempt(request)
            except V2TransientError as error:
                await self._sleeper(
                    self._retry.delay_seconds(
                        attempt, retry_after_seconds=error.retry_after_seconds
                    )
                )
        return await self._attempt(request)

    async def _attempt(self, request: V2PageRequest) -> object:
        await self._budget.acquire()
        return await self._transport.fetch(request)


@dataclass
class _WalkState:
    """Mutable bookkeeping for one bounded walk."""

    max_pages: int
    proof: ProofCheck | None
    pages: list[V2Page] = field(default_factory=list[V2Page])
    observations: list[TradeObservation] = field(default_factory=list[TradeObservation])
    invalid_rows: list[InvalidRow] = field(default_factory=list[InvalidRow])
    raw_rows: int = 0
    cursor: str | None = None
    seen_cursors: set[str] = field(default_factory=set[str])
    proven: bool = False

    def at_page_limit(self) -> bool:
        return len(self.pages) >= self.max_pages

    def next_request(self, *, taker_only: bool, start: int, end: int) -> V2PageRequest:
        return V2PageRequest(taker_only=taker_only, start=start, end=end, cursor=self.cursor)

    def record(self, page: V2Page, *, now: int) -> StopReason | None:
        """Record one trusted page; return a stop reason, or ``None`` to keep walking.

        A page larger than the documented ``limit`` is a provider contract violation, so it raises
        rather than being silently dropped; ``MAX_WALK_ROWS`` then holds as an invariant over the
        20-page, 1,000-row-per-page maximum. The ``proof`` callback sees the cumulative
        observations of the walk so far, in page order.
        """
        _require_documented_page_size(page)
        self._append(page, now=now)
        assert self.raw_rows <= MAX_WALK_ROWS
        if self.proof is not None and self.proof(tuple(self.observations)):
            self.proven = True
            return StopReason.PROOF
        return self._advance(page.pagination.next_cursor)

    def _append(self, page: V2Page, *, now: int) -> None:
        self.pages.append(page)
        self.raw_rows += len(page.rows)
        page_observations, page_invalid = _parse_rows(page.rows, now=now)
        self.observations.extend(page_observations)
        self.invalid_rows.extend(page_invalid)

    def _advance(self, next_cursor: str | None) -> StopReason | None:
        if next_cursor is None:
            return StopReason.EXHAUSTED
        if next_cursor in self.seen_cursors:
            raise V2PaginationError("repeated cursor: the feed returned a cursor already followed")
        self.seen_cursors.add(next_cursor)
        self.cursor = next_cursor
        return None

    def finish(self, stop_reason: StopReason) -> WalkResult:
        return WalkResult(
            pages=tuple(self.pages),
            observations=tuple(self.observations),
            invalid_rows=tuple(self.invalid_rows),
            stop_reason=stop_reason,
            completeness_proven=self.proven,
        )


def _require_documented_page_size(page: V2Page) -> None:
    """A page larger than the documented limit is a source failure, never a silent drop."""
    if len(page.rows) > V2_PAGE_LIMIT:
        raise V2EnvelopeError("page exceeds the documented limit of 1000 rows")


def _parse_rows(
    rows: Sequence[object], *, now: int
) -> tuple[tuple[TradeObservation, ...], tuple[InvalidRow, ...]]:
    observations: list[TradeObservation] = []
    invalid: list[InvalidRow] = []
    for row in rows:
        parsed = parse_v2_trade_row(row, now=now)
        if isinstance(parsed, TradeObservation):
            observations.append(parsed)
        else:
            invalid.append(parsed)
    return tuple(observations), tuple(invalid)
