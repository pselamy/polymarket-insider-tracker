"""Shared harness for the bounded v2 cursor-walk tests.

Builds a :class:`CursorWalk` over a scripted :class:`FakeCursorFeed` and a counting attempt budget
driven by a :class:`FakeClock`, so request sequences, budget admission, and retry sleeps are all
deterministic and observable. Not collected as tests (its name does not start with ``test_``).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field

from polymarket_insider_tracker.ingestor.v2_cursor_walk import (
    MAX_RETRIES,
    MAX_WALK_PAGES,
    CursorWalk,
    ProofCheck,
    WalkResult,
)
from polymarket_insider_tracker.ingestor.v2_page import V2PageRequest
from tests.fakes import CountingBudget, FakeClock, FakeCursorFeed, trade_row, v2_envelope, v2_row

NOW = 1_788_983_720
START = NOW - 600
END = NOW


def v2_rows(count: int, *, offset: int = 0) -> list[dict[str, object]]:
    """``count`` synthetic v2 rows with distinct transaction and wallet identities."""
    return [
        v2_row(trade_row(timestamp=NOW - 5, transaction=offset + index, wallet=offset + index))
        for index in range(count)
    ]


@dataclass
class Harness:
    walk: CursorWalk
    feed: FakeCursorFeed
    budget: CountingBudget
    budget_clock: FakeClock


def build_walk(
    feed: FakeCursorFeed,
    *,
    max_pages: int = MAX_WALK_PAGES,
    max_retries: int = MAX_RETRIES,
    random_source: Callable[[], float] = lambda: 1.0,
    is_cancelled: Callable[[], bool] | None = None,
) -> Harness:
    budget_clock = FakeClock(0.0)
    budget = CountingBudget(clock=budget_clock, sleep=budget_clock.sleep)
    walk = CursorWalk(
        feed,
        budget=budget,
        clock=FakeClock(float(NOW)),
        sleeper=budget_clock.sleep,
        random_source=random_source,
        max_pages=max_pages,
        max_retries=max_retries,
        is_cancelled=is_cancelled,
    )
    return Harness(walk=walk, feed=feed, budget=budget, budget_clock=budget_clock)


async def run_walk(
    feed: FakeCursorFeed,
    *,
    proof: ProofCheck | None = None,
    taker_only: bool = False,
    max_pages: int = MAX_WALK_PAGES,
    max_retries: int = MAX_RETRIES,
    is_cancelled: Callable[[], bool] | None = None,
) -> tuple[WalkResult, Harness]:
    harness = build_walk(
        feed, max_pages=max_pages, max_retries=max_retries, is_cancelled=is_cancelled
    )
    result = await harness.walk.run(taker_only=taker_only, start=START, end=END, proof=proof)
    return result, harness


@dataclass
class BlockingFeed:
    """A transport whose fetch blocks until released, so cancellation is observable."""

    entered: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    requests: list[V2PageRequest] = field(default_factory=list)

    async def fetch(self, request: V2PageRequest) -> object:
        self.requests.append(request)
        self.entered.set()
        await self.release.wait()
        return v2_envelope([])
