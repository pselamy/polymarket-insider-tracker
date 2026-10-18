"""Property tests for the rolling attempt budget (V2 contract item 5).

For any generated sequence of clock advances and attempt requests, two
properties must hold:

1. Safety: no rolling ``window_seconds`` interval ever contains more than
   ``max_attempts`` recorded starts.
2. Liveness: the limiter never delays a start that the window would allow — it
   starts at exactly the earliest time an independent brute-force oracle says is
   permitted.

The oracle recomputes the earliest permitted time from the recorded start times
alone, never from the limiter's internal state, so it is an independent check.
Hypothesis is not a dependency of this locked project, so the generators are a
seeded ``random.Random`` plus an exhaustive small-alphabet sweep; the oracle and
the properties are the same ones a Hypothesis strategy would drive.
"""

from __future__ import annotations

import itertools
import random
from collections.abc import Iterable

import pytest

from polymarket_insider_tracker.ingestor._attempt_budget import AttemptBudget
from tests.fakes import FakeClock

WINDOW_SECONDS = 10.0
ADVANCE_ALPHABET = (0, 1, 5, 10, 11)


def oracle_earliest_start(
    starts: Iterable[float], now: float, max_attempts: int, window_seconds: float
) -> float:
    """Earliest time a start is permitted, computed from recorded start times alone."""
    active = [start for start in starts if now - start < window_seconds]
    if len(active) < max_attempts:
        return now
    return min(active) + window_seconds


async def drive(advances: Iterable[int], max_attempts: int, window_seconds: float) -> list[float]:
    """Advance the clock then acquire; assert the limiter matches the oracle."""
    clock = FakeClock(0.0)
    budget = AttemptBudget(
        max_attempts=max_attempts, window_seconds=window_seconds, clock=clock, sleep=clock.sleep
    )
    starts: list[float] = []
    for advance in advances:
        clock.advance(advance)
        expected = oracle_earliest_start(starts, clock.now, max_attempts, window_seconds)
        await budget.acquire()
        assert clock.now == expected
        starts.append(clock.now)
    return starts


def assert_no_window_exceeds(
    starts: Iterable[float], max_attempts: int, window_seconds: float
) -> None:
    """Every rolling window anchored at a recorded start holds at most the limit."""
    recorded = list(starts)
    for anchor in recorded:
        count = sum(1 for start in recorded if anchor <= start < anchor + window_seconds)
        assert count <= max_attempts


class TestSeededRandomProperty:
    @pytest.mark.parametrize("seed", range(50))
    async def test_safety_and_liveness(self, seed: int) -> None:
        rng = random.Random(seed)
        max_attempts = rng.randint(1, 5)
        advances = [rng.randint(0, 15) for _ in range(40)]
        starts = await drive(advances, max_attempts, WINDOW_SECONDS)
        assert_no_window_exceeds(starts, max_attempts, WINDOW_SECONDS)


class TestExhaustiveSmallSequences:
    @pytest.mark.parametrize(
        "advances", list(itertools.product(ADVANCE_ALPHABET, repeat=4)), ids=str
    )
    async def test_safety_and_liveness(self, advances: tuple[int, ...]) -> None:
        starts = await drive(advances, max_attempts=2, window_seconds=WINDOW_SECONDS)
        assert_no_window_exceeds(starts, 2, WINDOW_SECONDS)
