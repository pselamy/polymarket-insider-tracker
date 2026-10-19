"""Property tests for the rolling attempt budget (V2 contract item 5).

For any generated sequence of clock advances and attempt requests, two
properties must hold:

1. Safety: no rolling ``window_seconds`` interval ever contains more than
   ``max_attempts`` recorded starts.
2. Liveness: the limiter starts each attempt at exactly the earliest time an
   independent oracle permits, so it never over-delays a start the window would
   allow.

The oracle recomputes the earliest permitted start from the attempt request
time and the previously recorded start times alone, never from the limiter's
internal state. Hypothesis is not a dependency of this locked project, so the
generators are a seeded ``random.Random`` plus an exhaustive small-alphabet
sweep; each generator drives many cases inside one collected test so the test
count stays small while case coverage is preserved.
"""

from __future__ import annotations

import itertools
import random
from collections.abc import Iterable, Sequence

import pytest

from polymarket_insider_tracker.ingestor._attempt_budget import AttemptBudget
from tests.fakes import FakeClock

WINDOW_SECONDS = 10.0
ADVANCE_ALPHABET = (0, 1, 5, 10, 11)
SEEDED_CASES = 50
SEQUENCE_LENGTH = 40


def oracle_start_time(
    prior_starts: Sequence[float],
    requested_at: float,
    max_attempts: int,
    window_seconds: float,
) -> float:
    """Earliest permitted start from prior starts and the request time alone."""
    active = [start for start in prior_starts if requested_at - start < window_seconds]
    if len(active) < max_attempts:
        return requested_at
    return min(active) + window_seconds


def assert_no_window_exceeds(
    starts: Iterable[float], max_attempts: int, window_seconds: float
) -> None:
    """Every rolling window anchored at a recorded start holds at most the limit."""
    recorded = list(starts)
    for anchor in recorded:
        count = sum(1 for start in recorded if anchor <= start < anchor + window_seconds)
        assert count <= max_attempts, f"window at {anchor} holds {count} starts"


async def drive_and_check(
    advances: Iterable[float],
    max_attempts: int,
    window_seconds: float,
    *,
    over_delay: float = 0.0,
) -> list[float]:
    """Advance the clock then acquire, asserting the oracle's exact start time."""
    clock = FakeClock(0.0)

    async def sleep(seconds: float) -> None:
        await clock.sleep(seconds)
        clock.advance(over_delay)

    budget = AttemptBudget(
        max_attempts=max_attempts,
        window_seconds=window_seconds,
        clock=clock,
        sleep=sleep,
    )
    starts: list[float] = []
    for advance in advances:
        clock.advance(advance)
        requested_at = clock.now
        expected = oracle_start_time(starts, requested_at, max_attempts, window_seconds)
        await budget.acquire()
        assert clock.now == pytest.approx(expected), (
            f"request at {requested_at} started at {clock.now}, oracle says {expected}; "
            "limiter over-delayed or under-delayed"
        )
        starts.append(clock.now)
    return starts


class TestSeededRandomProperty:
    async def test_safety_and_liveness_against_independent_oracle(self) -> None:
        for seed in range(SEEDED_CASES):
            rng = random.Random(seed)
            max_attempts = rng.randint(1, 5)
            advances = [rng.randint(0, 15) for _ in range(SEQUENCE_LENGTH)]
            starts = await drive_and_check(advances, max_attempts, WINDOW_SECONDS)
            assert_no_window_exceeds(starts, max_attempts, WINDOW_SECONDS)


class TestExhaustiveSmallSequences:
    async def test_safety_and_liveness_against_independent_oracle(self) -> None:
        for advances in itertools.product(ADVANCE_ALPHABET, repeat=4):
            starts = await drive_and_check(advances, max_attempts=2, window_seconds=WINDOW_SECONDS)
            assert_no_window_exceeds(starts, 2, WINDOW_SECONDS)


class TestLivenessDetectsOverBlocking:
    async def test_deliberately_over_blocking_limiter_fails(self) -> None:
        with pytest.raises(AssertionError, match="over-delayed"):
            await drive_and_check(
                [0, 0], max_attempts=1, window_seconds=WINDOW_SECONDS, over_delay=1.0
            )
