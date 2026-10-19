"""Example tests for the rolling attempt budget (V2 contract item 5).

These pin the observable contract of
:mod:`polymarket_insider_tracker.ingestor._attempt_budget`: at most
``max_attempts`` physical attempt *starts* in any rolling ``window_seconds``
interval per acquisition client, with a cancellable async wait before the next
start. The component is pure and clock-injectable, and is deliberately not
wired into the HTTP client, retries, or poller in this card.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from polymarket_insider_tracker.ingestor._attempt_budget import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_WINDOW_SECONDS,
    AttemptBudget,
    RollingAttemptWindow,
)
from tests.fakes import FakeClock


class BlockingSleep:
    """A working sleep that blocks until released, so cancellation is observable."""

    def __init__(self) -> None:
        self.delays: list[float] = []
        self.entered = asyncio.Event()
        self._release = asyncio.Event()

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        self.entered.set()
        await self._release.wait()


class TestDefaults:
    def test_defaults_match_the_contract(self) -> None:
        budget = AttemptBudget()
        assert budget.max_attempts == 30
        assert budget.window_seconds == 10.0
        assert DEFAULT_MAX_ATTEMPTS == 30
        assert DEFAULT_WINDOW_SECONDS == 10.0


class TestInvalidParameters:
    @pytest.mark.parametrize(
        ("max_attempts", "window_seconds"),
        [
            (0, 10.0),
            (-1, 10.0),
            (True, 10.0),
            (2.5, 10.0),
            (30, 0.0),
            (30, -1.0),
            (30, float("inf")),
            (30, float("nan")),
        ],
    )
    def test_invalid_parameters_rejected(self, max_attempts: object, window_seconds: float) -> None:
        build: Callable[..., AttemptBudget] = AttemptBudget  # deliberately ill-typed input
        with pytest.raises(ValueError):
            build(max_attempts=max_attempts, window_seconds=window_seconds)


class TestRollingWindow:
    """The pure window counts starts and reopens capacity as they age out."""

    def test_allows_up_to_max_attempts(self) -> None:
        window = RollingAttemptWindow(3, 10.0)
        assert [window.try_start(0.0) for _ in range(3)] == [True, True, True]
        assert window.try_start(0.0) is False
        assert window.recorded_starts() == (0.0, 0.0, 0.0)
        assert window.seconds_until_available(0.0) == 10.0

    def test_slot_reopens_when_oldest_ages_out(self) -> None:
        window = RollingAttemptWindow(1, 10.0)
        assert window.try_start(0.0) is True
        assert window.seconds_until_available(9.999) == pytest.approx(0.001)
        assert window.try_start(10.0) is True

    def test_start_at_the_window_boundary_is_expired(self) -> None:
        window = RollingAttemptWindow(1, 10.0)
        window.try_start(5.0)
        assert window.try_start(14.999) is False
        assert window.try_start(15.0) is True


class TestAttemptBudgetAsync:
    """The async wait spaces physical attempt starts by the rolling window."""

    async def test_thirty_starts_within_window_are_allowed(self) -> None:
        clock = FakeClock(0.0)
        budget = AttemptBudget(clock=clock, sleep=clock.sleep)
        for _ in range(30):
            await budget.acquire()
        assert clock.sleeps == []
        assert len(budget.recorded_starts()) == 30
        assert budget.seconds_until_available() == 10.0

    async def test_thirty_first_waits_until_oldest_ages_out(self) -> None:
        clock = FakeClock(0.0)
        budget = AttemptBudget(clock=clock, sleep=clock.sleep)
        for _ in range(30):
            await budget.acquire()
        await budget.acquire()
        assert clock.sleeps == [10.0]
        assert clock.now == 10.0
        assert budget.recorded_starts() == (10.0,)

    async def test_starts_are_counted_not_completions(self) -> None:
        clock = FakeClock(0.0)
        budget = AttemptBudget(clock=clock, sleep=clock.sleep)
        for _ in range(30):
            await budget.acquire()
        assert budget.seconds_until_available() == 10.0
        assert len(budget.recorded_starts()) == 30

    async def test_limits_are_constructor_parameters(self) -> None:
        clock = FakeClock(0.0)
        budget = AttemptBudget(max_attempts=2, window_seconds=5.0, clock=clock, sleep=clock.sleep)
        await budget.acquire()
        await budget.acquire()
        assert clock.sleeps == []
        await budget.acquire()
        assert clock.sleeps == [5.0]

    async def test_wait_is_cancellable_without_consuming_a_slot(self) -> None:
        clock = FakeClock(0.0)
        sleeper = BlockingSleep()
        budget = AttemptBudget(max_attempts=1, window_seconds=10.0, clock=clock, sleep=sleeper)
        await budget.acquire()
        assert len(budget.recorded_starts()) == 1
        task = asyncio.create_task(budget.acquire())
        await sleeper.entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert sleeper.delays == [10.0]
        assert len(budget.recorded_starts()) == 1
