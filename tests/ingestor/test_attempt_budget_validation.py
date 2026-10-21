"""Validation messages and real-time defaults of the attempt budget (mutation step 1)."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from polymarket_insider_tracker.ingestor._attempt_budget import (
    AttemptBudget,
    RollingAttemptWindow,
)
from tests.fakes import FakeClock

# Deliberately ill-typed input: the constructors validate at runtime.
build_window: Callable[..., RollingAttemptWindow] = RollingAttemptWindow


@pytest.mark.parametrize("value", [0, -1, True, 2.5, "3"])
def test_max_attempts_rejection_names_the_parameter_and_value(value: object) -> None:
    with pytest.raises(ValueError) as caught:
        build_window(value, 10.0)
    assert str(caught.value) == f"max_attempts must be a positive integer, got {value!r}"


@pytest.mark.parametrize("value", [True, False, "10", None])
def test_non_numeric_window_is_rejected_as_not_a_number(value: object) -> None:
    with pytest.raises(ValueError) as caught:
        build_window(3, value)
    assert str(caught.value) == f"window_seconds must be a positive number, got {value!r}"


@pytest.mark.parametrize("value", [0, 0.0, -1.0, float("inf"), float("nan")])
def test_non_positive_or_non_finite_window_is_rejected(value: float) -> None:
    with pytest.raises(ValueError) as caught:
        build_window(3, value)
    assert str(caught.value) == f"window_seconds must be a positive finite number, got {value!r}"


@pytest.mark.parametrize("window_seconds", [0.5, 1, 1.0])
def test_small_positive_windows_are_accepted(window_seconds: float) -> None:
    assert RollingAttemptWindow(3, window_seconds).window_seconds == float(window_seconds)


def test_a_fresh_window_has_no_wait_and_a_full_one_waits_out_the_oldest_start() -> None:
    window = RollingAttemptWindow(1, 10.0)
    assert window.seconds_until_available(5.0) == 0.0
    assert window.try_start(5.0) is True
    assert window.seconds_until_available(7.0) == 8.0
    assert window.try_start(7.0) is False
    assert window.recorded_starts() == (5.0,)


async def test_acquire_sleeps_for_the_remaining_window_computed_at_the_read_time() -> None:
    clock = FakeClock(100.0)
    budget = AttemptBudget(max_attempts=1, window_seconds=4.0, clock=clock, sleep=clock.sleep)
    await budget.acquire()
    clock.advance(1.0)
    await budget.acquire()
    assert clock.sleeps == [3.0]
    assert budget.recorded_starts() == (104.0,)


async def test_default_clock_and_sleep_space_real_starts() -> None:
    budget = AttemptBudget(max_attempts=1, window_seconds=0.01)
    await budget.acquire()
    await budget.acquire()
    assert len(budget.recorded_starts()) == 1
