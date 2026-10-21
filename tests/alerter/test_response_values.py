"""Validation of untrusted provider response values, one shape at a time."""

from __future__ import annotations

import math

import pytest

from polymarket_insider_tracker.alerter.channels.response_values import (
    DEFAULT_RETRY_AFTER_SECONDS,
    validated_integer,
    validated_retry_delay,
)


@pytest.mark.parametrize("value", [0, 7, -3, 50027])
def test_plain_integers_are_returned_unchanged(value: int) -> None:
    assert validated_integer(value) == value


@pytest.mark.parametrize("value", [True, False, 1.0, "7", None, [1], {"code": 1}])
def test_non_integers_collapse_to_none(value: object) -> None:
    assert validated_integer(value) is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0, 0.0), (0.0, 0.0), (2, 2.0), (0.25, 0.25), (1, 1.0), (10**6, 1e6)],
)
def test_finite_non_negative_numbers_are_used_as_floats(value: float, expected: float) -> None:
    delay = validated_retry_delay(value)

    assert delay == expected
    assert isinstance(delay, float)


def test_a_fractional_delay_below_one_is_not_raised_to_the_default() -> None:
    assert validated_retry_delay(0.5) == 0.5
    assert DEFAULT_RETRY_AFTER_SECONDS != 0.5


@pytest.mark.parametrize(
    "value",
    [True, False, "2", None, [2], -0.5, -1, math.inf, -math.inf, math.nan, 10**400],
)
def test_unusable_delays_collapse_to_the_fixed_default(value: object) -> None:
    assert validated_retry_delay(value) == DEFAULT_RETRY_AFTER_SECONDS
