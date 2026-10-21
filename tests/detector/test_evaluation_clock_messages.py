"""Evaluation-clock error contracts: the exact messages and the offset-less tzinfo case."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from polymarket_insider_tracker.detector.evaluation_clock import (
    parse_stored_instant,
    utc_instant,
)


class OffsetlessInstant(datetime):
    """A datetime that carries a tzinfo yet, like a naive one, cannot say its UTC offset."""

    def utcoffset(self) -> timedelta | None:
        return None


def test_a_naive_instant_names_the_timezone_requirement() -> None:
    with pytest.raises(ValueError, match=r"^evaluated_at must be timezone-aware$"):
        utc_instant(datetime(2026, 10, 19, 12, 0, 0))


def test_a_tzinfo_without_an_offset_is_still_naive() -> None:
    with pytest.raises(ValueError, match=r"^evaluated_at must be timezone-aware$"):
        utc_instant(OffsetlessInstant(2026, 10, 19, 12, 0, 0, tzinfo=UTC))


@pytest.mark.parametrize("text", ["2026-10-19T12:00:00", "2026-10-19T12:00:00+01:00"])
def test_a_stored_instant_without_a_utc_offset_names_the_requirement(text: str) -> None:
    with pytest.raises(ValueError, match=r"^stored instants carry an explicit \+00:00 offset$"):
        parse_stored_instant(text)


def test_a_utc_stored_instant_round_trips() -> None:
    assert parse_stored_instant("2026-10-19T12:00:00+00:00") == datetime(
        2026, 10, 19, 12, tzinfo=UTC
    )
