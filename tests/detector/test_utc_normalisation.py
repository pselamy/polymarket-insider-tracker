"""UTC normalisation must not fall back to the machine's local zone."""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone

from polymarket_insider_tracker.detector.evaluation_clock import utc_instant
from polymarket_insider_tracker.detector.volume_state import read_stamp
from polymarket_insider_tracker.ingestor.volume_observation import VolumeObservation

PLUS_TWO = timezone(timedelta(hours=2))
NOON_UTC = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)


@contextmanager
def tokyo_local_zone() -> Iterator[None]:
    """A local zone (UTC+9) that differs from both UTC and the test offsets."""
    previous = os.environ.get("TZ")
    os.environ["TZ"] = "Asia/Tokyo"
    time.tzset()
    try:
        yield
    finally:
        if previous is None:
            del os.environ["TZ"]
        else:
            os.environ["TZ"] = previous
        time.tzset()


def test_an_evaluation_instant_is_utc_not_local() -> None:
    with tokyo_local_zone():
        instant = utc_instant(NOON_UTC.astimezone(PLUS_TWO))

    assert (instant, instant.utcoffset()) == (NOON_UTC, timedelta(0))


def test_a_stored_stamp_is_read_as_utc_not_local() -> None:
    with tokyo_local_zone():
        stamp = read_stamp("2026-10-19T14:00:00+02:00", NOON_UTC)

    assert stamp.acquired_at is not None
    assert (stamp.acquired_at, stamp.acquired_at.utcoffset()) == (NOON_UTC, timedelta(0))


def test_a_page_stamp_is_written_as_utc_not_local() -> None:
    with tokyo_local_zone():
        observation = VolumeObservation.from_page(
            present=True, raw="5", acquired_at=NOON_UTC.astimezone(PLUS_TWO)
        )

    assert observation.acquired_at == "2026-10-19T12:00:00+00:00"
