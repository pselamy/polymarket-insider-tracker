"""Exact readiness verdicts and error text for a running ingestion poller."""

from __future__ import annotations

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.ingestor.health import ComponentStatus, HealthMonitor
from polymarket_insider_tracker.ingestor.readiness import running_ingestion_status
from polymarket_insider_tracker.ingestor.trade_poller import IngestionState
from tests.fakes import StubPoller

NEVER_ACQUIRED = "trade source has not completed a successful acquisition"


def _status(
    redis: FakeAsyncRedis,
    state: IngestionState,
    *,
    last_error: str | None = None,
    age: float | None = 1.0,
) -> ComponentStatus:
    poller = StubPoller(redis, state, last_error, age)
    return running_ingestion_status(poller, HealthMonitor(stale_threshold_seconds=60))


def test_never_acquired_without_error_reports_only_the_fixed_message(
    fake_redis: FakeAsyncRedis,
) -> None:
    status = _status(fake_redis, IngestionState.STARTING, age=None)

    assert status == ComponentStatus(status="down", last_error=NEVER_ACQUIRED)


def test_never_acquired_appends_the_last_error(fake_redis: FakeAsyncRedis) -> None:
    status = _status(fake_redis, IngestionState.DEGRADED, last_error="refused", age=None)

    assert status == ComponentStatus(status="down", last_error=f"{NEVER_ACQUIRED}: refused")


def test_stale_success_without_error_reports_only_the_age_and_bound(
    fake_redis: FakeAsyncRedis,
) -> None:
    status = _status(fake_redis, IngestionState.RUNNING, age=120.4)

    assert status == ComponentStatus(
        status="down",
        last_error="last successful acquisition was 120s ago (staleness bound 60s)",
    )


def test_stale_success_appends_the_last_error(fake_redis: FakeAsyncRedis) -> None:
    status = _status(fake_redis, IngestionState.DEGRADED, last_error="503", age=61.0)

    assert status == ComponentStatus(
        status="down",
        last_error="last successful acquisition was 61s ago (staleness bound 60s): 503",
    )


def test_success_exactly_at_the_staleness_bound_is_still_fresh(fake_redis: FakeAsyncRedis) -> None:
    status = _status(fake_redis, IngestionState.RUNNING, age=60.0)

    assert status == ComponentStatus(status="up")


@pytest.mark.parametrize("state", [IngestionState.DEGRADED, IngestionState.POSSIBLE_DATA_LOSS])
def test_a_fresh_degraded_source_reports_its_last_error(
    fake_redis: FakeAsyncRedis, state: IngestionState
) -> None:
    status = _status(fake_redis, state, last_error="page 3 unproven")

    assert status == ComponentStatus(status="degraded", last_error="page 3 unproven")


@pytest.mark.parametrize("state", [IngestionState.DEGRADED, IngestionState.POSSIBLE_DATA_LOSS])
def test_a_fresh_degraded_source_without_an_error_names_its_state(
    fake_redis: FakeAsyncRedis, state: IngestionState
) -> None:
    status = _status(fake_redis, state)

    assert status == ComponentStatus(
        status="degraded", last_error=f"ingestion state: {state.value}"
    )


def test_a_fresh_running_source_is_up_without_an_error(fake_redis: FakeAsyncRedis) -> None:
    status = _status(fake_redis, IngestionState.RUNNING, last_error="stale text")

    assert status == ComponentStatus(status="up")
