"""Edge behaviour of the health monitor: error provider, zero-epoch clocks, failing loop."""

import asyncio

import pytest

from polymarket_insider_tracker.ingestor.health import (
    ComponentStatus,
    HealthMonitor,
    HealthReport,
    StreamStatus,
)
from tests.fakes import FakeClock
from tests.fakes.health_http import served


def test_first_failure_reason_without_a_down_component_is_unhealthy() -> None:
    components = {"database": ComponentStatus(status="degraded"), "redis": ComponentStatus("up")}

    assert HealthMonitor.first_failure_reason(components) == "unhealthy"


def test_first_failure_reason_names_the_first_down_component() -> None:
    components = {
        "redis": ComponentStatus(status="up"),
        "ingestion": ComponentStatus(status="down"),
        "database": ComponentStatus(status="down"),
    }

    assert HealthMonitor.first_failure_reason(components) == "ingestion_worker_failed"


@pytest.mark.asyncio
async def test_failing_last_error_provider_is_reported_not_raised() -> None:
    monitor = HealthMonitor()

    def failing_provider() -> str | None:
        raise RuntimeError("provider exploded")

    monitor.set_last_error_provider(failing_provider)

    async with served(monitor) as client:
        resp = await client.get("/health")
        body = await resp.json()

    assert body["last_error"] == "last-error provider failed: provider exploded"


def test_stream_connected_at_epoch_zero_has_no_staleness_deadline() -> None:
    """A clock reading 0.0 gives no usable connection time, so the stream stays active."""
    clock = FakeClock(0.0)
    monitor = HealthMonitor(stale_threshold_seconds=1, clock=clock)

    monitor.set_stream_connected("trades")
    clock.advance(100)

    assert monitor.get_health_report().streams["trades"].status == StreamStatus.ACTIVE


class _FlakyClock:
    """Clock that raises on one chosen reading and otherwise advances by one per call."""

    def __init__(self, fail_on_call: int) -> None:
        self._fail_on_call = fail_on_call
        self.calls = 0

    def __call__(self) -> float:
        self.calls += 1
        if self.calls == self._fail_on_call:
            raise RuntimeError("clock boom")
        return 1_000.0 + self.calls


class _ReportRecorder:
    def __init__(self) -> None:
        self.reports: list[HealthReport] = []

    async def __call__(self, report: HealthReport) -> None:
        self.reports.append(report)


@pytest.mark.asyncio
async def test_health_loop_survives_an_unexpected_step_failure() -> None:
    """A failure inside a check step is logged and the loop resumes checking afterwards."""
    recorder = _ReportRecorder()
    clock = _FlakyClock(fail_on_call=2)
    monitor = HealthMonitor(health_check_interval=0.01, on_health_change=recorder, clock=clock)
    monitor.register_stream("trades")
    await monitor.start()
    try:
        async with asyncio.timeout(5):
            while not recorder.reports:
                await asyncio.sleep(0.05)
    finally:
        await monitor.stop()

    assert clock.calls > 2
    assert recorder.reports[0].streams.keys() == {"trades"}


@pytest.mark.asyncio
async def test_stop_after_start_failed_before_the_task_existed_still_stops() -> None:
    """A clock failure inside start() leaves the monitor running with no task to cancel."""
    monitor = HealthMonitor(clock=_FlakyClock(fail_on_call=1))
    with pytest.raises(RuntimeError, match="clock boom"):
        await monitor.start()
    assert monitor.is_running

    await monitor.stop()

    assert not monitor.is_running


class _LoopDriver(HealthMonitor):
    async def run_loop(self) -> None:
        await self._health_check_loop()


@pytest.mark.asyncio
async def test_health_loop_exits_without_checking_once_the_monitor_is_not_running() -> None:
    recorder = _ReportRecorder()
    monitor = _LoopDriver(on_health_change=recorder)

    async with asyncio.timeout(5):
        await monitor.run_loop()

    assert recorder.reports == []
