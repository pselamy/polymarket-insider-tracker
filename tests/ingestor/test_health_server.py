"""Tests for HTTP health server endpoints (/live, /ready, /health, /metrics).

Tests verify:
1. /live always returns 200 {"live": true}
2. /ready returns 200 when database, redis, and ingestion are up
3. /ready returns 503 when any required dependency is down
4. /health separates acquisition freshness from trade freshness
5. /health reports status healthy during quiet periods when acquisition is fresh
6. /health returns 503 when a dependency is down
7. /metrics exposes Prometheus metrics
8. port override starts HTTP server on the configured port
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp
import pytest

from polymarket_insider_tracker.ingestor.health import (
    ComponentStatus,
    HealthMonitor,
)
from tests.fakes import FakeClock


def _static_checker(status: ComponentStatus) -> Callable[[], Awaitable[ComponentStatus]]:
    async def check() -> ComponentStatus:
        return status

    return check


def wire_components(monitor: HealthMonitor, **statuses: ComponentStatus) -> None:
    """Install a checker per component that always reports the given status."""
    for name, status in statuses.items():
        monitor.set_component_checker(name, _static_checker(status))


async def probe(monitor: HealthMonitor, port: int, route: str) -> tuple[int, Any]:
    """Serve ``monitor`` on ``port``, GET ``route`` and return (status, JSON body)."""
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{port}{route}") as resp,
        ):
            return resp.status, await resp.json()
    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_live_endpoint() -> None:
    """Test /live endpoint returns 200 and live: true."""
    status, data = await probe(HealthMonitor(), 19101, "/live")
    assert status == 200
    assert data == {"live": True}


@pytest.mark.asyncio
async def test_ready_endpoint_all_up() -> None:
    """Test /ready endpoint returns 200 when all components are up."""
    monitor = HealthMonitor()
    wire_components(
        monitor,
        database=ComponentStatus(status="up", latency_ms=1.5),
        redis=ComponentStatus(status="up", latency_ms=0.5),
        ingestion=ComponentStatus(status="up"),
    )
    status, data = await probe(monitor, 19102, "/ready")
    assert status == 200
    assert data["ready"] is True
    assert data["components"] == {
        "database": "up",
        "redis": "up",
        "ingestion": "up",
    }


@pytest.mark.asyncio
async def test_ready_endpoint_database_down() -> None:
    """Test /ready endpoint returns 503 when database is down."""
    monitor = HealthMonitor()
    wire_components(
        monitor,
        database=ComponentStatus(status="down", last_error="connection refused"),
        redis=ComponentStatus(status="up", latency_ms=0.5),
        ingestion=ComponentStatus(status="up"),
    )
    status, data = await probe(monitor, 19103, "/ready")
    assert status == 503
    assert data["ready"] is False
    assert data["reason"] == "database_unreachable"
    assert data["components"]["database"] == "down"


@pytest.mark.asyncio
async def test_ready_endpoint_ingestion_crashed() -> None:
    """Test /ready endpoint returns 503 when ingestion worker has failed."""
    monitor = HealthMonitor()
    wire_components(
        monitor,
        database=ComponentStatus(status="up"),
        redis=ComponentStatus(status="up"),
        ingestion=ComponentStatus(status="down", last_error="ingestion_worker_failed"),
    )
    status, data = await probe(monitor, 19104, "/ready")
    assert status == 503
    assert data["ready"] is False
    assert data["reason"] == "ingestion_worker_failed"
    assert data["components"]["ingestion"] == "down"


@pytest.mark.asyncio
async def test_health_endpoint_quiet_market_period() -> None:
    """Test /health distinguishes acquisition freshness from trade freshness.

    In a quiet market, trade events may not have occurred for minutes, but if
    acquisition polling ran 2 seconds ago, the status is healthy. The monitor holds
    the production stream state (a previously active "trades" stream), so trade
    silence alone must not mark the reachable source stale.
    """
    now = 1_000_000.0
    clock = FakeClock(now - 120.0)
    monitor = HealthMonitor(clock=clock)
    # The stream received a trade once, 120 seconds ago (production state after a
    # quiet interval), and acquisition polling last succeeded 2 seconds ago.
    monitor.record_event("trades")
    clock.advance(120.0)
    monitor.record_acquisition(now - 2.0)
    monitor.record_trade_arrival(now - 120.0)

    wire_components(
        monitor,
        database=ComponentStatus(status="up", latency_ms=1.2),
        redis=ComponentStatus(status="up", latency_ms=0.4),
        ingestion=ComponentStatus(status="up"),
    )

    port = 19105
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{port}/health") as resp,
        ):
            assert resp.status == 200
            assert monitor.last_acquisition_time is not None and monitor.last_trade_time is not None
            assert math.isclose(monitor.last_acquisition_time, now - 2.0, rel_tol=0, abs_tol=1.0)
            assert math.isclose(monitor.last_trade_time, now - 120.0, rel_tol=0, abs_tol=1.0)
            data = await resp.json()
            assert data["status"] == "healthy"
            assert math.isclose(data["last_acquisition_time"], now - 2.0, rel_tol=0, abs_tol=1.0)
            assert math.isclose(data["last_trade_time"], now - 120.0, rel_tol=0, abs_tol=1.0)
            assert math.isclose(data["acquisition_freshness_seconds"], 2.0, rel_tol=0, abs_tol=1.0)
            assert math.isclose(data["trade_freshness_seconds"], 120.0, rel_tol=0, abs_tol=1.0)
            assert data["components"]["database"]["status"] == "up"
            assert data["components"]["redis"]["status"] == "up"
            assert data["components"]["ingestion"]["status"] == "up"
    finally:
        await monitor.stop()


def quiet_trades_monitor(acquired_seconds_ago: float) -> HealthMonitor:
    """A monitor whose only trade arrived 120 seconds ago and whose last acquisition is given."""
    clock = FakeClock(1_000_000.0 - 120.0)
    monitor = HealthMonitor(stale_threshold_seconds=60, clock=clock)
    monitor.record_event("trades")
    clock.advance(120.0)
    monitor.record_acquisition(clock() - acquired_seconds_ago)
    return monitor


def test_quiet_stream_with_fresh_acquisition_stays_active() -> None:
    """A previously active stream with no recent trades but fresh acquisitions is active."""
    monitor = quiet_trades_monitor(acquired_seconds_ago=2.0)

    report = monitor.get_health_report()

    assert report.streams["trades"].status.value == "active"
    assert report.status.value == "healthy"


def test_stream_goes_stale_when_acquisition_also_stops() -> None:
    """When both trades and acquisitions age past the threshold the stream is stale."""
    monitor = quiet_trades_monitor(acquired_seconds_ago=90.0)

    report = monitor.get_health_report()

    assert report.streams["trades"].status.value == "stale"
    assert report.status.value == "degraded"


def test_never_evented_stream_with_fresh_acquisition_stays_connected() -> None:
    """A connected stream that has produced no trade yet is not stale while acquiring."""
    clock = FakeClock(1_000_000.0 - 120.0)
    monitor = HealthMonitor(stale_threshold_seconds=60, clock=clock)
    monitor.set_stream_connected("trades")
    clock.advance(120.0)
    monitor.record_acquisition(clock() - 1.0)

    report = monitor.get_health_report()

    assert report.streams["trades"].status.value == "active"


@pytest.mark.asyncio
async def test_ready_endpoint_reports_degraded_component_without_failing() -> None:
    """A degraded (recoverable) component is visible in /ready but does not fail readiness.

    FR-002 fails readiness only for an unavailable dependency, a terminal ingestion
    failure, or blocked progress; a degraded source that is still making progress is
    reported truthfully instead of being hidden as "up".
    """
    monitor = HealthMonitor()
    wire_components(
        monitor,
        database=ComponentStatus(status="up"),
        redis=ComponentStatus(status="up"),
        ingestion=ComponentStatus(status="degraded", last_error="acquisition cycle failed: 503"),
    )
    status, data = await probe(monitor, 19111, "/ready")
    assert status == 200
    assert data["ready"] is True
    assert data["components"]["ingestion"] == "degraded"


@pytest.mark.asyncio
async def test_health_endpoint_reports_degraded_component_with_error() -> None:
    """/health carries a degraded component's error and reports overall degraded (200)."""
    monitor = HealthMonitor()
    wire_components(
        monitor,
        ingestion=ComponentStatus(status="degraded", last_error="possible-data-loss"),
    )
    status, data = await probe(monitor, 19112, "/health")
    assert status == 200
    assert data["status"] == "degraded"
    assert data["components"]["ingestion"]["status"] == "degraded"
    assert data["components"]["ingestion"]["last_error"] == "possible-data-loss"


@pytest.mark.asyncio
async def test_health_routes_only_on_effective_port(
    unused_tcp_port_factory: Any,
) -> None:
    """SC-006: every route responds on the override port and none on the superseded port."""
    superseded = unused_tcp_port_factory()
    effective = unused_tcp_port_factory()
    monitor = HealthMonitor()
    await monitor.start()
    await monitor.start_http_server(port=effective)
    try:
        async with aiohttp.ClientSession() as session:
            for route in ("/live", "/ready", "/health", "/metrics"):
                async with session.get(f"http://127.0.0.1:{effective}{route}") as resp:
                    assert resp.status == 200, route

            assert {port for _, port in monitor.http_addresses} == {effective}

            with pytest.raises(aiohttp.ClientConnectorError):
                await session.get(
                    f"http://127.0.0.1:{superseded}/live",
                    timeout=aiohttp.ClientTimeout(total=2),
                )
    finally:
        await monitor.stop()
    assert monitor.http_addresses == []


@pytest.mark.asyncio
async def test_health_endpoint_returns_503_when_component_down() -> None:
    """Test /health returns 503 when a component is unhealthy."""
    monitor = HealthMonitor()
    wire_components(
        monitor,
        redis=ComponentStatus(status="down", last_error="redis connection refused"),
    )
    status, data = await probe(monitor, 19106, "/health")
    assert status == 503
    assert data["status"] == "unhealthy"
    assert data["components"]["redis"]["status"] == "down"
    assert "redis connection refused" in data["components"]["redis"]["last_error"]


@pytest.mark.asyncio
async def test_port_already_in_use_raises_actionable_error() -> None:
    """A second server on an occupied health port must fail loudly, not bind silently."""
    first = HealthMonitor()
    second = HealthMonitor()
    port = 19108
    await first.start()
    await first.start_http_server(port=port)
    try:
        await second.start()
        with pytest.raises(OSError):
            await second.start_http_server(port=port)
    finally:
        await second.stop()
        await first.stop()


@pytest.mark.asyncio
async def test_hanging_component_check_is_bounded_and_marked_down() -> None:
    """A hung dependency check must be bounded and reported down, not hang readiness."""
    monitor = HealthMonitor()

    async def hanging_checker() -> ComponentStatus:
        await asyncio.sleep(30)
        return ComponentStatus(status="up")

    monitor.set_component_checker("database", hanging_checker)

    components = await asyncio.wait_for(monitor.evaluate_components(), timeout=5.0)

    assert components["database"].status == "down"
    assert "timed out" in (components["database"].last_error or "")


def _gauge_value(metrics_text: str, name: str) -> float:
    for line in metrics_text.splitlines():
        if line.startswith(f"{name} "):
            return float(line.split(" ", 1)[1])
    raise AssertionError(f"gauge {name} not found in metrics output")


@pytest.mark.asyncio
async def test_prometheus_health_gauge_agrees_with_health_endpoint() -> None:
    """Round-4 finding 5: a down dependency made /health unhealthy while
    polymarket_health_status still exported 1.0. Both must derive from one
    combined component-and-stream snapshot."""
    monitor = HealthMonitor()
    monitor.record_event("trades")
    wire_components(
        monitor,
        database=ComponentStatus(status="down", last_error="connection refused"),
    )
    port = 19113
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/health") as resp:
                assert resp.status == 503
                assert (await resp.json())["status"] == "unhealthy"

            async with session.get(f"http://127.0.0.1:{port}/metrics") as resp:
                text = await resp.text()
        assert _gauge_value(text, "polymarket_health_status") == 0.0
    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_prometheus_health_gauge_reports_degraded_component() -> None:
    """A degraded component exports 0.5, matching the /health "degraded" verdict."""
    monitor = HealthMonitor()
    monitor.record_event("trades")
    wire_components(
        monitor,
        ingestion=ComponentStatus(status="degraded", last_error="possible-data-loss"),
    )
    port = 19114
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/health") as resp:
                assert resp.status == 200
                assert (await resp.json())["status"] == "degraded"
            async with session.get(f"http://127.0.0.1:{port}/metrics") as resp:
                text = await resp.text()
        assert _gauge_value(text, "polymarket_health_status") == 0.5
    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_metrics_scrape_alone_evaluates_components_for_the_gauge() -> None:
    """A scrape without any prior /health call must still export the combined truth."""
    monitor = HealthMonitor()
    monitor.record_event("trades")
    wire_components(
        monitor,
        redis=ComponentStatus(status="down", last_error="refused"),
    )
    port = 19115
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{port}/metrics") as resp,
        ):
            text = await resp.text()
        assert _gauge_value(text, "polymarket_health_status") == 0.0
    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_component_checks_run_concurrently() -> None:
    """Round-4 finding 9 (deterministic half): all checks must be in flight at once.

    Each checker blocks on a shared barrier, so sequential evaluation would time
    every one of them out; concurrent evaluation reports all up.
    """
    monitor = HealthMonitor()
    barrier = asyncio.Barrier(3)

    def barrier_checker() -> Any:
        async def check() -> ComponentStatus:
            await barrier.wait()
            return ComponentStatus(status="up")

        return check

    for name in ("database", "redis", "ingestion"):
        monitor.set_component_checker(name, barrier_checker())

    components = await asyncio.wait_for(monitor.evaluate_components(), timeout=5.0)

    assert {name: c.status for name, c in components.items()} == {
        "database": "up",
        "redis": "up",
        "ingestion": "up",
    }


@pytest.mark.asyncio
async def test_probe_with_every_component_hung_stays_inside_one_budget() -> None:
    """Round-4 finding 9: three hung checks used to serialize into ~3s; the whole
    probe must stay within a single sub-100ms check budget (plus scheduling slack)."""
    monitor = HealthMonitor()

    async def hanging_checker() -> ComponentStatus:
        await asyncio.sleep(30)
        return ComponentStatus(status="up")

    for name in ("database", "redis", "ingestion"):
        monitor.set_component_checker(name, hanging_checker)

    loop = asyncio.get_running_loop()
    started = loop.time()
    components = await asyncio.wait_for(monitor.evaluate_components(), timeout=5.0)
    elapsed = loop.time() - started

    assert all(c.status == "down" for c in components.values())
    assert all("timed out" in (c.last_error or "") for c in components.values())
    assert elapsed < 0.5, f"probe took {elapsed:.3f}s; hung checks must not accumulate"


@pytest.mark.asyncio
async def test_metrics_endpoint() -> None:
    """Test /metrics endpoint returns Prometheus metrics in plain text."""
    monitor = HealthMonitor()
    port = 19107
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{port}/metrics") as resp,
        ):
            assert resp.status == 200
            assert "text/plain" in resp.headers["Content-Type"]
            text = await resp.text()
            assert "polymarket_health_status" in text
            assert "polymarket_events_total" in text
    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_health_endpoint_reports_pipeline_last_error() -> None:
    """/health must surface the pipeline's most recent processing error (FR-003)."""
    monitor = HealthMonitor()
    monitor.set_last_error_provider(lambda: "detector exploded for trade t1")
    port = 19109
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{port}/health") as resp,
        ):
            data = await resp.json()
            assert data["last_error"] == "detector exploded for trade t1"
    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_health_endpoint_last_error_is_null_without_recorded_errors() -> None:
    """A healthy run reports an explicit null last_error, not a missing key."""
    monitor = HealthMonitor()
    port = 19110
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{port}/health") as resp,
        ):
            data = await resp.json()
            assert "last_error" in data
            assert data["last_error"] is None
    finally:
        await monitor.stop()
