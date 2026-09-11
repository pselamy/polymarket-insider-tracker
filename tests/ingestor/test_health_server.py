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
import time
from typing import Any

import aiohttp
import pytest

from polymarket_insider_tracker.ingestor.health import (
    ComponentStatus,
    HealthMonitor,
)


@pytest.mark.asyncio
async def test_live_endpoint() -> None:
    """Test /live endpoint returns 200 and live: true."""
    monitor = HealthMonitor()
    port = 19101
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{port}/live") as resp,
        ):
            assert resp.status == 200
            data = await resp.json()
            assert data == {"live": True}
    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_ready_endpoint_all_up() -> None:
    """Test /ready endpoint returns 200 when all components are up."""
    monitor = HealthMonitor()
    monitor.set_component_checker(
        "database",
        lambda: asyncio.sleep(0, result=ComponentStatus(status="up", latency_ms=1.5)),
    )
    monitor.set_component_checker(
        "redis",
        lambda: asyncio.sleep(0, result=ComponentStatus(status="up", latency_ms=0.5)),
    )
    monitor.set_component_checker(
        "ingestion",
        lambda: asyncio.sleep(0, result=ComponentStatus(status="up")),
    )
    port = 19102
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{port}/ready") as resp,
        ):
            assert resp.status == 200
            data = await resp.json()
            assert data["ready"] is True
            assert data["components"] == {
                "database": "up",
                "redis": "up",
                "ingestion": "up",
            }
    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_ready_endpoint_database_down() -> None:
    """Test /ready endpoint returns 503 when database is down."""
    monitor = HealthMonitor()
    monitor.set_component_checker(
        "database",
        lambda: asyncio.sleep(
            0,
            result=ComponentStatus(status="down", last_error="connection refused"),
        ),
    )
    monitor.set_component_checker(
        "redis",
        lambda: asyncio.sleep(0, result=ComponentStatus(status="up", latency_ms=0.5)),
    )
    monitor.set_component_checker(
        "ingestion",
        lambda: asyncio.sleep(0, result=ComponentStatus(status="up")),
    )
    port = 19103
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{port}/ready") as resp,
        ):
            assert resp.status == 503
            data = await resp.json()
            assert data["ready"] is False
            assert data["reason"] == "database_unreachable"
            assert data["components"]["database"] == "down"
    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_ready_endpoint_ingestion_crashed() -> None:
    """Test /ready endpoint returns 503 when ingestion worker has failed."""
    monitor = HealthMonitor()
    monitor.set_component_checker(
        "database",
        lambda: asyncio.sleep(0, result=ComponentStatus(status="up")),
    )
    monitor.set_component_checker(
        "redis",
        lambda: asyncio.sleep(0, result=ComponentStatus(status="up")),
    )
    monitor.set_component_checker(
        "ingestion",
        lambda: asyncio.sleep(
            0,
            result=ComponentStatus(status="down", last_error="ingestion_worker_failed"),
        ),
    )
    port = 19104
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{port}/ready") as resp,
        ):
            assert resp.status == 503
            data = await resp.json()
            assert data["ready"] is False
            assert data["reason"] == "ingestion_worker_failed"
            assert data["components"]["ingestion"] == "down"
    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_health_endpoint_quiet_market_period() -> None:
    """Test /health distinguishes acquisition freshness from trade freshness.

    In a quiet market, trade events may not have occurred for minutes, but if
    acquisition polling ran 2 seconds ago, the status is healthy. The monitor holds
    the production stream state (a previously active "trades" stream), so trade
    silence alone must not mark the reachable source stale.
    """
    monitor = HealthMonitor()
    now = time.time()
    # The stream received a trade once, 120 seconds ago (production state after a
    # quiet interval), and acquisition polling last succeeded 2 seconds ago.
    monitor.record_event("trades")
    monitor._streams["trades"].last_event_time = now - 120.0
    monitor.record_acquisition(now - 2.0)
    monitor.record_trade_arrival(now - 120.0)

    monitor.set_component_checker(
        "database",
        lambda: asyncio.sleep(0, result=ComponentStatus(status="up", latency_ms=1.2)),
    )
    monitor.set_component_checker(
        "redis",
        lambda: asyncio.sleep(0, result=ComponentStatus(status="up", latency_ms=0.4)),
    )
    monitor.set_component_checker(
        "ingestion",
        lambda: asyncio.sleep(0, result=ComponentStatus(status="up")),
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
            assert monitor.last_acquisition_time == pytest.approx(now - 2.0, abs=1.0)
            assert monitor.last_trade_time == pytest.approx(now - 120.0, abs=1.0)
            data = await resp.json()
            assert data["status"] == "healthy"
            assert data["last_acquisition_time"] == pytest.approx(now - 2.0, abs=1.0)
            assert data["last_trade_time"] == pytest.approx(now - 120.0, abs=1.0)
            assert data["acquisition_freshness_seconds"] == pytest.approx(2.0, abs=1.0)
            assert data["trade_freshness_seconds"] == pytest.approx(120.0, abs=1.0)
            assert data["components"]["database"]["status"] == "up"
            assert data["components"]["redis"]["status"] == "up"
            assert data["components"]["ingestion"]["status"] == "up"
    finally:
        await monitor.stop()


def test_quiet_stream_with_fresh_acquisition_stays_active() -> None:
    """A previously active stream with no recent trades but fresh acquisitions is active."""
    monitor = HealthMonitor(stale_threshold_seconds=60)
    now = time.time()
    monitor.record_event("trades")
    monitor._streams["trades"].last_event_time = now - 120.0
    monitor.record_acquisition(now - 2.0)

    report = monitor.get_health_report()

    assert report.streams["trades"].status.value == "active"
    assert report.status.value == "healthy"


def test_stream_goes_stale_when_acquisition_also_stops() -> None:
    """When both trades and acquisitions age past the threshold the stream is stale."""
    monitor = HealthMonitor(stale_threshold_seconds=60)
    now = time.time()
    monitor.record_event("trades")
    monitor._streams["trades"].last_event_time = now - 120.0
    monitor.record_acquisition(now - 90.0)

    report = monitor.get_health_report()

    assert report.streams["trades"].status.value == "stale"
    assert report.status.value == "degraded"


def test_never_evented_stream_with_fresh_acquisition_stays_connected() -> None:
    """A connected stream that has produced no trade yet is not stale while acquiring."""
    monitor = HealthMonitor(stale_threshold_seconds=60)
    monitor.set_stream_connected("trades")
    monitor._streams["trades"].connected_since = time.time() - 120.0
    monitor.record_acquisition(time.time() - 1.0)

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
    monitor.set_component_checker(
        "database",
        lambda: asyncio.sleep(0, result=ComponentStatus(status="up")),
    )
    monitor.set_component_checker(
        "redis",
        lambda: asyncio.sleep(0, result=ComponentStatus(status="up")),
    )
    monitor.set_component_checker(
        "ingestion",
        lambda: asyncio.sleep(
            0,
            result=ComponentStatus(status="degraded", last_error="acquisition cycle failed: 503"),
        ),
    )
    port = 19111
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{port}/ready") as resp,
        ):
            assert resp.status == 200
            data = await resp.json()
            assert data["ready"] is True
            assert data["components"]["ingestion"] == "degraded"
    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_health_endpoint_reports_degraded_component_with_error() -> None:
    """/health carries a degraded component's error and reports overall degraded (200)."""
    monitor = HealthMonitor()
    monitor.set_component_checker(
        "ingestion",
        lambda: asyncio.sleep(
            0,
            result=ComponentStatus(status="degraded", last_error="possible-data-loss"),
        ),
    )
    port = 19112
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{port}/health") as resp,
        ):
            assert resp.status == 200
            data = await resp.json()
            assert data["status"] == "degraded"
            assert data["components"]["ingestion"]["status"] == "degraded"
            assert data["components"]["ingestion"]["last_error"] == "possible-data-loss"
    finally:
        await monitor.stop()


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
    monitor.set_component_checker(
        "redis",
        lambda: asyncio.sleep(
            0,
            result=ComponentStatus(status="down", last_error="redis connection refused"),
        ),
    )
    port = 19106
    await monitor.start()
    await monitor.start_http_server(port=port)
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{port}/health") as resp,
        ):
            assert resp.status == 503
            data = await resp.json()
            assert data["status"] == "unhealthy"
            assert data["components"]["redis"]["status"] == "down"
            assert "redis connection refused" in data["components"]["redis"]["last_error"]
    finally:
        await monitor.stop()


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
