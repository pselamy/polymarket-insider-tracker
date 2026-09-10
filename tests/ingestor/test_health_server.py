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
    acquisition polling ran 2 seconds ago, the status is healthy.
    """
    monitor = HealthMonitor()
    now = time.time()
    # Acquisition was 2 seconds ago
    monitor.record_acquisition(now - 2.0)
    # Trade was 120 seconds ago
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
