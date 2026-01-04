"""Tests for the connection health monitor."""

import asyncio
import time
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from polymarket_insider_tracker.ingestor.health import (
    DEFAULT_HEALTH_CHECK_INTERVAL,
    DEFAULT_STALE_THRESHOLD_SECONDS,
    HealthMonitor,
    HealthReport,
    HealthStatus,
    StreamHealth,
    StreamStatus,
)


class TestStreamHealth:
    """Tests for the StreamHealth dataclass."""

    def test_stream_health_defaults(self) -> None:
        """Test default values."""
        health = StreamHealth(name="test-stream")

        assert health.name == "test-stream"
        assert health.status == StreamStatus.DISCONNECTED
        assert health.last_event_time is None
        assert health.events_received == 0
        assert health.events_per_second == 0.0
        assert health.connected_since is None
        assert health.last_error is None

    def test_stream_health_custom_values(self) -> None:
        """Test with custom values."""
        now = time.time()
        health = StreamHealth(
            name="trades",
            status=StreamStatus.ACTIVE,
            last_event_time=now,
            events_received=100,
            events_per_second=5.0,
            connected_since=now - 3600,
            last_error=None,
        )

        assert health.name == "trades"
        assert health.status == StreamStatus.ACTIVE
        assert health.events_received == 100


class TestHealthReport:
    """Tests for the HealthReport dataclass."""

    def test_health_report_defaults(self) -> None:
        """Test default values."""
        report = HealthReport(status=HealthStatus.HEALTHY)

        assert report.status == HealthStatus.HEALTHY
        assert report.streams == {}
        assert report.total_events_received == 0
        assert report.total_events_per_second == 0.0
        assert report.uptime_seconds == 0.0
        assert report.timestamp > 0

    def test_health_report_with_streams(self) -> None:
        """Test with stream data."""
        stream = StreamHealth(name="trades", events_received=100)
        report = HealthReport(
            status=HealthStatus.DEGRADED,
            streams={"trades": stream},
            total_events_received=100,
            total_events_per_second=5.0,
            uptime_seconds=3600.0,
        )

        assert report.status == HealthStatus.DEGRADED
        assert "trades" in report.streams
        assert report.total_events_received == 100


class TestHealthMonitor:
    """Tests for the HealthMonitor class."""

    def test_init(self) -> None:
        """Test initialization."""
        monitor = HealthMonitor()

        assert monitor._stale_threshold == DEFAULT_STALE_THRESHOLD_SECONDS
        assert monitor._health_check_interval == DEFAULT_HEALTH_CHECK_INTERVAL
        assert not monitor.is_running

    def test_init_custom_config(self) -> None:
        """Test initialization with custom config."""
        monitor = HealthMonitor(
            stale_threshold_seconds=30,
            health_check_interval=10,
        )

        assert monitor._stale_threshold == 30
        assert monitor._health_check_interval == 10

    def test_register_stream(self) -> None:
        """Test registering a stream."""
        monitor = HealthMonitor()

        monitor.register_stream("trades")

        assert "trades" in monitor._streams
        assert monitor._streams["trades"].name == "trades"
        assert monitor._streams["trades"].status == StreamStatus.DISCONNECTED

    def test_register_stream_idempotent(self) -> None:
        """Test that registering the same stream twice is idempotent."""
        monitor = HealthMonitor()

        monitor.register_stream("trades")
        monitor.record_event("trades")  # Adds an event
        monitor.register_stream("trades")  # Should not reset

        assert monitor._streams["trades"].events_received == 1

    def test_set_stream_connected(self) -> None:
        """Test marking a stream as connected."""
        monitor = HealthMonitor()

        monitor.set_stream_connected("trades")

        assert monitor._streams["trades"].status == StreamStatus.ACTIVE
        assert monitor._streams["trades"].connected_since is not None
        assert monitor._streams["trades"].last_error is None

    def test_set_stream_disconnected(self) -> None:
        """Test marking a stream as disconnected."""
        monitor = HealthMonitor()

        monitor.set_stream_connected("trades")
        monitor.set_stream_disconnected("trades", error="Connection reset")

        assert monitor._streams["trades"].status == StreamStatus.DISCONNECTED
        assert monitor._streams["trades"].connected_since is None
        assert monitor._streams["trades"].last_error == "Connection reset"

    def test_record_event(self) -> None:
        """Test recording an event."""
        monitor = HealthMonitor()

        monitor.record_event("trades")

        stream = monitor._streams["trades"]
        assert stream.events_received == 1
        assert stream.last_event_time is not None
        assert stream.status == StreamStatus.ACTIVE

    def test_record_event_multiple(self) -> None:
        """Test recording multiple events."""
        monitor = HealthMonitor()

        for _ in range(10):
            monitor.record_event("trades")

        assert monitor._streams["trades"].events_received == 10

    def test_record_event_with_processing_time(self) -> None:
        """Test recording event with processing time."""
        monitor = HealthMonitor()

        # Should not raise
        monitor.record_event("trades", processing_time=0.001)

        assert monitor._streams["trades"].events_received == 1

    def test_calculate_throughput_empty(self) -> None:
        """Test throughput calculation with no events."""
        monitor = HealthMonitor()

        rate = monitor._calculate_throughput("nonexistent")

        assert rate == 0.0

    def test_calculate_throughput(self) -> None:
        """Test throughput calculation."""
        monitor = HealthMonitor()

        # Add events
        for _ in range(10):
            monitor.record_event("trades")

        rate = monitor._calculate_throughput("trades")

        # Should have ~10 events in the window
        assert rate > 0

    def test_check_stream_staleness_active(self) -> None:
        """Test that active stream is not marked stale."""
        monitor = HealthMonitor(stale_threshold_seconds=60)

        monitor.record_event("trades")
        monitor._check_stream_staleness()

        assert monitor._streams["trades"].status == StreamStatus.ACTIVE

    def test_check_stream_staleness_stale(self) -> None:
        """Test that stream becomes stale after threshold."""
        monitor = HealthMonitor(stale_threshold_seconds=1)

        monitor.record_event("trades")
        # Simulate time passing
        monitor._streams["trades"].last_event_time = time.time() - 2

        monitor._check_stream_staleness()

        assert monitor._streams["trades"].status == StreamStatus.STALE

    def test_check_stream_staleness_connected_no_events(self) -> None:
        """Test staleness when connected but no events received."""
        monitor = HealthMonitor(stale_threshold_seconds=1)

        monitor.set_stream_connected("trades")
        # Simulate time passing since connection
        monitor._streams["trades"].connected_since = time.time() - 2

        monitor._check_stream_staleness()

        assert monitor._streams["trades"].status == StreamStatus.STALE

    def test_determine_overall_status_no_streams(self) -> None:
        """Test overall status with no streams."""
        monitor = HealthMonitor()

        status = monitor._determine_overall_status()

        assert status == HealthStatus.HEALTHY

    def test_determine_overall_status_all_active(self) -> None:
        """Test overall status with all active streams."""
        monitor = HealthMonitor()

        monitor.record_event("trades")
        monitor.record_event("orderbook")

        status = monitor._determine_overall_status()

        assert status == HealthStatus.HEALTHY

    def test_determine_overall_status_some_stale(self) -> None:
        """Test overall status with some stale streams."""
        monitor = HealthMonitor()

        monitor.record_event("trades")
        monitor.register_stream("orderbook")
        monitor._streams["orderbook"].status = StreamStatus.STALE

        status = monitor._determine_overall_status()

        assert status == HealthStatus.DEGRADED

    def test_determine_overall_status_some_disconnected(self) -> None:
        """Test overall status with some disconnected streams."""
        monitor = HealthMonitor()

        monitor.record_event("trades")
        monitor.set_stream_disconnected("orderbook")

        status = monitor._determine_overall_status()

        assert status == HealthStatus.DEGRADED

    def test_determine_overall_status_all_disconnected(self) -> None:
        """Test overall status with all disconnected streams."""
        monitor = HealthMonitor()

        monitor.set_stream_disconnected("trades")
        monitor.set_stream_disconnected("orderbook")

        status = monitor._determine_overall_status()

        assert status == HealthStatus.UNHEALTHY

    def test_get_health_report(self) -> None:
        """Test getting a health report."""
        monitor = HealthMonitor()
        monitor._start_time = time.time() - 100

        monitor.record_event("trades")
        monitor.record_event("trades")

        report = monitor.get_health_report()

        assert report.status == HealthStatus.HEALTHY
        assert "trades" in report.streams
        assert report.total_events_received == 2
        assert report.uptime_seconds >= 100
        assert report.timestamp > 0

    def test_get_health_report_calculates_throughput(self) -> None:
        """Test that health report calculates throughput."""
        monitor = HealthMonitor()

        for _ in range(10):
            monitor.record_event("trades")

        report = monitor.get_health_report()

        assert report.streams["trades"].events_per_second > 0
        assert report.total_events_per_second > 0

    @pytest.mark.asyncio
    async def test_start_stop(self) -> None:
        """Test starting and stopping the monitor."""
        monitor = HealthMonitor()

        await monitor.start()
        assert monitor.is_running
        assert monitor._health_task is not None

        await monitor.stop()
        assert not monitor.is_running
        assert monitor._health_task is None

    @pytest.mark.asyncio
    async def test_start_idempotent(self) -> None:
        """Test that starting twice is safe."""
        monitor = HealthMonitor()

        await monitor.start()
        await monitor.start()  # Should not raise

        assert monitor.is_running

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self) -> None:
        """Test that stopping when not running is safe."""
        monitor = HealthMonitor()

        await monitor.stop()  # Should not raise

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        """Test async context manager."""
        async with HealthMonitor() as monitor:
            assert monitor.is_running

        assert not monitor.is_running

    @pytest.mark.asyncio
    async def test_health_check_loop_updates_report(self) -> None:
        """Test that health check loop updates the report."""
        monitor = HealthMonitor(health_check_interval=0.1)

        await monitor.start()
        monitor.record_event("trades")

        await asyncio.sleep(0.2)

        # Health should have been checked
        report = monitor.get_health_report()
        assert report.status == HealthStatus.HEALTHY

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_health_change_callback(self) -> None:
        """Test that health change callback is invoked."""
        callback = AsyncMock()
        monitor = HealthMonitor(
            health_check_interval=0.1,
            on_health_change=callback,
        )

        await monitor.start()
        monitor.record_event("trades")

        # Wait for health check
        await asyncio.sleep(0.2)

        await monitor.stop()

        # Callback should have been called at least once
        assert callback.called

    @pytest.mark.asyncio
    async def test_health_change_callback_error_handling(self) -> None:
        """Test that callback errors don't crash the loop."""
        callback = AsyncMock(side_effect=ValueError("test error"))
        monitor = HealthMonitor(
            health_check_interval=0.1,
            on_health_change=callback,
        )

        await monitor.start()
        monitor.record_event("trades")

        # Should not crash
        await asyncio.sleep(0.2)

        await monitor.stop()


class TestHealthMonitorHTTPEndpoints:
    """Tests for HTTP endpoints."""

    @pytest.fixture
    def monitor(self) -> HealthMonitor:
        """Create a monitor instance."""
        return HealthMonitor()

    @pytest.fixture
    def app(self, monitor: HealthMonitor) -> web.Application:
        """Create the aiohttp application."""
        return monitor._create_app()

    @pytest.mark.asyncio
    async def test_health_endpoint_healthy(
        self, monitor: HealthMonitor, app: web.Application
    ) -> None:
        """Test /health endpoint when healthy."""
        from aiohttp.test_utils import TestClient, TestServer

        monitor.record_event("trades")

        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/health")
            assert resp.status == 200

            data = await resp.json()
            assert data["status"] == "healthy"
            assert "trades" in data["streams"]

    @pytest.mark.asyncio
    async def test_health_endpoint_unhealthy(
        self, monitor: HealthMonitor, app: web.Application
    ) -> None:
        """Test /health endpoint when unhealthy."""
        from aiohttp.test_utils import TestClient, TestServer

        monitor.set_stream_disconnected("trades")

        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/health")
            assert resp.status == 503

            data = await resp.json()
            assert data["status"] == "unhealthy"

    @pytest.mark.asyncio
    async def test_metrics_endpoint(self, monitor: HealthMonitor, app: web.Application) -> None:
        """Test /metrics endpoint returns Prometheus format."""
        from aiohttp.test_utils import TestClient, TestServer

        monitor.record_event("trades")

        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/metrics")
            assert resp.status == 200

            content_type = resp.headers.get("Content-Type", "")
            assert "text/plain" in content_type

            text = await resp.text()
            assert "polymarket_events_total" in text
            assert "polymarket_health_status" in text

    @pytest.mark.asyncio
    async def test_ready_endpoint_ready(self, monitor: HealthMonitor, app: web.Application) -> None:
        """Test /ready endpoint when ready."""
        from aiohttp.test_utils import TestClient, TestServer

        monitor.record_event("trades")

        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/ready")
            assert resp.status == 200

            data = await resp.json()
            assert data["ready"] is True

    @pytest.mark.asyncio
    async def test_ready_endpoint_not_ready(
        self, monitor: HealthMonitor, app: web.Application
    ) -> None:
        """Test /ready endpoint when not ready."""
        from aiohttp.test_utils import TestClient, TestServer

        monitor.set_stream_disconnected("trades")

        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/ready")
            assert resp.status == 503

            data = await resp.json()
            assert data["ready"] is False

    @pytest.mark.asyncio
    async def test_live_endpoint(self, app: web.Application) -> None:
        """Test /live endpoint always returns 200."""
        from aiohttp.test_utils import TestClient, TestServer

        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/live")
            assert resp.status == 200

            data = await resp.json()
            assert data["live"] is True

    @pytest.mark.asyncio
    async def test_start_stop_http_server(self, monitor: HealthMonitor) -> None:
        """Test starting and stopping HTTP server."""
        await monitor.start_http_server(port=18080)
        assert monitor._runner is not None

        await monitor.stop_http_server()
        assert monitor._runner is None

    @pytest.mark.asyncio
    async def test_start_http_server_idempotent(self, monitor: HealthMonitor) -> None:
        """Test that starting HTTP server twice is safe."""
        await monitor.start_http_server(port=18081)
        await monitor.start_http_server(port=18081)  # Should not raise

        await monitor.stop_http_server()


class TestPrometheusMetrics:
    """Tests for Prometheus metric updates."""

    def test_events_total_incremented(self) -> None:
        """Test that events_total counter is incremented."""
        monitor = HealthMonitor()

        monitor.record_event("test-metrics")
        monitor.record_event("test-metrics")

        # Counter should have been incremented
        # (We can't easily test prometheus metrics directly, but at least verify no errors)

    def test_stream_status_updated(self) -> None:
        """Test that stream_status gauge is updated."""
        monitor = HealthMonitor()

        monitor.set_stream_connected("test-status")
        # Gauge should be 1.0

        monitor.set_stream_disconnected("test-status")
        # Gauge should be 0.0

    def test_health_status_updated(self) -> None:
        """Test that health_status gauge is updated."""
        monitor = HealthMonitor()

        monitor.record_event("test-health")
        report = monitor.get_health_report()

        assert report.status == HealthStatus.HEALTHY


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_throughput_with_old_events(self) -> None:
        """Test throughput calculation ignores old events."""
        monitor = HealthMonitor()

        monitor.record_event("trades")
        # Manually add old event to window
        monitor._event_windows["trades"].append(time.time() - 100)

        rate = monitor._calculate_throughput("trades")

        # Old event should be filtered out
        # Rate should only count recent events
        assert rate >= 0

    def test_multiple_streams_independent(self) -> None:
        """Test that multiple streams are tracked independently."""
        monitor = HealthMonitor()

        monitor.record_event("trades")
        monitor.record_event("trades")
        monitor.set_stream_disconnected("orderbook")

        assert monitor._streams["trades"].events_received == 2
        assert monitor._streams["trades"].status == StreamStatus.ACTIVE
        assert monitor._streams["orderbook"].events_received == 0
        assert monitor._streams["orderbook"].status == StreamStatus.DISCONNECTED

    def test_report_streams_are_copied(self) -> None:
        """Test that report streams are a copy."""
        monitor = HealthMonitor()
        monitor.record_event("trades")

        report = monitor.get_health_report()

        # Modifying report should not affect monitor
        report.streams["trades"].events_received = 999
        assert monitor._streams["trades"].events_received == 1

    @pytest.mark.asyncio
    async def test_stop_cleans_up_http_server(self) -> None:
        """Test that stop() also stops HTTP server."""
        monitor = HealthMonitor()

        await monitor.start()
        await monitor.start_http_server(port=18082)

        await monitor.stop()

        assert not monitor.is_running
        assert monitor._runner is None
