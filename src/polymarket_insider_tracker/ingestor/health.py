"""Connection health monitor with metrics and HTTP endpoints.

This module provides health monitoring for the data ingestion layer,
tracking connection states, event throughput, and staleness detection.
"""

import asyncio
import contextlib
import copy
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from aiohttp import web
from prometheus_client import Counter, Gauge, Histogram, generate_latest

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_STALE_THRESHOLD_SECONDS = 60  # No events for 60s = stale
DEFAULT_HEALTH_CHECK_INTERVAL = 5  # seconds
DEFAULT_HTTP_PORT = 8080


class HealthStatus(Enum):
    """Overall health status."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class StreamStatus(Enum):
    """Status of an individual stream."""

    ACTIVE = "active"
    STALE = "stale"
    DISCONNECTED = "disconnected"


@dataclass
class StreamHealth:
    """Health status for an individual stream."""

    name: str
    status: StreamStatus = StreamStatus.DISCONNECTED
    last_event_time: float | None = None
    events_received: int = 0
    events_per_second: float = 0.0
    connected_since: float | None = None
    last_error: str | None = None


@dataclass
class HealthReport:
    """Comprehensive health report for all streams."""

    status: HealthStatus
    streams: dict[str, StreamHealth] = field(default_factory=dict)
    total_events_received: int = 0
    total_events_per_second: float = 0.0
    uptime_seconds: float = 0.0
    timestamp: float = field(default_factory=time.time)


# Type aliases
HealthCallback = Callable[[HealthReport], Awaitable[None]]


# Prometheus metrics
EVENTS_TOTAL = Counter(
    "polymarket_events_total",
    "Total number of events received",
    ["stream"],
)

EVENTS_PER_SECOND = Gauge(
    "polymarket_events_per_second",
    "Current events per second rate",
    ["stream"],
)

STREAM_STATUS = Gauge(
    "polymarket_stream_status",
    "Stream status (1=active, 0.5=stale, 0=disconnected)",
    ["stream"],
)

LAST_EVENT_TIMESTAMP = Gauge(
    "polymarket_last_event_timestamp",
    "Unix timestamp of last event received",
    ["stream"],
)

EVENT_LATENCY = Histogram(
    "polymarket_event_latency_seconds",
    "Event processing latency in seconds",
    ["stream"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

HEALTH_STATUS = Gauge(
    "polymarket_health_status",
    "Overall health status (1=healthy, 0.5=degraded, 0=unhealthy)",
)


class HealthMonitor:
    """Monitor connection health and expose metrics.

    This class tracks the health of multiple streams, calculates throughput,
    detects stale streams, and exposes Prometheus-compatible metrics.

    Example:
        ```python
        monitor = HealthMonitor(stale_threshold_seconds=60)
        await monitor.start()

        # Record events
        monitor.record_event("trades", processing_time=0.001)

        # Update connection state
        monitor.set_stream_connected("trades")

        # Get health report
        report = monitor.get_health_report()

        # HTTP endpoints: /health and /metrics
        # Start HTTP server with monitor.start_http_server(port=8080)
        ```
    """

    def __init__(
        self,
        *,
        stale_threshold_seconds: float = DEFAULT_STALE_THRESHOLD_SECONDS,
        health_check_interval: float = DEFAULT_HEALTH_CHECK_INTERVAL,
        on_health_change: HealthCallback | None = None,
    ) -> None:
        """Initialize the health monitor.

        Args:
            stale_threshold_seconds: Seconds without events before stream is stale.
            health_check_interval: Seconds between health check updates.
            on_health_change: Optional callback when health status changes.
        """
        self._stale_threshold = stale_threshold_seconds
        self._health_check_interval = health_check_interval
        self._on_health_change = on_health_change

        self._streams: dict[str, StreamHealth] = {}
        self._start_time: float | None = None
        self._running = False
        self._health_task: asyncio.Task[None] | None = None
        self._last_health_status: HealthStatus | None = None

        # For throughput calculation
        self._event_windows: dict[str, list[float]] = {}
        self._window_duration = 10.0  # 10 second sliding window

        # HTTP server
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None

    @property
    def is_running(self) -> bool:
        """Return True if the monitor is running."""
        return self._running

    def register_stream(self, name: str) -> None:
        """Register a stream for monitoring.

        Args:
            name: Unique name for the stream.
        """
        if name not in self._streams:
            self._streams[name] = StreamHealth(name=name)
            self._event_windows[name] = []
            logger.info("Registered stream for monitoring: %s", name)

    def set_stream_connected(self, name: str) -> None:
        """Mark a stream as connected.

        Args:
            name: Stream name.
        """
        self.register_stream(name)
        stream = self._streams[name]
        stream.status = StreamStatus.ACTIVE
        stream.connected_since = time.time()
        stream.last_error = None
        STREAM_STATUS.labels(stream=name).set(1.0)
        logger.debug("Stream connected: %s", name)

    def set_stream_disconnected(self, name: str, error: str | None = None) -> None:
        """Mark a stream as disconnected.

        Args:
            name: Stream name.
            error: Optional error message.
        """
        self.register_stream(name)
        stream = self._streams[name]
        stream.status = StreamStatus.DISCONNECTED
        stream.connected_since = None
        stream.last_error = error
        STREAM_STATUS.labels(stream=name).set(0.0)
        logger.debug("Stream disconnected: %s (error: %s)", name, error)

    def record_event(
        self,
        stream_name: str,
        *,
        processing_time: float | None = None,
    ) -> None:
        """Record an event received from a stream.

        Args:
            stream_name: Name of the stream.
            processing_time: Optional processing latency in seconds.
        """
        self.register_stream(stream_name)
        now = time.time()

        stream = self._streams[stream_name]
        stream.events_received += 1
        stream.last_event_time = now
        stream.status = StreamStatus.ACTIVE

        # Update metrics
        EVENTS_TOTAL.labels(stream=stream_name).inc()
        LAST_EVENT_TIMESTAMP.labels(stream=stream_name).set(now)
        STREAM_STATUS.labels(stream=stream_name).set(1.0)

        if processing_time is not None:
            EVENT_LATENCY.labels(stream=stream_name).observe(processing_time)

        # Add to sliding window for throughput
        window = self._event_windows[stream_name]
        window.append(now)

        # Clean old entries from window
        cutoff = now - self._window_duration
        self._event_windows[stream_name] = [t for t in window if t > cutoff]

    def _calculate_throughput(self, stream_name: str) -> float:
        """Calculate events per second for a stream.

        Args:
            stream_name: Name of the stream.

        Returns:
            Events per second rate.
        """
        window = self._event_windows.get(stream_name, [])
        if not window:
            return 0.0

        now = time.time()
        cutoff = now - self._window_duration

        # Count events in window
        recent_events = [t for t in window if t > cutoff]
        if not recent_events:
            return 0.0

        # Calculate rate based on window
        window_span = now - cutoff
        return len(recent_events) / window_span if window_span > 0 else 0.0

    def _check_stream_staleness(self) -> None:
        """Check all streams for staleness."""
        now = time.time()

        for name, stream in self._streams.items():
            if stream.status == StreamStatus.DISCONNECTED:
                continue

            if stream.last_event_time is None:
                # Connected but no events yet - check connection time
                if stream.connected_since:
                    since_connect = now - stream.connected_since
                    if since_connect > self._stale_threshold:
                        stream.status = StreamStatus.STALE
                        STREAM_STATUS.labels(stream=name).set(0.5)
            else:
                since_event = now - stream.last_event_time
                if since_event > self._stale_threshold:
                    stream.status = StreamStatus.STALE
                    STREAM_STATUS.labels(stream=name).set(0.5)
                else:
                    stream.status = StreamStatus.ACTIVE
                    STREAM_STATUS.labels(stream=name).set(1.0)

    def _determine_overall_status(self) -> HealthStatus:
        """Determine overall health status based on stream states.

        Returns:
            Overall health status.
        """
        if not self._streams:
            return HealthStatus.HEALTHY  # No streams = healthy (nothing to monitor)

        statuses = [s.status for s in self._streams.values()]

        if all(s == StreamStatus.DISCONNECTED for s in statuses):
            return HealthStatus.UNHEALTHY

        if any(s == StreamStatus.DISCONNECTED for s in statuses):
            return HealthStatus.DEGRADED

        if any(s == StreamStatus.STALE for s in statuses):
            return HealthStatus.DEGRADED

        return HealthStatus.HEALTHY

    def get_health_report(self) -> HealthReport:
        """Generate a comprehensive health report.

        Returns:
            HealthReport with current status of all streams.
        """
        self._check_stream_staleness()

        # Update throughput metrics
        total_eps = 0.0
        for name, stream in self._streams.items():
            eps = self._calculate_throughput(name)
            stream.events_per_second = eps
            EVENTS_PER_SECOND.labels(stream=name).set(eps)
            total_eps += eps

        overall_status = self._determine_overall_status()
        HEALTH_STATUS.set(
            1.0 if overall_status == HealthStatus.HEALTHY
            else 0.5 if overall_status == HealthStatus.DEGRADED
            else 0.0
        )

        uptime = 0.0
        if self._start_time:
            uptime = time.time() - self._start_time

        # Deep copy streams to prevent mutations affecting internal state
        streams_copy = {name: copy.copy(stream) for name, stream in self._streams.items()}

        return HealthReport(
            status=overall_status,
            streams=streams_copy,
            total_events_received=sum(s.events_received for s in self._streams.values()),
            total_events_per_second=total_eps,
            uptime_seconds=uptime,
        )

    async def _health_check_loop(self) -> None:
        """Background task for periodic health checks."""
        while self._running:
            try:
                report = self.get_health_report()

                # Notify on status change
                if (
                    self._on_health_change
                    and report.status != self._last_health_status
                ):
                    self._last_health_status = report.status
                    try:
                        await self._on_health_change(report)
                    except Exception as e:
                        logger.error("Error in health change callback: %s", e)

                await asyncio.sleep(self._health_check_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in health check loop: %s", e)
                await asyncio.sleep(1)

    async def start(self) -> None:
        """Start the health monitor.

        Begins periodic health checks and staleness detection.
        """
        if self._running:
            return

        self._running = True
        self._start_time = time.time()
        self._health_task = asyncio.create_task(self._health_check_loop())
        logger.info("Health monitor started")

    async def stop(self) -> None:
        """Stop the health monitor."""
        if not self._running:
            return

        self._running = False

        if self._health_task:
            self._health_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._health_task
            self._health_task = None

        await self.stop_http_server()
        logger.info("Health monitor stopped")

    # HTTP Server methods

    async def _handle_health(self, _request: web.Request) -> web.Response:
        """Handle /health endpoint."""
        report = self.get_health_report()

        status_code = 200 if report.status == HealthStatus.HEALTHY else 503

        body: dict[str, Any] = {
            "status": report.status.value,
            "uptime_seconds": report.uptime_seconds,
            "total_events_received": report.total_events_received,
            "total_events_per_second": round(report.total_events_per_second, 2),
            "streams": {},
        }

        for name, stream in report.streams.items():
            body["streams"][name] = {
                "status": stream.status.value,
                "events_received": stream.events_received,
                "events_per_second": round(stream.events_per_second, 2),
                "last_event_time": stream.last_event_time,
                "last_error": stream.last_error,
            }

        return web.json_response(body, status=status_code)

    async def _handle_metrics(self, _request: web.Request) -> web.Response:
        """Handle /metrics endpoint (Prometheus format)."""
        # Ensure latest values are calculated
        self.get_health_report()

        metrics = generate_latest()
        return web.Response(
            body=metrics,
            content_type="text/plain",
            charset="utf-8",
        )

    async def _handle_ready(self, _request: web.Request) -> web.Response:
        """Handle /ready endpoint for k8s readiness probe."""
        report = self.get_health_report()

        if report.status == HealthStatus.UNHEALTHY:
            return web.json_response(
                {"ready": False, "reason": "unhealthy"},
                status=503,
            )

        return web.json_response({"ready": True}, status=200)

    async def _handle_live(self, _request: web.Request) -> web.Response:
        """Handle /live endpoint for k8s liveness probe."""
        # Always return 200 if the server is running
        return web.json_response({"live": True}, status=200)

    def _create_app(self) -> web.Application:
        """Create the aiohttp application."""
        app = web.Application()
        app.router.add_get("/health", self._handle_health)
        app.router.add_get("/metrics", self._handle_metrics)
        app.router.add_get("/ready", self._handle_ready)
        app.router.add_get("/live", self._handle_live)
        return app

    async def start_http_server(self, port: int = DEFAULT_HTTP_PORT) -> None:
        """Start the HTTP server for health and metrics endpoints.

        Args:
            port: Port to listen on.
        """
        if self._runner:
            logger.warning("HTTP server already running")
            return

        self._app = self._create_app()
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()

        site = web.TCPSite(self._runner, "0.0.0.0", port)
        await site.start()

        logger.info("Health HTTP server started on port %d", port)

    async def stop_http_server(self) -> None:
        """Stop the HTTP server."""
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
            self._app = None
            logger.info("Health HTTP server stopped")

    async def __aenter__(self) -> "HealthMonitor":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit."""
        await self.stop()
