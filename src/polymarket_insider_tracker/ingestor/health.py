"""Connection health monitor with metrics and HTTP endpoints.

This module provides health monitoring for the data ingestion layer,
tracking connection states, event throughput, and staleness detection.
"""

import asyncio
import contextlib
import copy
import dataclasses
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from aiohttp import web
from prometheus_client import Counter, Gauge, Histogram, generate_latest

from polymarket_insider_tracker.redaction import redact_text

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_STALE_THRESHOLD_SECONDS = 60  # No events for 60s = stale
DEFAULT_HEALTH_CHECK_INTERVAL = 5  # seconds
DEFAULT_HTTP_PORT = 8080
# Component checks run concurrently, each under this bound, so a probe with every
# dependency hung still answers inside the plan's <100ms health-probe goal instead
# of accumulating one timeout per component.
COMPONENT_CHECK_TIMEOUT_SECONDS = 0.09


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
class ComponentStatus:
    """Health and latency status of an external dependency or worker.

    ``status`` is ``"up"``, ``"degraded"`` (reachable and progressing, but with a
    recoverable failure or possible-data-loss condition worth surfacing), or
    ``"down"`` (unavailable or terminally failed).
    """

    status: str
    latency_ms: float | None = None
    last_error: str | None = None

    @property
    def is_up(self) -> bool:
        """Return True if component status is up."""
        return self.status == "up"

    @property
    def is_down(self) -> bool:
        """Return True if the component is unavailable or terminally failed."""
        return self.status == "down"

    def to_dict(self) -> dict[str, Any]:
        """Convert component status to dictionary for health output (error redacted)."""
        return {
            "status": self.status,
            "latency_ms": self.latency_ms,
            "last_error": redact_text(self.last_error) if self.last_error else self.last_error,
        }


@dataclass
class HealthReport:
    """Comprehensive health report for all streams."""

    status: HealthStatus
    streams: dict[str, StreamHealth] = field(default_factory=dict[str, StreamHealth])
    total_events_received: int = 0
    total_events_per_second: float = 0.0
    uptime_seconds: float = 0.0
    timestamp: float = field(default_factory=time.time)


# Type aliases
HealthCallback = Callable[[HealthReport], Awaitable[None]]
ComponentChecker = Callable[[], Awaitable[ComponentStatus]]
LastErrorProvider = Callable[[], str | None]


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

        # Component checkers & freshness tracking
        self._component_checkers: dict[str, ComponentChecker] = {}
        self._last_acquisition_time: float | None = None
        self._last_trade_time: float | None = None
        self._last_error_provider: LastErrorProvider | None = None

    @property
    def is_running(self) -> bool:
        """Return True if the monitor is running."""
        return self._running

    @property
    def stale_threshold_seconds(self) -> float:
        """The single staleness bound shared by stream state and readiness gating."""
        return self._stale_threshold

    @property
    def last_acquisition_time(self) -> float | None:
        """Return timestamp of last polling acquisition."""
        return self._last_acquisition_time

    @property
    def last_trade_time(self) -> float | None:
        """Return timestamp of last trade arrival."""
        return self._last_trade_time

    def set_component_checker(self, name: str, checker: ComponentChecker) -> None:
        """Register an async health checker for a component."""
        self._component_checkers[name] = checker

    def set_last_error_provider(self, provider: LastErrorProvider) -> None:
        """Register a callable that reports the pipeline's most recent error, if any."""
        self._last_error_provider = provider

    def _current_last_error(self) -> str | None:
        if self._last_error_provider is None:
            return None
        try:
            reported = self._last_error_provider()
        except Exception as exc:
            return f"last-error provider failed: {redact_text(str(exc))}"
        return redact_text(reported) if reported is not None else None

    def record_acquisition(self, timestamp: float | None = None) -> None:
        """Record a polling acquisition cycle timestamp."""
        self._last_acquisition_time = time.time() if timestamp is None else timestamp

    def record_trade_arrival(self, timestamp: float | None = None) -> None:
        """Record trade arrival timestamp."""
        self._last_trade_time = time.time() if timestamp is None else timestamp

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
        if stream_name == "trades":
            self._last_trade_time = now

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

    def _latest_activity_time(self, stream: StreamHealth) -> float | None:
        """Freshest proof of source progress: a received event or a successful acquisition.

        A quiet market produces successful empty acquisitions without trade events;
        trade silence alone must not mark a reachable, progressing source stale.
        """
        times = [t for t in (stream.last_event_time, self._last_acquisition_time) if t is not None]
        return max(times) if times else None

    def _evaluate_stream_without_activity(
        self, name: str, stream: StreamHealth, now: float
    ) -> None:
        if not stream.connected_since:
            return
        if (now - stream.connected_since) > self._stale_threshold:
            stream.status = StreamStatus.STALE
            STREAM_STATUS.labels(stream=name).set(0.5)

    def _evaluate_stream_activity(
        self, name: str, stream: StreamHealth, activity: float, now: float
    ) -> None:
        if (now - activity) > self._stale_threshold:
            stream.status = StreamStatus.STALE
            STREAM_STATUS.labels(stream=name).set(0.5)
        else:
            stream.status = StreamStatus.ACTIVE
            STREAM_STATUS.labels(stream=name).set(1.0)

    def _check_single_stream_staleness(self, name: str, stream: StreamHealth, now: float) -> None:
        if stream.status == StreamStatus.DISCONNECTED:
            return
        activity = self._latest_activity_time(stream)
        if activity is None:
            self._evaluate_stream_without_activity(name, stream, now)
        else:
            self._evaluate_stream_activity(name, stream, activity, now)

    def _check_stream_staleness(self) -> None:
        """Check all streams for staleness."""
        now = time.time()
        for name, stream in self._streams.items():
            self._check_single_stream_staleness(name, stream, now)

    def _has_degraded_stream(self) -> bool:
        return any(
            stream.status in (StreamStatus.DISCONNECTED, StreamStatus.STALE)
            for stream in self._streams.values()
        )

    def _all_disconnected(self) -> bool:
        return all(stream.status == StreamStatus.DISCONNECTED for stream in self._streams.values())

    def _determine_overall_status(self) -> HealthStatus:
        """Determine overall health status based on stream states.

        Returns:
            Overall health status.
        """
        if not self._streams:
            return HealthStatus.HEALTHY
        if self._all_disconnected():
            return HealthStatus.UNHEALTHY
        if self._has_degraded_stream():
            return HealthStatus.DEGRADED
        return HealthStatus.HEALTHY

    def _update_stream_metrics(self) -> float:
        total_eps = 0.0
        for name, stream in self._streams.items():
            eps = self._calculate_throughput(name)
            stream.events_per_second = eps
            EVENTS_PER_SECOND.labels(stream=name).set(eps)
            total_eps += eps
        return total_eps

    def _set_prometheus_health_status(self, overall_status: HealthStatus) -> None:
        status_map = {
            HealthStatus.HEALTHY: 1.0,
            HealthStatus.DEGRADED: 0.5,
            HealthStatus.UNHEALTHY: 0.0,
        }
        HEALTH_STATUS.set(status_map.get(overall_status, 0.0))

    def get_health_report(self) -> HealthReport:
        """Generate a stream-level health report.

        The Prometheus overall-health gauge is deliberately not set here: it follows
        the combined component-and-stream snapshot (``evaluate_overall_health``) so
        `/metrics` can never disagree with `/health` about the same state.

        Returns:
            HealthReport with current status of all streams.
        """
        self._check_stream_staleness()
        total_eps = self._update_stream_metrics()
        overall_status = self._determine_overall_status()

        uptime = time.time() - self._start_time if self._start_time else 0.0
        streams_copy = {name: copy.copy(stream) for name, stream in self._streams.items()}

        return HealthReport(
            status=overall_status,
            streams=streams_copy,
            total_events_received=sum(s.events_received for s in self._streams.values()),
            total_events_per_second=total_eps,
            uptime_seconds=uptime,
        )

    async def _notify_health_change(self, report: HealthReport) -> None:
        if not self._on_health_change or report.status == self._last_health_status:
            return
        self._last_health_status = report.status
        try:
            await self._on_health_change(report)
        except Exception as e:
            logger.error("Error in health change callback: %s", e)

    async def _run_health_check_step(self) -> None:
        try:
            report, _, _ = await self.evaluate_overall_health()
            await self._notify_health_change(report)
            await asyncio.sleep(self._health_check_interval)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Error in health check loop: %s", e)
            await asyncio.sleep(1)

    async def _health_check_loop(self) -> None:
        """Background task for periodic health checks."""
        while self._running:
            try:
                await self._run_health_check_step()
            except asyncio.CancelledError:
                break

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

    async def _evaluate_single_component(self, checker: ComponentChecker) -> ComponentStatus:
        try:
            return await asyncio.wait_for(checker(), timeout=COMPONENT_CHECK_TIMEOUT_SECONDS)
        except TimeoutError:
            return ComponentStatus(
                status="down",
                last_error=f"health check timed out after {COMPONENT_CHECK_TIMEOUT_SECONDS}s",
            )
        except Exception as exc:
            return ComponentStatus(status="down", last_error=redact_text(str(exc)))

    async def evaluate_components(self) -> dict[str, ComponentStatus]:
        """Evaluate all registered component checks concurrently under one probe budget.

        Sequential evaluation would accumulate one timeout per hung dependency and
        blow the documented sub-100ms probe goal; concurrent evaluation bounds the
        whole probe by the single per-check timeout.
        """
        names = list(self._component_checkers)
        statuses = await asyncio.gather(
            *(self._evaluate_single_component(self._component_checkers[name]) for name in names)
        )
        return dict(zip(names, statuses, strict=True))

    async def evaluate_overall_health(
        self,
    ) -> tuple[HealthReport, dict[str, ComponentStatus], HealthStatus]:
        """One combined component-and-stream snapshot; the Prometheus gauge follows it.

        `/health`, `/metrics`, and the periodic check all derive from this single
        evaluation, so the exported ``polymarket_health_status`` value always agrees
        with the `/health` verdict for the same state.
        """
        report = self.get_health_report()
        components = await self.evaluate_components()
        overall = self._determine_health_status(report.status, components)
        self._set_prometheus_health_status(overall)
        return dataclasses.replace(report, status=overall), components, overall

    @staticmethod
    def _reason_for_component(name: str) -> str:
        if name == "ingestion":
            return "ingestion_worker_failed"
        return f"{name}_unreachable"

    @classmethod
    def first_failure_reason(cls, components: dict[str, ComponentStatus]) -> str:
        for name, comp in components.items():
            if comp.is_down:
                return cls._reason_for_component(name)
        return "unhealthy"

    def _legacy_ready_response(self) -> web.Response:
        report = self.get_health_report()
        if report.status == HealthStatus.UNHEALTHY:
            return web.json_response({"ready": False, "reason": "unhealthy"}, status=503)
        return web.json_response({"ready": True}, status=200)

    async def _handle_ready(self, _request: web.Request) -> web.Response:
        """Handle /ready endpoint for readiness probe."""
        if not self._component_checkers:
            return self._legacy_ready_response()

        components = await self.evaluate_components()
        comp_summary = self.components_summary(components)
        if not self.all_components_up(components):
            reason = self.first_failure_reason(components)
            return web.json_response(
                {"ready": False, "reason": reason, "components": comp_summary},
                status=503,
            )

        return web.json_response({"ready": True, "components": comp_summary}, status=200)

    @classmethod
    def all_components_up(cls, components: dict[str, ComponentStatus]) -> bool:
        """Readiness gate: only a down component fails readiness (FR-002).

        A degraded component is reachable and still making progress; it is surfaced
        in the components summary and detailed health rather than failing readiness.
        """
        return not cls._has_down_component(components)

    @staticmethod
    def components_summary(components: dict[str, ComponentStatus]) -> dict[str, str]:
        return {name: comp.status for name, comp in components.items()}

    @staticmethod
    def _has_down_component(components: dict[str, ComponentStatus]) -> bool:
        return any(comp.is_down for comp in components.values())

    @staticmethod
    def _has_degraded_component(components: dict[str, ComponentStatus]) -> bool:
        return any(not comp.is_up and not comp.is_down for comp in components.values())

    @classmethod
    def _determine_health_status(
        cls,
        report_status: HealthStatus,
        components: dict[str, ComponentStatus],
    ) -> HealthStatus:
        if cls._has_down_component(components):
            return HealthStatus.UNHEALTHY
        if cls._has_degraded_component(components) and report_status == HealthStatus.HEALTHY:
            return HealthStatus.DEGRADED
        return report_status

    @staticmethod
    def _compute_freshness(timestamp: float | None, now: float) -> float | None:
        if timestamp is None:
            return None
        return round(now - timestamp, 1)

    @staticmethod
    def _format_streams(streams: dict[str, StreamHealth]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for name, stream in streams.items():
            result[name] = {
                "status": stream.status.value,
                "events_received": stream.events_received,
                "events_per_second": round(stream.events_per_second, 2),
                "last_event_time": stream.last_event_time,
                "last_error": redact_text(stream.last_error) if stream.last_error else None,
            }
        return result

    def _build_health_body(
        self,
        report: HealthReport,
        components: dict[str, ComponentStatus],
        overall_status: HealthStatus,
        now: float,
    ) -> dict[str, Any]:
        return {
            "status": overall_status.value,
            "uptime_seconds": round(report.uptime_seconds, 1),
            "last_acquisition_time": self._last_acquisition_time,
            "last_trade_time": self._last_trade_time,
            "acquisition_freshness_seconds": self._compute_freshness(
                self._last_acquisition_time, now
            ),
            "trade_freshness_seconds": self._compute_freshness(self._last_trade_time, now),
            "total_events_received": report.total_events_received,
            "total_events_per_second": round(report.total_events_per_second, 2),
            "components": {name: c.to_dict() for name, c in components.items()},
            "streams": self._format_streams(report.streams),
            "last_error": self._current_last_error(),
            "timestamp": now,
        }

    async def _handle_health(self, _request: web.Request) -> web.Response:
        """Handle /health diagnostic endpoint."""
        report, components, overall_status = await self.evaluate_overall_health()
        status_code = 503 if overall_status == HealthStatus.UNHEALTHY else 200

        now = time.time()
        body = self._build_health_body(report, components, overall_status, now)
        return web.json_response(body, status=status_code)

    async def _handle_live(self, _request: web.Request) -> web.Response:
        """Handle /live endpoint for k8s liveness probe."""
        return web.json_response({"live": True}, status=200)

    async def _handle_metrics(self, _request: web.Request) -> web.Response:
        """Handle /metrics endpoint (Prometheus format)."""
        # The gauge exports the same combined verdict /health would return right now.
        await self.evaluate_overall_health()
        metrics = generate_latest()
        return web.Response(
            body=metrics,
            headers={"Content-Type": "text/plain; version=0.0.4; charset=utf-8"},
        )

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

        # reuse_address only: SO_REUSEPORT would let a second process bind the same port
        # silently instead of failing actionably when the health port is already in use.
        site = web.TCPSite(self._runner, "0.0.0.0", port, reuse_address=True)
        await site.start()

        logger.info("Health HTTP server started on port %d", port)

    @property
    def http_addresses(self) -> list[tuple[str, int]]:
        """The (host, port) pairs the health HTTP server is actually bound to."""
        if not self._runner:
            return []
        return [(address[0], address[1]) for address in self._runner.addresses]

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
