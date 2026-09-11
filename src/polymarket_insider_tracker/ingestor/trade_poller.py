"""Near-real-time acquisition loop over the documented public trades query.

``TradePoller`` runs one acquisition cycle per poll interval: it fetches the newest page with a
strictly increasing cutoff, parses rows strictly, proves that the page reached the durable
boundary, delivers new observations oldest-first through the existing trade callback, retains
each identity after its callback returns, and advances the checkpoint only after the whole proven
page. Lifecycle states, the status snapshot, and Prometheus metrics follow
``contracts/status-and-config.md``; boundary semantics follow ``contracts/observation-boundary.md``.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

import httpx
from prometheus_client import Counter, Gauge, Histogram
from redis.asyncio import Redis

from polymarket_insider_tracker.config import PolymarketSettings
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.ingestor.observation_boundary import (
    BoundaryOrigin,
    BoundarySchemaError,
    LossEvent,
    LossReason,
    ObservationBoundary,
    ProofResult,
)
from polymarket_insider_tracker.ingestor.trade_rows import (
    InvalidRow,
    OutcomeResolution,
    RowDisposition,
    TradeObservation,
    classify_observation,
    parse_trade_row,
    repair_outcome,
    zero_disposition_counts,
)
from polymarket_insider_tracker.ingestor.trades_source import (
    RequestAttempt,
    TradesPage,
    TradesSourceClient,
    TradesTerminalError,
)
from polymarket_insider_tracker.redaction import redact_exception_message, redact_text

logger = logging.getLogger(__name__)

MAX_INVALID_DIAGNOSTICS_PER_CYCLE = 5
STATUS_LOSS_EVENTS = 5
_WALLET_PATTERN = re.compile(r"0x[0-9a-fA-F]{40}")
_QUERY_PATTERN = re.compile(r"\?[^\s]*")
# Both branches mask to end of line: the quoted token after ``Invalid port:``
# is repr-rendered upstream, so it may switch quote style or contain
# whitespace, and no delimiter-based match can bound a credential-shaped
# token safely.
_BARE_PORT_TOKEN_PATTERN = re.compile(
    r"[Ii]nvalid port:[^\n]*|[Pp]ort (?:could not be cast|out of range)[^\n]*"
)

TradeCallback = Callable[[TradeEvent], Awaitable[None]]
Clock = Callable[[], float]
Sleeper = Callable[[float], Awaitable[None]]


class IngestionState(StrEnum):
    """Lifecycle states of the poller."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    POSSIBLE_DATA_LOSS = "possible-data-loss"
    FAILED = "failed"


STATE_GAUGE_VALUES: dict[IngestionState, int] = {
    IngestionState.STOPPED: 0,
    IngestionState.STARTING: 1,
    IngestionState.RUNNING: 2,
    IngestionState.DEGRADED: 3,
    IngestionState.POSSIBLE_DATA_LOSS: 4,
    IngestionState.FAILED: 5,
}
STATE_LOG_LEVELS: dict[IngestionState, int] = {
    IngestionState.DEGRADED: logging.WARNING,
    IngestionState.POSSIBLE_DATA_LOSS: logging.ERROR,
    IngestionState.FAILED: logging.ERROR,
}
StateCallback = Callable[[IngestionState], None]


class CachedMarketLookup(Protocol):
    """The cache-only metadata surface used for outcome repair."""

    async def get_cached_market(self, condition_id: str) -> MarketMetadata | None: ...


INGEST_STATE = Gauge(
    "polymarket_ingest_state",
    "Ingestion state (0 stopped, 1 starting, 2 running, 3 degraded, 4 possible-data-loss, 5 failed)",
)
INGEST_REQUESTS_TOTAL = Counter(
    "polymarket_ingest_requests_total", "Trade source HTTP attempts, retries included", ["outcome"]
)
INGEST_ROWS_TOTAL = Counter(
    "polymarket_ingest_rows_total", "Parsed trade rows by disposition", ["disposition"]
)
INGEST_BOUNDARY_TIMESTAMP = Gauge(
    "polymarket_ingest_boundary_timestamp_seconds", "Durable complete-through boundary time"
)
INGEST_PROVIDER_LAG = Gauge(
    "polymarket_ingest_provider_lag_seconds",
    "Local clock minus newest cycle-eligible provider timestamp; not first-publication latency",
)
INGEST_PAGE_SPAN = Gauge(
    "polymarket_ingest_page_span_seconds", "Newest minus oldest timestamp in the last page"
)
INGEST_LOSS_EVENTS_TOTAL = Counter(
    "polymarket_ingest_loss_events_total", "Loss events written", ["reason"]
)
INGEST_REQUEST_DURATION = Histogram(
    "polymarket_ingest_request_duration_seconds", "Trade source HTTP request latency"
)


@dataclass(frozen=True)
class IngestionStatus:
    """Point-in-time snapshot of ingestion; see the data model."""

    state: IngestionState
    coverage: str
    boundary_time: int | None
    boundary_origin: BoundaryOrigin | None
    last_acquisition_at: datetime | None
    last_success_at: datetime | None
    last_trade_at: datetime | None
    latest_seen_at: datetime | None
    provider_lag_seconds: float | None
    processing_lag_seconds: float
    consecutive_failures: int
    last_error: str | None
    counts: dict[str, int]
    outcome_counts: dict[str, int]
    page_rows: int
    page_span_seconds: int
    loss_events: tuple[LossEvent, ...]
    requests_last_10s: int


def _zero_counts() -> dict[str, int]:
    counts = {disposition.value: 0 for disposition in zero_disposition_counts()}
    counts.update(polls=0, recovery_pages=0, empty_responses=0, retries=0, callback_errors=0)
    return counts


@dataclass
class _Tallies:
    """Mutable status fields updated as cycles run."""

    last_acquisition_at: float | None = None
    last_success_at: float | None = None
    last_trade_at: int | None = None
    latest_seen_at: int | None = None
    provider_lag: float | None = None
    processing_lag: float = 0.0
    consecutive_failures: int = 0
    last_error: str | None = None
    counts: dict[str, int] = field(default_factory=_zero_counts)
    outcome_counts: dict[str, int] = field(
        default_factory=lambda: {resolution.value: 0 for resolution in OutcomeResolution}
    )
    page_rows: int = 0
    page_span: int = 0


@dataclass
class PageAnalysis:
    """Strictly parsed page content: unique eligible observations plus per-row tallies."""

    cutoff: int
    observations: dict[str, TradeObservation] = field(default_factory=dict[str, TradeObservation])
    outcomes: list[TradeObservation] = field(default_factory=list[TradeObservation])
    resolved_outcomes: int = 0
    invalid: list[InvalidRow] = field(default_factory=list[InvalidRow])
    deferred: int = 0
    repeats: int = 0
    row_count: int = 0
    latest_seen: int | None = None

    def add_rows(self, rows: Iterable[object], *, now: int) -> None:
        for raw in rows:
            self._add(parse_trade_row(raw, now=now))

    def _add(self, parsed: TradeObservation | InvalidRow) -> None:
        self.row_count += 1
        if isinstance(parsed, InvalidRow):
            self.invalid.append(parsed)
            return
        self.outcomes.append(parsed)
        self.latest_seen = max(parsed.provider_timestamp, self.latest_seen or 0)
        if parsed.provider_timestamp > self.cutoff:
            self.deferred += 1
            return
        self._remember_observation(parsed)

    def _remember_observation(self, parsed: TradeObservation) -> None:
        existing = self.observations.get(parsed.identity)
        if existing is None:
            self.observations[parsed.identity] = parsed
            return
        self.repeats += 1
        if existing.outcome_resolution is OutcomeResolution.UNKNOWN:
            self.observations[parsed.identity] = parsed

    @property
    def oldest(self) -> int | None:
        timestamps = [observation.provider_timestamp for observation in self.observations.values()]
        return min(timestamps) if timestamps else None

    @property
    def newest(self) -> int | None:
        timestamps = [observation.provider_timestamp for observation in self.observations.values()]
        return max(timestamps) if timestamps else None

    @property
    def span_seconds(self) -> int:
        if self.newest is None or self.oldest is None:
            return 0
        return self.newest - self.oldest


@dataclass
class _OpenGap:
    """An interval the durable boundary could not be proven across; held until it resolves."""

    from_time: int
    to_time: int
    reason: LossReason
    detected_at: int
    pages_examined: int

    def as_loss_event(self, recorded_at: int) -> LossEvent:
        return LossEvent(
            from_time=self.from_time,
            to_time=self.to_time,
            reason=self.reason,
            detected_at=self.detected_at,
            recorded_at=recorded_at,
            pages_examined=self.pages_examined,
        )


def redact_error(text: str) -> str:
    """Sanitize a poller error before it reaches logs or stored status.

    The legacy wallet/query scrub keeps non-secret operational context
    readable; every URL-shaped value (including a credential-bearing trades
    endpoint path) goes through the central fail-closed policy first, and a
    bare non-URL diagnostic that names a value the URL layer cannot see
    (for example an httpx ``Invalid port`` quoting the token) is masked to
    its failure class.
    """
    return _scrub_poller_text(redact_text(text))


def redact_failure(exc: BaseException) -> str:
    """Sanitize a failure exception before it reaches logs or stored status.

    ``str(exc)`` interpolates raw arguments, so a bytes or container
    argument would re-emit its secret verbatim before any text-level scrub
    could see it; the argument-aware central renderer runs first, and the
    rendered text then takes the same wallet/query/bare-port scrub as any
    other poller diagnostic.
    """
    return _scrub_poller_text(redact_exception_message(exc))


def _scrub_poller_text(text: str) -> str:
    """The wallet/query scrub plus the bare-port mask over centrally redacted text."""
    scrubbed = _WALLET_PATTERN.sub("0x…", _QUERY_PATTERN.sub("?…", text))
    return _mask_bare_port_token(scrubbed)


def _mask_bare_port_token(text: str) -> str:
    """Mask a quoted port-shaped token in a non-URL error diagnostic."""
    return _BARE_PORT_TOKEN_PATTERN.sub("invalid request port: '***'", text)


def _utc(timestamp: float | None) -> datetime | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=UTC)


class TradePoller:
    """Acquire public trades on a bounded cadence and hand each observation downstream once."""

    def __init__(
        self,
        on_trade: TradeCallback,
        *,
        redis: Redis,
        settings: PolymarketSettings,
        metadata: CachedMarketLookup | None = None,
        on_state_change: StateCallback | None = None,
        http_client: httpx.AsyncClient | None = None,
        clock: Clock = time.time,
        sleeper: Sleeper = asyncio.sleep,
        random_source: Callable[[], float] = random.random,
    ) -> None:
        self._on_trade = on_trade
        self._metadata = metadata
        self._on_state_change = on_state_change
        self._clock = clock
        self._sleeper = sleeper
        self._coverage = settings.trades_coverage.value
        self._interval = float(settings.trades_poll_interval_seconds)
        self._horizon = settings.trades_recovery_horizon_seconds
        self._owns_client = http_client is None
        self._http = http_client or httpx.AsyncClient()
        self._client = TradesSourceClient(
            self._http,
            url=settings.trades_url,
            coverage=self._coverage,
            clock=clock,
            sleeper=sleeper,
            random_source=random_source,
            on_attempt=self._record_attempt,
        )
        self.boundary = ObservationBoundary(
            redis,
            trades_url=settings.trades_url,
            coverage=self._coverage,
            horizon_seconds=self._horizon,
            clock=clock,
        )
        self._state = IngestionState.STOPPED
        self._tallies = _Tallies()
        self._loaded = False
        self._restart_check_pending = False
        self._provisional: int | None = None
        self._gaps: list[_OpenGap] = []
        self._unresolved: set[str] = set()
        self._cycle_pages = 0
        self._running = False
        self._loop_task: asyncio.Task[object] | None = None
        self._stop_event = asyncio.Event()
        self._stopped = asyncio.Event()
        self._interruptible: asyncio.Task[object] | None = None

    @property
    def state(self) -> IngestionState:
        return self._state

    @property
    def is_running(self) -> bool:
        """Return True if poller loop is running."""
        return self._running

    @property
    def seconds_since_last_success(self) -> float | None:
        """Age of the last successful acquisition, on the poller's own clock.

        None until a page has actually been fetched: readiness must gate on proven
        source reachability, not on a request having merely been started.
        """
        last_success = self._tallies.last_success_at
        if last_success is None:
            return None
        return max(0.0, self._clock() - last_success)

    @property
    def status(self) -> IngestionStatus:
        """A snapshot of every documented status field."""
        tallies = self._tallies
        checkpoint = self.boundary.checkpoint
        return IngestionStatus(
            state=self._state,
            coverage=self._coverage,
            boundary_time=checkpoint.boundary_time if checkpoint else None,
            boundary_origin=checkpoint.boundary_origin if checkpoint else None,
            last_acquisition_at=_utc(tallies.last_acquisition_at),
            last_success_at=_utc(tallies.last_success_at),
            last_trade_at=_utc(tallies.last_trade_at),
            latest_seen_at=_utc(tallies.latest_seen_at),
            provider_lag_seconds=tallies.provider_lag,
            processing_lag_seconds=tallies.processing_lag,
            consecutive_failures=tallies.consecutive_failures,
            last_error=tallies.last_error,
            counts=dict(tallies.counts),
            outcome_counts=dict(tallies.outcome_counts),
            page_rows=tallies.page_rows,
            page_span_seconds=tallies.page_span,
            loss_events=self.boundary.loss_events[:STATUS_LOSS_EVENTS],
            requests_last_10s=self._client.requests_in_window(),
        )

    # ----- lifecycle -----------------------------------------------------------------------------

    async def start(self) -> None:
        """Run acquisition cycles on the poll cadence until ``stop()`` is called."""
        if self._running:
            logger.warning("Trade poller already running")
            return
        self._running = True
        self._loop_task = asyncio.current_task()
        self._stop_event = asyncio.Event()
        self._stopped = asyncio.Event()
        try:
            await self._loop()
        finally:
            self._running = False
            await self._close_client()
            if self._state is not IngestionState.FAILED:
                self._set_state(IngestionState.STOPPED)
            self._stopped.set()

    async def stop(self) -> None:
        """Stop cleanly: cancel the in-flight request or wait, let the callback finish, join.

        Called from another task, it returns once the loop has exited; called from inside the
        trade callback it only signals, so the loop can finish that delivery and exit.
        """
        self._stop_event.set()
        if self._interruptible is not None:
            self._interruptible.cancel()
        if self._running and asyncio.current_task() is not self._loop_task:
            await self._stopped.wait()
            return
        if not self._running:
            await self._close_client()
            self._set_state(IngestionState.STOPPED)

    async def _close_client(self) -> None:
        if self._owns_client:
            await self._http.aclose()

    async def _loop(self) -> None:
        while self._should_continue():
            await self._iteration()

    def _should_continue(self) -> bool:
        return not self._stop_event.is_set() and self._state is not IngestionState.FAILED

    async def _iteration(self) -> None:
        """One cycle plus the cadence wait; a cancellation caused by ``stop()`` ends quietly."""
        started = self._clock()
        try:
            await self.run_cycle()
            await self._wait_for_next_cycle(started)
        except asyncio.CancelledError:
            if not self._stop_event.is_set():
                raise

    async def _wait_for_next_cycle(self, started: float) -> None:
        delay = max(0.0, started + self._interval - self._clock())
        if self._stop_event.is_set():
            return
        await self._interruptibly(self._sleeper(delay))

    async def _interruptibly(self, awaitable: Awaitable[object]) -> object:
        task: asyncio.Task[object] = asyncio.ensure_future(awaitable)
        self._interruptible = task
        try:
            return await task
        finally:
            self._interruptible = None

    # ----- one cycle -----------------------------------------------------------------------------

    async def run_cycle(self) -> None:
        """Run exactly one acquisition cycle; failures become states, never exceptions."""
        if self._state is IngestionState.FAILED:
            return
        try:
            await self._ensure_loaded()
            await self._acquire_and_process()
        except (TradesTerminalError, BoundarySchemaError) as exc:
            self._fail(exc)
        except Exception as exc:
            self._degrade(exc)

    async def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        self._set_state(IngestionState.STARTING)
        checkpoint = await self.boundary.load()
        self._client.restore_last_end(checkpoint.last_request_end if checkpoint else None)
        self._restart_check_pending = checkpoint is not None
        self._loaded = True

    async def _acquire_and_process(self) -> None:
        self._tallies.counts["polls"] += 1
        self._tallies.processing_lag = 0.0
        self._cycle_pages = 0
        page = await self._fetch_primary()
        analysis = PageAnalysis(cutoff=page.request.end)
        self._absorb(page, analysis)
        try:
            await self._process(page, analysis)
        finally:
            self._record_analysis(analysis)

    async def _fetch_primary(self) -> TradesPage:
        self._tallies.last_acquisition_at = self._clock()
        checkpoint = self.boundary.checkpoint
        page = await self._interruptibly(
            self._client.fetch_primary(
                boundary_time=checkpoint.boundary_time if checkpoint else None,
                horizon_seconds=self._horizon,
            )
        )
        assert isinstance(page, TradesPage)
        return page

    def _absorb(self, page: TradesPage, analysis: PageAnalysis) -> None:
        """Parse one response into the cycle's analysis and record its reachability."""
        analysis.add_rows(page.rows, now=int(page.received_at))
        self._cycle_pages += 1
        self._tallies.last_success_at = page.received_at
        self._tallies.page_rows = analysis.row_count

    def _record_analysis(self, analysis: PageAnalysis) -> None:
        tallies = self._tallies
        tallies.page_span = analysis.span_seconds
        if analysis.latest_seen is not None:
            tallies.latest_seen_at = max(analysis.latest_seen, tallies.latest_seen_at or 0)
        self._count(RowDisposition.DEFERRED_FUTURE_CYCLE, analysis.deferred)
        self._count(RowDisposition.DUPLICATE, analysis.repeats)
        for invalid in analysis.invalid:
            self._count(invalid.disposition)
        for invalid in analysis.invalid[:MAX_INVALID_DIAGNOSTICS_PER_CYCLE]:
            logger.warning("invalid trade row %s: %s", invalid.row_hash[:16], invalid.field)
        INGEST_PAGE_SPAN.set(analysis.span_seconds)

    async def _process(self, page: TradesPage, analysis: PageAnalysis) -> None:
        await self._resolve_and_count_outcomes(analysis)
        if analysis.newest is None:
            self._tallies.counts["empty_responses"] += 1
            self._mark_healthy()
            return
        if self.boundary.checkpoint is None:
            await self._anchor(page, analysis, BoundaryOrigin.FIRST_START, ())
            return
        if self._restart_is_beyond_horizon():
            await self._anchor_after_lost_restart(page, analysis)
            return
        await self._prove_and_advance(page, analysis)

    def _restart_is_beyond_horizon(self) -> bool:
        """True once, on the first non-empty page after a restart older than the horizon."""
        if not self._restart_check_pending:
            return False
        assert self.boundary.checkpoint is not None
        beyond_horizon = int(self._clock()) - self.boundary.checkpoint.boundary_time > self._horizon
        if not beyond_horizon:
            self._restart_check_pending = False
        return beyond_horizon

    async def _anchor_after_lost_restart(self, page: TradesPage, analysis: PageAnalysis) -> None:
        assert self.boundary.checkpoint is not None and analysis.newest is not None
        now = int(self._clock())
        event = LossEvent(
            from_time=self.boundary.checkpoint.boundary_time,
            to_time=analysis.newest,
            reason=LossReason.RESTART_BEYOND_HORIZON,
            detected_at=now,
            recorded_at=now,
            pages_examined=self._cycle_pages,
        )
        await self._anchor(page, analysis, BoundaryOrigin.RE_ANCHORED, (event,))
        self._restart_check_pending = False
        self._log_loss(event)

    async def _prove_and_advance(self, page: TradesPage, analysis: PageAnalysis) -> None:
        """Prove against the durable boundary, recover once if needed, else freeze or continue."""
        proof = await self._prove_with_recovery(page, analysis)
        if proof.proven:
            self._close_gaps()
            await self._advance_proven(page, analysis)
            return
        if self._provisional is None:
            self._open_gap(self.boundary.checkpoint, proof)
        else:
            self._continue_provisionally(analysis)
        await self._retain_provisionally(page, analysis)
        await self._expire_gaps_if_aged(page)

    async def _prove_with_recovery(self, page: TradesPage, analysis: PageAnalysis) -> ProofResult:
        proof = self._prove(analysis, None)
        if proof.proven:
            return proof
        recovery = await self._interruptibly(self._client.fetch_recovery(page.request))
        assert isinstance(recovery, TradesPage)
        self._tallies.counts["recovery_pages"] += 1
        self._absorb(recovery, analysis)
        await self._resolve_and_count_outcomes(analysis)
        return self._prove(analysis, None)

    def _prove(self, analysis: PageAnalysis, boundary_time: int | None) -> ProofResult:
        """Prove against the durable boundary, or provisionally, ignoring known gap identities."""
        return self.boundary.prove(
            page_identities=analysis.observations.keys(),
            oldest=analysis.oldest,
            boundary_time=boundary_time,
            ignored=self._unresolved if boundary_time is not None else (),
        )

    def _continue_provisionally(self, analysis: PageAnalysis) -> None:
        """Prove continuity between consecutive pages so every unproven interval is one gap."""
        proof = self._prove(analysis, self._provisional)
        if not proof.proven:
            self._open_gap(None, proof)

    def _open_gap(self, checkpoint: object, proof: ProofResult) -> None:
        from_time = self._provisional if checkpoint is None else self._durable_time()
        reason = LossReason.CONTINUITY_MISMATCH if proof.reach else LossReason.HORIZON_EXPIRED
        assert from_time is not None and proof.oldest is not None
        gap = _OpenGap(
            from_time=from_time,
            to_time=proof.oldest,
            reason=reason,
            detected_at=int(self._clock()),
            pages_examined=0,
        )
        self._gaps.append(gap)
        self._unresolved.update(proof.missing_identities)
        logger.error(
            "possible data loss: interval [%d, %d) unproven (%s); boundary frozen",
            gap.from_time,
            gap.to_time,
            gap.reason.value,
        )
        self._set_state(IngestionState.POSSIBLE_DATA_LOSS)

    def _durable_time(self) -> int:
        checkpoint = self.boundary.checkpoint
        assert checkpoint is not None
        return checkpoint.boundary_time

    def _close_gaps(self) -> None:
        if self._provisional is None:
            return
        logger.info("possible-data-loss resolved: a later page proved the frozen boundary")
        self._forget_gaps()

    def _forget_gaps(self) -> None:
        self._gaps.clear()
        self._unresolved.clear()
        self._provisional = None

    async def _retain_provisionally(self, page: TradesPage, analysis: PageAnalysis) -> None:
        """Keep observations flowing behind a frozen boundary without moving it."""
        newest = analysis.newest
        assert newest is not None
        candidates = self._select_candidates(analysis, floor=self._candidate_floor(newest))
        delivered = await self._deliver(candidates)
        for gap in self._gaps:
            gap.pages_examined += self._cycle_pages
        if self._stop_event.is_set() and len(delivered) < len(candidates):
            return
        await self.boundary.retain(
            identities=delivered,
            trim_floor=newest - self._horizon,
            last_request_end=page.request.end,
        )
        self._provisional = newest
        self._tallies.provider_lag = page.received_at - newest
        INGEST_PROVIDER_LAG.set(self._tallies.provider_lag)

    async def _expire_gaps_if_aged(self, page: TradesPage) -> None:
        """Write every open gap as a loss event once the frozen boundary is older than the horizon."""
        now = int(self._clock())
        if self._provisional is None or now - self._durable_time() <= self._horizon:
            return
        events = tuple(gap.as_loss_event(now) for gap in self._gaps)
        for event in events:
            self._log_loss(event)
        await self.boundary.advance(
            boundary_time=self._provisional,
            origin=BoundaryOrigin.RE_ANCHORED,
            last_request_end=page.request.end,
            identities={},
            loss_events=events,
        )
        self._forget_gaps()
        INGEST_BOUNDARY_TIMESTAMP.set(self._durable_time())
        self._mark_healthy()

    @staticmethod
    def _log_loss(event: LossEvent) -> None:
        INGEST_LOSS_EVENTS_TOTAL.labels(reason=event.reason.value).inc()
        logger.error(
            "loss event recorded: [%d, %d] %s after %d pages",
            event.from_time,
            event.to_time,
            event.reason.value,
            event.pages_examined,
        )

    async def _anchor(
        self,
        page: TradesPage,
        analysis: PageAnalysis,
        origin: BoundaryOrigin,
        loss_events: tuple[LossEvent, ...],
    ) -> None:
        """Establish the boundary at the newest eligible row without emitting anything."""
        newest = analysis.newest
        assert newest is not None
        floor = newest - self._horizon
        retained: dict[str, int] = {}
        for identity, observation in analysis.observations.items():
            disposition = self._anchor_disposition(observation, floor)
            self._count(disposition)
            if disposition is RowDisposition.ANCHOR_HISTORY:
                retained[identity] = observation.provider_timestamp
        await self.boundary.advance(
            boundary_time=newest,
            origin=origin,
            last_request_end=page.request.end,
            identities=retained,
            loss_events=loss_events,
        )
        self._after_advance(page, analysis)

    @staticmethod
    def _anchor_disposition(observation: TradeObservation, floor: int) -> RowDisposition:
        if observation.provider_timestamp < floor:
            return RowDisposition.PADDING
        return RowDisposition.ANCHOR_HISTORY

    def _candidate_floor(self, newest: int) -> int:
        """Rows older than the horizon or the durable start/re-anchor floor are padding.

        The durable emission floor does not drift when sparse retained identities are trimmed.
        Once the tracker has run longer than the horizon the two bounds coincide.
        """
        horizon_floor = newest - self._horizon
        checkpoint = self.boundary.checkpoint
        assert checkpoint is not None
        return max(horizon_floor, checkpoint.emission_floor)

    async def _advance_proven(self, page: TradesPage, analysis: PageAnalysis) -> None:
        newest = analysis.newest
        assert newest is not None
        candidates = self._select_candidates(analysis, floor=self._candidate_floor(newest))
        delivered = await self._deliver(candidates)
        if self._stop_event.is_set() and len(delivered) < len(candidates):
            return
        await self.boundary.advance(
            boundary_time=newest,
            origin=BoundaryOrigin.PROVEN,
            last_request_end=page.request.end,
            identities=delivered,
        )
        self._after_advance(page, analysis)

    def _select_candidates(self, analysis: PageAnalysis, *, floor: int) -> list[TradeObservation]:
        """Classify every eligible observation; return the new ones oldest-first."""
        emitted: list[TradeObservation] = []
        for observation in analysis.observations.values():
            disposition = classify_observation(
                observation, cutoff=analysis.cutoff, floor=floor, retained=self.boundary.window
            )
            if disposition is RowDisposition.EMITTED:
                emitted.append(observation)
            else:
                self._count(disposition)
        return sorted(emitted, key=lambda item: (item.provider_timestamp, item.identity))

    async def _deliver(self, candidates: list[TradeObservation]) -> dict[str, int]:
        delivered: dict[str, int] = {}
        for observation in candidates:
            if self._stop_event.is_set():
                break
            await self._deliver_one(observation)
            delivered[observation.identity] = observation.provider_timestamp
        return delivered

    async def _deliver_one(self, observation: TradeObservation) -> None:
        started = self._clock()
        try:
            await self._on_trade(observation.event)
        except Exception as exc:
            logger.error(
                "trade callback failed for %s: %s",
                observation.identity[:12],
                redact_exception_message(exc),
            )
            self._tallies.counts["callback_errors"] += 1
        self._tallies.processing_lag += max(0.0, self._clock() - started)
        await self.boundary.record_identity(observation.identity, observation.provider_timestamp)
        self._tallies.last_trade_at = max(
            observation.provider_timestamp, self._tallies.last_trade_at or 0
        )
        self._count(RowDisposition.EMITTED)

    async def _resolve_and_count_outcomes(self, analysis: PageAnalysis) -> None:
        """Resolve every valid raw row, independently of its eventual disposition."""
        resolved_by_identity: dict[str, TradeObservation] = {}
        for observation in analysis.outcomes[analysis.resolved_outcomes :]:
            resolved = await self._repair(observation)
            self._tallies.outcome_counts[resolved.outcome_resolution.value] += 1
            self._remember_resolved(resolved_by_identity, resolved)
        analysis.resolved_outcomes = len(analysis.outcomes)
        for identity, resolved in resolved_by_identity.items():
            if identity in analysis.observations:
                self._remember_resolved(analysis.observations, resolved)

    @staticmethod
    def _remember_resolved(
        observations: dict[str, TradeObservation], candidate: TradeObservation
    ) -> None:
        current = observations.get(candidate.identity)
        if current is None or current.outcome_resolution is OutcomeResolution.UNKNOWN:
            observations[candidate.identity] = candidate

    async def _repair(self, observation: TradeObservation) -> TradeObservation:
        if observation.outcome_known or self._metadata is None:
            return observation
        try:
            metadata = await self._metadata.get_cached_market(observation.event.market_id)
        except Exception as exc:
            logger.warning(
                "metadata lookup failed during outcome repair: %s", redact_exception_message(exc)
            )
            metadata = None
        return repair_outcome(observation, metadata)

    def _after_advance(self, page: TradesPage, analysis: PageAnalysis) -> None:
        newest = analysis.newest
        assert newest is not None
        self._tallies.provider_lag = page.received_at - newest
        INGEST_PROVIDER_LAG.set(self._tallies.provider_lag)
        checkpoint = self.boundary.checkpoint
        if checkpoint is not None:
            INGEST_BOUNDARY_TIMESTAMP.set(checkpoint.boundary_time)
        self._mark_healthy()

    # ----- bookkeeping ---------------------------------------------------------------------------

    def _count(self, disposition: RowDisposition, amount: int = 1) -> None:
        if amount == 0:
            return
        self._tallies.counts[disposition.value] += amount
        INGEST_ROWS_TOTAL.labels(disposition=disposition.value).inc(amount)

    def _record_attempt(self, attempt: RequestAttempt) -> None:
        INGEST_REQUESTS_TOTAL.labels(outcome=attempt.outcome).inc()
        INGEST_REQUEST_DURATION.observe(attempt.duration_seconds)
        if attempt.attempt > 0:
            self._tallies.counts["retries"] += 1

    def _mark_healthy(self) -> None:
        self._tallies.consecutive_failures = 0
        self._tallies.last_error = None
        if self._provisional is None:
            self._set_state(IngestionState.RUNNING)

    def _degrade(self, exc: Exception) -> None:
        self._tallies.consecutive_failures += 1
        self._tallies.last_error = redact_failure(exc)
        logger.warning("acquisition cycle failed: %s", self._tallies.last_error)
        self._set_state(IngestionState.DEGRADED)

    def _fail(self, exc: Exception) -> None:
        self._tallies.consecutive_failures += 1
        self._tallies.last_error = redact_failure(exc)
        logger.error("acquisition stopped: %s", self._tallies.last_error)
        self._set_state(IngestionState.FAILED)

    def _set_state(self, new_state: IngestionState) -> None:
        if new_state is self._state:
            return
        previous, self._state = self._state, new_state
        logger.log(
            STATE_LOG_LEVELS.get(new_state, logging.INFO),
            "ingestion state: %s -> %s",
            previous.value,
            new_state.value,
        )
        INGEST_STATE.set(STATE_GAUGE_VALUES[new_state])
        self._notify_state(new_state)

    def _notify_state(self, new_state: IngestionState) -> None:
        if self._on_state_change is None:
            return
        try:
            self._on_state_change(new_state)
        except Exception as exc:
            logger.error("ingestion state callback failed: %s", redact_exception_message(exc))
