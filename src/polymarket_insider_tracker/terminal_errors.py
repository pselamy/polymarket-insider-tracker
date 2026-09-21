"""Durable terminal-error persistence and shutdown drain (G2).

One cohesive home for the failure-persistence path so the oversized
pipeline module stays a thin delegator. The write is retained as a
task and drained across the real worker-stop and shutdown boundary,
so a terminal reason survives process exit. Worker identity is an
explicit parameter with a named default used only when no caller
passes one.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from polymarket_insider_tracker.pipeline_lifecycle import TerminalLifecycle
from polymarket_insider_tracker.redaction import redact_exception_message, redact_text

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from polymarket_insider_tracker.pipeline import PipelineStats
    from polymarket_insider_tracker.storage.database import DatabaseManager

logger = logging.getLogger(__name__)

DEFAULT_TERMINAL_WORKER = "trade_poller"

TERMINAL_FALLBACK_REASON = "ingestion_worker_failed"


def terminal_write(
    db_manager: DatabaseManager | None,
    reason: str,
    worker: str,
    on_error: Callable[[str], None],
) -> Coroutine[None, None, None]:
    """Build one retained durable write for the caller's pending set."""

    async def _write() -> None:
        await write_terminal_row(db_manager, reason, worker, on_error)

    return _write()


def schedule_terminal_write(
    pending: set[asyncio.Task[None]],
    write: Coroutine[None, None, None],
) -> asyncio.Task[None] | None:
    """Retain one durable-write task; return None when no loop runs."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        close = getattr(write, "close", None)
        if close is not None:
            close()
        return None
    task = loop.create_task(write)
    pending.add(task)
    task.add_done_callback(pending.discard)
    return task


async def write_terminal_row(
    db_manager: DatabaseManager | None,
    reason: str,
    worker: str,
    on_error: Callable[[str], None],
) -> None:
    """Insert one pipeline_terminal_errors row. Never raises."""
    if db_manager is None:
        return
    try:
        from polymarket_insider_tracker.storage.models import PipelineTerminalErrorModel

        async with db_manager.get_async_session() as session:
            session.add(PipelineTerminalErrorModel(reason=reason, worker=worker))
    except Exception as exc:
        on_error(f"Failed to persist terminal error: {redact_exception_message(exc)}")


async def read_latest_terminal_reason(db_manager: DatabaseManager | None) -> str | None:
    """Newest durable terminal reason, or None when none was recorded."""
    if db_manager is None:
        return None
    from sqlalchemy import select

    from polymarket_insider_tracker.storage.models import PipelineTerminalErrorModel

    async with db_manager.get_async_session() as session:
        result = await session.execute(
            select(PipelineTerminalErrorModel)
            .order_by(PipelineTerminalErrorModel.id.desc())
            .limit(1)
        )
        row: PipelineTerminalErrorModel | None = result.scalar_one_or_none()
        if row is None:
            return None
        reason: str = row.reason
        return reason


async def drain_terminal_writes(
    pending: set[asyncio.Task[None]],
    on_error: Callable[[str], None],
) -> None:
    """Await every retained write; never raises, never cancels, refills raced adds."""
    while pending:
        done, _ = await asyncio.wait(pending)
        for task in done:
            _report_write_outcome(pending, task, on_error)


def _report_write_outcome(
    pending: set[asyncio.Task[None]],
    task: asyncio.Task[None],
    on_error: Callable[[str], None],
) -> None:
    """Drop one finished write; surface its failure without raising."""
    pending.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        on_error(f"Failed to persist terminal error: {redact_exception_message(exc)}")


async def cancel_task(task: asyncio.Task[None] | None) -> None:
    """Cancel one background task and await its exit. Never raises."""
    if task is None:
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def resolve_terminal_reason(
    poller_error: str | None,
    durable_reason: str | None,
    stats_error: str | None,
) -> str:
    """Newest failure text: live poller error, durable row, stats, fallback."""
    return poller_error or durable_reason or stats_error or TERMINAL_FALLBACK_REASON


async def resolve_stored_reason(
    db_manager: DatabaseManager | None,
    is_error: bool,
    stats_error: str | None,
) -> str | None:
    """Newest durable terminal reason, or None when none was recorded."""
    reason = await read_latest_terminal_reason(db_manager)
    if reason is not None:
        return reason
    return stats_error if is_error else None


async def resolve_terminal_reason_live(
    db_manager: DatabaseManager | None,
    poller_error: str | None,
    is_error: bool,
    stats_error: str | None,
) -> str:
    """Newest failure text with the durable row read for the caller."""
    stored = await resolve_stored_reason(db_manager, is_error, stats_error)
    return resolve_terminal_reason(poller_error, stored, stats_error)


class TerminalPipeline(Protocol):
    """The pipeline surface the terminal home records failures against.

    ``state`` is compared by string value against the terminal lifecycle, so
    any string-valued lifecycle enum (including PipelineState) satisfies it.
    """

    @property
    def state(self) -> StrEnum: ...

    @property
    def stats(self) -> PipelineStats: ...

    @property
    def stop_event(self) -> asyncio.Event | None: ...

    @property
    def db_manager(self) -> DatabaseManager | None: ...

    @property
    def terminal_writes(self) -> set[asyncio.Task[None]]: ...

    def enter_terminal_state(self) -> None: ...

    def record_processing_error(self, message: str) -> None: ...


def record_terminal_failure(pipeline: TerminalPipeline, error: str, worker: str) -> None:
    """Enter ERROR once; retain one durable write for the stop drain."""
    if not apply_terminal_transition(pipeline, error):
        return
    schedule_failure_write(
        pipeline.terminal_writes,
        pipeline.db_manager,
        pipeline.stats.last_error,
        worker,
        pipeline.record_processing_error,
    )


def apply_terminal_transition(pipeline: TerminalPipeline, error: str) -> bool:
    """Record terminal state/stats; return True on the first terminal call."""
    first = pipeline.state in (TerminalLifecycle.STARTING, TerminalLifecycle.RUNNING)
    if first:
        pipeline.enter_terminal_state()
    pipeline.stats.errors += 1
    pipeline.stats.last_error = redact_text(error)
    stop_event = pipeline.stop_event
    if stop_event is not None and not stop_event.is_set():
        stop_event.set()
    return first


def note_poller_failure(pipeline: TerminalPipeline, poller_error: str | None) -> None:
    """Record a FAILED poller as a terminal trade-poller failure."""
    record_terminal_failure(
        pipeline, poller_error or TERMINAL_FALLBACK_REASON, DEFAULT_TERMINAL_WORKER
    )


def schedule_failure_write(
    pending: set[asyncio.Task[None]],
    db_manager: DatabaseManager | None,
    stats_error: str | None,
    worker: str,
    on_error: Callable[[str], None],
) -> None:
    """Retain one durable write for the stop drain."""
    schedule_terminal_write(
        pending,
        terminal_write(db_manager, stats_error or TERMINAL_FALLBACK_REASON, worker, on_error),
    )
