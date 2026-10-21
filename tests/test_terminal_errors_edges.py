"""Terminal-error persistence edges: no running loop, failed writes, durable reads, drain."""

from __future__ import annotations

import asyncio
import inspect

from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import PipelineTerminalErrorModel
from polymarket_insider_tracker.terminal_errors import (
    drain_terminal_writes,
    read_latest_terminal_reason,
    resolve_stored_reason,
    schedule_terminal_write,
    terminal_write,
    write_terminal_row,
)
from tests.fakes.database import failing_database

SECRET_URL = "https://hooks.example.com/v2/s3cr3t-k3y-value"


def test_a_write_scheduled_with_no_running_loop_is_closed_and_not_retained() -> None:
    pending: set[asyncio.Task[None]] = set()
    errors: list[str] = []
    write = terminal_write(None, "boom", "trade_poller", errors.append)

    task = schedule_terminal_write(pending, write)

    assert task is None
    assert pending == set()
    assert errors == []
    assert inspect.getcoroutinestate(write) == inspect.CORO_CLOSED


async def test_a_failed_row_write_is_reported_without_raising_or_leaking_secrets() -> None:
    errors: list[str] = []
    database = failing_database(RuntimeError(f"cannot reach {SECRET_URL}"))

    await write_terminal_row(database, "boom", "trade_poller", errors.append)

    assert len(errors) == 1
    assert errors[0].startswith("Failed to persist terminal error: ")
    assert "s3cr3t-k3y-value" not in errors[0]


async def test_no_database_means_no_durable_reason() -> None:
    assert await read_latest_terminal_reason(None) is None


async def test_an_empty_table_has_no_durable_reason(db_manager: DatabaseManager) -> None:
    assert await read_latest_terminal_reason(db_manager) is None


async def test_the_newest_durable_reason_wins(db_manager: DatabaseManager) -> None:
    async with db_manager.get_async_session() as session:
        session.add(PipelineTerminalErrorModel(reason="older", worker="trade_poller"))
    async with db_manager.get_async_session() as session:
        session.add(PipelineTerminalErrorModel(reason="newest", worker="trade_poller"))

    assert await read_latest_terminal_reason(db_manager) == "newest"


async def test_drain_reports_a_failed_write_and_skips_a_cancelled_one() -> None:
    async def fail() -> None:
        raise RuntimeError("write exploded")

    async def hang() -> None:
        await asyncio.Event().wait()

    failed = asyncio.ensure_future(fail())
    cancelled = asyncio.ensure_future(hang())
    await asyncio.sleep(0)
    cancelled.cancel()
    pending = {failed, cancelled}
    errors: list[str] = []

    await drain_terminal_writes(pending, errors.append)

    assert pending == set()
    assert errors == ["Failed to persist terminal error: write exploded"]


async def test_a_durable_reason_outranks_the_stats_error(db_manager: DatabaseManager) -> None:
    async with db_manager.get_async_session() as session:
        session.add(PipelineTerminalErrorModel(reason="durable", worker="trade_poller"))

    assert await resolve_stored_reason(db_manager, True, "from stats") == "durable"
