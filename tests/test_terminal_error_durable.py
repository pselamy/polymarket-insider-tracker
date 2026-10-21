"""Terminal worker failure is durable after actual stop/shutdown; logs carry no secret."""

from __future__ import annotations

import logging

import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from polymarket_insider_tracker.ingestor.trade_poller import IngestionState
from polymarket_insider_tracker.pipeline import Pipeline, PipelineState
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import PipelineTerminalErrorModel
from polymarket_insider_tracker.terminal_errors import (
    DEFAULT_TERMINAL_WORKER,
    drain_terminal_writes,
    record_terminal_failure,
)
from tests.fakes import (
    FakeEth,
    StubPoller,
    Substitutes,
    WiredPipeline,
    ingestion_component,
    make_test_settings,
    run_until,
    terminal,
    wire_pipeline,
)
from tests.fakes.trades import FakeClock, FakeTradesServer

POLL_START = 1_700_000_000.0
SECRET_URL = "https://hooks.example.com/v2/s3cr3t-k3y-value"


async def _terminal_rows(engine: AsyncEngine) -> list[PipelineTerminalErrorModel]:
    from sqlalchemy.ext.asyncio import async_sessionmaker

    async with async_sessionmaker(bind=engine, expire_on_commit=False)() as session:
        return list((await session.execute(select(PipelineTerminalErrorModel))).scalars().all())


async def _pipeline_with_terminal_401(
    fake_redis: FakeAsyncRedis, db_manager: DatabaseManager, health_port: int
) -> WiredPipeline:
    """The real pipeline over fake boundaries, whose next trades poll fails terminally with 401."""
    clock = FakeClock(POLL_START)
    server = FakeTradesServer(clock=clock)
    server.fail_next(terminal(401))
    return await wire_pipeline(
        make_test_settings(health_port=health_port),
        redis=fake_redis,
        eth=FakeEth(),
        db_manager=db_manager,
        trades=server,
        poll_clock=clock,
    )


async def _run_until_poller_failure_is_terminal(pipeline: Pipeline) -> None:
    """Start the pipeline and wait for its failed poller to move it to ERROR."""
    await pipeline.start()
    await run_until(lambda: pipeline.state is PipelineState.ERROR)


async def _running_pipeline(
    fake_redis: FakeAsyncRedis, db_manager: DatabaseManager, health_port: int
) -> WiredPipeline:
    """The real pipeline over fake boundaries, started and running with no trade source."""
    pipeline = await wire_pipeline(
        make_test_settings(health_port=health_port),
        redis=fake_redis,
        eth=FakeEth(),
        db_manager=db_manager,
    )
    await pipeline.start()
    return pipeline


async def _drain(pipeline: Pipeline) -> None:
    """Await the retained durable failure writes, as the stop boundary does."""
    await drain_terminal_writes(pipeline.terminal_writes, pipeline.record_processing_error)


async def _assert_poller_401_row(engine: AsyncEngine) -> None:
    """Exactly one durable row records the poller's terminal 401."""
    rows = await _terminal_rows(engine)
    assert len(rows) == 1
    assert "401" in rows[0].reason
    assert rows[0].worker == "trade_poller"


class TestTerminalErrorDurable:
    async def test_failure_persisted_after_worker_exit_and_read_by_health(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        unused_tcp_port: int,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        pipeline = await _pipeline_with_terminal_401(fake_redis, db_manager, unused_tcp_port)

        with caplog.at_level(logging.ERROR):
            await _run_until_poller_failure_is_terminal(pipeline)

        assert pipeline.state is PipelineState.ERROR
        ready, reason, components = await pipeline.check_readiness()
        assert ready is False
        assert components.get("ingestion") == "down"
        assert reason == "ingestion_worker_failed"
        await _drain(pipeline)

        await _assert_poller_401_row(async_engine)

        ingestion = await ingestion_component(pipeline)
        assert ingestion.status == "down"
        assert ingestion.last_error is not None and "401" in ingestion.last_error
        await pipeline.stop()

    async def test_row_survives_stop_boundary(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        unused_tcp_port: int,
    ) -> None:
        pipeline = await _pipeline_with_terminal_401(fake_redis, db_manager, unused_tcp_port)
        await _run_until_poller_failure_is_terminal(pipeline)
        await _drain(pipeline)
        await _assert_poller_401_row(async_engine)
        await pipeline.stop()

    async def test_structured_log_carries_no_secret(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        unused_tcp_port: int,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        pipeline = await _running_pipeline(fake_redis, db_manager, unused_tcp_port)

        with caplog.at_level(logging.ERROR):
            record_terminal_failure(
                pipeline, f"acquisition stopped: {SECRET_URL}", DEFAULT_TERMINAL_WORKER
            )

        await _drain(pipeline)
        rows = await _terminal_rows(async_engine)
        assert len(rows) == 1
        assert "s3cr3t-k3y-value" not in rows[0].reason
        assert "s3cr3t-k3y-value" not in caplog.text
        await pipeline.stop()

    async def test_non_default_worker_identity_recorded(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        unused_tcp_port: int,
    ) -> None:
        pipeline = await _running_pipeline(fake_redis, db_manager, unused_tcp_port)

        record_terminal_failure(pipeline, "metadata crawl failed", "metadata_sync")
        await _drain(pipeline)
        rows = await _terminal_rows(async_engine)
        assert len(rows) == 1
        assert rows[0].worker == "metadata_sync"
        assert rows[0].worker != "trade_poller"
        await pipeline.stop()

    async def test_terminal_row_survives_stop_without_explicit_drain(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        unused_tcp_port: int,
    ) -> None:
        """The real stop() boundary drains the retained write (drop-drain mutant)."""
        pipeline = await _pipeline_with_terminal_401(fake_redis, db_manager, unused_tcp_port)
        await _run_until_poller_failure_is_terminal(pipeline)

        await pipeline.stop()

        await _assert_poller_401_row(async_engine)

    async def test_fresh_pipeline_reads_terminal_reason_from_durable_row(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
    ) -> None:
        """A fresh pipeline with empty stats reads the prior reason (drop-read mutant)."""
        from sqlalchemy.ext.asyncio import async_sessionmaker

        async with async_sessionmaker(bind=async_engine, expire_on_commit=False)() as session:
            session.add(
                PipelineTerminalErrorModel(reason="prior process: HTTP 401", worker="trade_poller")
            )
            await session.commit()

        failed_poller = StubPoller(fake_redis, IngestionState.FAILED)
        fresh = await wire_pipeline(
            make_test_settings(),
            redis=fake_redis,
            eth=FakeEth(),
            db_manager=db_manager,
            substitutes=Substitutes(poller=failed_poller),
        )
        fresh.enter_terminal_state()
        assert failed_poller.status.last_error is None
        assert fresh.stats.last_error is None

        ingestion = await ingestion_component(fresh)
        assert ingestion.status == "down"
        assert ingestion.last_error == "prior process: HTTP 401"
        await fresh.stop()
