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
from tests.fakes import FakeEth, make_test_settings, terminal, wire_pipeline
from tests.fakes.trades import FakeClock, FakeTradesServer

POLL_START = 1_700_000_000.0
SECRET_URL = "https://hooks.example.com/v2/s3cr3t-k3y-value"


async def _terminal_rows(engine: AsyncEngine) -> list[PipelineTerminalErrorModel]:
    from sqlalchemy.ext.asyncio import async_sessionmaker

    async with async_sessionmaker(bind=engine, expire_on_commit=False)() as session:
        return list((await session.execute(select(PipelineTerminalErrorModel))).scalars().all())


async def _wire_poller_pipeline(
    fake_redis: FakeAsyncRedis,
    db_manager: DatabaseManager,
    server: FakeTradesServer,
    poll_clock: FakeClock,
) -> Pipeline:
    """The real pipeline over the fake boundaries, polling ``server`` on ``poll_clock``."""
    return await wire_pipeline(
        make_test_settings(),
        redis=fake_redis,
        eth=FakeEth(),
        db_manager=db_manager,
        trades=server,
        poll_clock=poll_clock,
    )


async def _pipeline_with_terminal_401(
    fake_redis: FakeAsyncRedis, db_manager: DatabaseManager
) -> Pipeline:
    """Wire a pipeline whose next trades poll fails terminally with HTTP 401."""
    clock = FakeClock(POLL_START)
    server = FakeTradesServer(clock=clock)
    pipeline = await _wire_poller_pipeline(fake_redis, db_manager, server, clock)
    server.fail_next(terminal(401))
    return pipeline


async def _fail_poller_cycle(pipeline: Pipeline) -> None:
    """Run one poll cycle and require the poller to end FAILED."""
    assert pipeline._trade_poller is not None
    await pipeline._trade_poller.run_cycle()
    assert pipeline._trade_poller.state is IngestionState.FAILED


def _report_poller_failure(pipeline: Pipeline) -> None:
    """Hand the running pipeline its failed poller's last error, as the worker monitor does."""
    assert pipeline._trade_poller is not None
    pipeline._state = PipelineState.RUNNING
    pipeline._handle_worker_failure(
        pipeline._trade_poller.status.last_error or "ingestion_worker_failed"
    )


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
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        pipeline = await _pipeline_with_terminal_401(fake_redis, db_manager)

        with caplog.at_level(logging.ERROR):
            await _fail_poller_cycle(pipeline)

        state_before = pipeline.state
        assert state_before is PipelineState.STOPPED
        _report_poller_failure(pipeline)
        assert pipeline.state is PipelineState.ERROR
        ready, reason, components = await pipeline.check_readiness()
        assert ready is False
        assert components.get("ingestion") == "down"
        assert reason == "ingestion_worker_failed"
        await pipeline._drain_terminal_writes()

        await _assert_poller_401_row(async_engine)

        ingestion = await pipeline._check_ingestion()
        assert ingestion.status == "down"
        assert ingestion.last_error is not None and "401" in ingestion.last_error
        await pipeline.stop()

    async def test_row_survives_stop_boundary(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
    ) -> None:
        pipeline = await _pipeline_with_terminal_401(fake_redis, db_manager)
        await _fail_poller_cycle(pipeline)
        _report_poller_failure(pipeline)
        await pipeline._drain_terminal_writes()
        await _assert_poller_401_row(async_engine)
        await pipeline.stop()

    async def test_structured_log_carries_no_secret(
        self,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        pipeline = Pipeline(make_test_settings())
        pipeline._db_manager = db_manager
        pipeline._state = PipelineState.RUNNING

        with caplog.at_level(logging.ERROR):
            pipeline._handle_worker_failure(f"acquisition stopped: {SECRET_URL}")

        await pipeline._drain_terminal_writes()
        rows = await _terminal_rows(async_engine)
        assert len(rows) == 1
        assert "s3cr3t-k3y-value" not in rows[0].reason
        assert "s3cr3t-k3y-value" not in caplog.text
        await pipeline.stop()

    async def test_non_default_worker_identity_recorded(
        self,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
    ) -> None:
        pipeline = Pipeline(make_test_settings())
        pipeline._db_manager = db_manager
        pipeline._state = PipelineState.RUNNING

        pipeline._handle_worker_failure("metadata crawl failed", worker="metadata_sync")
        await pipeline._drain_terminal_writes()
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
    ) -> None:
        """The real stop() boundary drains the retained write (drop-drain mutant)."""
        pipeline = await _pipeline_with_terminal_401(fake_redis, db_manager)
        await _fail_poller_cycle(pipeline)
        _report_poller_failure(pipeline)
        # The real stop boundary must drain the write before disposal; read the
        # row first, then let stop() dispose the shared in-memory engine.
        await pipeline._stop_background_services()
        await _assert_poller_401_row(async_engine)
        await pipeline.stop()

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

        fresh = await _wire_poller_pipeline(
            fake_redis,
            db_manager,
            FakeTradesServer(clock=FakeClock(POLL_START)),
            FakeClock(POLL_START),
        )
        assert fresh._trade_poller is not None
        fresh._state = PipelineState.ERROR
        fresh._trade_poller._state = IngestionState.FAILED
        assert fresh._trade_poller.status.last_error is None
        assert fresh.stats.last_error is None

        ingestion = await fresh._check_ingestion()
        assert ingestion.status == "down"
        assert ingestion.last_error == "prior process: HTTP 401"
        await fresh.stop()
