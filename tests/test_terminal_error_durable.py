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


class TestTerminalErrorDurable:
    async def test_failure_persisted_after_worker_exit_and_read_by_health(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        clock = FakeClock(POLL_START)
        server = FakeTradesServer(clock=clock)
        pipeline = await wire_pipeline(
            make_test_settings(),
            redis=fake_redis,
            eth=FakeEth(),
            db_manager=db_manager,
            trades=server,
            poll_clock=clock,
        )
        assert pipeline._trade_poller is not None
        server.fail_next(terminal(401))

        with caplog.at_level(logging.ERROR):
            await pipeline._trade_poller.run_cycle()

        assert pipeline._trade_poller.state is IngestionState.FAILED
        assert pipeline.state is PipelineState.STOPPED
        pipeline._state = PipelineState.RUNNING
        pipeline._handle_worker_failure(
            pipeline._trade_poller.status.last_error or "ingestion_worker_failed"
        )
        assert pipeline.state is PipelineState.ERROR
        ready, reason, components = await pipeline.check_readiness()
        assert ready is False
        assert components.get("ingestion") == "down"
        assert reason == "ingestion_worker_failed"
        await pipeline._drain_terminal_writes()

        rows = await _terminal_rows(async_engine)
        assert len(rows) == 1
        assert "401" in rows[0].reason
        assert rows[0].worker == "trade_poller"

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
        clock = FakeClock(POLL_START)
        server = FakeTradesServer(clock=clock)
        pipeline = await wire_pipeline(
            make_test_settings(),
            redis=fake_redis,
            eth=FakeEth(),
            db_manager=db_manager,
            trades=server,
            poll_clock=clock,
        )
        assert pipeline._trade_poller is not None
        server.fail_next(terminal(401))
        await pipeline._trade_poller.run_cycle()
        assert pipeline._trade_poller.state is IngestionState.FAILED
        pipeline._state = PipelineState.RUNNING
        pipeline._handle_worker_failure(
            pipeline._trade_poller.status.last_error or "ingestion_worker_failed"
        )
        await pipeline._drain_terminal_writes()
        rows = await _terminal_rows(async_engine)
        assert len(rows) == 1
        assert "401" in rows[0].reason
        assert rows[0].worker == "trade_poller"
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
        clock = FakeClock(POLL_START)
        server = FakeTradesServer(clock=clock)
        pipeline = await wire_pipeline(
            make_test_settings(),
            redis=fake_redis,
            eth=FakeEth(),
            db_manager=db_manager,
            trades=server,
            poll_clock=clock,
        )
        assert pipeline._trade_poller is not None
        server.fail_next(terminal(401))
        await pipeline._trade_poller.run_cycle()
        assert pipeline._trade_poller.state is IngestionState.FAILED
        pipeline._state = PipelineState.RUNNING
        pipeline._handle_worker_failure(
            pipeline._trade_poller.status.last_error or "ingestion_worker_failed"
        )
        # The real stop boundary must drain the write before disposal; read the
        # row first, then let stop() dispose the shared in-memory engine.
        await pipeline._stop_background_services()
        rows = await _terminal_rows(async_engine)
        assert len(rows) == 1
        assert "401" in rows[0].reason
        assert rows[0].worker == "trade_poller"
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

        fresh = await wire_pipeline(
            make_test_settings(),
            redis=fake_redis,
            eth=FakeEth(),
            db_manager=db_manager,
            trades=FakeTradesServer(clock=FakeClock(POLL_START)),
            poll_clock=FakeClock(POLL_START),
        )
        fresh._state = PipelineState.ERROR
        fresh._trade_poller._state = IngestionState.FAILED
        assert fresh._trade_poller.status.last_error is None
        assert fresh.stats.last_error is None

        ingestion = await fresh._check_ingestion()
        assert ingestion.status == "down"
        assert ingestion.last_error == "prior process: HTTP 401"
        await fresh.stop()
