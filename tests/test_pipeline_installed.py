"""The pipeline's installed-set invariant, driven through its public lifecycle.

Before the first install a pipeline has no detector, scorer, formatter, dispatcher or metadata
sync; ``install_components`` supplies all of them at once and a re-install replaces them all,
and the health monitor exists for the pipeline's whole life. These tests pin each boundary.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json

from fakeredis import FakeAsyncRedis
from sqlalchemy.ext.asyncio import AsyncEngine

from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.pipeline import Pipeline, PipelineState
from polymarket_insider_tracker.storage.database import DatabaseManager
from tests.fakes import (
    FailingDetector,
    FakeAlertChannel,
    FakeEth,
    Substitutes,
    make_test_settings,
    wire_pipeline,
)
from tests.integration._journey_harness import (
    make_sample_trade_event,
    persisted_assessments,
    stamped_volume_market,
)


class NoBackgroundServicesPipeline(Pipeline):
    """Starts nothing in the background, as ``tests/test_pipeline.py`` startup-failure doubles do."""

    async def _start_background_services(self) -> None:
        return None


async def test_start_builds_the_production_set_before_background_services(
    unused_tcp_port: int,
) -> None:
    pipeline = NoBackgroundServicesPipeline(make_test_settings(health_port=unused_tcp_port))

    await pipeline.start()
    try:
        assert pipeline.state is PipelineState.RUNNING
        assert pipeline.db_manager is not None
    finally:
        await pipeline.stop()

    assert pipeline.db_manager is None


async def test_health_monitor_is_the_same_object_before_install_and_across_start_and_stop(
    fake_redis: FakeAsyncRedis, unused_tcp_port: int
) -> None:
    pipeline = Pipeline(make_test_settings(health_port=unused_tcp_port))
    monitor = pipeline.health_monitor
    trade = make_sample_trade_event()

    await pipeline.process_trade(trade)
    assert monitor.last_trade_time is not None

    wired = await wire_pipeline(
        make_test_settings(health_port=unused_tcp_port), redis=fake_redis, eth=FakeEth()
    )
    wired_monitor = wired.health_monitor
    await wired.start()
    await wired.stop()

    assert pipeline.health_monitor is monitor
    assert wired.health_monitor is wired_monitor
    assert wired.state is PipelineState.STOPPED


async def test_trade_before_install_is_not_scored_and_the_next_one_after_install_is(
    fake_redis: FakeAsyncRedis,
) -> None:
    trade = make_sample_trade_event()
    market: MarketMetadata = stamped_volume_market(trade)
    channel = FakeAlertChannel("discord")
    settings = make_test_settings(persist_assessments=False, alert_threshold=0.4)
    source = await wire_pipeline(
        settings,
        redis=fake_redis,
        eth=FakeEth(transaction_count=0),
        market=market,
        channels=[channel],
    )
    assert source.components is not None
    pipeline = Pipeline(settings)

    await pipeline.process_trade(trade)
    assert (pipeline.stats.signals_generated, len(channel.deliveries)) == (0, 0)

    pipeline.install_components(source.components)
    await pipeline.process_trade(trade)

    assert pipeline.stats.trades_processed == 2
    assert pipeline.stats.signals_generated == 1
    assert len(channel.deliveries) == 1


async def test_reinstalled_scorer_and_dispatcher_replace_the_first_set_together(
    fake_redis: FakeAsyncRedis,
) -> None:
    trade = make_sample_trade_event()
    first, second = FakeAlertChannel("first"), FakeAlertChannel("second")
    settings = make_test_settings(persist_assessments=False, alert_threshold=0.4)
    market = stamped_volume_market(trade)
    eth = FakeEth(transaction_count=0)
    pipeline = await wire_pipeline(
        settings, redis=fake_redis, eth=eth, market=market, channels=[first]
    )
    replacement = await wire_pipeline(
        settings, redis=FakeAsyncRedis(), eth=eth, market=market, channels=[second]
    )
    assert replacement.components is not None

    pipeline.install_components(replacement.components)
    await pipeline.process_trade(trade)

    assert (len(first.deliveries), len(second.deliveries)) == (0, 1)


async def test_metadata_sync_installed_before_the_crawl_runs_is_the_one_crawled_and_stopped(
    fake_redis: FakeAsyncRedis, unused_tcp_port: int
) -> None:
    settings = make_test_settings(health_port=unused_tcp_port)
    first_log: list[str] = []
    second_log: list[str] = []
    pipeline = await wire_pipeline(settings, redis=fake_redis, eth=FakeEth(), state_log=first_log)
    replacement = await wire_pipeline(
        settings, redis=fake_redis, eth=FakeEth(), state_log=second_log
    )
    assert pipeline.components is not None and replacement.metadata_sync is not None

    await pipeline.start()
    pipeline.install_components(
        dataclasses.replace(pipeline.components, metadata_sync=replacement.metadata_sync)
    )
    try:
        for _ in range(20):
            await asyncio.sleep(0)
    finally:
        await pipeline.stop()

    assert first_log == []
    assert second_log[0] == "metadata:starting"
    assert second_log[-1] == "metadata:stopped"


async def test_skip_row_records_detector_failures_and_no_evaluation_evidence(
    fake_redis: FakeAsyncRedis, db_manager: DatabaseManager, async_engine: AsyncEngine
) -> None:
    trade: TradeEvent = make_sample_trade_event()
    pipeline = await wire_pipeline(
        make_test_settings(),
        redis=fake_redis,
        eth=FakeEth(),
        db_manager=db_manager,
        substitutes=Substitutes(
            fresh_detector=FailingDetector(RuntimeError("polygon rpc down")),
            size_detector=FailingDetector(RuntimeError("metadata down")),
        ),
    )

    await pipeline.process_trade(trade)

    rows = await persisted_assessments(async_engine)
    assert [row.delivery_disposition for row in rows] == ["detector_failure"]
    assert rows[0].scoring_config is not None
    document = json.loads(rows[0].scoring_config)
    assert [failure["detector"] for failure in document["detector_failures"]] == [
        "fresh_wallet",
        "size_anomaly",
    ]
    assert document["evaluation_evidence"] is None
