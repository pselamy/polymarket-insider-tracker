"""Real-service journeys on the approved v2 default: migrated PostgreSQL and real Redis.

Two proofs share the same service boundaries. ``test_v2_default_journey_through_real_postgres_and_redis``
runs the dry-run journey with ``POLYMARKET_TRADES_URL`` unset (the official Data API v2 default)
and asserts exactly one stored explainable assessment, zero channel deliveries, zero
successful-delivery dedup keys, and the durable observation-boundary checkpoint in real Redis under
the official namespace.
``test_v2_default_restart_resumes_the_boundary_and_deduplicates_replay`` runs one v2 pipeline until
it advances the boundary, stops it, starts a new pipeline against the same real Redis, and asserts
the new pipeline resumes from the stored checkpoint: no first-start re-anchor, no new gap, and a
replayed trade deduplicated by the retained identity window.

Selection mirrors the service-profile gates: both tests run only when ``DATABASE_URL`` and
``REDIS_URL`` are present (the ``services`` gates inherit both), skip cleanly when the deterministic
compatibility suite scrubs service configuration, and reject a non-loopback ``REDIS_URL`` before any
client exists. The services profile's only pytest path is ``test_redis_contract``, which re-exports
both tests. The shared journey scaffolding lives in ``_journey_harness`` and the disposable
database helper in ``test_postgres_journey``.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from redis.asyncio import Redis
from scripts._service_safety import validate_loopback_redis_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from tests.fakes import (
    FakeClock,
    FakeEth,
    FakeV2TradesServer,
    make_test_settings,
    v2_row,
    wire_pipeline,
)
from tests.fakes.redis_await import settled
from tests.integration._journey_harness import (
    POLL_START,
    JourneyEnvironment,
    market_for_trade,
    persisted_assessments,
    pipeline_row,
    run_dry_run_journey,
    seeded_v2_server,
)
from tests.integration._journey_harness import run_until as _run_until
from tests.integration.test_postgres_journey import _disposable_database
from tests.integration.test_scoring_config_rows import evidence_of, member, read_row

from polymarket_insider_tracker.detector.volume_state import VolumeState, classify_volume
from polymarket_insider_tracker.endpoints import OFFICIAL_V2_TRADES_URL
from polymarket_insider_tracker.ingestor.gamma_client import GammaCrawl, GammaMarketStats
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.ingestor.observation_boundary import (
    KEY_PREFIX,
    BoundaryOrigin,
    Checkpoint,
    source_id,
)
from polymarket_insider_tracker.ingestor.volume_observation import GAMMA_SOURCE
from polymarket_insider_tracker.ingestor.volume_store import (
    DEFAULT_VOLUME_KEY_PREFIX,
    VOLUME_RECORD_TTL_SECONDS,
    VolumeRecord,
    VolumeStore,
)
from polymarket_insider_tracker.pipeline import Pipeline
from polymarket_insider_tracker.storage.database import DatabaseManager

DEFAULT_COVERAGE = "all"
FIRST_TX = 9
RESTART_TX = 77


def _required_redis_url() -> str:
    """Return the loopback Redis URL or skip when the deterministic suite scrubbed it."""
    value = os.environ.get("REDIS_URL")
    if not value:
        pytest.skip("REDIS_URL is required for the real-service v2 default journeys")
    return validate_loopback_redis_url(value)


async def _connect_redis(url: str) -> Redis:
    client: Redis = Redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
    try:
        assert await settled(client.ping()) is True
    except BaseException:
        await client.aclose()
        raise
    return client


def _boundary_keys(coverage: str) -> tuple[str, str, str]:
    """The three official-namespace keys the boundary owns for this URL and coverage."""
    prefix = f"{KEY_PREFIX}{source_id(OFFICIAL_V2_TRADES_URL, coverage)}:"
    return (f"{prefix}checkpoint", f"{prefix}identities", f"{prefix}loss-events")


async def _scan_keys(redis: Redis) -> set[bytes]:
    return {key async for key in redis.scan_iter()}


async def _assert_boundary_checkpoint(redis: Redis, coverage: str) -> None:
    """The durable checkpoint is present and decodes under the official namespace."""
    raw = await settled(redis.hgetall(_boundary_keys(coverage)[0]))
    assert raw, "the observation boundary checkpoint was not written to real Redis"
    assert Checkpoint.from_mapping(raw).coverage == coverage


async def _delete_keys_created_since(redis: Redis, baseline: set[bytes]) -> None:
    created = await _scan_keys(redis) - baseline
    if created:
        await redis.delete(*created)


async def _clear_boundary_and_snapshot(redis: Redis) -> set[bytes]:
    """Start from a fresh boundary and return the keys that already existed."""
    await redis.delete(*_boundary_keys(DEFAULT_COVERAGE))
    return await _scan_keys(redis)


async def _serve_volume_through_real_redis(redis: Redis, market: MarketMetadata) -> MarketMetadata:
    """T025: store ``market``'s stamped volume as the real Redis volume record the Gamma refresh
    writes (product TTL), and return the volume-less CLOB metadata the journey caches beside it."""
    assert market.volume is not None
    await redis.setex(
        DEFAULT_VOLUME_KEY_PREFIX + market.condition_id,
        VOLUME_RECORD_TTL_SECONDS,
        VolumeRecord(market.volume).to_json(),
    )
    return replace(market, volume=None)


def _crawl_of(condition_id: str, acquired_at: datetime) -> GammaCrawl:
    stats = GammaMarketStats(condition_id, acquired_at, True, "10000", GAMMA_SOURCE)
    return GammaCrawl({condition_id: stats}, 1, 0, False, acquired_at, acquired_at)


async def _state_at(
    store: VolumeStore, metadata: MarketMetadata, now: datetime
) -> tuple[VolumeState, str]:
    """The detector's classification of the record ``store`` reads back from real Redis."""
    merged = await store.merge(metadata)
    verdict = classify_volume(
        merged.volume, evaluated_at=now, unavailable_reason=merged.volume_unavailable_reason
    )
    return verdict.state, verdict.reason


async def _expired(redis: Redis, key: str) -> None:
    deadline = asyncio.get_running_loop().time() + 5.0
    while await settled(redis.exists(key)) and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.05)


async def _run_real_journey(
    *,
    database_url: str,
    redis_url: str,
    health_port: int,
    sample_trade: TradeEvent,
    market: MarketMetadata,
) -> None:
    redis = await _connect_redis(redis_url)
    engine = create_async_engine(database_url)
    manager = DatabaseManager(database_url, async_mode=True)
    baseline: set[bytes] | None = None
    try:
        baseline = await _clear_boundary_and_snapshot(redis)
        metadata = await _serve_volume_through_real_redis(redis, market)
        settings = make_test_settings(
            dry_run=True, alert_threshold=0.4, health_port=health_port, trades_url=None
        )
        assert settings.polymarket.trades_url == OFFICIAL_V2_TRADES_URL
        clock = FakeClock(POLL_START)
        environment = JourneyEnvironment(
            health_port=health_port, redis=redis, db_manager=manager, engine=engine
        )
        assessment = await run_dry_run_journey(
            environment, settings, seeded_v2_server(clock), clock, sample_trade, metadata
        )
        # T025: the stamped volume read back from the real Redis record scores 0.450.
        size = member(evidence_of(read_row(assessment)), "size")
        assert (size["volume_state"], size["daily_volume"]) == ("available", "10000")
        assert str(assessment.weighted_score) == "0.450"
        await _assert_boundary_checkpoint(redis, DEFAULT_COVERAGE)
    finally:
        if baseline is not None:
            await _delete_keys_created_since(redis, baseline)
        await engine.dispose()
        await manager.dispose_async()
        await redis.aclose()


async def _wired_pipeline(
    *,
    redis: Redis,
    db_manager: DatabaseManager,
    market: MarketMetadata,
    server: FakeV2TradesServer,
    clock: FakeClock,
    health_port: int,
) -> Pipeline:
    return await wire_pipeline(
        make_test_settings(
            dry_run=True, alert_threshold=0.4, health_port=health_port, trades_url=None
        ),
        redis=redis,
        eth=FakeEth(transaction_count=0),
        market=market,
        db_manager=db_manager,
        channels=[],
        trades=server,
        poll_clock=clock,
    )


async def _run_until_stopped(pipeline: Pipeline, action: Callable[[], Awaitable[None]]) -> None:
    """Start the pipeline, run ``action``, and always stop the background services."""
    try:
        await asyncio.wait_for(pipeline._start_background_services(), timeout=2.0)
        await action()
    finally:
        await asyncio.wait_for(pipeline._stop_background_services(), timeout=2.0)


async def _restart_journey(
    redis: Redis,
    *,
    health_port: int,
    db_manager: DatabaseManager,
    engine: AsyncEngine,
    sample_trade: TradeEvent,
    market: MarketMetadata,
) -> None:
    clock = FakeClock(POLL_START)
    server = seeded_v2_server(clock)
    first = await _wired_pipeline(
        redis=redis,
        db_manager=db_manager,
        market=market,
        server=server,
        clock=clock,
        health_port=health_port,
    )

    async def first_cycles() -> None:
        await _run_until(lambda: len(server.requests) >= 3)
        server.publish(v2_row(pipeline_row(sample_trade, clock, tx_num=FIRST_TX)))
        await _run_until(lambda: first.stats.trades_processed >= 1)
        await _run_until(lambda: len(server.requests) >= 6)

    await _run_until_stopped(first, first_cycles)
    assert first._trade_poller is not None
    boundary_at_stop = first._trade_poller.status.boundary_time
    assert boundary_at_stop is not None
    assert first._trade_poller.status.boundary_origin is BoundaryOrigin.PROVEN

    clock.advance(5)
    second = await _wired_pipeline(
        redis=redis,
        db_manager=db_manager,
        market=market,
        server=server,
        clock=clock,
        health_port=health_port,
    )

    async def second_cycles() -> None:
        server.publish(v2_row(pipeline_row(sample_trade, clock, tx_num=RESTART_TX)))
        await _run_until(lambda: second.stats.trades_processed >= 1)

    await _run_until_stopped(second, second_cycles)
    assert second._trade_poller is not None
    status = second._trade_poller.status
    assert status.boundary_origin is BoundaryOrigin.PROVEN
    assert status.boundary_time is not None and status.boundary_time >= boundary_at_stop
    assert status.loss_events == ()
    assert second.stats.trades_processed == 1

    rows = await persisted_assessments(engine)
    trade_ids = [row.trade_id for row in rows]
    assert trade_ids.count("0x" + f"{FIRST_TX:064x}") == 1
    assert "0x" + f"{RESTART_TX:064x}" in trade_ids
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_v2_default_journey_through_real_postgres_and_redis(
    unused_tcp_port: int,
    sample_trade_event: TradeEvent,
    niche_market: MarketMetadata,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default URL, real PostgreSQL and Redis: one explainable assessment, checkpoint durable."""
    monkeypatch.delenv("POLYMARKET_TRADES_URL", raising=False)
    redis_url = _required_redis_url()
    with _disposable_database(monkeypatch) as database_url:
        await _run_real_journey(
            database_url=database_url,
            redis_url=redis_url,
            health_port=unused_tcp_port,
            sample_trade=sample_trade_event,
            market=niche_market,
        )


@pytest.mark.asyncio
async def test_v2_default_restart_resumes_the_boundary_and_deduplicates_replay(
    unused_tcp_port: int,
    db_manager: DatabaseManager,
    async_engine: AsyncEngine,
    sample_trade_event: TradeEvent,
    niche_market: MarketMetadata,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Restart on the v2 default against real Redis resumes the checkpoint and dedups a replay."""
    monkeypatch.delenv("POLYMARKET_TRADES_URL", raising=False)
    redis_url = _required_redis_url()
    redis = await _connect_redis(redis_url)
    baseline: set[bytes] | None = None
    try:
        baseline = await _clear_boundary_and_snapshot(redis)
        await _restart_journey(
            redis,
            health_port=unused_tcp_port,
            db_manager=db_manager,
            engine=async_engine,
            sample_trade=sample_trade_event,
            market=niche_market,
        )
    finally:
        if baseline is not None:
            await _delete_keys_created_since(redis, baseline)
        await redis.aclose()


@pytest.mark.asyncio
async def test_a_real_redis_volume_record_reads_available_then_stale_then_expires(
    sample_trade_event: TradeEvent,
) -> None:
    """T025 / T012d (4): the 0-600 s rule over real Redis, then real TTL expiry.

    A record stamped 30 s ago is ``available`` and one stamped 601 s ago is still readable as
    ``stale`` (the product TTL outlives the recency rule). After real Redis expires the record and
    the crawl status, the market reads ``unavailable`` with ``no_volume_record``. A unique key
    prefix keeps the shared local Redis untouched.
    """
    redis = await _connect_redis(_required_redis_url())
    prefix = f"pit-test:volume:{uuid.uuid4().hex}:"
    metadata = market_for_trade(sample_trade_event, "science")
    product, expiring = VolumeStore(redis, key_prefix=prefix), VolumeStore(
        redis, key_prefix=prefix, ttl_seconds=1
    )
    now = datetime.now(UTC)
    try:
        await product.write_crawl(
            _crawl_of(metadata.condition_id, now - timedelta(seconds=30)), now
        )
        assert 600 < await settled(redis.ttl(prefix + metadata.condition_id)) <= 1200
        assert await _state_at(product, metadata, now) == (VolumeState.AVAILABLE, "gamma")

        stale_at = now - timedelta(seconds=601)
        await expiring.write_crawl(_crawl_of(metadata.condition_id, stale_at), now)
        state, _ = await _state_at(expiring, metadata, now)
        assert state is VolumeState.STALE

        await _expired(redis, prefix + metadata.condition_id)
        await _expired(redis, prefix + "_crawl")
        assert await _state_at(expiring, metadata, now) == (
            VolumeState.UNAVAILABLE,
            "no_volume_record",
        )
    finally:
        await redis.delete(prefix + metadata.condition_id, prefix + "_crawl")
        await redis.aclose()
