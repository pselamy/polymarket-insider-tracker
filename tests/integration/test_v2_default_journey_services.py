"""The v2-default dry-run journey across real migrated PostgreSQL and real Redis.

The same dry-run contract as ``test_v2_default_journey`` — one stored explainable assessment,
zero channel deliveries, and zero successful-delivery dedup keys — but with BOTH service
boundaries real at once. With ``POLYMARKET_TRADES_URL`` unset the approved default (the official
Data API v2 endpoint) is what the poller runs; the assessment persists through the production
repositories into a uniquely named, Alembic-migrated PostgreSQL database; and the durable
observation-boundary checkpoint is written to a real loopback Redis under the official namespace.

Selection mirrors the service-profile gates: the test runs only when ``DATABASE_URL`` and
``REDIS_URL`` are present (the ``services`` gates inherit both), skips cleanly when the
deterministic compatibility suite scrubs service configuration, and rejects a non-loopback
``REDIS_URL`` before any client exists. The services profile's only pytest path is
``test_redis_contract``, which re-exports this test. The shared journey scaffolding lives in
``_v2_journey_harness`` and the disposable-database helper in ``test_postgres_journey``.
"""

from __future__ import annotations

import os

import pytest
from redis.asyncio import Redis
from scripts._service_safety import validate_loopback_redis_url
from sqlalchemy.ext.asyncio import create_async_engine
from tests.fakes import FakeClock, make_test_settings
from tests.integration._v2_journey_harness import (
    POLL_START,
    JourneyEnvironment,
    run_dry_run_journey,
    seeded_v2_server,
)
from tests.integration.test_postgres_journey import _disposable_database

from polymarket_insider_tracker.endpoints import OFFICIAL_V2_TRADES_URL
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.ingestor.observation_boundary import (
    KEY_PREFIX,
    Checkpoint,
    source_id,
)
from polymarket_insider_tracker.storage.database import DatabaseManager

DEFAULT_COVERAGE = "all"


def _required_redis_url() -> str:
    """Return the loopback Redis URL or skip when the deterministic suite scrubbed it."""
    value = os.environ.get("REDIS_URL")
    if not value:
        pytest.skip("REDIS_URL is required for the real-service v2 default journey")
    return validate_loopback_redis_url(value)


async def _connect_redis(url: str) -> Redis:
    client = Redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
    try:
        assert await client.ping() is True
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
    raw = await redis.hgetall(_boundary_keys(coverage)[0])
    assert raw, "the observation boundary checkpoint was not written to real Redis"
    assert Checkpoint.from_mapping(raw).coverage == coverage


async def _delete_keys_created_since(redis: Redis, baseline: set[bytes]) -> None:
    created = await _scan_keys(redis) - baseline
    if created:
        await redis.delete(*created)


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
        await redis.delete(*_boundary_keys(DEFAULT_COVERAGE))
        baseline = await _scan_keys(redis)
        settings = make_test_settings(
            dry_run=True, alert_threshold=0.4, health_port=health_port, trades_url=None
        )
        assert settings.polymarket.trades_url == OFFICIAL_V2_TRADES_URL
        clock = FakeClock(POLL_START)
        environment = JourneyEnvironment(
            health_port=health_port, redis=redis, db_manager=manager, engine=engine
        )
        assessment = await run_dry_run_journey(
            environment, settings, seeded_v2_server(clock), clock, sample_trade, market
        )
        assert assessment.scoring_algorithm_version == "003.1"
        await _assert_boundary_checkpoint(redis, DEFAULT_COVERAGE)
    finally:
        if baseline is not None:
            await _delete_keys_created_since(redis, baseline)
        await engine.dispose()
        await manager.dispose_async()
        await redis.aclose()


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
