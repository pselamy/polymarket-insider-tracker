"""Fixtures shared by the integration journeys in this directory.

The journey fixtures have one home in ``test_end_to_end`` and are re-exported here so the sibling
v2 journeys use the same objects without a second definition. ``journey_environment`` binds the
standard per-test fixtures for the v2 journeys; the non-fixture scaffolding lives in
``_journey_harness``.
"""

from __future__ import annotations

import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy.ext.asyncio import AsyncEngine
from tests.integration._journey_harness import JourneyEnvironment
from tests.integration.test_end_to_end import niche_market, sample_trade_event

from polymarket_insider_tracker.storage.database import DatabaseManager

__all__ = ["journey_environment", "niche_market", "sample_trade_event"]


@pytest.fixture
def journey_environment(
    unused_tcp_port: int,
    fake_redis: FakeAsyncRedis,
    db_manager: DatabaseManager,
    async_engine: AsyncEngine,
) -> JourneyEnvironment:
    """Bundle the standard journey fixtures for the v2 journey tests."""
    return JourneyEnvironment(
        health_port=unused_tcp_port,
        redis=fake_redis,
        db_manager=db_manager,
        engine=async_engine,
    )
