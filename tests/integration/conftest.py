"""Fixtures shared by the integration journeys in this directory.

The v2 journey fixtures live here once so each journey file carries only its own configuration
difference; the non-fixture journey scaffolding stays in ``_v2_journey_harness``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy.ext.asyncio import AsyncEngine
from tests.integration._v2_journey_harness import JourneyEnvironment

from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.storage.database import DatabaseManager


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


@pytest.fixture
def sample_trade_event() -> TradeEvent:
    return TradeEvent(
        trade_id="0x" + "a" * 64,
        wallet_address="0x" + "b" * 40,
        market_id="0x" + "c" * 64,
        asset_id="asset_123",
        side="BUY",
        price=Decimal("0.65"),
        size=Decimal("5000"),
        timestamp=datetime.now(UTC),
        outcome="Yes",
        outcome_index=0,
        event_title="Test Market",
        market_slug="test-market",
    )


@pytest.fixture
def niche_market(sample_trade_event: TradeEvent) -> MarketMetadata:
    return MarketMetadata(
        condition_id=sample_trade_event.market_id,
        question="Test market?",
        description="",
        tokens=(
            Token(
                token_id=sample_trade_event.asset_id,
                outcome=sample_trade_event.outcome,
                price=sample_trade_event.price,
            ),
        ),
        category="science",
    )
