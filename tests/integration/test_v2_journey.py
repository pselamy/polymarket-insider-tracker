"""The deterministic dry-run journey over the v2 cursor feed.

The same pipeline, fakes, and assertions as the v1 journey in ``test_end_to_end.py``, but with
``POLYMARKET_TRADES_URL`` pointed at the official v2 endpoint and a ``{data, pagination}`` cursor
provider behind it. It proves the wiring end to end at the dry-run boundary: exactly one stored
assessment, zero channel deliveries, and zero successful-delivery dedup keys. The shared
scaffolding lives in ``_journey_harness``; this file carries only the explicit-v2-URL
configuration.
"""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine
from tests.fakes import FakeClock, make_test_settings
from tests.integration._journey_harness import (
    POLL_START,
    JourneyEnvironment,
    persisted_assessments,
    pipeline_row,
    run_dry_run_journey,
    run_until,
    seeded_v2_server,
)

from polymarket_insider_tracker.endpoints import OFFICIAL_V2_TRADES_URL
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent


@pytest.mark.asyncio
async def test_end_to_end_dry_run_pipeline_v2(
    journey_environment: JourneyEnvironment,
    sample_trade_event: TradeEvent,
    niche_market: MarketMetadata,
) -> None:
    """Dry run over v2: one assessment, zero delivery calls, zero dedup keys."""
    clock = FakeClock(POLL_START)
    await run_dry_run_journey(
        journey_environment,
        make_test_settings(
            dry_run=True,
            alert_threshold=0.4,
            health_port=journey_environment.health_port,
            trades_url=OFFICIAL_V2_TRADES_URL,
        ),
        seeded_v2_server(clock),
        clock,
        sample_trade_event,
        niche_market,
    )


async def test_journey_harness_characterization(
    sample_trade_event: TradeEvent,
    niche_market: MarketMetadata,
    async_engine: AsyncEngine,
) -> None:
    """Pin the shared journey knowledge: fixtures, row shape, waiter, persistence read."""
    from tests.integration._journey_harness import market_for_trade

    other = market_for_trade(sample_trade_event, "science")
    assert niche_market.condition_id == other.condition_id
    assert niche_market.question == other.question
    assert niche_market.description == other.description
    assert niche_market.tokens == other.tokens
    assert niche_market.category == other.category == "science"

    clock = FakeClock(POLL_START)
    row = pipeline_row(sample_trade_event, clock)
    assert row["proxyWallet"] == sample_trade_event.wallet_address
    assert row["timestamp"] == int(clock.now) + 1
    assert row["conditionId"] == sample_trade_event.market_id
    assert row["asset"] == sample_trade_event.asset_id

    await run_until(lambda: True)
    with pytest.raises(AssertionError):
        await run_until(lambda: False, timeout=0.01)

    assert await persisted_assessments(async_engine) == []
