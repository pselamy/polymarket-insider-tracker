"""The deterministic dry-run journey through the DEFAULT trades URL (approved v2 amendment, item 1).

The same dry-run contract as the other journeys, but with ``POLYMARKET_TRADES_URL`` left unset so
the approved default — the official Data API v2 endpoint — is what the poller actually runs,
through the real acquisition wiring (``ingestor.acquisition``). It proves the default path end to
end at the dry-run boundary: exactly one stored explainable assessment, zero channel deliveries,
and zero successful-delivery dedup keys. No guard, no skip: the wiring is merged. The shared
scaffolding lives in ``_journey_harness``; this file carries only the default-URL configuration
and the assessment-quality assertions.
"""

from __future__ import annotations

import pytest
from tests.fakes import FakeClock, make_test_settings
from tests.integration._journey_harness import (
    POLL_START,
    JourneyEnvironment,
    run_dry_run_journey,
    seeded_v2_server,
)
from tests.integration.test_scoring_config_rows import contract_errors

from polymarket_insider_tracker.detector.scoring_config import Document004, read_scoring_config
from polymarket_insider_tracker.endpoints import OFFICIAL_V2_TRADES_URL
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent


@pytest.mark.asyncio
async def test_end_to_end_dry_run_pipeline_through_the_default_url(
    journey_environment: JourneyEnvironment,
    sample_trade_event: TradeEvent,
    niche_market: MarketMetadata,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dry run with POLYMARKET_TRADES_URL unset: one assessment, zero deliveries, zero dedup keys."""
    monkeypatch.delenv("POLYMARKET_TRADES_URL", raising=False)
    clock = FakeClock(POLL_START)
    settings = make_test_settings(
        dry_run=True,
        alert_threshold=0.4,
        health_port=journey_environment.health_port,
        trades_url=None,
    )
    assert settings.polymarket.trades_url == OFFICIAL_V2_TRADES_URL
    assessment = await run_dry_run_journey(
        journey_environment,
        settings,
        seeded_v2_server(clock),
        clock,
        sample_trade_event,
        niche_market,
    )
    # The stored row must remain explainable: the exact algorithm and configuration that
    # produced it are recorded on the assessment itself.
    assert assessment.signals_triggered >= 1
    assert assessment.scoring_algorithm_version == "004.1"
    document = read_scoring_config("004.1", assessment.scoring_config)
    assert isinstance(document, Document004)
    assert document.evaluation_evidence is not None
    # T024: the stored envelope also parses under the contract schema and reconstruction rules.
    assert contract_errors(assessment.scoring_config) == []
