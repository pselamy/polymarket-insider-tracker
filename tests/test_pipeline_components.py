"""``build_components`` wiring: the scorer and alert history get their configured settings."""

from collections.abc import AsyncIterator

import pytest

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.detector.models import SizeAnomalySignal
from polymarket_insider_tracker.detector.scorer import SignalBundle
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.ingestor.trade_poller import IngestionState
from polymarket_insider_tracker.pipeline_components import PipelineComponents, build_components
from tests.closeness import is_close
from tests.fakes.pipeline import make_test_settings


async def _ignore_trade(_trade: TradeEvent) -> None:
    return None


def _ignore_state(_state: IngestionState) -> None:
    return None


def _with_dedup_window(settings: Settings, seconds: int) -> Settings:
    detector = settings.detector.model_copy(update={"dedup_window_seconds": seconds})
    return settings.model_copy(update={"detector": detector})


@pytest.fixture
async def components() -> AsyncIterator[PipelineComponents]:
    """Production components over lazy connections; nothing is contacted or delivered."""
    settings = _with_dedup_window(make_test_settings(alert_threshold=0.4), 1234)
    built = await build_components(
        settings, dry_run=True, on_trade=_ignore_trade, on_ingestion_state=_ignore_state
    )
    yield built
    if built.db_manager is not None:
        await built.db_manager.dispose_async()
    await built.redis.aclose()


def test_alert_history_receives_the_configured_dedup_window(
    components: PipelineComponents,
) -> None:
    history = components.alert_dispatcher.history
    assert history is not None
    assert history.dedup_ttl == 1234


def _niche_size_signal(trade: TradeEvent) -> SizeAnomalySignal:
    metadata = MarketMetadata(
        condition_id=trade.market_id, question="Wired?", description="", tokens=()
    )
    return SizeAnomalySignal(
        trade_event=trade,
        market_metadata=metadata,
        volume_impact=0.0,
        book_impact=0.2,
        is_niche_market=True,
        confidence=0.8,
        factors={},
    )


async def test_built_scorer_assesses_with_the_configured_threshold(
    components: PipelineComponents, sample_trade: TradeEvent
) -> None:
    bundle = SignalBundle(
        trade_event=sample_trade, size_anomaly_signal=_niche_size_signal(sample_trade)
    )

    assessment = await components.risk_scorer.assess(bundle)

    assert is_close(components.risk_scorer.alert_threshold, 0.4)
    assert assessment.signals_triggered == 1
    assert assessment.should_alert is True
