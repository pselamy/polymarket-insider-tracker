"""Edge behaviour of the size-anomaly detector: the confidence floor and unscored niche input."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from tests.fakes.metadata import FakeMetadataSync


class _TimidDetector(SizeAnomalyDetector):
    """A detector whose scoring finds only a faint signal."""

    def calculate_confidence(
        self, *, volume_impact: float, book_impact: float, is_niche: bool
    ) -> tuple[float, dict[str, float]]:
        return 0.05, {"volume": volume_impact, "book": book_impact, "niche": float(is_niche)}


def _trade() -> TradeEvent:
    return TradeEvent(
        market_id="market_edge",
        trade_id="tx_edge",
        wallet_address="0xedge",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("0.50"),
        size=Decimal("100000"),
        timestamp=datetime.now(UTC),
        asset_id="token_edge",
    )


@pytest.mark.asyncio
async def test_a_trade_over_the_thresholds_is_dropped_when_its_confidence_is_below_the_floor() -> (
    None
):
    metadata = FakeMetadataSync()
    metadata.fallback = MarketMetadata(
        condition_id="market_edge",
        question="Edge?",
        description="",
        tokens=(Token(token_id="token_edge", outcome="Yes", price=Decimal("0.5")),),
        category="sports",
    )
    detector = _TimidDetector(metadata)

    signal = await detector.analyze(
        _trade(), daily_volume=Decimal("1000000"), book_depth=Decimal("100000")
    )

    assert signal is None


def test_a_negative_volume_threshold_yields_no_niche_boost_and_a_floored_confidence() -> None:
    detector = SizeAnomalyDetector(FakeMetadataSync(), volume_threshold=-0.02)

    confidence, factors = detector.calculate_confidence(
        volume_impact=0.05, book_impact=0.0, is_niche=True
    )

    assert confidence == 0.0
    assert set(factors) == {"volume_impact"}
    assert factors["volume_impact"] < 0
