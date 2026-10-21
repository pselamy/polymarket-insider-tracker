"""The size detector records volume evidence beside the signal and leaves the score alone."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from polymarket_insider_tracker.detector.models import SizeAnomalySignal
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.detector.volume_state import VolumeState
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.ingestor.volume_observation import GAMMA_SOURCE, VolumeObservation
from tests.fakes.metadata import FakeMetadataSync

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
TRADE = TradeEvent(
    market_id="market_abc123",
    trade_id="tx_001",
    wallet_address="0x1234567890abcdef",
    side="BUY",
    outcome="Yes",
    outcome_index=0,
    price=Decimal("0.65"),
    size=Decimal("5000"),  # $3,250 notional
    timestamp=NOW,
    asset_id="token_123",
)


def _observation(raw: str | None = "10000", age: int = 30) -> VolumeObservation:
    stamp = (NOW - timedelta(seconds=age)).isoformat()
    return VolumeObservation(True, raw, stamp, GAMMA_SOURCE)


def _metadata(
    volume: VolumeObservation | None = None, reason: str | None = None, category: str = "science"
) -> MarketMetadata:
    return MarketMetadata(
        condition_id=TRADE.market_id,
        question="Will it rain tomorrow?",
        description="",
        tokens=(Token("token_123", "Yes", Decimal("0.65")),),
        category=category,
        volume=volume,
        volume_unavailable_reason=reason,
    )


class CountingClock:
    def __init__(self) -> None:
        self.reads = 0

    def __call__(self) -> datetime:
        self.reads += 1
        return NOW


async def _signal(
    metadata: MarketMetadata | None,
    *,
    daily_volume: Decimal | None = None,
    clock: CountingClock | None = None,
) -> SizeAnomalySignal:
    source = FakeMetadataSync()
    if metadata is not None:
        source.markets[TRADE.market_id] = metadata
    detector = SizeAnomalyDetector(source, clock=clock or CountingClock())
    signal = await detector.analyze(TRADE, daily_volume=daily_volume)
    assert signal is not None
    return signal


@pytest.mark.asyncio
async def test_stamped_volume_is_recorded_with_the_measured_impact() -> None:
    signal = await _signal(_metadata(_observation()))

    evidence = signal.volume_evidence
    assert evidence is not None
    assert evidence.verdict.state is VolumeState.AVAILABLE
    assert evidence.verdict.age_seconds == "30"
    assert evidence.volume_impact_measured == Decimal("0.3250")
    assert (evidence.niche_basis, evidence.category_source) == ("numeric", "metadata")


@pytest.mark.asyncio
async def test_scoring_inputs_are_identical_with_and_without_the_volume_evidence() -> None:
    with_volume = await _signal(_metadata(_observation()))
    without = await _signal(_metadata())

    assert (with_volume.confidence, with_volume.factors) == (without.confidence, without.factors)
    assert (with_volume.volume_impact, with_volume.is_niche_market) == (0.0, True)
    assert with_volume.confidence == pytest.approx(0.2)


@pytest.mark.asyncio
async def test_no_volume_record_names_the_reason_and_measures_nothing() -> None:
    signal = await _signal(_metadata(reason="absent_from_complete_crawl"))

    evidence = signal.volume_evidence
    assert evidence is not None
    assert (evidence.verdict.state, evidence.verdict.reason) == (
        VolumeState.UNAVAILABLE,
        "absent_from_complete_crawl",
    )
    assert evidence.volume_impact_measured is None
    assert evidence.niche_basis == "category"


@pytest.mark.asyncio
async def test_a_failed_metadata_lookup_gives_no_category_basis_but_the_same_score() -> None:
    signal = await _signal(None)

    evidence = signal.volume_evidence
    assert evidence is not None
    assert evidence.verdict.reason == "metadata_lookup_failed"
    assert (evidence.category_source, evidence.niche_basis) == ("minimal", "none")
    assert signal.confidence == pytest.approx(0.2)  # scoring still reads the placeholder


@pytest.mark.asyncio
async def test_stale_volume_is_recorded_as_stale_without_an_impact() -> None:
    signal = await _signal(_metadata(_observation(age=601)))

    evidence = signal.volume_evidence
    assert evidence is not None
    assert (evidence.verdict.state, evidence.volume_impact_measured) == (VolumeState.STALE, None)
    assert evidence.niche_basis == "none"


@pytest.mark.asyncio
async def test_explicit_daily_volume_is_recorded_as_a_caller_assertion() -> None:
    signal = await _signal(_metadata(_observation("abc")), daily_volume=Decimal("10000"))

    evidence = signal.volume_evidence
    assert evidence is not None
    assert (evidence.verdict.state, evidence.verdict.source) == (VolumeState.AVAILABLE, "caller")
    assert evidence.volume_impact_measured == Decimal("0.3250")


@pytest.mark.asyncio
async def test_the_clock_is_read_once_per_analyzed_trade() -> None:
    clock = CountingClock()

    await _signal(_metadata(_observation()), clock=clock)

    assert clock.reads == 1
