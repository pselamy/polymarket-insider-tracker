"""The size detector scores from the classified volume state and records it beside the signal."""

from __future__ import annotations

import math
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


async def _detect(
    metadata: MarketMetadata | None,
    *,
    daily_volume: Decimal | None = None,
    clock: CountingClock | None = None,
) -> SizeAnomalySignal | None:
    source = FakeMetadataSync()
    if metadata is not None:
        source.markets[TRADE.market_id] = metadata
    detector = SizeAnomalyDetector(source, clock=clock or CountingClock())
    return await detector.analyze(TRADE, daily_volume=daily_volume)


async def _signal(
    metadata: MarketMetadata | None,
    *,
    daily_volume: Decimal | None = None,
    clock: CountingClock | None = None,
) -> SizeAnomalySignal:
    signal = await _detect(metadata, daily_volume=daily_volume, clock=clock)
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
async def test_an_available_volume_drives_the_score_and_no_volume_scores_the_category_base() -> (
    None
):
    with_volume = await _signal(_metadata(_observation()))
    without = await _signal(_metadata())

    assert (with_volume.volume_impact, with_volume.is_niche_market) == (0.325, True)
    assert math.isclose(
        with_volume.confidence, 0.75, rel_tol=1e-6, abs_tol=1e-12
    )  # 0.5 volume term x 1.5 niche
    assert with_volume.factors == {"volume_impact": 0.5, "niche_multiplier": 1.5}
    assert (without.volume_impact, without.is_niche_market) == (0.0, True)
    assert (without.confidence, without.factors) == (0.2, {"niche_base": 0.2})


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
async def test_a_failed_metadata_lookup_supplies_no_category_evidence_and_no_signal() -> None:
    assert await _detect(None) is None  # D3 (d): the placeholder category is not real metadata


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", ["0", "-5", "abc", "1e400"])
async def test_an_observed_but_unusable_volume_gives_no_size_signal(raw: str) -> None:
    assert await _detect(_metadata(_observation(raw))) is None


@pytest.mark.asyncio
async def test_a_stale_or_unqualified_volume_gives_no_size_signal() -> None:
    assert await _detect(_metadata(_observation(age=601))) is None
    unqualified = VolumeObservation(True, "10000", None, None)
    assert await _detect(_metadata(unqualified)) is None


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
