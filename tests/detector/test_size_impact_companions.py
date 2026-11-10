"""Tests for SizeAnomalySignal nullable measured companions (Spec 004 T011/T016)."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from polymarket_insider_tracker.detector.models import SizeAnomalySignal
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.detector.volume_state import (
    SizeVolumeEvidence,
    VolumeState,
    VolumeVerdict,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from tests.fakes.metadata import FakeMetadataSync


@pytest.fixture
def sample_metadata() -> MarketMetadata:
    return MarketMetadata(
        condition_id="market_abc123",
        question="Will BTC reach 100k?",
        description="Bitcoin market",
        tokens=(),
        category="crypto",
    )


def _make_evidence(
    state: VolumeState,
    daily_volume: Decimal | None,
    impact: Decimal | None,
) -> SizeVolumeEvidence:
    verdict = VolumeVerdict(
        state=state,
        reason="ok" if state == VolumeState.AVAILABLE else "unmeasured",
        daily_volume=daily_volume,
        present=daily_volume is not None,
        raw=None,
        source=None,
        acquired_at=None,
        age_seconds=None,
    )
    return SizeVolumeEvidence(
        verdict=verdict,
        volume_impact_measured=impact,
        niche_basis="numeric" if state == VolumeState.AVAILABLE else "none",
        category="crypto",
        category_source="metadata",
    )


def _signal(
    trade: TradeEvent,
    metadata: MarketMetadata,
    *,
    evidence: SizeVolumeEvidence | None = None,
    volume_impact_measured: Decimal | None = None,
) -> SizeAnomalySignal:
    return SizeAnomalySignal(
        trade_event=trade,
        market_metadata=metadata,
        volume_impact=0.065 if evidence is not None else 0.0,
        book_impact=0.0,
        is_niche_market=evidence is None,
        confidence=0.8,
        factors={},
        volume_evidence=evidence,
        volume_impact_measured=volume_impact_measured,
    )


class TestSizeAnomalySignalCompanions:
    """Contract tests for volume_impact_measured and book_impact_measured."""

    def test_volume_impact_measured_resolves_from_evidence(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        # Arrange
        evidence = _make_evidence(VolumeState.AVAILABLE, Decimal("100000"), Decimal("0.0650"))
        signal = _signal(sample_trade, sample_metadata, evidence=evidence)

        # Act
        measured = signal.volume_impact_measured

        # Assert
        assert measured == Decimal("0.0650")

    def test_book_impact_measured_is_none_with_available_evidence(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        # Arrange
        evidence = _make_evidence(VolumeState.AVAILABLE, Decimal("100000"), Decimal("0.0650"))
        signal = _signal(sample_trade, sample_metadata, evidence=evidence)

        # Act
        measured = signal.book_impact_measured

        # Assert
        assert measured is None

    def test_volume_impact_measured_is_none_for_unmeasured_evidence(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        # Arrange
        evidence = _make_evidence(VolumeState.STALE, None, None)
        signal = _signal(sample_trade, sample_metadata, evidence=evidence)

        # Act
        measured = signal.volume_impact_measured

        # Assert
        assert measured is None

    def test_book_impact_measured_is_none_for_unmeasured_evidence(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        # Arrange
        evidence = _make_evidence(VolumeState.STALE, None, None)
        signal = _signal(sample_trade, sample_metadata, evidence=evidence)

        # Act
        measured = signal.book_impact_measured

        # Assert
        assert measured is None

    def test_volume_impact_measured_is_none_without_evidence(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        # Arrange
        signal = _signal(sample_trade, sample_metadata)

        # Act
        measured = signal.volume_impact_measured

        # Assert
        assert measured is None

    def test_book_impact_measured_is_none_without_evidence(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        # Arrange
        signal = _signal(sample_trade, sample_metadata)

        # Act
        measured = signal.book_impact_measured

        # Assert
        assert measured is None

    def test_explicit_volume_impact_measured_overrides_none(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        # Arrange
        signal = _signal(sample_trade, sample_metadata, volume_impact_measured=Decimal("0.1000"))

        # Act
        measured = signal.volume_impact_measured

        # Assert
        assert measured == Decimal("0.1000")

    def test_to_dict_stringifies_the_measured_volume_impact(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        # Arrange
        evidence = _make_evidence(VolumeState.AVAILABLE, Decimal("100000"), Decimal("0.0650"))
        signal = _signal(sample_trade, sample_metadata, evidence=evidence)

        # Act
        data = signal.to_dict()

        # Assert
        assert data["volume_impact_measured"] == "0.0650"

    def test_to_dict_keeps_the_book_impact_measured_none(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        # Arrange
        evidence = _make_evidence(VolumeState.AVAILABLE, Decimal("100000"), Decimal("0.0650"))
        signal = _signal(sample_trade, sample_metadata, evidence=evidence)

        # Act
        data = signal.to_dict()

        # Assert
        assert data["book_impact_measured"] is None


class TestSizeAnomalyDetectorEmitsCompanions:
    """Test SizeAnomalyDetector emits measured companions on signal."""

    @pytest.mark.asyncio
    async def test_detector_signal_carries_measured_volume_impact(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        # Arrange
        meta_sync = FakeMetadataSync()
        meta_sync.markets[sample_trade.market_id] = sample_metadata
        detector = SizeAnomalyDetector(meta_sync, volume_threshold=0.01)

        # Act
        evaluation = await detector.evaluate(
            sample_trade,
            evaluated_at=datetime.now(UTC),
            daily_volume=Decimal("100000"),
            book_depth=None,
        )

        # Assert
        assert (evaluation.signal.volume_impact_measured if evaluation.signal else None) == Decimal(
            "0.0650"
        )

    @pytest.mark.asyncio
    async def test_detector_signal_carries_no_measured_book_impact(
        self, sample_trade: TradeEvent, sample_metadata: MarketMetadata
    ) -> None:
        # Arrange
        meta_sync = FakeMetadataSync()
        meta_sync.markets[sample_trade.market_id] = sample_metadata
        detector = SizeAnomalyDetector(meta_sync, volume_threshold=0.01)

        # Act
        evaluation = await detector.evaluate(
            sample_trade,
            evaluated_at=datetime.now(UTC),
            daily_volume=Decimal("100000"),
            book_depth=None,
        )

        # Assert
        assert (evaluation.signal.book_impact_measured if evaluation.signal else "signal") is None

    @pytest.mark.asyncio
    async def test_detector_niche_signal_with_stale_volume_has_none_volume_impact(
        self, sample_trade: TradeEvent
    ) -> None:
        # Arrange: a niche category and a low threshold create a niche signal without volume.
        metadata = MarketMetadata(
            condition_id=sample_trade.market_id,
            question="Niche market?",
            description="",
            tokens=(),
            category="science",
        )
        meta_sync = FakeMetadataSync()
        meta_sync.markets[sample_trade.market_id] = metadata
        detector = SizeAnomalyDetector(meta_sync, volume_threshold=0.5)

        # Act
        evaluation = await detector.evaluate(
            sample_trade,
            evaluated_at=datetime.now(UTC),
            daily_volume=None,
            book_depth=None,
        )

        # Assert
        assert (evaluation.signal.volume_impact_measured if evaluation.signal else "signal") is None

    @pytest.mark.asyncio
    async def test_detector_niche_signal_with_stale_volume_has_none_book_impact(
        self, sample_trade: TradeEvent
    ) -> None:
        # Arrange: a niche category and a low threshold create a niche signal without volume.
        metadata = MarketMetadata(
            condition_id=sample_trade.market_id,
            question="Niche market?",
            description="",
            tokens=(),
            category="science",
        )
        meta_sync = FakeMetadataSync()
        meta_sync.markets[sample_trade.market_id] = metadata
        detector = SizeAnomalyDetector(meta_sync, volume_threshold=0.5)

        # Act
        evaluation = await detector.evaluate(
            sample_trade,
            evaluated_at=datetime.now(UTC),
            daily_volume=None,
            book_depth=None,
        )

        # Assert
        assert (evaluation.signal.book_impact_measured if evaluation.signal else "signal") is None
