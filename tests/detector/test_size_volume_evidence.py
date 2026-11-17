"""The size detector scores from the classified volume state and records it beside the signal."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from hypothesis import given, settings
from hypothesis import strategies as st

from polymarket_insider_tracker.detector.models import RiskAssessment, SizeAnomalySignal
from polymarket_insider_tracker.detector.scorer import RiskScorer, SignalBundle
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.detector.volume_state import VolumeState
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.ingestor.volume_observation import (
    GAMMA_SOURCE,
    JsonValue,
    VolumeObservation,
)
from tests.closeness import is_close
from tests.detector.test_volume_state import (
    replay_corpus,
    replay_expected,
    replay_raw,
    replay_stamp,
    replay_stored,
)
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
    if signal is None:
        raise RuntimeError("expected size signal")
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
    assert is_close(
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


REPLAY_CASES = replay_corpus()
REPLAY_EXPECTED = replay_expected()


def _obs_dict(value: JsonValue) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise TypeError("expected observation mapping")
    return value


def _build_replay_trade(case_id: str, notional: Decimal) -> TradeEvent:
    return TradeEvent(
        market_id=f"market_{case_id}",
        trade_id=f"tx_{case_id}",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("1"),
        size=notional,
        timestamp=NOW,
        asset_id="token_replay",
    )


def _build_replay_observation(observed: dict[str, JsonValue]) -> VolumeObservation | None:
    if observed.get("kind") == "unavailable":
        return None
    source_name = observed.get("source")
    return replay_stored(
        VolumeObservation(
            bool(observed.get("present", True)),
            replay_raw(observed),
            replay_stamp(observed.get("acquired_age_seconds")),
            source_name if isinstance(source_name, str) else None,
        )
    )


def _build_replay_source(spec: dict[str, JsonValue], market_id: str) -> FakeMetadataSync:
    source = FakeMetadataSync()
    if spec.get("category_source") == "minimal":
        return source
    observed = _obs_dict(spec["observation"])
    reason = observed.get("reason")
    source.markets[market_id] = MarketMetadata(
        condition_id=market_id,
        question="Replay question?",
        description="",
        tokens=(),
        category=str(spec["category"]),
        volume=_build_replay_observation(observed),
        volume_unavailable_reason=reason if isinstance(reason, str) else None,
    )
    return source


REPLAY_FACTS = (
    "volume_state",
    "volume_reason",
    "size_confidence",
    "niche_basis",
    "signals_triggered",
    "weighted_score",
)


_NON_NICHE_CSV_LABEL = "numeric-not-niche"

_REPLAY_FACT_EXPECTATIONS: dict[str, Callable[[dict[str, str]], object]] = {
    "size_confidence": lambda row: row["size_confidence"] or None,
    "signals_triggered": lambda row: int(row["groups_pre_T027"]),
    "weighted_score": lambda row: row["score_pre_T027"],
    # data-model section 1: an available, non-niche volume has basis "none" (numeric only below $50,000).
    "niche_basis": lambda row: (
        "none" if row["niche_basis"] == _NON_NICHE_CSV_LABEL else row["niche_basis"]
    ),
}


def _expected_replay_fact(row: dict[str, str], fact: str) -> object:
    """The expected value of one fact, read from the replay CSV row (the source of truth)."""
    expectation = _REPLAY_FACT_EXPECTATIONS.get(fact)
    return row[fact] if expectation is None else expectation(row)


async def _replay_observed(spec: dict[str, JsonValue]) -> dict[str, object]:
    """One evaluation of a replay case; shared corpus helpers validate loaded fixture shapes."""
    case_id = str(spec["case_id"])
    trade = _build_replay_trade(case_id, Decimal(str(spec["notional"])))
    source = _build_replay_source(spec, trade.market_id)
    explicit = spec.get("explicit_daily_volume")
    explicit_dec = Decimal(str(explicit)) if explicit else None
    size_eval = await SizeAnomalyDetector(source, clock=lambda: NOW).evaluate(
        trade, evaluated_at=NOW, daily_volume=explicit_dec
    )
    assessment = await RiskScorer(FakeAsyncRedis()).assess(
        SignalBundle(trade, None, size_eval.signal, None)
    )
    signal = size_eval.signal
    return {
        "volume_state": size_eval.volume.verdict.state.value,
        "volume_reason": size_eval.volume.verdict.reason,
        "size_confidence": None if signal is None else f"{signal.confidence:.3f}",
        "niche_basis": size_eval.volume.niche_basis,
        "signals_triggered": assessment.signals_triggered,
        "weighted_score": f"{assessment.weighted_score:.3f}",
    }


def test_the_replay_live_corpus_has_65_cases() -> None:
    """SC-001 guard: the parametrized suite is only the full 65-case proof while the corpus is 65."""
    # Arrange
    cases = REPLAY_CASES

    # Act
    count = len(cases)

    # Assert
    assert count == 65


def test_the_replay_live_expectations_have_65_rows() -> None:
    """SC-001 guard: every corpus case has exactly one recorded expectation row."""
    # Arrange
    rows = REPLAY_EXPECTED

    # Act
    count = len(rows)

    # Assert
    assert count == 65


@pytest.mark.asyncio
@pytest.mark.parametrize("fact", REPLAY_FACTS)
@pytest.mark.parametrize("spec", REPLAY_CASES, ids=[str(c["case_id"]) for c in REPLAY_CASES])
async def test_every_replay_live_case_evaluates_and_scores_through_size_detector(
    spec: dict[str, JsonValue], fact: str
) -> None:
    # Arrange
    expected = _expected_replay_fact(REPLAY_EXPECTED[str(spec["case_id"])], fact)

    # Act
    observed = await _replay_observed(spec)

    # Assert
    assert observed[fact] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("size", "expected_signal"),
    [(Decimal("500"), False), (Decimal("501"), True)],
    ids=["boundary_500_strict_false", "boundary_501_strict_true"],
)
async def test_size_volume_threshold_boundary_override(
    size: Decimal, expected_signal: bool
) -> None:
    # Arrange
    detector = _boundary_detector(size)

    # Act
    size_eval = await detector.evaluate(_boundary_trade(size), evaluated_at=NOW)

    # Assert
    assert (size_eval.signal is not None) is expected_signal


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("size", "expected_signal"),
    [(Decimal("500"), False), (Decimal("501"), True)],
    ids=["boundary_500_strict_false", "boundary_501_strict_true"],
)
async def test_size_volume_threshold_boundary_impact(size: Decimal, expected_signal: bool) -> None:
    # Arrange
    detector = _boundary_detector(size)

    # Act
    size_eval = await detector.evaluate(_boundary_trade(size), evaluated_at=NOW)
    measured = size_eval.signal.volume_impact_measured if size_eval.signal else None

    # Assert
    assert measured == (Decimal(f"{size / 10000:.4f}") if expected_signal else None)


def _boundary_trade(size: Decimal) -> TradeEvent:
    return _build_replay_trade(f"bound_{size}", size)


def _boundary_detector(size: Decimal) -> SizeAnomalyDetector:
    source = FakeMetadataSync()
    source.markets[_boundary_trade(size).market_id] = MarketMetadata(
        condition_id=_boundary_trade(size).market_id,
        question="Politics boundary question?",
        description="",
        tokens=(),
        category="politics",
        volume=VolumeObservation(True, "10000", NOW.isoformat(), GAMMA_SOURCE),
    )
    return SizeAnomalyDetector(
        source,
        volume_threshold=0.05,
        niche_volume_threshold=Decimal("1000"),
        clock=lambda: NOW,
    )


@settings(derandomize=True, max_examples=200, deadline=None)
@given(size=st.integers(min_value=1, max_value=2000))
def test_the_volume_impact_signal_is_emitted_only_strictly_above_the_threshold(size: int) -> None:
    """Property: with volume 10,000 and a 5% threshold, a signal is emitted iff size > 500 (strict)."""
    # Arrange
    notional = Decimal(size)

    # Act
    size_eval = asyncio.run(
        _boundary_detector(notional).evaluate(_boundary_trade(notional), evaluated_at=NOW)
    )

    # Assert
    assert (size_eval.signal is not None) == (size > 500)


async def _size_only_assessment(threshold: float) -> RiskAssessment:
    source = FakeMetadataSync()
    source.markets[TRADE.market_id] = _metadata(_observation())
    size_eval = await SizeAnomalyDetector(source, clock=lambda: NOW).evaluate(
        TRADE, evaluated_at=NOW
    )
    return await RiskScorer(FakeAsyncRedis(), alert_threshold=threshold).assess(
        SignalBundle(TRADE, None, size_eval.signal, None)
    )


@pytest.mark.asyncio
async def test_the_size_only_assessment_scores_0_45() -> None:
    # Arrange
    assessment = await _size_only_assessment(0.450)

    # Act
    score = assessment.weighted_score

    # Assert
    assert score == 0.45


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("threshold", "expected_alert"),
    [(0.450, True), (0.451, False)],
    ids=["threshold_450_inclusive_true", "threshold_451_false"],
)
async def test_risk_scorer_alert_threshold_boundary_override(
    threshold: float, expected_alert: bool
) -> None:
    # Arrange
    target_threshold = threshold

    # Act
    assessment = await _size_only_assessment(target_threshold)

    # Assert
    assert assessment.should_alert is expected_alert


@settings(derandomize=True, max_examples=200, deadline=None)
@given(thousandths=st.integers(min_value=400, max_value=500))
def test_the_alert_threshold_is_inclusive_at_every_thousandth_near_the_score(
    thousandths: int,
) -> None:
    """Property: a 0.450 score alerts iff the threshold is at most 0.450 (inclusive)."""
    # Arrange
    threshold = thousandths / 1000

    # Act
    assessment = asyncio.run(_size_only_assessment(threshold))

    # Assert
    assert assessment.should_alert is (thousandths <= 450)
