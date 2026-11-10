"""The size detector scores from the classified volume state and records it beside the signal."""

from __future__ import annotations

import csv
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.detector.evaluation import SizeEvaluation
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


EVIDENCE = (
    Path(__file__).resolve().parents[2]
    / "specs"
    / "004-truthful-detection-contract"
    / "evidence"
    / "replay-live"
)
NAMED_STAMPS: dict[str, str | None] = {
    "absent": None,
    "naive": "2026-10-19T11:59:30",
    "unparseable": "yesterday",
    "nonpositive_epoch": "1970-01-01T00:00:00+00:00",
}
RAW_BUILDERS: dict[str, Callable[[str], JsonValue]] = {
    "float": float,
    "nested_float": lambda text: [float(text)],
    "repeat_string": lambda text: "x" * int(text),
    "big_int_digits": lambda text: int("9" * int(text)),
}


def _merge_case(defaults: dict[str, JsonValue], case: dict[str, JsonValue]) -> dict[str, JsonValue]:
    merged = {**defaults, **{k: v for k, v in case.items() if k != "observation"}}
    observed = case.get("observation", {})
    assert isinstance(observed, dict)
    default_obs = defaults["observation"]
    assert isinstance(default_obs, dict)
    base = {} if observed.get("kind") == "unavailable" else default_obs
    merged["observation"] = {**base, **observed}
    return merged


def _replay_corpus() -> list[dict[str, JsonValue]]:
    data: dict[str, JsonValue] = json.loads((EVIDENCE / "cases.json").read_text())
    defaults = data["defaults"]
    assert isinstance(defaults, dict)
    raw_cases = data["cases"]
    assert isinstance(raw_cases, list)
    return [_merge_case(defaults, c) for c in raw_cases if isinstance(c, dict)]


def _replay_expected() -> dict[str, dict[str, str]]:
    with (EVIDENCE / "results.csv").open() as handle:
        return {row["case_id"]: row for row in csv.DictReader(handle)}


def _case_raw(spec: dict[str, JsonValue]) -> JsonValue:
    raw_spec = spec.get("raw_spec")
    if not isinstance(raw_spec, dict):
        return spec.get("raw")
    ((kind, argument),) = raw_spec.items()
    return RAW_BUILDERS[kind](str(argument))


def _case_stamp(age: JsonValue) -> str | None:
    if isinstance(age, str):
        return NAMED_STAMPS[age]
    assert isinstance(age, int | float) and not isinstance(age, bool)
    microseconds = int(Decimal(str(age)) * 1_000_000)
    return (NOW - timedelta(microseconds=microseconds)).isoformat()


def _case_stored(observation: VolumeObservation) -> VolumeObservation:
    restored = VolumeObservation.from_record(json.loads(json.dumps(observation.to_record())))
    assert restored is not None
    return restored


REPLAY_CASES = _replay_corpus()
REPLAY_EXPECTED = _replay_expected()


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
    return _case_stored(
        VolumeObservation(
            bool(observed.get("present", True)),
            _case_raw(observed),
            _case_stamp(observed.get("acquired_age_seconds")),
            source_name if isinstance(source_name, str) else None,
        )
    )


def _build_replay_source(spec: dict[str, JsonValue], market_id: str) -> FakeMetadataSync:
    source = FakeMetadataSync()
    if spec.get("category_source") == "minimal":
        return source
    observed = spec["observation"]
    assert isinstance(observed, dict)
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


def _assert_replay_evaluation(
    size_eval: SizeEvaluation,
    assessment: RiskAssessment,
    row: dict[str, str],
) -> None:
    evidence = size_eval.volume
    assert (
        evidence.verdict.state.value,
        evidence.verdict.reason,
    ) == (row["volume_state"], row["volume_reason"])
    if not row["size_confidence"]:
        assert size_eval.signal is None
        assert evidence.niche_basis == "none"
        assert assessment.signals_triggered == 0
        assert f"{assessment.weighted_score:.3f}" == row["score_pre_T027"]
        return
    assert size_eval.signal is not None
    assert f"{size_eval.signal.confidence:.3f}" == row["size_confidence"]
    assert assessment.signals_triggered == int(row["groups_pre_T027"])
    assert f"{assessment.weighted_score:.3f}" == row["score_pre_T027"]


def test_the_replay_live_corpus_has_all_65_cases() -> None:
    """SC-001 guard: the parametrized suite is only the full 65-case proof while the corpus is 65."""
    assert len(REPLAY_CASES) == 65
    assert len(REPLAY_EXPECTED) == 65


@pytest.mark.asyncio
@pytest.mark.parametrize("spec", REPLAY_CASES, ids=[str(c["case_id"]) for c in REPLAY_CASES])
async def test_every_replay_live_case_evaluates_and_scores_through_size_detector(
    spec: dict[str, JsonValue],
) -> None:
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
    _assert_replay_evaluation(size_eval, assessment, REPLAY_EXPECTED[case_id])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("size", "expected_signal"),
    [(Decimal("500"), False), (Decimal("501"), True)],
    ids=["boundary_500_strict_false", "boundary_501_strict_true"],
)
async def test_size_volume_threshold_boundary_override(
    size: Decimal, expected_signal: bool
) -> None:
    trade = _build_replay_trade(f"bound_{size}", size)
    source = FakeMetadataSync()
    source.markets[trade.market_id] = MarketMetadata(
        condition_id=trade.market_id,
        question="Politics boundary question?",
        description="",
        tokens=(),
        category="politics",
        volume=VolumeObservation(True, "10000", NOW.isoformat(), GAMMA_SOURCE),
    )
    detector = SizeAnomalyDetector(
        source,
        volume_threshold=0.05,
        niche_volume_threshold=Decimal("1000"),
        clock=lambda: NOW,
    )
    size_eval = await detector.evaluate(trade, evaluated_at=NOW)
    if expected_signal:
        assert size_eval.signal is not None
        assert size_eval.signal.volume_impact_measured == Decimal(f"{size / 10000:.4f}")
    else:
        assert size_eval.signal is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("threshold", "expected_alert"),
    [(0.450, True), (0.451, False)],
    ids=["threshold_450_inclusive_true", "threshold_451_false"],
)
async def test_risk_scorer_alert_threshold_boundary_override(
    threshold: float, expected_alert: bool
) -> None:
    source = FakeMetadataSync()
    source.markets[TRADE.market_id] = _metadata(_observation())
    size_eval = await SizeAnomalyDetector(source, clock=lambda: NOW).evaluate(
        TRADE, evaluated_at=NOW
    )
    assert size_eval.signal is not None

    bundle = SignalBundle(TRADE, None, size_eval.signal, None)
    assessment = await RiskScorer(FakeAsyncRedis(), alert_threshold=threshold).assess(bundle)
    assert assessment.weighted_score == 0.45
    assert assessment.should_alert is expected_alert
