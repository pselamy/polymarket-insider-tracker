"""T011 (minimal): the detectors' ``evaluate`` results and the 004.1 document composed from them."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from scripts.spec004_evidence.check_envelope import reconstruct, schema_for, validate
from scripts.spec004_evidence.replay_envelopes import PRE_T027

from polymarket_insider_tracker.detector.detector_config import (
    capture_detector_config,
    default_detector_config,
)
from polymarket_insider_tracker.detector.evaluation import (
    DetectorError,
    TradeEvidence,
    WalletClassification,
)
from polymarket_insider_tracker.detector.evidence_document import row_scoring_config
from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.models import RiskAssessment
from polymarket_insider_tracker.detector.scorer import RiskScorer, SignalBundle
from polymarket_insider_tracker.detector.scoring_config import VERSION_003, DetectorConfig
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.ingestor.models import TradeEvent
from tests.detector.test_fresh_wallet import (
    FakeWalletAnalyzer,
    create_trade_event,
    create_wallet_profile,
)
from tests.fakes.metadata import FakeMetadataSync
from tests.integration._journey_harness import stamped_volume_market

EVALUATED_AT = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)
WALLET = "0x1234567890123456789012345678901234567890"


def wallet_detector(nonce: int = 2, age_hours: float | None = 24.0) -> FreshWalletDetector:
    analyzer = FakeWalletAnalyzer()
    analyzer.register(create_wallet_profile(address=WALLET, nonce=nonce, age_hours=age_hours))
    return FreshWalletDetector(analyzer)


def size_detector(trade: TradeEvent) -> SizeAnomalyDetector:
    metadata = FakeMetadataSync()
    metadata.fallback = stamped_volume_market(trade)
    return SizeAnomalyDetector(metadata)


@pytest.mark.parametrize(
    ("size", "nonce", "age_hours", "expected"),
    [
        (Decimal("100"), 0, None, WalletClassification.NOT_REQUESTED),
        (Decimal("2000"), 9, 24.0, WalletClassification.KNOWN_NONQUALIFYING),
        (Decimal("2000"), 2, 72.0, WalletClassification.KNOWN_NONQUALIFYING),
        (Decimal("2000"), 0, None, WalletClassification.LOW_ACTIVITY_UNKNOWN_AGE),
        (Decimal("2000"), 2, 24.0, WalletClassification.FRESH),
    ],
)
async def test_the_wallet_evaluation_classifies_signal_or_not(
    size: Decimal, nonce: int, age_hours: float | None, expected: WalletClassification
) -> None:
    detector = wallet_detector(nonce, age_hours)
    trade = create_trade_event(size=size, price=Decimal("1"))

    evaluation = await detector.evaluate(trade, evaluated_at=EVALUATED_AT)

    assert evaluation.classification is expected
    assert evaluation.boundary == detector.fresh_transaction_boundary == 6
    assert (evaluation.profile is None) is (expected is WalletClassification.NOT_REQUESTED)
    qualifies = expected in (
        WalletClassification.FRESH,
        WalletClassification.LOW_ACTIVITY_UNKNOWN_AGE,
    )
    assert (evaluation.signal is not None) is qualifies


async def test_analyze_returns_the_signal_evaluate_builds() -> None:
    detector = wallet_detector()
    trade = create_trade_event()

    signal = await detector.analyze(trade)
    evaluation = await detector.evaluate(trade, evaluated_at=EVALUATED_AT)

    assert signal is not None and evaluation.signal is not None
    assert (signal.confidence, signal.factors) == (
        evaluation.signal.confidence,
        evaluation.signal.factors,
    )


async def test_both_evaluations_normalise_the_instant_and_reject_a_naive_one() -> None:
    trade = create_trade_event()
    offset = EVALUATED_AT.astimezone(timezone(timedelta(hours=2)))
    wallet = await wallet_detector().evaluate(trade, evaluated_at=offset)
    size = await size_detector(trade).evaluate(trade, evaluated_at=offset)
    assert wallet.evaluated_at == size.evaluated_at == EVALUATED_AT
    assert wallet.evaluated_at.tzinfo is UTC

    naive = EVALUATED_AT.replace(tzinfo=None)
    with pytest.raises(ValueError, match="timezone-aware"):
        await wallet_detector().evaluate(trade, evaluated_at=naive)
    with pytest.raises(ValueError, match="timezone-aware"):
        await size_detector(trade).evaluate(trade, evaluated_at=naive)


async def test_the_size_evaluation_keeps_the_volume_evidence_without_a_signal() -> None:
    trade = create_trade_event(size=Decimal("10"), price=Decimal("1"))

    evaluation = await SizeAnomalyDetector(FakeMetadataSync()).evaluate(
        trade, evaluated_at=EVALUATED_AT
    )

    assert evaluation.signal is None
    assert evaluation.notional == Decimal("10")
    assert (evaluation.volume.verdict.state, evaluation.volume.verdict.reason) == (
        "unavailable",
        "metadata_lookup_failed",
    )


def test_the_captured_detector_config_is_read_from_the_running_detectors() -> None:
    trade = create_trade_event()
    defaults = capture_detector_config(wallet_detector(), size_detector(trade))
    assert defaults == default_detector_config()

    custom = capture_detector_config(
        FreshWalletDetector(FakeWalletAnalyzer(), max_nonce=9, min_trade_size=Decimal("750")),
        SizeAnomalyDetector(FakeMetadataSync(), volume_threshold=0.05),
    )
    assert (custom.fresh_transaction_boundary, custom.fresh_min_notional) == (10, Decimal("750"))
    assert custom.size_volume_threshold == Decimal("0.05")


def test_a_recorded_detector_failure_is_bounded() -> None:
    recorded = DetectorError("size_anomaly", "E" * 300, "m" * 300).recorded()
    assert (len(recorded.error_class), len(recorded.message)) == (128, 200)


async def scored(nonce: int, age_hours: float | None) -> RiskAssessment:
    trade = create_trade_event(size=Decimal("3250"), price=Decimal("1"))
    wallet = await wallet_detector(nonce, age_hours).evaluate(trade, evaluated_at=EVALUATED_AT)
    size = await size_detector(trade).evaluate(trade, evaluated_at=EVALUATED_AT)
    evidence = TradeEvidence(EVALUATED_AT, default_detector_config(), wallet, size)
    bundle = SignalBundle(trade, wallet.signal, size.signal, evidence)
    return await RiskScorer(FakeAsyncRedis()).assess(bundle)


def stored(assessment: RiskAssessment, config: DetectorConfig) -> dict[str, object]:
    text = row_scoring_config(assessment, config)
    assert text is not None
    document: dict[str, object] = json.loads(text)
    schema = schema_for(PRE_T027)
    assert validate(document, schema, schema) + reconstruct(document) == []
    return document


async def test_a_known_age_wallet_is_recorded_with_its_chain_basis() -> None:
    assessment = await scored(nonce=2, age_hours=1.5)

    document = stored(assessment, default_detector_config())

    evidence = document["evaluation_evidence"]
    assert isinstance(evidence, dict)
    assert evidence["wallet"]["classification"] == "fresh"
    assert (evidence["wallet"]["age_basis"], evidence["wallet"]["age_hours"]) == (
        "chain_first_transaction",
        "1.5",
    )
    assert evidence["evaluated_at"] == "2026-10-21T12:00:00+00:00"


async def test_an_assessment_without_evidence_records_the_given_config_and_no_observations() -> (
    None
):
    config = replace(default_detector_config(), fresh_transaction_boundary=3)
    assessment = replace(await scored(nonce=2, age_hours=None), evidence=None)

    document = stored(assessment, config)

    assert document["evaluation_evidence"] is None
    detector_config = document["detector_config"]
    assert isinstance(detector_config, dict)
    assert detector_config["fresh_transaction_boundary"] == 3


async def test_a_not_requested_wallet_keeps_the_captured_boundary() -> None:
    assessment = await scored(nonce=2, age_hours=None)
    assert assessment.evidence is not None
    assessment = replace(assessment, evidence=replace(assessment.evidence, wallet=None))

    document = stored(assessment, default_detector_config())

    evidence = document["evaluation_evidence"]
    assert isinstance(evidence, dict)
    assert (evidence["wallet"]["classification"], evidence["wallet"]["boundary"]) == (
        "not_requested",
        6,
    )


async def test_other_versions_and_missing_configurations_are_written_as_captured() -> None:
    assessment = await scored(nonce=2, age_hours=None)
    legacy = replace(assessment, scoring_algorithm_version=VERSION_003)
    assert row_scoring_config(legacy, default_detector_config()) == assessment.scoring_config
    bare = replace(assessment, scoring_config=None)
    assert row_scoring_config(bare, default_detector_config()) is None
