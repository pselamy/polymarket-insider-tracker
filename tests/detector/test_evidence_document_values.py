"""Exact values of the 004.1 evaluation evidence composed for a stored assessment row."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, localcontext

from polymarket_insider_tracker.detector.detector_config import default_detector_config
from polymarket_insider_tracker.detector.evaluation import (
    SizeEvaluation,
    TradeEvidence,
    WalletClassification,
    WalletEvaluation,
)
from polymarket_insider_tracker.detector.evidence_document import (
    compose_document,
    evaluation_evidence,
)
from polymarket_insider_tracker.detector.models import RiskAssessment, SizeAnomalySignal
from polymarket_insider_tracker.detector.scoring_config import (
    DetectorFailure,
    JsonValue,
    PureConfig,
    WalletGroupStatus,
    serialize_pure_config,
)
from polymarket_insider_tracker.detector.volume_state import (
    SizeVolumeEvidence,
    VolumeState,
    VolumeVerdict,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token
from polymarket_insider_tracker.profiler.models import WalletProfile
from tests.detector.test_fresh_wallet import create_trade_event

EVALUATED_AT = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)
TRADE = create_trade_event(size=Decimal("200"), price=Decimal("0.5"))
METADATA = MarketMetadata("m", "Q?", "", (Token("t", "Yes", TRADE.price),), category="science")
WEIGHTS = {
    "fresh_wallet": Decimal("0.40"),
    "size_anomaly": Decimal("0.35"),
    "niche_market": Decimal("0.25"),
}


def pure(weights: Mapping[str, Decimal] = WEIGHTS) -> PureConfig:
    return PureConfig(Decimal("0.600"), Decimal("1.2"), Decimal("1.3"), Decimal("0.001"), weights)


def size(
    *,
    niche_basis: str = "none",
    confidence: float | None = 0.5,
    factors: dict[str, float] | None = None,
    daily_volume: Decimal | None = None,
    impact: Decimal | None = None,
) -> SizeEvaluation:
    verdict = VolumeVerdict(
        VolumeState.AVAILABLE if daily_volume is not None else VolumeState.MISSING,
        "reason",
        daily_volume,
        daily_volume is not None,
        None,
        None,
        None,
        None,
    )
    evidence = SizeVolumeEvidence(verdict, impact, niche_basis, "science", "metadata")
    signal = None
    if confidence is not None:
        signal = SizeAnomalySignal(
            TRADE, METADATA, 0.0, 0.0, False, confidence, factors or {}, volume_evidence=evidence
        )
    return SizeEvaluation(EVALUATED_AT, Decimal("100"), evidence, signal)


def assessment(weighted_score: float = 0.5) -> RiskAssessment:
    return RiskAssessment(
        TRADE, TRADE.wallet_address, TRADE.market_id, weighted_score=weighted_score
    )


def group(
    name: str, evidence: TradeEvidence, weights: Mapping[str, Decimal] = WEIGHTS
) -> Mapping[str, JsonValue]:
    document = evaluation_evidence(assessment(), pure(weights), evidence)
    assert document is not None
    value = document[name]
    assert isinstance(value, Mapping)
    return value


def trade_evidence(
    wallet: WalletEvaluation | None = None,
    failures: tuple[DetectorFailure, ...] = (),
    evaluation: SizeEvaluation | None = None,
) -> TradeEvidence:
    return TradeEvidence(
        EVALUATED_AT, default_detector_config(), wallet, evaluation or size(), failures
    )


def wallet_evaluation(boundary: int) -> WalletEvaluation:
    profile = WalletProfile(
        TRADE.wallet_address,
        3,
        None,
        1.5,
        True,
        3,
        Decimal(0),
        Decimal(0),
        analyzed_at=datetime(2026, 10, 21, 14, 30, tzinfo=timezone(timedelta(hours=2))),
    )
    return WalletEvaluation(EVALUATED_AT, WalletClassification.FRESH, boundary, profile, None)


def test_the_size_group_records_the_volume_niche_and_signal_values() -> None:
    evaluation = size(
        niche_basis="numeric",
        factors={"b": 0.5, "a": 0.25},
        daily_volume=Decimal("1234.50"),
        impact=Decimal("0.0125"),
    )

    recorded = group("size", trade_evidence(evaluation=evaluation))

    assert recorded == {
        "volume_state": "available",
        "volume_reason": "reason",
        "volume24hr_present": True,
        "volume24hr_raw": None,
        "daily_volume": "1234.50",
        "daily_volume_acquired_at": None,
        "daily_volume_age_seconds": None,
        "daily_volume_source": None,
        "volume_impact": "0.0125",
        "book_state": "unavailable",
        "book_impact": None,
        "niche": True,
        "niche_basis": "numeric",
        "category": "science",
        "category_source": "metadata",
        "signal_emitted": True,
        "confidence": "0.500",
        "factors": {"a": "0.25", "b": "0.5"},
    }


def test_the_size_group_without_volume_impact_or_signal_records_nulls() -> None:
    recorded = group("size", trade_evidence(evaluation=size(confidence=None)))

    assert recorded["daily_volume"] is None
    assert recorded["volume_impact"] is None
    assert recorded["niche"] is False
    assert recorded["signal_emitted"] is False
    assert recorded["confidence"] is None
    assert recorded["factors"] == {}


def test_decimals_are_written_positionally_never_in_exponent_form() -> None:
    evaluation = size(daily_volume=Decimal("1E+3"), impact=Decimal("1E-7"))

    recorded = group("size", trade_evidence(evaluation=evaluation))

    assert (recorded["daily_volume"], recorded["volume_impact"]) == ("1000", "0.0000001")


def test_a_non_finite_factor_is_written_as_infinity() -> None:
    recorded = group("size", trade_evidence(evaluation=size(factors={"x": float("inf")})))

    assert recorded["factors"] == {"x": "Infinity"}


def test_scores_round_half_even_whatever_the_ambient_decimal_rounding() -> None:
    with localcontext(rounding=ROUND_HALF_UP):
        recorded = group("size", trade_evidence(evaluation=size(confidence=0.0025)))
        document = evaluation_evidence(assessment(0.0025), pure(), trade_evidence())

    assert recorded["confidence"] == "0.002"
    assert document is not None
    assert document["result"] == {
        "weighted_score": "0.002",
        "signals_triggered": 0,
        "should_alert": False,
    }


def test_each_contribution_is_the_quantised_confidence_times_its_weight() -> None:
    evidence = trade_evidence(evaluation=size(niche_basis="numeric"))

    assert group("contributions", evidence) == {
        "fresh_wallet": "0",
        "size_anomaly": "0.17500",
        "niche_market": "0.12500",
        "bonus": "1",
        "capped_score": "0.30000",
    }


def test_the_capped_score_never_exceeds_one() -> None:
    weights = {"size_anomaly": Decimal("0.9"), "niche_market": Decimal("0.9")}
    evidence = trade_evidence(evaluation=size(niche_basis="numeric", confidence=1.0))

    assert group("contributions", evidence, weights)["capped_score"] == "1"


def test_a_market_without_niche_basis_contributes_a_plain_zero_niche_term() -> None:
    contributions = group("contributions", trade_evidence(evaluation=size(niche_basis="none")))

    assert contributions["niche_market"] == "0"
    assert contributions["size_anomaly"] == "0.17500"


def test_weights_missing_from_the_configuration_contribute_zero() -> None:
    evidence = trade_evidence(evaluation=size(niche_basis="category"))

    contributions = group("contributions", evidence, {})

    assert contributions["size_anomaly"] == "0.000"
    assert contributions["niche_market"] == "0.000"


def test_contributions_are_zero_without_a_size_signal() -> None:
    contributions = group("contributions", trade_evidence(evaluation=size(confidence=None)))

    assert contributions["size_anomaly"] == contributions["niche_market"] == "0"


def test_a_profiled_wallet_records_its_own_boundary_and_observation_time() -> None:
    recorded = group("wallet", trade_evidence(wallet=wallet_evaluation(boundary=9)))

    assert recorded == {
        "subject_kind": "proxy_wallet",
        "group_status": WalletGroupStatus.UNQUALIFIED_PRE_T027.value,
        "classification": "fresh",
        "boundary": 9,
        "retrieval_error": None,
        "confidence": None,
        "factors": {},
        "tx_count": 3,
        "observation_time": "2026-10-21T12:30:00+00:00",
        "age_state": "known",
        "age_basis": "chain_first_transaction",
        "age_hours": "1.5",
    }


def test_the_first_fresh_wallet_failure_is_the_retrieval_error() -> None:
    failures = (
        DetectorFailure("size_anomaly", "OtherError", "m"),
        DetectorFailure("fresh_wallet", "E" * 130, "m"),
        DetectorFailure("fresh_wallet", "SecondError", "m"),
    )

    recorded = group("wallet", trade_evidence(failures=failures))

    assert recorded["retrieval_error"] == "E" * 128
    assert recorded["classification"] == "retrieval_failed"
    assert recorded["boundary"] == default_detector_config().fresh_transaction_boundary


def test_other_detectors_failures_are_not_a_wallet_retrieval_error() -> None:
    failures = (DetectorFailure("size_anomaly", "OtherError", "m"),)

    recorded = group("wallet", trade_evidence(failures=failures))

    assert recorded["retrieval_error"] is None
    assert recorded["classification"] == "not_requested"


def test_composing_a_document_with_observations_marks_the_wallet_group_unqualified() -> None:
    text = serialize_pure_config(pure())

    document = compose_document(assessment(), text, trade_evidence())

    assert document.wallet_group_status is WalletGroupStatus.UNQUALIFIED_PRE_T027
    assert document.evaluation_evidence is not None


def test_composing_a_document_without_a_size_evaluation_has_no_observations() -> None:
    text = serialize_pure_config(pure())
    evidence = TradeEvidence(EVALUATED_AT, default_detector_config())

    document = compose_document(assessment(), text, evidence)

    assert document.wallet_group_status is None
    assert document.evaluation_evidence is None
