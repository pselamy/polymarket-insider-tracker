"""Compose the 004.1 ``scoring_config`` document of one assessment row (task T017).

Contract: ``specs/004-truthful-detection-contract/contracts/scoring-config.md`` sections 2 and 4.
The document is the captured pure configuration plus the captured detector configuration, the
detector failures and the evaluation evidence; it is composed at persist time from the
assessment and never rereads mutable settings. Pre-T027 (FR-007c) the wallet group is recorded
evidence only: no confidence, no factors, contribution ``0``.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal

from polymarket_insider_tracker.detector.evaluation import (
    SizeEvaluation,
    TradeEvidence,
    WalletClassification,
    WalletEvaluation,
)
from polymarket_insider_tracker.detector.models import RiskAssessment
from polymarket_insider_tracker.detector.scoring_config import (
    VERSION_004_1,
    DetectorConfig,
    Document004,
    JsonValue,
    PureConfig,
    WalletGroupStatus,
    read_pure_config,
    serialize_document,
)
from polymarket_insider_tracker.detector.volume_state import VolumeVerdict

_SCORE_QUANTUM = Decimal("0.001")
_ERROR_CLASS_LIMIT = 128


def _plain(value: Decimal) -> str:
    return format(value, "f")


def _plain_float(value: float) -> str:
    return _plain(Decimal(str(value)))


def _score3(value: float) -> str:
    return _plain(Decimal(str(value)).quantize(_SCORE_QUANTUM, rounding=ROUND_HALF_EVEN))


def _utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _retrieval_error(evidence: TradeEvidence) -> str | None:
    failed = [f.error_class for f in evidence.failures if f.detector == "fresh_wallet"]
    return failed[0][:_ERROR_CLASS_LIMIT] if failed else None


def _wallet_class(evidence: TradeEvidence) -> WalletClassification:
    if evidence.wallet is not None:
        return evidence.wallet.classification
    if _retrieval_error(evidence) is not None:
        return WalletClassification.RETRIEVAL_FAILED
    return WalletClassification.NOT_REQUESTED


def _profile_fields(wallet: WalletEvaluation | None) -> dict[str, JsonValue]:
    profile = None if wallet is None else wallet.profile
    if profile is None:
        return {"tx_count": None, "observation_time": None, **_age_fields(None)}
    fields: dict[str, JsonValue] = {"tx_count": profile.nonce, **_age_fields(profile.age_hours)}
    fields["observation_time"] = _utc(profile.analyzed_at)
    return fields


def _age_fields(age_hours: float | None) -> dict[str, JsonValue]:
    if age_hours is None:
        return {"age_state": "unknown", "age_basis": "none", "age_hours": None}
    # The profiler's only age source is the chain's first transaction.
    known: dict[str, JsonValue] = {"age_state": "known", "age_basis": "chain_first_transaction"}
    return {**known, "age_hours": _plain_float(age_hours)}


def _wallet(evidence: TradeEvidence) -> dict[str, JsonValue]:
    boundary = evidence.detector_config.fresh_transaction_boundary
    return {
        "subject_kind": "proxy_wallet",
        "group_status": WalletGroupStatus.UNQUALIFIED_PRE_T027.value,
        "classification": _wallet_class(evidence).value,
        "boundary": boundary if evidence.wallet is None else evidence.wallet.boundary,
        "retrieval_error": _retrieval_error(evidence),
        "confidence": None,
        "factors": {},
        **_profile_fields(evidence.wallet),
    }


def _volume(verdict: VolumeVerdict) -> dict[str, JsonValue]:
    volume = verdict.daily_volume
    return {
        "volume_state": verdict.state.value,
        "volume_reason": verdict.reason,
        "volume24hr_present": verdict.present,
        "volume24hr_raw": verdict.raw,
        "daily_volume": None if volume is None else _plain(volume),
        "daily_volume_acquired_at": verdict.acquired_at,
        "daily_volume_age_seconds": verdict.age_seconds,
        "daily_volume_source": verdict.source,
    }


def _size_signal(size: SizeEvaluation) -> dict[str, JsonValue]:
    signal = size.signal
    if signal is None:
        return {"signal_emitted": False, "confidence": None, "factors": {}}
    factors: dict[str, JsonValue] = {k: _plain_float(v) for k, v in sorted(signal.factors.items())}
    return {"signal_emitted": True, "confidence": _score3(signal.confidence), "factors": factors}


def _size(size: SizeEvaluation) -> dict[str, JsonValue]:
    volume = size.volume
    impact = volume.volume_impact_measured
    return {
        **_volume(volume.verdict),
        "volume_impact": None if impact is None else _plain(impact),
        "book_state": "unavailable",
        "book_impact": None,
        "niche": volume.niche_basis != "none",
        "niche_basis": volume.niche_basis,
        "category": volume.category,
        "category_source": volume.category_source,
        **_size_signal(size),
    }


def _size_terms(pure: PureConfig, size: SizeEvaluation) -> tuple[Decimal, Decimal]:
    """The size and niche terms: quantized confidence times weight (niche only when niche)."""
    if size.signal is None:
        return Decimal(0), Decimal(0)
    confidence = Decimal(_score3(size.signal.confidence))
    size_term = confidence * pure.weights.get("size_anomaly", Decimal(0))
    niche_weight = pure.weights.get("niche_market", Decimal(0))
    return size_term, confidence * niche_weight if size.volume.niche_basis != "none" else Decimal(0)


def _contributions(pure: PureConfig, size: SizeEvaluation) -> dict[str, JsonValue]:
    size_term, niche_term = _size_terms(pure, size)
    capped = min(size_term + niche_term, Decimal(1))
    return {
        "fresh_wallet": "0",
        "size_anomaly": _plain(size_term),
        "niche_market": _plain(niche_term),
        "bonus": "1",
        "capped_score": _plain(capped),
    }


def _result(assessment: RiskAssessment) -> dict[str, JsonValue]:
    return {
        "weighted_score": _score3(assessment.weighted_score),
        "signals_triggered": assessment.signals_triggered,
        "should_alert": assessment.should_alert,
    }


def evaluation_evidence(
    assessment: RiskAssessment, pure: PureConfig, evidence: TradeEvidence
) -> Mapping[str, JsonValue] | None:
    """The per-assessment observations, or None when no evaluation ran (a failure skip row)."""
    if evidence.size is None:
        return None
    return {
        "evaluated_at": _utc(evidence.evaluated_at),
        "notional": _plain(assessment.trade_event.notional_value),
        "wallet": _wallet(evidence),
        "size": _size(evidence.size),
        "contributions": _contributions(pure, evidence.size),
        "result": _result(assessment),
    }


def compose_document(
    assessment: RiskAssessment, pure_text: str, evidence: TradeEvidence
) -> Document004:
    """The 004.1 document of ``assessment`` from its captured configuration and evidence."""
    pure = read_pure_config(pure_text)
    observations = evaluation_evidence(assessment, pure, evidence)
    return Document004(
        pure=pure,
        evidence_schema=VERSION_004_1,
        detector_config=evidence.detector_config,
        detector_failures=evidence.failures,
        evaluation_evidence=observations,
        wallet_group_status=(
            None if observations is None else WalletGroupStatus.UNQUALIFIED_PRE_T027
        ),
    )


def row_scoring_config(assessment: RiskAssessment, detector_config: DetectorConfig) -> str | None:
    """The ``scoring_config`` column of ``assessment``'s row.

    A 004.1 assessment is written as its 004.1 document; one built without captured evidence
    (a library caller's own ``RiskAssessment``) records ``detector_config`` and no observations.
    Any other version, or a missing pure configuration, is written exactly as captured.
    """
    if assessment.scoring_algorithm_version != VERSION_004_1 or assessment.scoring_config is None:
        return assessment.scoring_config
    evidence = assessment.evidence or TradeEvidence(assessment.timestamp, detector_config)
    return serialize_document(compose_document(assessment, assessment.scoring_config, evidence))
