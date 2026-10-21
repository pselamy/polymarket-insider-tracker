"""Versioned ``scoring_config`` documents and the version-dispatched reader.

Contract: ``specs/004-truthful-detection-contract/contracts/scoring-config.md`` (sections 2,
3 and 4). The row's ``scoring_algorithm_version`` is the authoritative discriminator; a
mismatch, a schema violation or an unknown version is surfaced, never coerced.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import NoReturn, TypeAlias

from polymarket_insider_tracker.detector.evaluation_clock import parse_stored_instant

JsonValue: TypeAlias = "None | bool | int | str | list[JsonValue] | dict[str, JsonValue]"

LEGACY_VERSION = "legacy-unversioned"
VERSION_003 = "003.1"
VERSION_004_1 = "004.1"

_PURE_KEYS = frozenset(
    {"alert_threshold", "multi_signal_bonus_2", "multi_signal_bonus_3", "score_quantum", "weights"}
)
_DOCUMENT_KEYS = _PURE_KEYS | {
    "evidence_schema",
    "detector_config",
    "detector_failures",
    "evaluation_evidence",
}
_DETECTOR_CONFIG_KEYS = frozenset(
    {
        "fresh_transaction_boundary",
        "size_volume_threshold",
        "fresh_min_notional",
        "known_age_ceiling_hours",
        "niche_volume_threshold",
        "niche_min_notional",
        "volume_recency_seconds",
    }
)
_FAILURE_KEYS = frozenset({"detector", "error_class", "message"})
_FAILURE_DETECTORS = frozenset({"fresh_wallet", "size_anomaly"})
_PLAIN_DECIMAL = re.compile(r"(0|[1-9][0-9]*)(\.[0-9]+)?")
_THREE_DECIMALS = re.compile(r"[0-9]+\.[0-9]{3}")
_VOLUME_RECENCY_SECONDS = 600
_MAX_BOUNDARY = 2**53 - 1
_ERROR_CLASS_LIMIT = 128
_MESSAGE_LIMIT = 200
_FIXED_004_1 = {
    "multi_signal_bonus_2": "1.2",
    "multi_signal_bonus_3": "1",
    "score_quantum": "0.001",
}


class UnsupportedScoringVersion(ValueError):
    """The row's ``scoring_algorithm_version`` has no reader."""


class CorruptScoringConfig(ValueError):
    """The stored document contradicts its row version or its schema."""


class WalletGroupStatus(StrEnum):
    """Whether the wallet group may score (spec FR-007c)."""

    UNQUALIFIED_PRE_T027 = "unqualified_pre_T027"


DEFAULT_WALLET_GROUP_STATUS = WalletGroupStatus.UNQUALIFIED_PRE_T027


@dataclass(frozen=True)
class PureConfig:
    """The five-key pure configuration of a 003.1 row."""

    alert_threshold: Decimal
    multi_signal_bonus_2: Decimal
    multi_signal_bonus_3: Decimal
    score_quantum: Decimal
    weights: Mapping[str, Decimal]


@dataclass(frozen=True)
class DetectorConfig:
    """Static effective detector values captured with every 004.1 row."""

    fresh_transaction_boundary: int
    size_volume_threshold: Decimal
    fresh_min_notional: Decimal
    known_age_ceiling_hours: Decimal
    niche_volume_threshold: Decimal
    niche_min_notional: Decimal


@dataclass(frozen=True)
class DetectorFailure:
    """One redacted detector failure of a 004.1 failure row."""

    detector: str
    error_class: str
    message: str


@dataclass(frozen=True)
class Document004:
    """The 004.1 document: the pure configuration plus per-assessment evidence."""

    pure: PureConfig
    evidence_schema: str
    detector_config: DetectorConfig
    detector_failures: tuple[DetectorFailure, ...]
    evaluation_evidence: Mapping[str, JsonValue] | None
    wallet_group_status: WalletGroupStatus | None


ScoringConfig: TypeAlias = "PureConfig | Document004"


def _corrupt(what: str) -> CorruptScoringConfig:
    return CorruptScoringConfig(what)


def _reject_float(token: str) -> NoReturn:
    raise _corrupt(f"float {token} in scoring_config; decimals are strings")


def _object(value: JsonValue, where: str) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise _corrupt(f"{where} must be an object")
    return value


def _exact_keys(obj: Mapping[str, JsonValue], keys: frozenset[str], where: str) -> None:
    if frozenset(obj) != keys:
        raise _corrupt(f"{where} keys differ from the contract: {sorted(frozenset(obj) ^ keys)}")


def _text(value: JsonValue, where: str) -> str:
    if not isinstance(value, str):
        raise _corrupt(f"{where} must be a string")
    return value


def _decimal(value: JsonValue, where: str) -> Decimal:
    text = _text(value, where)
    if _PLAIN_DECIMAL.fullmatch(text) is None:
        raise _corrupt(f"{where} must be a plain-notation decimal string")
    return Decimal(text)


def _integer(value: JsonValue, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _corrupt(f"{where} must be an integer")
    return value


def _weights(value: JsonValue) -> dict[str, Decimal]:
    obj = _object(value, "weights")
    return {name: _decimal(weight, f"weights.{name}") for name, weight in sorted(obj.items())}


def _load_object(text: str) -> dict[str, JsonValue]:
    try:
        decoded: JsonValue = json.loads(text, parse_float=_reject_float)
    except json.JSONDecodeError as exc:
        raise _corrupt(f"scoring_config is not JSON: {exc.msg}") from exc
    return _object(decoded, "scoring_config")


def _pure_config(obj: Mapping[str, JsonValue]) -> PureConfig:
    return PureConfig(
        alert_threshold=_decimal(obj["alert_threshold"], "alert_threshold"),
        multi_signal_bonus_2=_decimal(obj["multi_signal_bonus_2"], "multi_signal_bonus_2"),
        multi_signal_bonus_3=_decimal(obj["multi_signal_bonus_3"], "multi_signal_bonus_3"),
        score_quantum=_decimal(obj["score_quantum"], "score_quantum"),
        weights=_weights(obj["weights"]),
    )


def _check_004_constants(obj: Mapping[str, JsonValue]) -> None:
    for key, expected in _FIXED_004_1.items():
        if obj[key] != expected:
            raise _corrupt(f"{key} must be {expected!r} in a 004.1 document")
    threshold = _text(obj["alert_threshold"], "alert_threshold")
    if _THREE_DECIMALS.fullmatch(threshold) is None:
        raise _corrupt("alert_threshold must have exactly 3 decimals")
    weights = _object(obj["weights"], "weights")
    if not {"fresh_wallet", "niche_market", "size_anomaly"} <= set(weights):
        raise _corrupt("weights must contain fresh_wallet, niche_market and size_anomaly")


def _detector_config(value: JsonValue) -> DetectorConfig:
    obj = _object(value, "detector_config")
    _exact_keys(obj, _DETECTOR_CONFIG_KEYS, "detector_config")
    boundary = _integer(obj["fresh_transaction_boundary"], "fresh_transaction_boundary")
    if not 1 <= boundary <= _MAX_BOUNDARY:
        raise _corrupt("fresh_transaction_boundary out of range")
    if obj["volume_recency_seconds"] != _VOLUME_RECENCY_SECONDS:
        raise _corrupt("volume_recency_seconds must be 600")
    return DetectorConfig(
        fresh_transaction_boundary=boundary,
        size_volume_threshold=_decimal(obj["size_volume_threshold"], "size_volume_threshold"),
        fresh_min_notional=_decimal(obj["fresh_min_notional"], "fresh_min_notional"),
        known_age_ceiling_hours=_decimal(obj["known_age_ceiling_hours"], "known_age_ceiling_hours"),
        niche_volume_threshold=_decimal(obj["niche_volume_threshold"], "niche_volume_threshold"),
        niche_min_notional=_decimal(obj["niche_min_notional"], "niche_min_notional"),
    )


def _failure(value: JsonValue) -> DetectorFailure:
    obj = _object(value, "detector_failures[]")
    _exact_keys(obj, _FAILURE_KEYS, "detector_failures[]")
    failure = DetectorFailure(
        detector=_text(obj["detector"], "detector"),
        error_class=_text(obj["error_class"], "error_class"),
        message=_text(obj["message"], "message"),
    )
    if failure.detector not in _FAILURE_DETECTORS:
        raise _corrupt(f"unknown failed detector {failure.detector!r}")
    if len(failure.error_class) > _ERROR_CLASS_LIMIT or len(failure.message) > _MESSAGE_LIMIT:
        raise _corrupt("detector failure text exceeds its bound")
    return failure


def _failures(value: JsonValue) -> tuple[DetectorFailure, ...]:
    if not isinstance(value, list):
        raise _corrupt("detector_failures must be an array")
    return tuple(_failure(item) for item in value)


def _wallet_group_status(evidence: Mapping[str, JsonValue]) -> WalletGroupStatus:
    wallet = _object(evidence.get("wallet"), "evaluation_evidence.wallet")
    status = wallet.get("group_status")
    if status != WalletGroupStatus.UNQUALIFIED_PRE_T027:
        raise _corrupt("a 004.1 wallet group_status must be unqualified_pre_T027")
    return WalletGroupStatus.UNQUALIFIED_PRE_T027


def _check_evaluated_at(evidence: Mapping[str, JsonValue]) -> None:
    stamp = _text(evidence.get("evaluated_at"), "evaluated_at")
    try:
        parse_stored_instant(stamp)
    except ValueError as exc:
        raise _corrupt("evaluated_at must be ISO-8601 with an explicit +00:00 offset") from exc


def _evidence_status(
    value: JsonValue,
) -> tuple[Mapping[str, JsonValue] | None, WalletGroupStatus | None]:
    if value is None:
        return None, None
    evidence = _object(value, "evaluation_evidence")
    _check_evaluated_at(evidence)
    return evidence, _wallet_group_status(evidence)


def _read_pure(text: str) -> PureConfig:
    obj = _load_object(text)
    _exact_keys(obj, _PURE_KEYS, "003.1 scoring_config")
    return _pure_config(obj)


def _read_004_1(text: str) -> Document004:
    obj = _load_object(text)
    _exact_keys(obj, _DOCUMENT_KEYS, "004.1 scoring_config")
    if obj["evidence_schema"] != VERSION_004_1:
        raise _corrupt("evidence_schema does not equal the row's scoring_algorithm_version")
    _check_004_constants(obj)
    evidence, status = _evidence_status(obj["evaluation_evidence"])
    return Document004(
        pure=_pure_config(obj),
        evidence_schema=VERSION_004_1,
        detector_config=_detector_config(obj["detector_config"]),
        detector_failures=_failures(obj["detector_failures"]),
        evaluation_evidence=evidence,
        wallet_group_status=status,
    )


_READERS: dict[str, Callable[[str], ScoringConfig]] = {
    VERSION_003: _read_pure,
    VERSION_004_1: _read_004_1,
}


def read_scoring_config(version: str, text: str | None) -> ScoringConfig | None:
    """Read a row's ``scoring_config`` according to its ``scoring_algorithm_version``."""
    if version == LEGACY_VERSION:
        if text is not None:
            raise _corrupt("a legacy-unversioned row carries no scoring_config")
        return None
    reader = _READERS.get(version)
    if reader is None:
        raise UnsupportedScoringVersion(version)
    if text is None:
        raise _corrupt(f"a {version} row requires a scoring_config")
    return reader(text)


def _canonical(payload: Mapping[str, JsonValue]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _pure_payload(pure: PureConfig) -> dict[str, JsonValue]:
    return {
        "alert_threshold": str(pure.alert_threshold),
        "multi_signal_bonus_2": str(pure.multi_signal_bonus_2),
        "multi_signal_bonus_3": str(pure.multi_signal_bonus_3),
        "score_quantum": str(pure.score_quantum),
        "weights": {name: str(weight) for name, weight in pure.weights.items()},
    }


def serialize_pure_config(pure: PureConfig) -> str:
    """Canonical JSON of a 003.1 pure configuration (the scorer's byte layout)."""
    return _canonical(_pure_payload(pure))


def _detector_config_payload(config: DetectorConfig) -> dict[str, JsonValue]:
    return {
        "fresh_transaction_boundary": config.fresh_transaction_boundary,
        "size_volume_threshold": str(config.size_volume_threshold),
        "fresh_min_notional": str(config.fresh_min_notional),
        "known_age_ceiling_hours": str(config.known_age_ceiling_hours),
        "niche_volume_threshold": str(config.niche_volume_threshold),
        "niche_min_notional": str(config.niche_min_notional),
        "volume_recency_seconds": _VOLUME_RECENCY_SECONDS,
    }


def serialize_document(document: Document004) -> str:
    """Canonical JSON of a 004.1 document (scoring-config.md section 2)."""
    failures: list[JsonValue] = [
        {"detector": f.detector, "error_class": f.error_class, "message": f.message}
        for f in document.detector_failures
    ]
    payload = _pure_payload(document.pure)
    payload["evidence_schema"] = document.evidence_schema
    payload["detector_config"] = _detector_config_payload(document.detector_config)
    payload["detector_failures"] = failures
    payload["evaluation_evidence"] = (
        None if document.evaluation_evidence is None else dict(document.evaluation_evidence)
    )
    return _canonical(payload)
