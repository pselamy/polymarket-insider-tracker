"""Exact refusal messages of the ``scoring_config`` readers, one corrupt document at a time."""

from __future__ import annotations

import copy
import json
from collections.abc import Callable

import pytest

from polymarket_insider_tracker.detector.scoring_config import (
    VERSION_003,
    VERSION_004_1,
    CorruptScoringConfig,
    JsonValue,
    UnsupportedScoringVersion,
    read_scoring_config,
)
from tests.detector.test_scoring_config import example_object, read_document

Edit = Callable[[dict[str, JsonValue]], None]
EXAMPLE = example_object("scoring-config-004.1.example.json")
FAILURE_ROW = example_object("scoring-config-004.1.failure-row.json")
PURE = json.dumps(
    {
        "alert_threshold": "0.800",
        "multi_signal_bonus_2": "1.2",
        "multi_signal_bonus_3": "1",
        "score_quantum": "0.001",
        "weights": {"fresh_wallet": "0.4", "niche_market": "0.25", "size_anomaly": "0.35"},
    }
)


def _section(document: dict[str, JsonValue], name: str) -> dict[str, JsonValue]:
    section = document[name]
    assert isinstance(section, dict)
    return section


def _refusal(version: str, text: str | None) -> str:
    with pytest.raises(CorruptScoringConfig) as caught:
        read_scoring_config(version, text)
    return str(caught.value)


def _edited(base: dict[str, JsonValue], edit: Edit) -> str:
    document = copy.deepcopy(base)
    edit(document)
    return json.dumps(document)


def _set(path: tuple[str, ...], value: JsonValue) -> Edit:
    def edit(document: dict[str, JsonValue]) -> None:
        target = document
        for step in path[:-1]:
            target = _section(target, step)
        target[path[-1]] = value

    return edit


def _drop(section: str, key: str) -> Edit:
    def edit(document: dict[str, JsonValue]) -> None:
        del _section(document, section)[key]

    return edit


def _failure_with(**changes: JsonValue) -> Edit:
    row: dict[str, JsonValue] = {"detector": "size_anomaly", "error_class": "E", "message": "m"}
    return _set(("detector_failures",), [{**row, **changes}])


DOCUMENT_CASES: list[tuple[Edit, str]] = [
    (_set(("weights",), []), "weights must be an object"),
    (_set(("weights", "fresh_wallet"), 0.4), "float in scoring_config; decimals are strings"),
    (
        _set(("weights", "fresh_wallet"), "4e-1"),
        "weights.fresh_wallet must be a plain-notation decimal string",
    ),
    (_set(("weights", "fresh_wallet"), 4), "weights.fresh_wallet must be a string"),
    (
        _drop("weights", "niche_market"),
        "weights must contain fresh_wallet, niche_market and size_anomaly",
    ),
    (_set(("alert_threshold",), "0.8"), "alert_threshold must have exactly 3 decimals"),
    (_set(("alert_threshold",), 1), "alert_threshold must be a string"),
    (
        _set(("multi_signal_bonus_2",), "1.3"),
        "multi_signal_bonus_2 must be '1.2' in a 004.1 document",
    ),
    (
        _set(("evidence_schema",), "004.2"),
        "evidence_schema does not equal the row's scoring_algorithm_version",
    ),
    (_set(("detector_config",), []), "detector_config must be an object"),
    (
        _set(("detector_config", "extra"), "x"),
        "detector_config keys differ from the contract: ['extra']",
    ),
    (
        _set(("detector_config", "fresh_transaction_boundary"), "5"),
        "fresh_transaction_boundary must be an integer",
    ),
    (
        _set(("detector_config", "fresh_transaction_boundary"), True),
        "fresh_transaction_boundary must be an integer",
    ),
    (
        _set(("detector_config", "fresh_transaction_boundary"), 0),
        "fresh_transaction_boundary out of range",
    ),
    (
        _set(("detector_config", "fresh_transaction_boundary"), 2**53),
        "fresh_transaction_boundary out of range",
    ),
    (
        _set(("detector_config", "volume_recency_seconds"), 601),
        "volume_recency_seconds must be 600",
    ),
    (
        _set(("detector_config", "size_volume_threshold"), "1e-2"),
        "size_volume_threshold must be a plain-notation decimal string",
    ),
    (
        _set(("detector_config", "fresh_min_notional"), "-1"),
        "fresh_min_notional must be a plain-notation decimal string",
    ),
    (
        _set(("detector_config", "known_age_ceiling_hours"), "x"),
        "known_age_ceiling_hours must be a plain-notation decimal string",
    ),
    (
        _set(("detector_config", "niche_volume_threshold"), "x"),
        "niche_volume_threshold must be a plain-notation decimal string",
    ),
    (
        _set(("detector_config", "niche_min_notional"), "x"),
        "niche_min_notional must be a plain-notation decimal string",
    ),
    (_set(("detector_failures",), {}), "detector_failures must be an array"),
    (_set(("detector_failures",), [1.5]), "float in scoring_config; decimals are strings"),
    (_set(("detector_failures",), [1]), "detector_failures[] must be an object"),
    (
        _set(("detector_failures",), [{"detector": "size_anomaly"}]),
        "detector_failures[] keys differ from the contract: ['error_class', 'message']",
    ),
    (_failure_with(detector=1), "detector must be a string"),
    (_failure_with(error_class=1), "error_class must be a string"),
    (_failure_with(message=1), "message must be a string"),
    (_failure_with(detector="wallet"), "unknown failed detector 'wallet'"),
    (_failure_with(error_class="E" * 129), "detector failure text exceeds its bound"),
    (_failure_with(message="m" * 201), "detector failure text exceeds its bound"),
    (_set(("evaluation_evidence",), []), "evaluation_evidence must be an object"),
    (_set(("evaluation_evidence", "wallet"), None), "evaluation_evidence.wallet must be an object"),
    (
        _set(("evaluation_evidence", "wallet", "group_status"), "qualified"),
        "a 004.1 wallet group_status must be unqualified_pre_T027",
    ),
    (_set(("evaluation_evidence", "evaluated_at"), 7), "evaluated_at must be a string"),
    (
        _set(("evaluation_evidence", "evaluated_at"), "2026-10-19T12:00:00"),
        "evaluated_at must be ISO-8601 with an explicit +00:00 offset",
    ),
    (
        _set(("evaluation_evidence", "size", "confidence"), 0.75),
        "float in scoring_config; decimals are strings",
    ),
    (
        _set(("evaluation_evidence", "size", "volume24hr_raw"), float("inf")),
        "float Infinity in scoring_config; decimals are strings",
    ),
]


@pytest.mark.parametrize(("edit", "message"), DOCUMENT_CASES)
def test_a_corrupt_004_1_document_is_refused_with_its_exact_message(
    edit: Edit, message: str
) -> None:
    assert _refusal(VERSION_004_1, _edited(EXAMPLE, edit)) == message


def test_a_non_finite_number_spelled_as_a_literal_is_refused_with_its_token() -> None:
    text = json.dumps(EXAMPLE).replace('"volume24hr_raw": "30000"', '"volume24hr_raw": 1e999')

    assert _refusal(VERSION_004_1, text) == "float 1e999 in scoring_config; decimals are strings"


def test_a_finite_verbatim_raw_volume_is_the_only_number_a_document_may_carry() -> None:
    text = _edited(EXAMPLE, _set(("evaluation_evidence", "size", "volume24hr_raw"), 30000.5))

    document = read_scoring_config(VERSION_004_1, text)

    assert document is not None


def test_a_failure_row_without_evidence_is_read() -> None:
    assert read_scoring_config(VERSION_004_1, json.dumps(FAILURE_ROW)) is not None


PURE_CASES: list[tuple[str, str]] = [
    ("not json", "scoring_config is not JSON: Expecting value"),
    ("[]", "scoring_config must be an object"),
    (
        '{"alert_threshold":"0.800"}',
        "003.1 scoring_config keys differ from the contract: "
        "['multi_signal_bonus_2', 'multi_signal_bonus_3', 'score_quantum', 'weights']",
    ),
    (
        PURE.replace('"0.800"', "0.8"),
        "float 0.8 in scoring_config; decimals are strings",
    ),
    (PURE.replace('"0.800"', "NaN"), "float NaN in scoring_config; decimals are strings"),
    (PURE.replace('"0.800"', '"x"'), "alert_threshold must be a plain-notation decimal string"),
    (
        PURE.replace('"1.2"', '"x"'),
        "multi_signal_bonus_2 must be a plain-notation decimal string",
    ),
    (
        PURE.replace('"1"', '"x"'),
        "multi_signal_bonus_3 must be a plain-notation decimal string",
    ),
    (
        PURE.replace('"0.001"', '"x"'),
        "score_quantum must be a plain-notation decimal string",
    ),
    (PURE.replace('"0.4"', '"x"'), "weights.fresh_wallet must be a plain-notation decimal string"),
]


@pytest.mark.parametrize(("text", "message"), PURE_CASES)
def test_a_corrupt_003_1_document_is_refused_with_its_exact_message(
    text: str, message: str
) -> None:
    assert _refusal(VERSION_003, text) == message


def test_a_non_object_weights_value_is_refused_in_a_pure_configuration() -> None:
    text = json.dumps({**json.loads(PURE), "weights": 3})

    assert _refusal(VERSION_003, text) == "weights must be an object"


def test_a_004_1_document_with_other_top_level_keys_is_refused() -> None:
    text = _edited(EXAMPLE, _set(("extra",), 1))

    assert _refusal(VERSION_004_1, text) == (
        "004.1 scoring_config keys differ from the contract: ['extra']"
    )


def test_dispatch_messages_name_the_row_version() -> None:
    assert (
        _refusal("legacy-unversioned", PURE) == "a legacy-unversioned row carries no scoring_config"
    )
    assert _refusal(VERSION_003, None) == "a 003.1 row requires a scoring_config"
    assert _refusal(VERSION_004_1, None) == "a 004.1 row requires a scoring_config"
    with pytest.raises(UnsupportedScoringVersion) as caught:
        read_scoring_config("9.9", PURE)
    assert str(caught.value) == "9.9"


@pytest.mark.parametrize("boundary", [1, 2**53 - 1])
def test_the_extreme_valid_transaction_boundaries_are_accepted(boundary: int) -> None:
    text = _edited(EXAMPLE, _set(("detector_config", "fresh_transaction_boundary"), boundary))

    document = read_document(text)

    assert document.detector_config.fresh_transaction_boundary == boundary


def test_failure_text_exactly_at_its_bounds_is_accepted() -> None:
    text = _edited(EXAMPLE, _failure_with(error_class="E" * 128, message="m" * 200))

    failures = read_document(text).detector_failures

    assert [(len(f.error_class), len(f.message)) for f in failures] == [(128, 200)]
