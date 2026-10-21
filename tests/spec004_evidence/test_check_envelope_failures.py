"""Characterise how ``check_envelope`` reports malformed envelopes (escape sweep step 4).

The evidence tools must keep reporting exactly the same messages and raising the same exception
classes for malformed input while their typing escapes are replaced by checked narrowing.
"""

from __future__ import annotations

import copy
from decimal import InvalidOperation

import pytest
from scripts.spec004_evidence import check_envelope as ce
from scripts.spec004_evidence.json_shapes import as_object, at
from scripts.spec004_evidence.replay_envelopes import PRE_T027

EXAMPLE = "scoring-config-004.1.example.json"


def example() -> dict[str, object]:
    return copy.deepcopy(ce.load(EXAMPLE))


def issues(doc: object) -> list[str]:
    schema = ce.schema_for(PRE_T027)
    return ce.validate(doc, schema, schema)


def test_the_committed_checker_passes(capsys: pytest.CaptureFixture[str]) -> None:
    assert ce.main() == 0
    assert capsys.readouterr().out == "ok\n"


@pytest.mark.parametrize(
    ("schema", "value", "expected"),
    [
        ({"type": "string"}, 1, ["$: type int not in ['string']"]),
        ({"type": ["string", "null"]}, 1.5, ["$: type float not in ['string', 'null']"]),
        ({"type": "integer"}, True, ["$: type bool not in ['integer']"]),
        ({"const": "a"}, "b", ["$: expected 'a', got 'b'"]),
        ({"enum": ["a", "b"]}, "c", ["$: 'c' not in enum"]),
        ({"type": "string", "pattern": "^a$"}, "b", ["$: 'b' fails pattern"]),
        ({"type": "string", "maxLength": 1}, "ab", ["$: longer than 1"]),
        ({"type": "integer", "minimum": 2}, 1, ["$: below minimum"]),
        ({"type": "integer", "maximum": 2}, 3, ["$: above maximum"]),
        ({"required": ["k"]}, {}, ["$: missing 'k'"]),
        (
            {"properties": {}, "additionalProperties": False},
            {"x": 1},
            ["$: unexpected key 'x'"],
        ),
        (
            {"additionalProperties": {"type": "string"}},
            {"x": 1},
            ["$.x: type int not in ['string']"],
        ),
        ({"items": {"type": "string"}}, ["a", 2], ["$[1]: type int not in ['string']"]),
        ({"oneOf": [{"type": "string"}, {"type": "string"}]}, "a", ["$: matches 2 oneOf branches"]),
    ],
)
def test_validate_reports_each_schema_violation_verbatim(
    schema: dict[str, object], value: object, expected: list[str]
) -> None:
    assert ce.validate(value, schema, {}) == expected


def test_validate_follows_a_ref_into_defs() -> None:
    root: dict[str, object] = {"$defs": {"n": {"type": "string"}}}
    assert ce.validate(1, {"$ref": "#/$defs/n"}, root) == ["$: type int not in ['string']"]


def test_validate_reports_a_dict_with_non_string_keys_as_unexpected_keys() -> None:
    assert ce.validate({1: "a"}, {"additionalProperties": False}, {}) == ["$: unexpected key 1"]


def test_a_missing_ref_target_is_a_key_error() -> None:
    with pytest.raises(KeyError):
        ce.validate(1, {"$ref": "#/$defs/none"}, {"$defs": {}})


def test_a_malformed_schema_node_is_a_type_error() -> None:
    with pytest.raises(TypeError):
        ce.validate(1, {"oneOf": ["not a schema"]}, {})


def test_the_committed_example_is_schema_valid_and_reconstructs() -> None:
    assert issues(example()) == []
    assert ce.reconstruct(example()) == []


@pytest.mark.parametrize(
    ("edit", "expected"),
    [
        ({"contributions": {"capped_score": "0.1"}}, "capped_score 0.1 != recomputed"),
        ({"result": {"weighted_score": "0.5"}}, "weighted_score 0.5 != recomputed"),
        ({"result": {"should_alert": True}}, "should_alert disagrees"),
        ({"result": {"signals_triggered": 2}}, "signals_triggered != scoring groups (1)"),
        ({"contributions": {"bonus": "1.2"}}, "bonus must be 1.2 exactly when two groups score"),
        ({"size": {"daily_volume_age_seconds": None}}, "daily_volume_acquired_at and"),
        ({"size": {"volume_impact": "9.0000"}}, "volume_impact 9.0000 != notional/daily_volume"),
    ],
)
def test_reconstruction_reports_each_cross_field_violation(
    edit: dict[str, object], expected: str
) -> None:
    doc = example()
    evidence = at(doc, "evaluation_evidence")
    for section, values in edit.items():
        at(evidence, section).update(as_object(values))
    messages = ce.reconstruct(doc)
    assert any(message.startswith(expected.split(" != ")[0]) for message in messages), messages


def test_a_null_evaluation_evidence_has_nothing_to_reconstruct() -> None:
    assert ce.reconstruct({"evaluation_evidence": None}) == []


def test_missing_evidence_keys_are_key_errors() -> None:
    doc = example()
    at(doc, "evaluation_evidence").pop("contributions")
    with pytest.raises(KeyError):
        ce.reconstruct(doc)


@pytest.mark.parametrize("shape", ["text", ["a"], 7])
def test_evidence_that_is_not_an_object_is_a_type_error(shape: object) -> None:
    with pytest.raises(TypeError):
        ce.reconstruct({"weights": {}, "evaluation_evidence": shape})


def test_a_non_numeric_amount_is_a_decimal_error() -> None:
    doc = example()
    at(doc, "evaluation_evidence", "contributions").update(bonus="x")
    with pytest.raises(InvalidOperation):
        ce.reconstruct(doc)


def test_a_null_amount_is_a_type_error() -> None:
    doc = example()
    at(doc, "evaluation_evidence", "contributions").update(bonus=None)
    with pytest.raises(TypeError):
        ce.reconstruct(doc)


def test_a_non_string_timestamp_is_a_type_error() -> None:
    doc = example()
    at(doc, "evaluation_evidence").update(evaluated_at=5)
    at(doc, "evaluation_evidence", "size").update(
        daily_volume_acquired_at="2026-10-19T12:00:00+00:00", daily_volume_age_seconds="0"
    )
    with pytest.raises(TypeError):
        ce.reconstruct(doc)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (float("nan"), "raw: non-finite number stored verbatim (must be a $nonfinite tag)"),
        ([float("inf")], "raw: non-finite number stored verbatim (must be a $nonfinite tag)"),
        ("x" * 300, "raw: verbatim value over 256 bytes (must be a marker)"),
    ],
)
def test_a_verbatim_raw_value_is_checked(raw: object, expected: str) -> None:
    assert expected in ce.stored_raw_errors(raw)


def test_every_pinned_mutation_is_rejected() -> None:
    assert ce.raw_mutation_failures() == []
    assert ce.gated_mutation_failures() == []
    assert ce.future_age_mutation_failures() == []
    assert ce.posture_mutation_failures() == []
    assert ce.mutation_failures(ce.load(ce.SCHEMA_FILE)) == []
