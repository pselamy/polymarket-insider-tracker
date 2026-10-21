"""The checked narrowing helpers reject wrong shapes with the exception class the tools had."""

from __future__ import annotations

from decimal import Decimal

import pytest
from scripts.spec004_evidence import replay_envelopes as re_
from scripts.spec004_evidence import replay_live as rl
from scripts.spec004_evidence.json_shapes import (
    as_array,
    as_decimal,
    as_int,
    as_mapping,
    as_number,
    as_object,
    as_text,
    at,
    is_object,
    matches_type,
)


@pytest.mark.parametrize("bad", [None, "x", ["a"], 1])
def test_a_non_object_is_a_type_error(bad: object) -> None:
    for narrow in (as_object, as_mapping):
        with pytest.raises(TypeError):
            narrow(bad)


def test_an_object_must_have_string_keys_but_a_mapping_need_not() -> None:
    assert not is_object({1: "a"})
    assert as_mapping({1: "a"}) == {1: "a"}


def test_narrowing_returns_the_same_values() -> None:
    assert as_object({"a": 1}) == {"a": 1}
    assert as_array([1, "b"]) == [1, "b"]
    assert as_text("t") == "t"
    assert as_number(2.5) == 2.5
    assert as_int("7") == 7
    assert as_decimal("0.10") == Decimal("0.1")
    assert at({"a": {"b": {"c": 1}}}, "a", "b") == {"c": 1}


@pytest.mark.parametrize(
    "narrow", [as_array, as_text, as_number, as_int, as_decimal], ids=lambda f: f.__name__
)
@pytest.mark.parametrize("bad", [None, {"k": 1}])
def test_a_wrong_scalar_shape_is_a_type_error(narrow: object, bad: object) -> None:
    assert callable(narrow)
    with pytest.raises(TypeError):
        narrow(bad)


def test_a_missing_path_is_a_key_error_and_a_non_object_step_a_type_error() -> None:
    with pytest.raises(KeyError):
        at({"a": {}}, "a", "b")
    with pytest.raises(TypeError):
        at({"a": 1}, "a")


@pytest.mark.parametrize("bad", [[1], (1, 2, 3)])
def test_a_sequence_amount_is_a_value_error_like_decimal(bad: object) -> None:
    with pytest.raises(ValueError, match="to Decimal"):
        as_decimal(bad)


def test_a_boolean_is_not_an_integer_for_schema_types() -> None:
    assert not matches_type(True, "integer")
    assert matches_type(1, "integer")
    assert matches_type(None, "null")


def test_a_raw_spec_with_a_non_string_key_is_an_unknown_builder() -> None:
    with pytest.raises(KeyError):
        rl.decoded_raw({"raw_spec": {1: "3"}, "raw": "5"})
    assert rl.decoded_raw({"raw_spec": "text", "raw": "5"}) == "5"
    assert rl.decoded_raw({"raw_spec": {"float": "2.5"}, "raw": "5"}) == 2.5


def test_merging_a_non_object_observation_is_a_type_error_not_an_attribute_error() -> None:
    defaults: dict[str, object] = {"observation": {"kind": "observed"}}
    with pytest.raises(TypeError):
        rl.merged(defaults, {"observation": "text"})
    with pytest.raises(TypeError):
        rl.merged({"observation": 1}, {"observation": {}})


def test_children_walks_lists_and_objects_only() -> None:
    assert re_.children([1, 2]) == [1, 2]
    assert re_.children({"a": 1}) == [1]
    assert re_.children("text") == []
