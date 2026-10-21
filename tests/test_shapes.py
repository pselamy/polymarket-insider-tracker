"""Narrowing of untrusted decoded values to typed containers."""

from __future__ import annotations

import pytest

from polymarket_insider_tracker.shapes import (
    object_list,
    object_mapping,
    object_sequence,
    string_keyed,
)


def test_object_list_copies_a_list_and_rejects_everything_else() -> None:
    source: list[object] = [1, "a"]

    copied = object_list(source)

    assert copied == source
    assert copied is not source
    assert object_list((1, 2)) is None
    assert object_list({"a": 1}) is None
    assert object_list("ab") is None
    assert object_list(None) is None


def test_object_sequence_accepts_lists_and_tuples_only() -> None:
    assert object_sequence([1, 2]) == [1, 2]
    assert object_sequence((1, 2)) == [1, 2]
    assert object_sequence({1, 2}) is None
    assert object_sequence(b"ab") is None
    assert object_sequence({"a": 1}) is None


def test_object_mapping_copies_any_dict() -> None:
    source: dict[object, object] = {1: "a", "b": 2}

    copied = object_mapping(source)

    assert copied == source
    assert copied is not source
    assert object_mapping([("a", 1)]) is None
    assert object_mapping(None) is None


def test_string_keyed_copies_a_dict_of_string_keys() -> None:
    assert string_keyed({"a": 1, "b": [2]}) == {"a": 1, "b": [2]}
    assert string_keyed({}) == {}


@pytest.mark.parametrize("value", [{1: "a"}, {"a": 1, 2: 2}, [("a", 1)], "a", None, 3])
def test_string_keyed_rejects_non_dicts_and_non_string_keys(value: object) -> None:
    assert string_keyed(value) is None
