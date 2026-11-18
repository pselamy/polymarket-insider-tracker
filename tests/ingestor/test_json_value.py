"""Property-based and boundary contract tests for ``ingestor._json_value``."""

from __future__ import annotations

import math
from collections.abc import Sequence
from decimal import Decimal
from types import MappingProxyType
from typing import TypeGuard

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from polymarket_insider_tracker.ingestor._json_value import json_value
from polymarket_insider_tracker.ingestor.volume_observation import JsonValue

JSON_SCALARS: st.SearchStrategy[JsonValue] = (
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(max_size=12)
)

JSON_CONTAINERS: st.SearchStrategy[JsonValue] = st.recursive(
    JSON_SCALARS,
    lambda children: (
        st.lists(children, max_size=4) | st.dictionaries(st.text(max_size=8), children, max_size=4)
    ),
    max_leaves=16,
)

NON_JSON_LEAVES: st.SearchStrategy[tuple[object, str]] = st.sampled_from(
    [
        ((1, 2), "tuple"),
        ({1, 2}, "set"),
        (frozenset({1}), "frozenset"),
        (b"raw", "bytes"),
        (bytearray(b"raw"), "bytearray"),
        (Decimal("1.5"), "Decimal"),
        (object(), "object"),
        (MappingProxyType({"a": 1}), "mappingproxy"),
        ({1: "a"}, "dict"),
        ({None: "a"}, "dict"),
        ({"ok": 1, 2: "bad"}, "dict"),
    ]
)


def _is_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _is_dict(value: object) -> TypeGuard[dict[object, object]]:
    return isinstance(value, dict)


def _same_list(left: list[object], right: list[object]) -> bool:
    return len(left) == len(right) and all(
        _same_json_tree(a, b) for a, b in zip(left, right, strict=True)
    )


def _same_dict(left: dict[object, object], right: dict[object, object]) -> bool:
    return left.keys() == right.keys() and all(
        _same_json_tree(left[key], right[key]) for key in left
    )


def _same_mapping(left: object, right: object) -> bool:
    return _is_dict(left) and _is_dict(right) and _same_dict(left, right)


def _same_json_tree(left: object, right: object) -> bool:
    if type(left) is not type(right):
        return False
    if _is_list(left) and _is_list(right):
        return _same_list(left, right)
    return _same_mapping(left, right) if _is_dict(left) else left == right


def _container_children(value: object) -> tuple[object, ...] | None:
    if _is_list(value):
        return tuple(value)
    if _is_dict(value):
        return tuple(value.values())
    return None


def _collect_container_ids(value: object, seen: set[int]) -> None:
    children = _container_children(value)
    if children is None:
        return
    seen.add(id(value))
    for item in children:
        _collect_container_ids(item, seen)


def _has_fresh_containers(actual: object, original: object) -> bool:
    original_ids: set[int] = set()
    actual_ids: set[int] = set()
    _collect_container_ids(original, original_ids)
    _collect_container_ids(actual, actual_ids)
    return original_ids.isdisjoint(actual_ids)


def _wrap_path(leaf: object, steps: Sequence[str | None]) -> object:
    current: object = leaf
    for step in reversed(steps):
        current = [current] if step is None else {step: current}
    return current


@given(payload=JSON_CONTAINERS)
@settings(max_examples=100, derandomize=True)
def test_json_value_round_trips_arbitrary_json_values_with_exact_types(
    payload: JsonValue,
) -> None:
    # Arrange
    raw: object = payload

    # Act
    actual = json_value(raw)

    # Assert
    assert _same_json_tree(actual, payload) is True


@given(payload=JSON_CONTAINERS)
@settings(max_examples=100, derandomize=True)
def test_json_value_is_idempotent_across_valid_trees(payload: JsonValue) -> None:
    # Arrange
    first = json_value(payload)

    # Act
    second = json_value(first)

    # Assert
    assert _same_json_tree(second, first) is True


@given(
    payload=st.lists(JSON_CONTAINERS, max_size=4)
    | st.dictionaries(st.text(max_size=8), JSON_CONTAINERS, max_size=4)
)
@settings(max_examples=100, derandomize=True)
def test_json_value_constructs_fresh_containers_without_input_aliasing(
    payload: object,
) -> None:
    # Arrange
    raw: object = payload

    # Act
    actual = json_value(raw)

    # Assert
    assert _has_fresh_containers(actual, payload) is True


@given(
    leaf_and_type=NON_JSON_LEAVES,
    steps=st.lists(st.none() | st.text(max_size=6), min_size=0, max_size=4),
)
@settings(max_examples=100, derandomize=True)
def test_json_value_rejects_non_json_values_at_root_or_nested_path(
    leaf_and_type: tuple[object, str],
    steps: list[str | None],
) -> None:
    # Arrange
    leaf, expected_type = leaf_and_type
    payload = _wrap_path(leaf, steps)
    pattern = rf"^not a JSON value: {expected_type}$"

    # Assert
    with pytest.raises(TypeError, match=pattern):
        # Act
        json_value(payload)


@pytest.mark.parametrize(
    "scalar_or_empty",
    [
        None,
        True,
        False,
        0,
        1,
        -42,
        0.0,
        2.5,
        float("inf"),
        "",
        "x",
        [],
        {},
        {"a": [1, 2.5, "x", None, True, False], "b": {"c": {}}},
    ],
)
def test_json_value_preserves_scalar_and_empty_container_runtime_types(
    scalar_or_empty: object,
) -> None:
    # Arrange
    raw: object = scalar_or_empty

    # Act
    actual = json_value(raw)

    # Assert
    assert _same_json_tree(actual, scalar_or_empty) is True


def test_json_value_preserves_nan_float_scalar() -> None:
    # Arrange
    raw: object = float("nan")

    # Act
    actual = json_value(raw)

    # Assert
    assert isinstance(actual, float) and math.isnan(actual)


@pytest.mark.parametrize(
    ("payload", "expected_type"),
    [
        ((1, 2), "tuple"),
        ({1: "a"}, "dict"),
        ({"a": [object()]}, "object"),
        ({"a": {"b": (1,)}}, "tuple"),
        ([bytearray(b"x")], "bytearray"),
        ({"ok": 1, 2: "bad"}, "dict"),
    ],
)
def test_json_value_rejects_specific_non_json_shapes_with_exact_type_name(
    payload: object,
    expected_type: str,
) -> None:
    # Arrange
    pattern = rf"^not a JSON value: {expected_type}$"

    # Assert
    with pytest.raises(TypeError, match=pattern):
        # Act
        json_value(payload)
