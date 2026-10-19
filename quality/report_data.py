"""Keep external JSON values unknown until their shape is checked."""

from typing import TypeGuard


def is_object(value: object) -> TypeGuard[dict[str, object]]:
    """JSON objects have string keys; retain unknown values."""
    return isinstance(value, dict)


def is_array(value: object) -> TypeGuard[list[object]]:
    """Retain unknown array elements at the serialization boundary."""
    return isinstance(value, list)


def record(value: object) -> dict[str, object]:
    if not is_object(value):
        raise ValueError("report field must be an object")
    return value


def array(value: object) -> list[object]:
    if not is_array(value):
        raise ValueError("report field must be an array")
    return value


def text(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("report field must be a nonempty string")
    return value


def unique(values: list[tuple[str, str, str]]) -> set[tuple[str, str, str]]:
    result: set[tuple[str, str, str]] = set()
    for value in values:
        if value in result:
            raise ValueError("duplicate package in dependency inventory")
        result.add(value)
    return result
