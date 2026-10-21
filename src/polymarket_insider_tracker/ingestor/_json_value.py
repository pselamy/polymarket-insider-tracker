"""Re-type a decoded value as a JSON value, rejecting what JSON cannot hold."""

from __future__ import annotations

from typing import TypeGuard

from polymarket_insider_tracker.ingestor.volume_observation import JsonValue
from polymarket_insider_tracker.shapes import object_list, string_keyed


def _is_json_scalar(value: object) -> TypeGuard[None | bool | int | float | str]:
    return value is None or isinstance(value, (bool, int, float, str))


def json_value(value: object) -> JsonValue:
    """``value`` re-typed as a decoded JSON value; anything JSON cannot hold is a ``TypeError``."""
    if _is_json_scalar(value):
        return value
    items = object_list(value)
    if items is not None:
        return [json_value(item) for item in items]
    return _json_object(value)


def _json_object(value: object) -> dict[str, JsonValue]:
    fields = string_keyed(value)
    if fields is None:
        raise TypeError(f"not a JSON value: {type(value).__name__}")
    return {key: json_value(item) for key, item in fields.items()}
