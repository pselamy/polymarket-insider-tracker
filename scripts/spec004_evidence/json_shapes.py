"""Checked narrowing of decoded evidence documents, replacing type escapes.

Decoded documents are ``object`` until a shape check proves otherwise. A wrong shape raises
``TypeError`` (a missing key stays a ``KeyError`` from the lookup), the outcome class the tools had
when they indexed or converted a value of the wrong type directly.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TypeGuard

from polymarket_insider_tracker.shapes import object_list, object_mapping

Obj = dict[str, object]


def is_object(value: object) -> TypeGuard[Obj]:
    """A ``dict`` whose every key is a ``str`` (the only objects a decoded document holds)."""
    items = object_mapping(value)
    return items is not None and all(isinstance(key, str) for key in items)


def is_array(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def as_object(value: object) -> Obj:
    if is_object(value):
        return value
    raise TypeError(f"expected a JSON object, got {type(value).__name__}")


def as_mapping(value: object) -> dict[object, object]:
    """A ``dict`` with keys of any type, for schema lookups by key."""
    items = object_mapping(value)
    if items is None:
        raise TypeError(f"expected a JSON object, got {type(value).__name__}")
    return items


def as_array(value: object) -> list[object]:
    items = object_list(value)
    if items is None:
        raise TypeError(f"expected a JSON array, got {type(value).__name__}")
    return items


def as_text(value: object) -> str:
    if isinstance(value, str):
        return value
    raise TypeError(f"expected a string, got {type(value).__name__}")


def as_decimal(value: object) -> Decimal:
    """``Decimal(value)`` for the types ``Decimal`` accepts from JSON: str, int and float."""
    if isinstance(value, str | int | float):
        return Decimal(value)
    if isinstance(value, list | tuple):
        raise ValueError("cannot convert a sequence to Decimal")
    raise TypeError(f"cannot convert {type(value).__name__} to Decimal")


def as_number(value: object) -> int | float:
    if isinstance(value, int | float):
        return value
    raise TypeError(f"expected a number, got {type(value).__name__}")


def as_int(value: object) -> int:
    if isinstance(value, str | int | float):
        return int(value)
    raise TypeError(f"cannot convert {type(value).__name__} to int")


def at(parent: Obj, *path: str) -> Obj:
    """The nested object under ``path``; a missing key is a ``KeyError``."""
    node = parent
    for key in path:
        node = as_object(node[key])
    return node


TYPES: dict[str, tuple[type, ...]] = {
    "object": (dict,),
    "array": (list,),
    "string": (str,),
    "integer": (int,),
    "boolean": (bool,),
    "null": (type(None),),
}


def matches_type(value: object, name: str) -> bool:
    if name == "integer" and isinstance(value, bool):
        return False
    return isinstance(value, TYPES[name])
