"""Narrow untrusted decoded values to precisely typed containers without a cast.

``isinstance(value, dict)`` leaves ``dict[Unknown, Unknown]``; the ``TypeGuard`` predicates here
re-type the container so its items can be checked one by one.
"""

from __future__ import annotations

from typing import TypeGuard


def _is_mapping(value: object) -> TypeGuard[dict[object, object]]:
    return isinstance(value, dict)


def _is_sequence(value: object) -> TypeGuard[list[object] | tuple[object, ...]]:
    return isinstance(value, (list, tuple))


def _is_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def object_list(value: object) -> list[object] | None:
    """A copy of ``value`` when it is a ``list``, else ``None``."""
    return list(value) if _is_list(value) else None


def object_mapping(value: object) -> dict[object, object] | None:
    """A copy of ``value`` when it is a ``dict``, else ``None``."""
    return dict(value) if _is_mapping(value) else None


def object_sequence(value: object) -> list[object] | None:
    """A copy of ``value`` when it is a ``list`` or ``tuple``, else ``None``."""
    return list(value) if _is_sequence(value) else None


def string_keyed(value: object) -> dict[str, object] | None:
    """A copy of ``value`` when it is a ``dict`` whose every key is a ``str``, else ``None``.

    JSON object keys are always strings, so a dict that fails this check did not come from a
    decoded document and is treated as the wrong shape rather than partially trusted.
    """
    items = object_mapping(value)
    if items is None:
        return None
    narrowed: dict[str, object] = {}
    for key, item in items.items():
        if not isinstance(key, str):
            return None
        narrowed[key] = item
    return narrowed
