"""Narrow attributes of dynamically imported pinned upstream modules at runtime."""

import importlib
from collections.abc import Callable


def native_callable(module_name: str, attribute: str) -> Callable[..., object]:
    """Return an upstream callable, rejecting a module whose pinned API has moved."""
    value: object = getattr(importlib.import_module(module_name), attribute, None)
    if not callable(value):
        raise TypeError(f"{module_name}.{attribute} is not callable")
    return value
