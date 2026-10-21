"""Resolve what a pickle stream references, without ever loading it."""

from __future__ import annotations

import importlib
import pickle
import pickletools


def _string_args(data: bytes) -> list[str]:
    """String operands of the opcodes up to and including the first global lookup."""
    strings: list[str] = []
    for opcode, arg, _ in pickletools.genops(data):
        if isinstance(arg, str):
            strings.append(arg)
        if opcode.name in {"GLOBAL", "STACK_GLOBAL"}:
            return strings
    raise AssertionError("pickle stream references no global")


def pickled_global(obj: object) -> tuple[str, str]:
    """Return the ``(module, name)`` the pickle of ``obj`` references first."""
    strings = _string_args(pickle.dumps(obj))
    if len(strings) == 1:
        module, name = strings[0].split(" ", 1)
        return module, name
    return strings[-2], strings[-1]


def resolve_pickled_global(obj: object) -> object:
    """Look up the global the pickle of ``obj`` names, as unpickling would."""
    module, name = pickled_global(obj)
    resolved: object = importlib.import_module(module)
    for part in name.split("."):
        resolved = getattr(resolved, part)
    return resolved
