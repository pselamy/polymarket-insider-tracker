"""Reject repository paths that change meaning across supported platforms."""

import re
import unicodedata
from pathlib import Path

FORBIDDEN = set('<>:"\\|?*')
DEVICE = re.compile(r"(?:CON|PRN|AUX|NUL|COM[1-9¹²³]|LPT[1-9¹²³])(?:[ .]|$)", re.I)
SNAKE_CASE = re.compile(r"_?[a-z][a-z0-9_]*")
SPECIAL_MODULES = {"__init__", "__main__"}


def verify_dots(component: str) -> None:
    """Reject absolute and relative traversal components."""
    if component in {"", ".", ".."}:
        raise ValueError("repository path contains a dot or empty component")


def verify_characters(component: str) -> None:
    """Reject characters unsupported by the hosted Windows filesystem."""
    if any(
        character in FORBIDDEN or unicodedata.category(character) in {"Cc"}
        for character in component
    ):
        raise ValueError("repository path contains a forbidden character")


def verify_ending(component: str) -> None:
    """Reject trailing characters that Windows strips from filenames."""
    if component.endswith(".") or component[-1].isspace():
        raise ValueError("repository path has a trailing dot or space")


def verify_component(component: str) -> None:
    """Reject ambiguous and Windows-reserved path components."""
    verify_dots(component)
    verify_characters(component)
    verify_ending(component)
    if DEVICE.match(component):
        raise ValueError("repository path uses a Windows device name")


def prefixes(name: str) -> list[str]:
    """Include directory prefixes so differently spelled parents collide."""
    parts = name.split("/")
    for part in parts:
        verify_component(part)
    return ["/".join(parts[:index]) for index in range(1, len(parts) + 1)]


def verify_unique_names(names: list[str]) -> None:
    """Reject empty or exactly repeated tracked path inventories."""
    if not names:
        raise ValueError("repository path inventory is empty or duplicated")
    names_seen: set[str] = set()
    for name in names:
        if name in names_seen:
            raise ValueError("repository path inventory is empty or duplicated")
        names_seen.add(name)


def verify_path_names(names: list[str]) -> None:
    """Reject empty, repeated, and Unicode/case-colliding tracked paths."""
    verify_unique_names(names)
    seen: dict[str, str] = {}
    for name in names:
        for prefix in prefixes(name):
            remember(seen, prefix)


def remember(seen: dict[str, str], prefix: str) -> None:
    """Compare each path prefix under normalized case-insensitive spelling."""
    key = unicodedata.normalize("NFC", prefix).casefold()
    prior = seen.get(key)
    if prior is not None and prior != prefix:
        raise ValueError("repository paths collide after Unicode/case normalization")
    seen[key] = prefix


def verify_python_name(path: Path) -> None:
    """Enforce conventional module and package names inside owned roots."""
    for directory in path.parts[1:-1]:
        if SNAKE_CASE.fullmatch(directory) is None:
            raise ValueError(f"Python package directory is not snake_case: {path}")
    if path.stem not in SPECIAL_MODULES and SNAKE_CASE.fullmatch(path.stem) is None:
        raise ValueError(f"Python module is not snake_case: {path}")
