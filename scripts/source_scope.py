#!/usr/bin/env python3
"""Verify complete authored Python scope and the fixed physical-size limit."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

MAXIMUM_LINES = 399
SOURCE_DIRECTORIES = {
    "src": ".py",
    "tests": ".py",
    "scripts": ".py",
    "alembic": ".py",
    "typings": ".pyi",
}
ROOT_SOURCE = "conftest.py"
PYTHON_SUFFIXES = frozenset({".py", ".pyi", ".pyw"})
GENERATED_DIRECTORIES = frozenset(
    {
        ".git",
        ".venv",
        ".codegraph",
        ".pytest_cache",
        ".hypothesis",
        ".mypy_cache",
        ".ruff_cache",
        ".complexipy_cache",
        ".quality-results",
        "node_modules",
        "dist",
        "coverage",
    }
)


def _walk(directory: Path) -> Iterator[Path]:
    for path in sorted(directory.iterdir()):
        yield from _visit(path)


def _visit(path: Path) -> Iterator[Path]:
    if path.is_symlink():
        raise ValueError(f"authored symlink is unsupported: {path}")
    if path.is_dir():
        yield from _walk(path)
    elif path.suffix.lower() in PYTHON_SUFFIXES:
        if not path.is_file():
            raise ValueError(f"source is not a regular file: {path}")
        yield path


def _discover(root: Path) -> Iterator[Path]:
    for path in sorted(root.iterdir()):
        if path.name not in GENERATED_DIRECTORIES:
            yield from _visit(path)


def _require_directory(path: Path) -> None:
    if path.is_symlink():
        raise ValueError(f"authored symlink is unsupported: {path}")
    if not path.is_dir():
        raise ValueError(f"missing required source directory: {path}")


def _require_roots(root: Path) -> None:
    for name in SOURCE_DIRECTORIES:
        _require_directory(root / name)
    path = root / ROOT_SOURCE
    if path.is_symlink():
        raise ValueError(f"authored symlink is unsupported: {path}")
    if not path.is_file():
        raise ValueError(f"missing required source file: {path}")


def _validate_path(root: Path, path: Path) -> None:
    relative = path.relative_to(root)
    if any(part in GENERATED_DIRECTORIES or part == "__pycache__" for part in relative.parts):
        raise ValueError(f"authored source in a generated/cache directory: {path}")
    if relative == Path(ROOT_SOURCE):
        return
    expected_suffix = SOURCE_DIRECTORIES.get(relative.parts[0])
    if path.suffix != expected_suffix:
        raise ValueError(f"Python file outside enrolled source scope: {path}")


def _verify_size(path: Path) -> None:
    lines = len(path.read_text(encoding="utf-8").splitlines())
    if lines > MAXIMUM_LINES:
        raise ValueError(f"{path}: {lines} physical lines exceeds {MAXIMUM_LINES}")


def _require_populated_roots(root: Path, paths: tuple[Path, ...]) -> None:
    populated = {path.relative_to(root).parts[0] for path in paths}
    missing = set(SOURCE_DIRECTORIES) - populated
    if missing:
        raise ValueError(f"empty enrolled source directories: {', '.join(sorted(missing))}")


def verify_sources(root: Path) -> tuple[Path, ...]:
    """Discover every authored runtime/declaration input and enforce full-tree policy."""
    _require_roots(root)
    paths = tuple(_discover(root))
    for path in paths:
        _validate_path(root, path)
        _verify_size(path)
    _require_populated_roots(root, paths)
    return paths


def _git_output(root: Path, arguments: tuple[str, ...]) -> bytes:
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    try:
        return subprocess.check_output(
            ("git", *arguments), cwd=root, env=environment, stderr=subprocess.PIPE
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ValueError("cannot read tracked inventory with native Git") from exc


def _tracked_paths(root: Path) -> tuple[Path, ...]:
    actual_root = _git_output(root, ("rev-parse", "--show-toplevel"))
    if Path(actual_root.decode("utf-8").rstrip("\n")).resolve() != root.resolve():
        raise ValueError("source root differs from the native Git worktree root")
    output = _git_output(root, ("ls-files", "-z"))
    if not output or not output.endswith(b"\0"):
        raise ValueError("tracked inventory is empty or incomplete")
    return tuple(Path(name) for name in output[:-1].decode("utf-8").split("\0"))


def _verify_tracked_path(root: Path, path: Path) -> None:
    if path.parts[0] in GENERATED_DIRECTORIES or "__pycache__" in path.parts:
        raise ValueError(f"tracked input in a generated/cache directory: {path}")
    if path.suffix.lower() not in PYTHON_SUFFIXES:
        return
    source = root / path
    if source.is_symlink() or not source.is_file():
        raise ValueError(f"missing or non-regular tracked source: {path}")


def verify_tracked(root: Path) -> None:
    """Reject tracked generated content and absent source independently of discovery."""
    for path in _tracked_paths(root):
        _verify_tracked_path(root, path)


def main() -> int:
    """Run the fixed repository policy without caller-controlled weakening flags."""
    argparse.ArgumentParser(description=__doc__).parse_args()
    entry = Path(__file__).absolute()
    try:
        if entry.is_symlink():
            raise ValueError(f"authored entry-point symlink is unsupported: {entry}")
        root = entry.parents[1].resolve()
        verify_tracked(root)
        paths = verify_sources(root)
    except (OSError, ValueError) as exc:
        print(f"[FAIL] source scope: {exc}", file=sys.stderr)
        return 1
    print(f"[PASS] source scope: {len(paths)} authored Python files; maximum {MAXIMUM_LINES} lines")
    return 0


if __name__ == "__main__":
    sys.exit(main())
