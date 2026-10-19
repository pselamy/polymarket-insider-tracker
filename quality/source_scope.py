"""Discover Python inputs independently of tool globs and ignore files."""

import subprocess
from collections.abc import Iterator
from pathlib import Path

from quality.portable_paths import verify_path_names, verify_python_name

SOURCE_ROOTS = {"src", "tests", "quality", "scripts", "alembic", "conftest.py"}
GENERATED_ROOTS = {
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
PYTHON_SUFFIXES = {".py", ".pyi", ".pyw"}
PYTHON_EXTENSION = ".py"
MAXIMUM_LINES = 399


def discover(root: Path) -> Iterator[Path]:
    """Only named top-level generated directories are omitted."""
    for path in root.iterdir():
        if path.name not in GENERATED_ROOTS:
            yield from visit(path)


def walk(directory: Path) -> Iterator[Path]:
    """Nested directories have no generated exemptions."""
    for path in directory.iterdir():
        yield from visit(path)


def visit(path: Path) -> Iterator[Path]:
    """Traverse real directories and retain supported language candidates."""
    if path.is_symlink():
        raise ValueError(f"authored symlinks are unsupported: {path}")
    if path.is_dir():
        yield from walk(path)
    elif path.suffix.lower() in PYTHON_SUFFIXES:
        yield path


def validate_path(root: Path, path: Path) -> None:
    """New source locations require policy enrollment before tools may run."""
    parts = path.relative_to(root).parts
    if parts[0] not in SOURCE_ROOTS or path.suffix != PYTHON_EXTENSION:
        raise ValueError(f"Python file outside supported scope: {path}")
    if any(part in GENERATED_ROOTS for part in parts):
        raise ValueError(f"authored Python in a generated directory: {path}")
    if "__pycache__" in parts:
        raise ValueError(f"authored Python in a cache directory: {path}")
    verify_python_name(path.relative_to(root))


def verify_size(path: Path) -> None:
    """Count physical lines, including blank lines and comments."""
    lines = len(path.read_text(encoding="utf-8").splitlines())
    if lines > MAXIMUM_LINES:
        raise ValueError(f"{path}: {lines} physical lines exceeds 399")


def verify_sources(root: Path) -> list[Path]:
    """Reject an empty repository, unenrolled source and oversized files."""
    paths = sorted(discover(root))
    if not paths:
        raise ValueError("no authored Python files discovered")
    for path in paths:
        validate_path(root, path)
        verify_size(path)
    return paths


def verify_tracked(root: Path) -> None:
    """A contributor cannot commit authored files inside a generated exemption."""
    output = subprocess.check_output(["git", "ls-files", "-z"], cwd=root)
    if not output.endswith(b"\0"):
        raise ValueError("tracked path inventory is empty or incomplete")
    names = output[:-1].decode("utf-8").split("\0")
    verify_path_names(names)
    for name in names:
        validate_tracked(Path(name))


def validate_tracked(path: Path) -> None:
    """Generated directories and bytecode caches must remain untracked."""
    if path.parts[0] in GENERATED_ROOTS or "__pycache__" in path.parts:
        raise ValueError(f"tracked file in a generated exemption: {path}")
