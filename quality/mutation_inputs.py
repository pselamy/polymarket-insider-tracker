"""One immutable source enrollment for coordinator and isolated workers."""

import os
import shutil
from pathlib import Path

INPUTS = ("src", "tests", "quality", "docs", ".github")
CONFIGURATION = (
    ".gitignore",
    "AGENTS.md",
    "LICENSE",
    "README.md",
    "pyproject.toml",
    "cosmic-ray.toml",
    "uv.lock",
    "mise.toml",
    "mise.lock",
)


def authored_file(path: Path) -> bool:
    """Include policy/rules/data while omitting reproducible bytecode caches."""
    return path.is_file() and "__pycache__" not in path.parts


def snapshot(root: Path) -> dict[str, bytes]:
    """Bind every authored input, including never-imported source and policy."""
    paths = [
        path
        for directory in INPUTS
        for path in (root / directory).rglob("*")
        if authored_file(path)
    ]
    paths.extend(root / name for name in CONFIGURATION)
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in paths}


def copy_inputs(root: Path, target: Path) -> None:
    """Copy exact source/configuration without environments or caches."""
    for directory in INPUTS:
        shutil.copytree(
            root / directory,
            target / directory,
            ignore=shutil.ignore_patterns("__pycache__"),
        )
    for name in CONFIGURATION:
        shutil.copy2(root / name, target / name)


def environment(root: Path, inherited: dict[str, str]) -> dict[str, str]:
    """Trials must import their own copy, never the coordinator checkout."""
    result = inherited.copy()
    result["PYTHONPATH"] = os.pathsep.join([str(root / "src"), str(root)])
    result["PYTHONDONTWRITEBYTECODE"] = "1"
    result["PYTEST_DEBUG_TEMPROOT"] = str(root / ".quality-results")
    return result
