"""Exercise the real source inventory and fixed physical-size boundary."""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Protocol, runtime_checkable

import pytest

GATE = Path(__file__).resolve().parents[2] / "scripts/source_scope.py"
LOCATIONS = ("src", "tests", "scripts", "alembic", "typings", "conftest.py")
GENERATED = (
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
)


@runtime_checkable
class ScopeAPI(Protocol):
    def verify_sources(self, root: Path) -> tuple[Path, ...]: ...

    def verify_tracked(self, root: Path) -> None: ...


def _gate() -> ScopeAPI:
    assert GATE.is_file(), "source-scope gate is not implemented"
    spec = importlib.util.spec_from_file_location("source_scope_under_test", GATE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert isinstance(module, ScopeAPI)
    return module


def _write(path: Path, content: str = "# authored input\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _tree(root: Path) -> Path:
    for directory in LOCATIONS[:-1]:
        suffix = ".pyi" if directory == "typings" else ".py"
        _write(root / directory / f"sample{suffix}")
    _write(root / "conftest.py")
    return root


def _git(root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(("git", *arguments), cwd=root, capture_output=True, text=True, check=True)


def _tracked_tree(root: Path) -> Path:
    _tree(root)
    _git(root, "init", "--quiet")
    _git(root, "add", ".")
    return root


def _cli(root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    assert GATE.is_file(), "source-scope gate is not implemented"
    destination = root / "scripts/source_scope.py"
    shutil.copyfile(GATE, destination)
    _git(root, "add", "scripts/source_scope.py")
    return subprocess.run(
        (sys.executable, str(destination), *arguments),
        cwd=root.parent,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )


def test_complete_runtime_and_declaration_inventory_is_returned(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    paths = _gate().verify_sources(root)
    assert {path.relative_to(root).as_posix() for path in paths} == {
        "src/sample.py",
        "tests/sample.py",
        "scripts/sample.py",
        "alembic/sample.py",
        "typings/sample.pyi",
        "conftest.py",
    }


@pytest.mark.parametrize("location", LOCATIONS)
def test_399_lines_pass_and_400_fail_in_every_owned_location(tmp_path: Path, location: str) -> None:
    root = _tree(tmp_path)
    suffix = ".pyi" if location == "typings" else ".py"
    name = "conftest.py" if location == "conftest.py" else f"{location}/boundary{suffix}"
    path = _write(root / name, "# comment and blank lines count\n" + "\n" * 398)
    assert path in _gate().verify_sources(root)
    path.write_text(path.read_text() + "\n")
    with pytest.raises(ValueError, match="399"):
        _gate().verify_sources(root)


@pytest.mark.parametrize(
    "name",
    (
        "other/module.py",
        "src/.venv/module.py",
        "src/__pycache__/module.py",
        "typings/runtime.py",
        "src/HIDDEN.PY",
        "src/hidden.pyw",
        "src/undeclared.pyi",
        "templates/example.pyi",
    ),
)
def test_unenrolled_authored_input_fails(tmp_path: Path, name: str) -> None:
    root = _tree(tmp_path)
    _write(root / name)
    with pytest.raises(ValueError, match="scope|generated|cache"):
        _gate().verify_sources(root)


@pytest.mark.parametrize("location", LOCATIONS)
def test_required_scope_cannot_disappear(tmp_path: Path, location: str) -> None:
    root = _tree(tmp_path)
    path = root / location
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()
    with pytest.raises(ValueError, match="missing"):
        _gate().verify_sources(root)


def test_empty_owned_directory_fails(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    (root / "src/sample.py").unlink()
    with pytest.raises(ValueError, match="empty"):
        _gate().verify_sources(root)


def test_authored_symlink_is_rejected_before_following_it(tmp_path: Path) -> None:
    root = _tree(tmp_path / "root")
    target = _write(tmp_path / "outside.txt")
    (root / "notes-link").symlink_to(target)
    with pytest.raises(ValueError, match="symlink"):
        _gate().verify_sources(root)


@pytest.mark.parametrize("directory", GENERATED)
def test_only_top_level_untracked_generated_directories_are_omitted(
    tmp_path: Path, directory: str
) -> None:
    root = _tree(tmp_path)
    _write(root / directory / "generated.py", "\n" * 400)
    assert len(_gate().verify_sources(root)) == 6


def test_real_git_ignored_authored_input_still_fails_the_size_limit(tmp_path: Path) -> None:
    root = _tracked_tree(tmp_path)
    _write(root / ".gitignore", "src/ignored/\n")
    path = _write(root / "src/ignored/hidden.py", "\n" * 399)
    _git(root, "check-ignore", "--quiet", str(path))
    assert path in _gate().verify_sources(root)
    path.write_text("\n" * 400)
    with pytest.raises(ValueError, match="399"):
        _gate().verify_sources(root)


@pytest.mark.parametrize("directory", GENERATED[1:])
@pytest.mark.parametrize("name", ("hidden.py", "notes.md"))
def test_tracked_source_cannot_hide_in_generated_omissions(
    tmp_path: Path, directory: str, name: str
) -> None:
    root = _tracked_tree(tmp_path)
    hidden = _write(root / directory / name)
    _git(root, "add", "--force", str(hidden))
    with pytest.raises(ValueError, match="generated"):
        _gate().verify_tracked(root)


def test_missing_tracked_source_fails(tmp_path: Path) -> None:
    root = _tracked_tree(tmp_path)
    (root / "src/sample.py").unlink()
    with pytest.raises(ValueError, match="missing"):
        _gate().verify_tracked(root)


@pytest.mark.parametrize("variable", ("GIT_DIR", "GIT_INDEX_FILE"))
def test_git_environment_cannot_substitute_another_tracked_inventory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, variable: str
) -> None:
    root = _tracked_tree(tmp_path / "candidate")
    other = _tracked_tree(tmp_path / "other")
    hidden = _write(root / ".venv/hidden.py")
    _git(root, "add", "--force", str(hidden))
    replacement = other / ".git" if variable == "GIT_DIR" else other / ".git/index"
    monkeypatch.setenv(variable, str(replacement))
    with pytest.raises(ValueError, match="generated"):
        _gate().verify_tracked(root)


def test_non_utf8_source_fails_closed(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    (root / "src/sample.py").write_bytes(b"\xff")
    with pytest.raises(ValueError, match="utf-8"):
        _gate().verify_sources(root)


def test_nested_directory_cannot_claim_the_parent_repository_inventory(tmp_path: Path) -> None:
    root = _tracked_tree(tmp_path / "repo")
    nested = _tree(root / "nested")
    _git(root, "add", ".")
    with pytest.raises(ValueError, match="root"):
        _gate().verify_tracked(nested)


def test_non_regular_source_fails_without_blocking_on_a_fifo(tmp_path: Path) -> None:
    root = _tracked_tree(tmp_path)
    os.mkfifo(root / "src/blocked.py")
    completed = _cli(root)
    assert completed.returncode == 1
    assert "regular file" in completed.stderr


def test_empty_git_inventory_is_not_a_clean_scan(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    _git(root, "init", "--quiet")
    with pytest.raises(ValueError, match="empty"):
        _gate().verify_tracked(root)


def test_real_cli_passes_from_an_unrelated_working_directory(tmp_path: Path) -> None:
    completed = _cli(_tracked_tree(tmp_path / "repo"))
    assert completed.returncode == 0, completed.stderr
    assert "7 authored Python files" in completed.stdout


def test_cli_cannot_lower_the_limit_or_exclude_source(tmp_path: Path) -> None:
    completed = _cli(_tracked_tree(tmp_path / "repo"), "--exclude", "src")
    assert completed.returncode == 2
    assert "unrecognized arguments" in completed.stderr


def test_symlinked_entry_point_cannot_validate_another_checkout(tmp_path: Path) -> None:
    root = _tracked_tree(tmp_path / "candidate")
    other = _tracked_tree(tmp_path / "other")
    assert _cli(root).returncode == 0
    assert _cli(other).returncode == 0
    entry = root / "scripts/source_scope.py"
    entry.unlink()
    entry.symlink_to(other / "scripts/source_scope.py")
    completed = subprocess.run(
        (sys.executable, str(entry)),
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert completed.returncode == 1
    assert "symlink" in completed.stderr


def test_git_unavailability_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _tracked_tree(tmp_path)
    monkeypatch.setenv("PATH", "")
    with pytest.raises(ValueError, match="tracked inventory"):
        _gate().verify_tracked(root)
