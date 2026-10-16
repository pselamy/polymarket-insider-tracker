"""Execute the native cyclomatic gate over complete isolated source trees."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCOPE = ("src", "tests", "scripts", "alembic", "conftest.py")
PREFIX = (
    sys.executable,
    "-m",
    "ruff",
    "check",
    "--isolated",
    "--select",
    "C901,E902",
    "--config",
    "lint.mccabe.max-complexity=10",
    "--ignore-noqa",
    "--no-respect-gitignore",
    "--exclude",
    "",
)


def _verifier() -> ModuleType:
    spec = importlib.util.spec_from_file_location("cyclomatic_verifier", ROOT / "scripts/verify.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _source(branches: int) -> str:
    decisions = "".join(
        f"    if value == {number}: return {number}\n" for number in range(branches)
    )
    return "def classify(value):  # noqa: C901\n" + decisions + "    return -1\n"


def _tree(root: Path) -> None:
    for name in SCOPE[:-1]:
        (root / name).mkdir()
    (root / "conftest.py").write_text("", encoding="utf-8")


def _run(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        _verifier().GATES["cyclomatic"].command,
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )


def test_cyclomatic_policy_is_explicit_and_enrolled_in_static_profile() -> None:
    verifier = _verifier()

    assert verifier.GATES["cyclomatic"].command == (*PREFIX, *SCOPE)
    assert "cyclomatic" in verifier.gate_ids_for_profile("static")
    assert "cyclomatic" in verifier.gate_ids_for_profile("all")
    assert "--isolated --select C901,E902" in dict(verifier.DIRECT_GATE_COMMANDS)["cyclomatic"]


def test_cyclomatic_limit_accepts_ten_paths(tmp_path: Path) -> None:
    _tree(tmp_path)
    (tmp_path / "src/control.py").write_text(_source(9), encoding="utf-8")

    completed = _run(tmp_path)

    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.parametrize("scope", SCOPE)
def test_cyclomatic_limit_rejects_eleven_paths_in_every_owned_root(
    tmp_path: Path, scope: str
) -> None:
    _tree(tmp_path)
    target = tmp_path / scope
    if target.is_dir():
        target /= "violation.py"
    target.write_text(_source(10), encoding="utf-8")
    (tmp_path / ".gitignore").write_text("src/\ntests/\nscripts/\nalembic/\n", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text(
        '[tool.ruff]\nexclude = ["**"]\n'
        '[tool.ruff.lint]\nignore = ["C901"]\n'
        "[tool.ruff.lint.mccabe]\nmax-complexity = 100\n",
        encoding="utf-8",
    )

    completed = _run(tmp_path)

    assert completed.returncode != 0
    assert "C901" in completed.stdout
    assert "11 > 10" in completed.stdout


def test_cyclomatic_gate_rejects_missing_owned_root(tmp_path: Path) -> None:
    _tree(tmp_path)
    (tmp_path / "src").rmdir()

    completed = _run(tmp_path)

    assert completed.returncode != 0
    assert "E902" in completed.stdout
