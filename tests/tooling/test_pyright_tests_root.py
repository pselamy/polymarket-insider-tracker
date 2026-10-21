"""Pyright enrols the whole ``tests`` tree as one root, so new test files are enforced."""

from __future__ import annotations

import json
import subprocess
import tomllib
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PLANTED_ERROR = 'VALUE: int = "not-an-int"\n'


def _pyright_settings() -> dict[str, object]:
    with (REPOSITORY_ROOT / "pyproject.toml").open("rb") as handle:
        tool = tomllib.load(handle)["tool"]
    settings: dict[str, object] = tool["pyright"]
    return settings


def _include() -> list[str]:
    with (REPOSITORY_ROOT / "pyproject.toml").open("rb") as handle:
        include: list[str] = tomllib.load(handle)["tool"]["pyright"]["include"]
    return include


def test_tests_is_enrolled_as_a_single_root() -> None:
    include = _include()

    assert include.count("tests") == 1
    assert [entry for entry in include if entry.startswith("tests/")] == []


def test_new_file_under_tests_with_a_type_error_fails_pyright(tmp_path: Path) -> None:
    planted = tmp_path / "tests" / "nested" / "test_planted.py"
    planted.parent.mkdir(parents=True)
    planted.write_text(PLANTED_ERROR, encoding="utf-8")
    settings = {**_pyright_settings(), "include": ["tests"], "stubPath": ""}
    (tmp_path / "pyrightconfig.json").write_text(json.dumps(settings), encoding="utf-8")

    completed = subprocess.run(
        ("uv", "run", "--locked", "pyright", "--project", str(tmp_path), "--outputjson"),
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    report = json.loads(completed.stdout)
    assert completed.returncode == 1
    assert [d["rule"] for d in report["generalDiagnostics"]] == ["reportAssignmentType"]
    assert report["generalDiagnostics"][0]["file"] == str(planted)
