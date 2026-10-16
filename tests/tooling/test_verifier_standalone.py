"""Preserve dependency-free verifier entry points during module extraction."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

VERIFIER = Path(__file__).resolve().parents[2] / "scripts/verify.py"


def test_help_works_without_site_packages_or_project_on_python_path(tmp_path: Path) -> None:
    completed = subprocess.run(
        (sys.executable, "-I", "-S", str(VERIFIER), "--help"),
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert completed.returncode == 0, completed.stderr
    assert "direct gate commands" in completed.stdout
    assert "--profile {static,compatibility,services,all}" in completed.stdout
    assert completed.stderr == ""


def test_arbitrary_module_loading_preserves_json_invocation_errors(tmp_path: Path) -> None:
    program = """
import importlib.util
import sys
spec = importlib.util.spec_from_file_location("independent_verifier", sys.argv[1])
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
raise SystemExit(module.main(["--profile", "unknown", "--json"]))
"""
    completed = subprocess.run(
        (sys.executable, "-I", "-S", "-c", program, str(VERIFIER)),
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert completed.returncode == 2
    assert completed.stderr == ""
    result = json.loads(completed.stdout)
    assert result["status"] == "error"
    assert result["exit_code"] == 2
    assert result["gates"] == []
    assert "unknown" in result["error"]
