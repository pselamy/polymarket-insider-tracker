"""Preserve dependency-free verifier entry points during module extraction."""

from __future__ import annotations

import inspect
import json
import shutil
import subprocess
import sys
from pathlib import Path

VERIFIER = Path(__file__).resolve().parents[2] / "scripts/verify.py"


def _run_program(tmp_path: Path, program: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        (
            sys.executable,
            "-I",
            "-S",
            "-W",
            "error::DeprecationWarning",
            "-c",
            program,
            *arguments,
        ),
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )


def _copy_verifier(tmp_path: Path, name: str) -> Path:
    scripts = tmp_path / name / "scripts"
    scripts.mkdir(parents=True)
    for source in VERIFIER.parent.glob("*.py"):
        shutil.copyfile(source, scripts / source.name)
    return scripts / "verify.py"


_LOAD_VERIFIER = """
import importlib.util
from pathlib import Path
import sys

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
"""


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


def test_foreign_cached_scripts_package_cannot_redirect_helpers(tmp_path: Path) -> None:
    foreign_scripts = tmp_path / "foreign" / "scripts"
    foreign_scripts.mkdir(parents=True)
    (foreign_scripts / "__init__.py").write_text('origin = "foreign"\n', encoding="utf-8")
    program = _LOAD_VERIFIER + """
sys.path.insert(0, sys.argv[2])
import scripts
original_path = list(sys.path)
module = load("independent_verifier", sys.argv[1])
assert scripts.origin == "foreign"
assert module.REPOSITORY_ROOT == Path(sys.argv[1]).resolve().parents[1]
assert module.GATES["lock"].command == ("uv", "lock", "--check")
assert sys.path == original_path
"""
    completed = _run_program(tmp_path, program, str(VERIFIER), str(foreign_scripts.parent))
    assert completed.returncode == 0, completed.stderr


def test_distinct_source_copies_execute_their_own_gate_policy(tmp_path: Path) -> None:
    first = _copy_verifier(tmp_path, "first")
    second = _copy_verifier(tmp_path, "second")
    policy = second.with_name("_verify_policy.py")
    original = policy.read_text(encoding="utf-8")
    before = 'Gate("lock", ("uv", "lock", "--check"))'
    after = 'Gate("lock", ("second-policy",))'
    assert original.count(before) == 1
    policy.write_text(original.replace(before, after), encoding="utf-8")
    program = _LOAD_VERIFIER + """
first = load("first_verifier", sys.argv[1])
second = load("other.verifier", sys.argv[2])
assert first.REPOSITORY_ROOT != second.REPOSITORY_ROOT
assert first.GATES is not second.GATES
assert first.__package__ == ""
assert second.__package__ == "other"
assert first.__spec__.name == "first_verifier"
assert second.__spec__.name == "other.verifier"
assert first.GATES["lock"].command == ("uv", "lock", "--check")
assert second.GATES["lock"].command == ("second-policy",)
result = second.run_verification("static", runner=lambda gate: second.CommandExecution(0))
assert result.gates[0].command == "second-policy"
assert result.status == "passed"
"""
    completed = _run_program(tmp_path, program, str(first), str(second))
    assert completed.returncode == 0, completed.stderr


def _write_public_result_payload(root: Path, destination: Path) -> None:
    """Executed as source in an isolated interpreter to exercise the public pickle boundary."""
    import pickle

    sys.path.insert(0, str(root))
    from scripts import verify

    result = verify.run_verification("static", runner=lambda _gate: verify.CommandExecution(0))
    values = (verify.GATES["lock"], verify.CommandExecution(0), result.gates[0], result)
    for value in values:
        assert type(value).__module__ == "scripts.verify"
    destination.write_bytes(pickle.dumps(values))


def test_public_result_values_pickle_across_fresh_interpreters(tmp_path: Path) -> None:
    payload = tmp_path / "results.pickle"
    program = "import sys\nfrom pathlib import Path\n" + inspect.getsource(
        _write_public_result_payload
    )
    program += "\n_write_public_result_payload(Path(sys.argv[1]), Path(sys.argv[2]))"
    completed = _run_program(tmp_path, program, str(VERIFIER.parents[1]), str(payload))
    assert completed.returncode == 0, completed.stderr
    program = """
import pickle
from pathlib import Path
import sys
sys.path.insert(0, sys.argv[1])
from scripts import verify
values = pickle.loads(Path(sys.argv[2]).read_bytes())
expected = (verify.Gate, verify.CommandExecution, verify.GateResult, verify.VerificationResult)
assert tuple(type(value) for value in values) == expected
assert values[-1].status == "passed"
"""
    completed = _run_program(tmp_path, program, str(VERIFIER.parents[1]), str(payload))
    assert completed.returncode == 0, completed.stderr
