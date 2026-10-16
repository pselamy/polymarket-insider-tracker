"""Keep service verification source-bound without requiring live service effects."""

from __future__ import annotations

import inspect
import json
import shutil
import subprocess
import sys
from pathlib import Path

SERVICE = Path(__file__).resolve().parents[2] / "scripts/runtime_services.py"

_LOAD = """
import importlib.util
from pathlib import Path
import sys

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
"""


def _run(tmp_path: Path, program: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        (sys.executable, "-I", "-W", "error::DeprecationWarning", "-c", program, *arguments),
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )


def _copy_scripts(tmp_path: Path, name: str) -> Path:
    scripts = tmp_path / name / "scripts"
    scripts.mkdir(parents=True)
    for source in SERVICE.parent.glob("*.py"):
        shutil.copyfile(source, scripts / source.name)
    return scripts / SERVICE.name


def test_service_help_works_from_unrelated_cwd(tmp_path: Path) -> None:
    completed = subprocess.run(
        (sys.executable, "-I", str(SERVICE), "--help"),
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
        timeout=15,
    )
    assert completed.returncode == 0, completed.stderr
    assert "--phase {probe,migrations,all}" in completed.stdout
    assert completed.stderr == ""


def test_foreign_cached_scripts_cannot_redirect_service_helper(tmp_path: Path) -> None:
    foreign = tmp_path / "foreign/scripts"
    foreign.mkdir(parents=True)
    (foreign / "__init__.py").write_text('origin = "foreign"\n', encoding="utf-8")
    program = _LOAD + """
sys.path.insert(0, sys.argv[2])
import scripts
before = list(sys.path)
module = load("independent.service", sys.argv[1])
assert scripts.origin == "foreign"
assert module.__package__ == "independent"
assert module.__spec__.name == "independent.service"
assert module.REPOSITORY_ROOT == Path(sys.argv[1]).resolve().parents[1]
assert module.validate_redis_url("redis://localhost:6379") == "redis://localhost:6379"
assert before == sys.path
"""
    completed = _run(tmp_path, program, str(SERVICE), str(foreign.parent))
    assert completed.returncode == 0, completed.stderr


def test_service_copies_use_their_own_safety_policy(tmp_path: Path) -> None:
    first = _copy_scripts(tmp_path, "first")
    second = _copy_scripts(tmp_path, "second")
    candidates = [
        p for p in second.parent.glob("*.py") if 'REDIS_SCHEME = "redis"' in p.read_text()
    ]
    assert len(candidates) == 1
    policy = candidates[0]
    policy.write_text(
        policy.read_text().replace('REDIS_SCHEME = "redis"', 'REDIS_SCHEME = "redis2"')
    )
    program = _LOAD + """
first = load("first_service", sys.argv[1])
second = load("second.service", sys.argv[2])
assert first.REPOSITORY_ROOT != second.REPOSITORY_ROOT
assert first.validate_redis_url("redis://localhost:6379") == "redis://localhost:6379"
assert second.validate_redis_url("redis2://localhost:6379") == "redis2://localhost:6379"
try:
    second.validate_redis_url("redis://localhost:6379")
except second.ServicePrerequisiteError:
    pass
else:
    raise AssertionError("second verifier ignored its local safety policy")
"""
    completed = _run(tmp_path, program, str(first), str(second))
    assert completed.returncode == 0, completed.stderr


def test_missing_environment_remains_json_prerequisite_error(tmp_path: Path) -> None:
    program = _LOAD + """
import os
os.environ.pop("DATABASE_URL", None)
os.environ.pop("REDIS_URL", None)
module = load("service_under_test", sys.argv[1])
raise SystemExit(module.main(["--phase", "all", "--json"]))
"""
    completed = _run(tmp_path, program, str(SERVICE))
    assert completed.returncode == 2
    assert completed.stderr == ""
    assert json.loads(completed.stdout) == {
        "phase": "all",
        "status": "error",
        "error": "DATABASE_URL and REDIS_URL must be set for --phase all",
    }


def _write_exception_payload(root: Path, destination: Path) -> None:
    """Run as source in an isolated process to verify public exception serialization."""
    import pickle

    sys.path.insert(0, str(root))
    from scripts import runtime_services

    errors = (
        runtime_services.ServicePrerequisiteError("prerequisite"),
        runtime_services.ServiceVerificationError("verification"),
    )
    for error in errors:
        assert type(error).__module__ == "scripts.runtime_services"
    destination.write_bytes(pickle.dumps(errors))


def test_service_errors_pickle_across_fresh_interpreters(tmp_path: Path) -> None:
    payload = tmp_path / "errors.pickle"
    program = "import sys\nfrom pathlib import Path\n" + inspect.getsource(_write_exception_payload)
    program += "\n_write_exception_payload(Path(sys.argv[1]), Path(sys.argv[2]))"
    completed = _run(tmp_path, program, str(SERVICE.parents[1]), str(payload))
    assert completed.returncode == 0, completed.stderr
    program = """
import pickle
from pathlib import Path
import sys
sys.path.insert(0, sys.argv[1])
from scripts import runtime_services
errors = pickle.loads(Path(sys.argv[2]).read_bytes())
assert tuple(type(error) for error in errors) == (
    runtime_services.ServicePrerequisiteError, runtime_services.ServiceVerificationError
)
assert tuple(str(error) for error in errors) == ("prerequisite", "verification")
"""
    completed = _run(tmp_path, program, str(SERVICE.parents[1]), str(payload))
    assert completed.returncode == 0, completed.stderr


def test_private_redaction_callable_pickles_across_fresh_interpreters(tmp_path: Path) -> None:
    payload = tmp_path / "redactor.pickle"
    program = """
import pickle
from pathlib import Path
import sys
sys.path.insert(0, sys.argv[1])
from scripts import runtime_services
Path(sys.argv[2]).write_bytes(pickle.dumps(runtime_services._redact_text))
"""
    completed = _run(tmp_path, program, str(SERVICE.parents[1]), str(payload))
    assert completed.returncode == 0, completed.stderr
    program = """
import pickle
from pathlib import Path
import sys
sys.path.insert(0, sys.argv[1])
from scripts import runtime_services
redact = pickle.loads(Path(sys.argv[2]).read_bytes())
assert redact is runtime_services._redact_text
assert redact.__qualname__ == "_redact_text"
assert redact("unchanged diagnostic") == "unchanged diagnostic"
"""
    completed = _run(tmp_path, program, str(SERVICE.parents[1]), str(payload))
    assert completed.returncode == 0, completed.stderr
