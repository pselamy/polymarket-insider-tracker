"""Characterize the script entry-point bootstrap shared by both entry points.

Cluster 28 of ``docs/duplication-inventory.md`` duplicates one piece of
knowledge between ``scripts/runtime_services.py`` and ``scripts/verify.py``:
each creates a unique synthetic helper package that owns its sibling modules,
temporarily points ``__package__``/``__spec__`` at it so relative imports
resolve even when the file is executed directly or loaded under an arbitrary
name, and restores both values afterwards. These tests pin that observable
behaviour for both call sites before the knowledge moves to one home.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
REPOSITORY_ROOT = SCRIPTS.parent
ENTRY_POINTS = (
    ("verify", "verify.py", "_polymarket_verifier_", "_verify_policy"),
    ("runtime_services", "runtime_services.py", "_polymarket_services_", "_service_safety"),
)

LOADER = """
import importlib.util
import json
import sys

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

def helpers(prefix):
    return sorted(name for name in sys.modules if name.startswith(prefix) and "." not in name)

name, path, prefix, sibling = sys.argv[1:5]
before = set(helpers(prefix))
module = load(name, path)
created = [item for item in helpers(prefix) if item not in before]
siblings = [key for key in sys.modules if created and key.startswith(created[0] + ".")]
print(json.dumps({
    "package": module.__package__,
    "spec": module.__spec__.name,
    "created": created,
    "paths": [list(sys.modules[item].__path__) for item in created],
    "specs": [sys.modules[item].__spec__.name for item in created],
    "siblings": sorted(siblings),
}))
"""

LOAD_TWICE = """
import importlib.util
import json
import sys

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

def helpers(prefix):
    return sorted(name for name in sys.modules if name.startswith(prefix) and "." not in name)

path, prefix = sys.argv[1:3]
first = load("first_entry", path)
second = load("second.entry", path)
names = helpers(prefix)
print(json.dumps({
    "names": names,
    "paths": [list(sys.modules[name].__path__) for name in names],
    "first_package": first.__package__,
    "first_spec": first.__spec__.name,
    "second_package": second.__package__,
    "second_spec": second.__spec__.name,
}))
"""


def _run(
    program: str, *arguments: str, cwd: Path, isolated: bool = True
) -> subprocess.CompletedProcess[str]:
    flags = (
        ("-I", "-W", "error::DeprecationWarning")
        if isolated
        else (
            "-W",
            "error::DeprecationWarning",
        )
    )
    return subprocess.run(
        (sys.executable, *flags, "-c", program, *arguments),
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


def _payload(completed: subprocess.CompletedProcess[str]) -> dict[str, object]:
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


@pytest.mark.parametrize(("_key", "filename", "_prefix", "_sibling"), ENTRY_POINTS)
def test_direct_script_help_works_from_an_unrelated_cwd(
    tmp_path: Path, _key: str, filename: str, _prefix: str, _sibling: str
) -> None:
    completed = subprocess.run(
        (
            sys.executable,
            "-I",
            "-W",
            "error::DeprecationWarning",
            str(SCRIPTS / filename),
            "--help",
        ),
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.startswith("usage:")
    assert completed.stderr == ""


@pytest.mark.parametrize(("_key", "filename", "_prefix", "_sibling"), ENTRY_POINTS)
def test_module_mode_help_works(_key: str, filename: str, _prefix: str, _sibling: str) -> None:
    completed = subprocess.run(
        (
            sys.executable,
            "-W",
            "error::DeprecationWarning",
            "-m",
            f"scripts.{Path(filename).stem}",
            "--help",
        ),
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.startswith("usage:")
    assert completed.stderr == ""


@pytest.mark.parametrize(("_key", "filename", "prefix", "sibling"), ENTRY_POINTS)
def test_loader_mode_owns_siblings_through_a_helper_package(
    tmp_path: Path, _key: str, filename: str, prefix: str, sibling: str
) -> None:
    completed = _run(
        LOADER,
        "independent.service",
        str(SCRIPTS / filename),
        prefix,
        sibling,
        cwd=tmp_path,
    )

    payload = _payload(completed)
    helper = payload["created"]
    assert isinstance(helper, list) and len(helper) == 1
    name = helper[0]
    assert isinstance(name, str) and name.startswith(prefix)
    assert payload["package"] == "independent"
    assert payload["spec"] == "independent.service"
    assert payload["paths"] == [[str(SCRIPTS)]]
    assert payload["specs"] == [name]
    assert f"{name}.{sibling}" in payload["siblings"]


@pytest.mark.parametrize(("_key", "filename", "prefix", "_sibling"), ENTRY_POINTS)
def test_each_load_gets_its_own_helper_package(
    tmp_path: Path, _key: str, filename: str, prefix: str, _sibling: str
) -> None:
    completed = _run(LOAD_TWICE, str(SCRIPTS / filename), prefix, cwd=tmp_path)

    payload = _payload(completed)
    names = payload["names"]
    assert isinstance(names, list) and len(names) == 2
    assert len(set(names)) == 2
    assert all(isinstance(name, str) and name.startswith(prefix) for name in names)
    assert payload["paths"] == [[str(SCRIPTS)], [str(SCRIPTS)]]
    assert payload["first_package"] == ""
    assert payload["first_spec"] == "first_entry"
    assert payload["second_package"] == "second"
    assert payload["second_spec"] == "second.entry"


@pytest.mark.parametrize(("_key", "filename", "prefix", "_sibling"), ENTRY_POINTS)
def test_copied_standalone_scripts_load_their_own_siblings(
    tmp_path: Path, _key: str, filename: str, prefix: str, _sibling: str
) -> None:
    copy_root = tmp_path / "copied" / "scripts"
    copy_root.mkdir(parents=True)
    for source in SCRIPTS.glob("*.py"):
        shutil.copyfile(source, copy_root / source.name)
    completed = _run(
        LOADER,
        "standalone.entry",
        str(copy_root / filename),
        prefix,
        "_verify_policy" if filename == "verify.py" else "_service_safety",
        cwd=tmp_path,
    )

    payload = _payload(completed)
    helper = payload["created"]
    assert isinstance(helper, list) and len(helper) == 1
    assert payload["package"] == "standalone"
    assert payload["spec"] == "standalone.entry"
    assert payload["paths"] == [[str(copy_root)]]
