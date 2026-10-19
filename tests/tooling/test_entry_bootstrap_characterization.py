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

def helpers(prefix):
    return sorted(name for name in sys.modules if name.startswith(prefix) and "." not in name)

mode, name, path, prefix = sys.argv[1:5]
names = [name] if mode == "single" else ["first_entry", "second.entry"]
before = set(helpers(prefix))
loaded = []
for current in names:
    spec = importlib.util.spec_from_file_location(current, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    loaded.append(module)
created = [item for item in helpers(prefix) if item not in before]
print(json.dumps({
    "package": loaded[0].__package__,
    "spec": loaded[0].__spec__.name,
    "created": created,
    "paths": [list(sys.modules[item].__path__) for item in created],
    "specs": [sys.modules[item].__spec__.name for item in created],
    "siblings": sorted(key for key in sys.modules if created and key.startswith(created[0] + ".")),
    "second_package": loaded[1].__package__ if len(loaded) > 1 else None,
    "second_spec": loaded[1].__spec__.name if len(loaded) > 1 else None,
}))
"""


def _python(*arguments: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        (sys.executable, *arguments),
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


def _help(*arguments: str, cwd: Path) -> None:
    completed = _python(*arguments, "--help", cwd=cwd)
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.startswith("usage:")
    assert completed.stderr == ""


def _loader(mode: str, name: str, path: str, prefix: str, cwd: Path) -> dict[str, object]:
    completed = _python(
        "-I", "-W", "error::DeprecationWarning", "-c", LOADER, mode, name, path, prefix, cwd=cwd
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert isinstance(payload, dict)
    return payload


@pytest.mark.parametrize(("_key", "filename", "_prefix", "_sibling"), ENTRY_POINTS)
def test_direct_script_help_works_from_an_unrelated_cwd(
    tmp_path: Path, _key: str, filename: str, _prefix: str, _sibling: str
) -> None:
    _help("-I", "-W", "error::DeprecationWarning", str(SCRIPTS / filename), cwd=tmp_path)


@pytest.mark.parametrize(("_key", "filename", "_prefix", "_sibling"), ENTRY_POINTS)
def test_module_mode_help_works(_key: str, filename: str, _prefix: str, _sibling: str) -> None:
    _help(
        "-W",
        "error::DeprecationWarning",
        "-m",
        f"scripts.{Path(filename).stem}",
        cwd=REPOSITORY_ROOT,
    )


@pytest.mark.parametrize(("_key", "filename", "prefix", "sibling"), ENTRY_POINTS)
def test_loader_mode_owns_siblings_through_a_helper_package(
    tmp_path: Path, _key: str, filename: str, prefix: str, sibling: str
) -> None:
    payload = _loader("single", "independent.service", str(SCRIPTS / filename), prefix, tmp_path)

    helper = payload["created"]
    assert isinstance(helper, list) and len(helper) == 1
    name = helper[0]
    assert isinstance(name, str) and name.startswith(prefix)
    assert payload["package"] == "independent"
    assert payload["spec"] == "independent.service"
    assert payload["paths"] == [[str(SCRIPTS)]]
    assert payload["specs"] == [name]
    siblings = payload["siblings"]
    assert isinstance(siblings, list)
    assert f"{name}.{sibling}" in siblings


@pytest.mark.parametrize(("_key", "filename", "prefix", "_sibling"), ENTRY_POINTS)
def test_each_load_gets_its_own_helper_package(
    tmp_path: Path, _key: str, filename: str, prefix: str, _sibling: str
) -> None:
    payload = _loader("double", "ignored", str(SCRIPTS / filename), prefix, tmp_path)

    names = payload["created"]
    assert isinstance(names, list) and len(names) == 2
    assert len(set(names)) == 2
    assert all(isinstance(name, str) and name.startswith(prefix) for name in names)
    assert payload["paths"] == [[str(SCRIPTS)], [str(SCRIPTS)]]
    assert payload["package"] == ""
    assert payload["spec"] == "first_entry"
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
    payload = _loader("single", "standalone.entry", str(copy_root / filename), prefix, tmp_path)

    helper = payload["created"]
    assert isinstance(helper, list) and len(helper) == 1
    assert payload["package"] == "standalone"
    assert payload["spec"] == "standalone.entry"
    assert payload["paths"] == [[str(copy_root)]]
