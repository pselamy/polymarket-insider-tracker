"""Characterize how the verifier and service entry points behave when loaded by file path.

Clusters 16 and 19 of ``docs/duplication/clusters`` repeat one piece of
knowledge in several test modules: load ``scripts/verify.py`` or
``scripts/runtime_services.py`` under an arbitrary module name with
``importlib.util.spec_from_file_location``. These tests pin the observable facts
the call sites depend on, using the raw stdlib recipe, so they hold before and
after the recipe moves to one home.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ENTRY_POINTS = ("verify.py", "runtime_services.py")

SUBPROCESS_LOADER = """
import importlib.util
import json
import sys

spec = importlib.util.spec_from_file_location(sys.argv[2], sys.argv[1])
module = importlib.util.module_from_spec(spec)
if sys.argv[3] == "register":
    sys.modules[sys.argv[2]] = module
spec.loader.exec_module(module)
print(json.dumps({
    "package": module.__package__,
    "spec": module.__spec__.name,
    "root": str(module.REPOSITORY_ROOT),
    "registered": sys.modules.get(sys.argv[2]) is module,
}))
"""


def _load_in_process(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("entry", ENTRY_POINTS)
def test_in_process_load_leaves_sys_modules_untouched(entry: str) -> None:
    name = "loader_characterization_" + entry.removesuffix(".py")
    module = _load_in_process(name, REPOSITORY_ROOT / "scripts" / entry)
    assert name not in sys.modules
    assert module.__spec__ is not None
    assert module.__spec__.name == name
    assert module.REPOSITORY_ROOT == REPOSITORY_ROOT


@pytest.mark.parametrize("mode", ("register", "plain"))
@pytest.mark.parametrize("name,package", (("flat_entry", ""), ("dotted.entry", "dotted")))
@pytest.mark.parametrize("entry", ENTRY_POINTS)
def test_isolated_load_reports_name_package_and_root(
    tmp_path: Path, entry: str, name: str, package: str, mode: str
) -> None:
    path = REPOSITORY_ROOT / "scripts" / entry
    completed = subprocess.run(
        (sys.executable, "-I", "-c", SUBPROCESS_LOADER, str(path), name, mode),
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == {
        "package": package,
        "spec": name,
        "root": str(REPOSITORY_ROOT),
        "registered": mode == "register",
    }
