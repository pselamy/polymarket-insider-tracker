"""Source policy: test code never uses ``unittest.mock`` or the ``mock`` backport.

The scan covers every Python file under ``tests/`` (this file included) and the root
``conftest.py``. It inspects import statements and attribute access in the AST, so string
literals, ``pytest.MonkeyPatch``, and functional transports such as ``httpx.MockTransport`` are
never mistaken for the forbidden framework.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = TESTS_DIR.parent
FORBIDDEN_MODULES = ("unittest.mock", "mock")


def _is_forbidden_module(name: str) -> bool:
    return name in FORBIDDEN_MODULES or name.startswith(("unittest.mock.", "mock."))


def _import_violations(node: ast.Import) -> list[str]:
    return [f"import {alias.name}" for alias in node.names if _is_forbidden_module(alias.name)]


def _mock_submodule_imports(node: ast.ImportFrom) -> list[str]:
    return [f"from unittest import {alias.name}" for alias in node.names if alias.name == "mock"]


def _import_from_violations(node: ast.ImportFrom) -> list[str]:
    module = node.module or ""
    if node.level == 0 and _is_forbidden_module(module):
        return [f"from {module} import {', '.join(alias.name for alias in node.names)}"]
    if module == "unittest":
        return _mock_submodule_imports(node)
    return []


def _import_alias_binding(alias: ast.alias) -> tuple[str, str]:
    module = alias.name if alias.asname else alias.name.split(".")[0]
    return alias.asname or module, module


def _import_bindings(node: ast.AST) -> dict[str, str]:
    if isinstance(node, ast.Import):
        return dict(map(_import_alias_binding, node.names))
    if isinstance(node, ast.ImportFrom):
        return _absolute_subimport_bindings(node)
    return {}


def _absolute_subimport_bindings(node: ast.ImportFrom) -> dict[str, str]:
    if node.level:
        return {}
    return {alias.asname or alias.name: f"{node.module}.{alias.name}" for alias in node.names}


def _qualified_name(node: ast.AST, bindings: dict[str, str]) -> str:
    if isinstance(node, ast.Name):
        return bindings.get(node.id, node.id)
    if isinstance(node, ast.Attribute):
        return f"{_qualified_name(node.value, bindings)}.{node.attr}"
    return ""


def _attribute_violations(node: ast.Attribute, bindings: dict[str, str]) -> list[str]:
    if _qualified_name(node, bindings) == "unittest.mock":
        return ["unittest.mock attribute access"]
    return []


def _module_argument(node: ast.Call) -> ast.AST | None:
    if node.args:
        return node.args[0]
    return next((argument.value for argument in node.keywords if argument.arg == "name"), None)


def _dynamic_import_violations(node: ast.Call, bindings: dict[str, str]) -> list[str]:
    importers = ("importlib.import_module", "__import__", "builtins.__import__")
    if _qualified_name(node.func, bindings) not in importers:
        return []
    argument = _module_argument(node)
    if not isinstance(argument, ast.Constant) or not isinstance(argument.value, str):
        return []
    return [f"dynamic import {argument.value}"] if _is_forbidden_module(argument.value) else []


def _node_violations(node: ast.AST, bindings: dict[str, str]) -> list[str]:
    if isinstance(node, ast.Import):
        return _import_violations(node)
    if isinstance(node, ast.ImportFrom):
        return _import_from_violations(node)
    if isinstance(node, ast.Attribute):
        return _attribute_violations(node, bindings)
    if isinstance(node, ast.Call):
        return _dynamic_import_violations(node, bindings)
    return []


def find_mock_usage(source: str, filename: str) -> list[tuple[int, str]]:
    """Return ``(line, description)`` for every forbidden mock usage in ``source``."""
    tree = ast.parse(source, filename=filename)
    bindings: dict[str, str] = {}
    for node in ast.walk(tree):
        bindings.update(_import_bindings(node))
    findings: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        line = getattr(node, "lineno", 0)
        findings.extend((line, message) for message in _node_violations(node, bindings))
    return findings


def policed_files() -> list[Path]:
    """Every file the policy applies to; nothing under ``tests/`` is exempt."""
    return sorted([*TESTS_DIR.rglob("*.py"), REPOSITORY_ROOT / "conftest.py"])


def test_no_test_code_uses_unittest_mock() -> None:
    violations = {
        path.relative_to(REPOSITORY_ROOT).as_posix(): findings
        for path in policed_files()
        if (findings := find_mock_usage(path.read_text(encoding="utf-8"), str(path)))
    }

    assert violations == {}, "\n".join(
        f"{path}:{line}: {message}"
        for path, findings in violations.items()
        for line, message in findings
    )


def test_policy_scans_its_own_file_without_self_triggering() -> None:
    own_file = Path(__file__).resolve()

    assert own_file in policed_files()
    assert find_mock_usage(own_file.read_text(encoding="utf-8"), str(own_file)) == []


@pytest.mark.parametrize(
    "source",
    [
        "from unittest.mock import patch",
        "from unittest.mock import AsyncMock as Fake",
        "from unittest import mock",
        "from unittest import mock as aliased",
        "import unittest.mock",
        "import unittest.mock as aliased",
        "from unittest.mock.support import thing",
        "import mock",
        "import mock as aliased",
        "from mock import MagicMock",
        "import unittest\nunittest.mock.patch('target')",
        "import unittest\nspy = unittest.mock.MagicMock()",
        "import unittest as ut\nspy = ut.mock.AsyncMock()",
        "import importlib\nimportlib.import_module('unittest.mock')",
        "import importlib as il\nil.import_module('mock')",
        "from importlib import import_module as load\nload('unittest.mock')",
        "import importlib\nimportlib.import_module(name='unittest.mock')",
        "__import__('unittest.mock')",
        "import builtins as bi\nbi.__import__('mock')",
    ],
    ids=lambda source: source.replace("\n", "; "),
)
def test_policy_rejects_every_mock_spelling(source: str) -> None:
    assert len(find_mock_usage(source, "<fixture>")) == 1


@pytest.mark.parametrize(
    "source",
    [
        "import pytest\n\ndef test_it(monkeypatch: pytest.MonkeyPatch) -> None:\n    pass",
        "import httpx\ntransport = httpx.MockTransport(handler)",
        "from fakeredis import FakeAsyncRedis",
        "FORBIDDEN = ('unittest.mock', 'mock', 'from unittest import mock')",
        "import unittest\n\nclass Suite(unittest.TestCase):\n    pass",
        "import mockingbird\nfrom mockups import sketch",
        "mock_data = {'mock': True}\nunittest = object()\nvalue = unittest.name",
        "import importlib\nimportlib.import_module('json')",
    ],
    ids=lambda source: source.replace("\n", "; ")[:60],
)
def test_policy_allows_monkeypatch_transports_and_strings(source: str) -> None:
    assert find_mock_usage(source, "<fixture>") == []
