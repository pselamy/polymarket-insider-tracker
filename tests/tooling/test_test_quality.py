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


def _attribute_violations(node: ast.Attribute) -> list[str]:
    if node.attr == "mock" and isinstance(node.value, ast.Name) and node.value.id == "unittest":
        return ["unittest.mock attribute access"]
    return []


def _node_violations(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Import):
        return _import_violations(node)
    if isinstance(node, ast.ImportFrom):
        return _import_from_violations(node)
    if isinstance(node, ast.Attribute):
        return _attribute_violations(node)
    return []


def find_mock_usage(source: str, filename: str) -> list[tuple[int, str]]:
    """Return ``(line, description)`` for every forbidden mock usage in ``source``."""
    tree = ast.parse(source, filename=filename)
    findings: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        line = getattr(node, "lineno", 0)
        findings.extend((line, message) for message in _node_violations(node))
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
        "import importlib\nimportlib.import_module('unittest.mock')",
    ],
    ids=lambda source: source.replace("\n", "; ")[:60],
)
def test_policy_allows_monkeypatch_transports_and_strings(source: str) -> None:
    assert find_mock_usage(source, "<fixture>") == []
