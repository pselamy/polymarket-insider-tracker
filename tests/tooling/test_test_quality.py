"""Regression test enforcing prohibition of unittest.mock across test files."""

import ast
from pathlib import Path


def _check_import_node(node: ast.Import) -> list[str]:
    """Check standard import statement for mock imports."""
    forbidden = ("unittest.mock", "mock")
    matches = [a.name for a in node.names if a.name in forbidden]
    return [f"import {name}" for name in matches]


def _check_unittest_subimport(node: ast.ImportFrom) -> list[str]:
    """Check from unittest import ... for mock submodule."""
    if node.module != "unittest":
        return []
    matches = [a.name for a in node.names if a.name == "mock"]
    return ["from unittest import mock"] if matches else []


def _check_import_from_node(node: ast.ImportFrom) -> list[str]:
    """Check from ... import statement for mock imports."""
    if node.module in ("unittest.mock", "mock"):
        names = ", ".join(a.name for a in node.names)
        return [f"from {node.module} import {names}"]
    return _check_unittest_subimport(node)


def _node_violations(node: ast.AST) -> list[str]:
    """Return violation messages for an AST import node."""
    if isinstance(node, ast.Import):
        return _check_import_node(node)
    if isinstance(node, ast.ImportFrom):
        return _check_import_from_node(node)
    return []


def _find_mock_imports_in_file(path: Path) -> list[tuple[int, str]]:
    """Scan a Python file for forbidden unittest.mock imports using AST."""
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    results: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        msgs = _node_violations(node)
        lineno = getattr(node, "lineno", 0)
        results.extend((lineno, msg) for msg in msgs)
    return results


def _collect_file_violations(
    file_path: Path, tests_dir: Path, out: dict[str, list[tuple[int, str]]]
) -> None:
    """Record violations for a single file if present."""
    if file_path.name == "test_test_quality.py":
        return
    viols = _find_mock_imports_in_file(file_path)
    if viols:
        rel = str(file_path.relative_to(tests_dir.parent))
        out[rel] = viols


def _format_violation_lines(path_key: str, viols: list[tuple[int, str]]) -> list[str]:
    """Format violations for one path."""
    return [f"  {path_key}:{line_no}: {stmt}" for line_no, stmt in viols]


def _format_error_message(violations: dict[str, list[tuple[int, str]]]) -> str:
    """Format comprehensive error message for test failure."""
    lines: list[str] = [f"Found {len(violations)} test files importing unittest.mock:"]
    for path_key, viols in violations.items():
        lines.extend(_format_violation_lines(path_key, viols))
    return "\n".join(lines)


def test_no_test_files_import_unittest_mock() -> None:
    """Every test file must use working fakes and real values rather than unittest.mock."""
    tests_dir = Path(__file__).resolve().parent.parent
    violations: dict[str, list[tuple[int, str]]] = {}
    for path in sorted(tests_dir.rglob("*.py")):
        _collect_file_violations(path, tests_dir, violations)

    assert not violations, _format_error_message(violations)
