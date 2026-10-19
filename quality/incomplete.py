"""Reject unfinished or disabled authored Python without scanning string fixtures."""

from __future__ import annotations

import ast
import io
import re
import tokenize
from pathlib import Path

from quality.source_scope import verify_sources

MARKERS = re.compile(r"\b(?:TODO|FIXME|XXX)\b", re.IGNORECASE)
SUPPRESSIONS = re.compile(
    r"\b(?:noqa|nosec|type:\s*ignore|pyright:\s*ignore|mypy:\s*ignore-errors|"
    r"pragma:\s*no\s*(?:cover|branch)|ruff:\s*noqa|fmt:\s*off|semgrep:\s*ignore)\b",
    re.IGNORECASE,
)
FORBIDDEN = {
    "breakpoint",
    "builtins.breakpoint",
    "pdb.set_trace",
    "pytest.set_trace",
    "pytest.skip",
    "pytest.xfail",
    "unittest.skip",
    "unittest.skipIf",
    "unittest.skipUnless",
    "unittest.expectedFailure",
    "pytest.mark.skip",
    "pytest.mark.skipif",
    "pytest.mark.xfail",
}
APPROVED_CASTS = {
    (
        "quality/document_style.py",
        "cast(Callable[..., str], formatter)",
    ): "Pinned external formatter API",
    (
        "quality/mutation_config.py",
        "cast(Callable[[object], str], module.serialize_config)",
    ): "Pinned native serializer API",
    (
        "quality/mutation_coordinator.py",
        "cast(Callable[..., None], module.cli)",
    ): "Pinned native CLI API",
    (
        "quality/mutation_worker.py",
        "cast(Handler, module.handle_mutate_and_test)",
    ): "Pinned native worker API",
    (
        "quality/mutation_workspace.py",
        "cast(BaseExceptionGroup[BaseException], error)",
    ): "Runtime exception-group narrowing",
    ("quality/runtime_data.py", "cast(YamlAPI, yaml)"): "Pinned YAML adapter API",
}


def module_aliases(node: ast.Import) -> dict[str, str]:
    """Map whole-module imports and their optional local aliases."""
    return {item.asname or item.name.split(".")[0]: item.name for item in node.names}


def member_aliases(node: ast.ImportFrom) -> dict[str, str]:
    """Map named imports; relative imports cannot name forbidden upstream APIs."""
    if node.module is None:
        return {}
    return {item.asname or item.name: f"{node.module}.{item.name}" for item in node.names}


def aliases(node: ast.AST) -> dict[str, str]:
    """Map one import's local bindings to their named upstream API."""
    if isinstance(node, ast.Import):
        return module_aliases(node)
    if isinstance(node, ast.ImportFrom):
        return member_aliases(node)
    return {}


def imported_names(tree: ast.AST) -> dict[str, str]:
    """Resolve direct imported aliases before checking debugger and test markers."""
    names: dict[str, str] = {}
    for node in ast.walk(tree):
        names.update(aliases(node))
    return names


def symbol(node: ast.expr, names: dict[str, str]) -> str | None:
    """Return the statically named API, including imported aliases."""
    if isinstance(node, ast.Name):
        return names.get(node.id, node.id)
    if isinstance(node, ast.Attribute):
        base = symbol(node.value, names)
        return f"{base}.{node.attr}" if base else None
    return None


def getattr_arguments(node: ast.Call) -> tuple[ast.expr, ast.expr] | None:
    """Recognize the common literal dynamic-attribute bypass shape."""
    if not isinstance(node.func, ast.Name) or node.func.id not in {"getattr"}:
        return None
    if len(node.args) not in (2, 3):
        return None
    return node.args[0], node.args[1]


def dynamic_api(node: ast.Call, names: dict[str, str]) -> str | None:
    """Resolve literal getattr on a statically named protected API."""
    pair = getattr_arguments(node)
    if pair is None or not isinstance(pair[1], ast.Constant):
        return None
    base = symbol(pair[0], names)
    name = pair[1].value
    return f"{base}.{name}" if base and isinstance(name, str) else None


def implementation_body(
    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
) -> list[ast.stmt]:
    """A leading docstring cannot make an otherwise empty body complete."""
    body = node.body
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        return body[1:]
    return body


def raises_unimplemented(node: ast.AST) -> bool:
    """Explicit unimplemented raises are never a complete implementation."""
    if isinstance(node, ast.Raise):
        exc = node.exc.func if isinstance(node.exc, ast.Call) else node.exc
        return isinstance(exc, ast.Name) and exc.id in {"NotImplementedError"}
    return False


def empty_body(node: ast.AST) -> bool:
    """A sole pass or ellipsis, with an optional docstring, is a placeholder."""
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return False
    body = implementation_body(node)
    if len(body) != 1:
        return False
    sole = next(iter(body))
    return isinstance(sole, ast.Pass) or (
        isinstance(sole, ast.Expr)
        and isinstance(sole.value, ast.Constant)
        and sole.value.value is Ellipsis
    )


def unfinished(node: ast.AST) -> bool:
    """Catch empty definitions and explicit NotImplementedError raises."""
    return raises_unimplemented(node) or empty_body(node)


def comments(source: str, path: Path) -> None:
    """Inspect lexical comments, leaving examples inside strings intact."""
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type in {tokenize.COMMENT} and (
            MARKERS.search(token.string) or SUPPRESSIONS.search(token.string)
        ):
            raise ValueError(f"{path}:{token.start[0]}: unfinished or suppressed comment")


def api(node: ast.AST, names: dict[str, str]) -> str | None:
    """Read a call target or a bare decorator/reference."""
    if isinstance(node, ast.Call):
        return dynamic_api(node, names) or symbol(node.func, names)
    if isinstance(node, (ast.Attribute, ast.Name)):
        return symbol(node, names)
    return None


def reject_any(node: ast.AST, names: dict[str, str], location: str) -> None:
    """Reject statically named explicit Any, including import aliases."""
    if isinstance(node, (ast.Name, ast.Attribute)) and symbol(node, names) in {
        "typing.Any",
        "typing_extensions.Any",
    }:
        raise ValueError(f"{location}: explicit Any is unsupported")


def reject_unapproved_cast(node: ast.AST, names: dict[str, str], path: Path, location: str) -> None:
    """Bind each existing third-party cast to its reviewed source expression."""
    if isinstance(node, ast.Call) and symbol(node.func, names) in {
        "typing.cast",
        "typing_extensions.cast",
    }:
        key = ("/".join(path.parts[-2:]), ast.unparse(node))
        if key not in APPROVED_CASTS:
            raise ValueError(f"{location}: unapproved type cast")


def verify_node(node: ast.AST, names: dict[str, str], path: Path) -> None:
    """Reject unfinished bodies, debug APIs and type escape hatches."""
    location = f"{path}:{getattr(node, 'lineno', 0)}"
    if unfinished(node):
        raise ValueError(f"{location}: incomplete implementation")
    if api(node, names) in FORBIDDEN:
        raise ValueError(f"{location}: debug or disabled-test API")
    reject_any(node, names, location)
    reject_unapproved_cast(node, names, path, location)


def verify_file(path: Path) -> None:
    """Reject static debugger/test-disable APIs and unfinished definitions."""
    source = path.read_text(encoding="utf-8")
    comments(source, path)
    tree = ast.parse(source, filename=str(path))
    names = imported_names(tree)
    for node in ast.walk(tree):
        verify_node(node, names, path)


def verify_incomplete(root: Path) -> None:
    """Use the complete independent authored-source inventory."""
    for path in verify_sources(root):
        verify_file(path)
