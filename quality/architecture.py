"""Check complete native import graphs and declared distribution boundaries."""

import importlib
import sys
import tomllib
from collections.abc import Mapping
from importlib.metadata import packages_distributions
from pathlib import Path

import grimp
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from quality.report_data import array, record, text
from quality.source_scope import verify_sources

SOURCE_DIRECTORY = "src"
SOURCE_ROOTS = frozenset({SOURCE_DIRECTORY})
INITIALIZER = "__init__"


def module_name(root: Path, path: Path) -> str:
    """Production uses a src layout; tooling and tests use root packages."""
    parts = list(path.relative_to(root).with_suffix("").parts)
    if parts[0] in SOURCE_ROOTS:
        parts.pop(0)
    if parts[-1] == INITIALIZER:
        parts.pop()
    return ".".join(parts)


def owned_modules(root: Path, paths: list[Path]) -> dict[str, str]:
    """Reject ambiguous names and shapes the native analyzer cannot enroll."""
    modules: dict[str, str] = {}
    for path in paths:
        name = module_name(root, path)
        if not name or name in modules:
            raise ValueError("architecture module names must be nonempty and unique")
        modules[name] = path.relative_to(root).parts[0]
    return modules


def declared_dependencies(root: Path) -> set[str]:
    """Only runtime declarations grant production access to installed packages."""
    value: object = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    project = record(record(value)["project"])
    return {
        canonicalize_name(Requirement(text(item)).name)
        for item in array(project.get("dependencies", []))
    }


def tooling_dependencies(root: Path, runtime: set[str]) -> set[str]:
    """All PEP 735 dependency groups are available to development modules."""
    value: object = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    groups = record(record(value).get("dependency-groups", {}))
    allowed = runtime.copy()
    for group in groups.values():
        for item in array(group):
            allowed.add(canonicalize_name(Requirement(text(item)).name))
    return allowed


def native_graph(root: Path, modules: dict[str, str]) -> grimp.ImportGraph:
    """Keep the native graph uncached and verify every expected module appears."""
    roots = sorted({name.split(".")[0] for name in modules})
    if not roots:
        raise ValueError("architecture inventory is empty")
    original = sys.path.copy()
    try:
        sys.path[:0] = [str(root / "src"), str(root)]
        importlib.invalidate_caches()
        graph = grimp.build_graph(
            *roots,
            include_external_packages=True,
            exclude_type_checking_imports=False,
            cache_dir=None,
        )
    finally:
        sys.path[:] = original
    actual = {name for name in graph.modules if not graph.is_module_squashed(name)}
    if actual != set(modules):
        raise ValueError("native architecture source inventory is incomplete")
    return graph


def verify_edge(
    graph: grimp.ImportGraph,
    modules: dict[str, str],
    importer: str,
    imported: str,
    runtime: set[str],
    distributions: Mapping[str, list[str]],
) -> None:
    """Include type-checking edges and cycles across separate root packages."""
    if imported in modules and graph.chain_exists(imported, importer):
        raise ValueError(f"dependency cycle: {importer} -> {imported}")
    if imported in modules:
        verify_boundary(modules, importer, imported)
        return
    verify_external(importer, imported, runtime, distributions)


def verify_boundary(modules: dict[str, str], importer: str, imported: str) -> None:
    """Tests and verifier modules may import production; the reverse is forbidden."""
    if modules[importer] in SOURCE_ROOTS and modules[imported] not in SOURCE_ROOTS:
        raise ValueError(f"production imports tooling: {importer} -> {imported}")


def verify_external(
    importer: str,
    imported: str,
    runtime: set[str],
    distributions: Mapping[str, list[str]],
) -> None:
    """Static imports must belong to stdlib or declared direct dependencies."""
    package = imported.split(".")[0]
    if package in sys.stdlib_module_names:
        return
    providers = {canonicalize_name(name) for name in distributions.get(package, [])}
    if not providers.intersection(runtime):
        raise ValueError(f"undeclared runtime dependency: {importer} -> {imported}")


def verify_architecture(root: Path) -> None:
    """Complete discovered scope, native cycle analysis and production boundaries."""
    modules = owned_modules(root, verify_sources(root))
    graph = native_graph(root, modules)
    runtime = declared_dependencies(root)
    tooling = tooling_dependencies(root, runtime)
    distributions = packages_distributions()
    for importer in modules:
        allowed = runtime if modules[importer] in SOURCE_ROOTS else tooling
        for imported in graph.find_modules_directly_imported_by(importer):
            verify_edge(graph, modules, importer, imported, allowed, distributions)
