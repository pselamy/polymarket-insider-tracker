"""Require the explicit public product file inventory and matching source bytes."""

from pathlib import Path

from quality.package_archive import sdist_files, wheel_files
from quality.package_metadata import identity, metadata, verify_record, wheel_metadata

MODULE = "relentless_example"
SOURCE_FILES = (
    "__init__.py",
    "cli.py",
    "normalization.py",
    "validation.py",
    "py.typed",
)
ENTRY_POINTS = b"[console_scripts]\nrelentless-example = relentless_example.cli:main\n"


def public_sources(root: Path) -> dict[str, bytes]:
    paths = [
        path
        for path in (root / "src").rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    ]
    files = {path.relative_to(root / "src").as_posix(): path.read_bytes() for path in paths}
    inventory(files, {f"{MODULE}/{name}" for name in SOURCE_FILES})
    return files


def inventory(actual: dict[str, bytes], wanted: set[str]) -> None:
    if set(actual) != wanted:
        raise ValueError("package archive file inventory differs from public policy")


def source_bytes(actual: dict[str, bytes], expected: dict[str, bytes]) -> None:
    for name, data in expected.items():
        if actual[name] != data:
            raise ValueError("archived file bytes differ from public source")


def verify_wheel(path: Path, root: Path, project: dict[str, object]) -> None:
    files = wheel_files(path)
    info = identity(project) + ".dist-info/"
    expected = public_sources(root)
    expected[info + "licenses/LICENSE"] = (root / "LICENSE").read_bytes()
    expected[info + "entry_points.txt"] = ENTRY_POINTS
    generated = {info + name for name in ("METADATA", "WHEEL", "RECORD")}
    inventory(files, set(expected).union(generated))
    source_bytes(files, expected)
    metadata(files[info + "METADATA"], project)
    wheel_metadata(files[info + "WHEEL"])
    verify_record(files, info + "RECORD")


def verify_sdist(path: Path, root: Path, project: dict[str, object]) -> None:
    prefix = identity(project) + "/"
    files = sdist_files(path)
    expected = {prefix + "src/" + name: data for name, data in public_sources(root).items()}
    for name in ("README.md", "LICENSE", "pyproject.toml"):
        expected[prefix + name] = (root / name).read_bytes()
    inventory(files, set(expected).union({prefix + "PKG-INFO"}))
    source_bytes(files, expected)
    metadata(files[prefix + "PKG-INFO"], project)
