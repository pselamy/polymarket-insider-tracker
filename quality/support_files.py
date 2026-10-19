"""Validate every authored support format and optional shell source."""

import configparser
import hashlib
import json
import re
import tomllib
from collections.abc import Iterator
from pathlib import Path

from quality.pipeline import command
from quality.runtime_data import read_yaml, unique_object
from quality.source_scope import GENERATED_ROOTS
from quality.workflows import policy, tool

FORMATS = {".json", ".toml", ".yml", ".yaml", ".ini", ".sh"}
TOML_LOCKS = {"mise.lock", "uv.lock"}
SHELL_SUPPRESSION = re.compile(r"#\s*shellcheck\s+disable\b", re.IGNORECASE)


def authored(root: Path, directory: Path) -> Iterator[Path]:
    """Ignore only protected generated roots; reject authored links and odd files."""
    for path in directory.iterdir():
        if not directory.relative_to(root).parts and path.name in GENERATED_ROOTS:
            continue
        yield from child(root, path)


def child(root: Path, path: Path) -> Iterator[Path]:
    """Traverse real authored paths without trusting symlinked directories."""
    if path.is_symlink():
        raise ValueError(f"authored support path is a symlink: {path}")
    if path.is_dir():
        yield from authored(root, path)
    elif path.is_file():
        yield path
    else:
        raise ValueError(f"unsupported authored support entry: {path}")


def format_of(path: Path) -> str:
    """Both native lockfiles use TOML syntax despite their suffix."""
    return ".toml" if path.name in TOML_LOCKS else path.suffix.lower()


def invalid_constant(name: str) -> object:
    """Python's permissive JSON NaN extension is not valid JSON policy data."""
    raise ValueError(f"support JSON contains nonfinite constant: {name}")


def read_ini(source: str) -> None:
    """Use strict duplicate-section/key checks without interpolation."""
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    parser.read_string(source)
    if not parser.sections() and not parser.defaults():
        raise ValueError("support INI is empty")


def parse(path: Path, source: str) -> None:
    """Select native strict syntax semantics for one enrolled support file."""
    kind = format_of(path)
    if kind in {".json"}:
        json.loads(source, object_pairs_hook=unique_object, parse_constant=invalid_constant)
    elif kind in {".toml"}:
        tomllib.loads(source)
    elif kind in {".yml", ".yaml"}:
        read_yaml(path)
    elif kind in {".ini"}:
        read_ini(source)


def shell(root: Path, paths: list[Path]) -> None:
    """Native ShellCheck is required whenever standalone shell appears."""
    if not paths:
        return
    for path in paths:
        if SHELL_SUPPRESSION.search(path.read_text(encoding="utf-8")):
            raise ValueError("inline ShellCheck suppression is unsupported")
    commands = policy(root)
    version = tool(root, command(commands["shellcheck"]))
    if f'version: {commands["shellcheckVersion"]}' not in version.splitlines():
        raise ValueError("required ShellCheck version receipt is missing or wrong")
    tool(root, ["shellcheck", "--norc", *(str(path) for path in paths)])


def prepare_output(root: Path) -> Path:
    """A stale or redirected receipt cannot satisfy this invocation."""
    parent = root / ".quality-results"
    output = parent / "support-files.json"
    if parent.is_symlink() or output.is_symlink():
        raise ValueError("support report directory may not be a symlink")
    output.unlink(missing_ok=True)
    return output


def support_inputs(root: Path) -> dict[Path, bytes]:
    """Freeze all independently discovered support-file bytes."""
    paths = sorted(path for path in authored(root, root) if format_of(path) in FORMATS)
    if not paths:
        raise ValueError("support file inventory must be nonempty")
    return {path: path.read_bytes() for path in paths}


def verify_formats(contents: dict[Path, bytes]) -> None:
    """All present config files must parse from strict UTF-8 bytes."""
    for path, content in contents.items():
        parse(path, content.decode("utf-8"))


def verify_unchanged(contents: dict[Path, bytes]) -> None:
    """A concurrent source edit invalidates the report."""
    if any(path.read_bytes() != content for path, content in contents.items()):
        raise ValueError("support inputs changed while being checked")


def receipt(root: Path, contents: dict[Path, bytes]) -> str:
    """Bind every validated file's identity to its exact original bytes."""
    return (
        json.dumps(
            {
                "files": {
                    str(path.relative_to(root)): hashlib.sha256(content).hexdigest()
                    for path, content in contents.items()
                }
            }
        )
        + "\n"
    )


def verify_support(root: Path) -> None:
    """Write a source-bound receipt only after all formats and shell pass."""
    output = prepare_output(root)
    contents = support_inputs(root)
    verify_formats(contents)
    shell(root, [path for path in contents if format_of(path) in {".sh"}])
    verify_unchanged(contents)
    output.parent.mkdir(exist_ok=True)
    output.write_text(receipt(root, contents))
