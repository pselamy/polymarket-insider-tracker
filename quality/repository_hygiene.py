"""Reject nonportable or unresolved authored repository contents."""

import re
import subprocess
from pathlib import Path

from quality.portable_paths import verify_path_names
from quality.source_scope import GENERATED_ROOTS

TEXT_SUFFIXES = {
    ".ignore",
    ".json",
    ".lock",
    ".md",
    ".py",
    ".toml",
    ".txt",
    ".typed",
    ".yaml",
    ".yml",
}
TEXT_NAMES = {".gitattributes", ".gitignore", ".github/CODEOWNERS", "LICENSE"}
GENERATED_ENTRIES = {".coverage", *GENERATED_ROOTS}
CONFLICT = re.compile(r"^(?:<{7,}|={7,}|>{7,}|\|{7,})(?:[ \t\r]|$)", re.MULTILINE)


def git_paths(root: Path) -> list[str]:
    """Read complete native NUL-delimited index paths."""
    data = subprocess.check_output(["git", "ls-files", "-z"], cwd=root)
    if not data or not data.endswith(b"\0"):
        raise ValueError("native Git path inventory is empty or incomplete")
    return data[:-1].decode("utf-8").split("\0")


def disk_files(root: Path, directory: Path, top_level: bool) -> list[str]:
    """Include ignored authored text while omitting exact generated roots."""
    names: list[str] = []
    for path in directory.iterdir():
        if top_level and path.name in GENERATED_ENTRIES:
            continue
        names.extend(walk_entry(root, path))
    return names


def walk_entry(root: Path, path: Path) -> list[str]:
    """Reject authored links and unsupported kinds before recursion."""
    if path.is_symlink():
        raise ValueError(f"authored symlink is unsupported: {path}")
    if path.is_dir():
        return [] if path.name in {"__pycache__"} else disk_files(root, path, top_level=False)
    if path.is_file():
        return [path.relative_to(root).as_posix()]
    raise ValueError(f"unsupported authored file kind: {path}")


def verify_text_kind(name: str) -> None:
    """Require every public artifact type to be enrolled explicitly."""
    if Path(name).suffix not in TEXT_SUFFIXES and name not in TEXT_NAMES:
        raise ValueError(f"unsupported authored text type: {name}")


def verify_text(data: bytes) -> None:
    """Reject binary data, invalid UTF-8 and unresolved merge markers."""
    value = data.decode("utf-8")
    if "\0" in value or CONFLICT.search(value):
        raise ValueError("repository text contains binary data or conflict markers")


def verify_repository(root: Path) -> None:
    """Validate native index and filesystem contents from the same root."""
    indexed = git_paths(root)
    verify_path_names(indexed)
    names = sorted(set(indexed + disk_files(root=root, directory=root, top_level=True)))
    verify_path_names(names)
    for name in names:
        verify_text_kind(name)
        verify_text((root / name).read_bytes())
