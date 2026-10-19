"""Freeze every authored Python input before a native duplication scan."""

import re
from pathlib import Path

from quality.source_scope import verify_sources

SUPPRESSION = re.compile(r"jscpd\s*:\s*ignore-(?:start|end)", re.I)


def duplication_inputs(root: Path) -> dict[str, bytes]:
    """Read enrolled source independently of scanner globs and ignore rules."""
    inputs: dict[str, bytes] = {}
    for path in verify_sources(root):
        content = path.read_bytes()
        text = content.decode("utf-8")
        if SUPPRESSION.search(text):
            raise ValueError(f"inline duplication suppression is unsupported: {path}")
        inputs[path.relative_to(root).as_posix()] = content
    return inputs


def stage_inputs(inputs: dict[str, bytes], stage: Path) -> list[Path]:
    """Copy frozen bytes so scanner settings cannot hide repository files."""
    paths: list[Path] = []
    for name, content in inputs.items():
        path = stage / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        paths.append(path)
    return paths


def verify_inputs(before: dict[str, bytes], after: dict[str, bytes]) -> None:
    """Reject any source insertion, deletion, or byte change during scanning."""
    if before != after:
        raise ValueError("authored inputs changed during duplication scan")
