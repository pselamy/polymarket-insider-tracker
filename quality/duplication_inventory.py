"""Validate one native scanner inventory against a frozen Python file."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TypeGuard

from quality.report_data import array, record, text


@dataclass(frozen=True)
class Eligible:
    bytes: int
    lines: int
    tokens: int


def plain_integer(value: object) -> TypeGuard[int]:
    """JSON bool is an int subclass but cannot be a native count."""
    return type(value) in {int}


def nonnegative(value: object) -> int:
    """JSON booleans and fractional numbers are not inventory counts."""
    if not plain_integer(value) or value < 0:
        raise ValueError("duplication inventory count must be a nonnegative integer")
    return value


def verify_fields(file: dict[str, object]) -> None:
    """Reject ambiguous or incomplete native file metadata."""
    expected = {
        "bytes",
        "complexity",
        "duplicatedLines",
        "duplicatedTokens",
        "format",
        "lines",
        "path",
        "tokens",
    }
    if set(file) != expected:
        raise ValueError("duplication inventory file fields differ")
    for field in ("complexity", "duplicatedLines", "duplicatedTokens"):
        nonnegative(file[field])


def verify_identity(file: dict[str, object], path: Path, content: bytes) -> None:
    """Bind reported path and byte length to the staged source."""
    if text(file["path"]) != str(path) or file["format"] not in ("python",):
        raise ValueError("duplication inventory substituted source identity")
    if nonnegative(file["bytes"]) != len(content):
        raise ValueError("duplication inventory substituted source bytes")


def item(value: object, path: Path, content: bytes) -> Eligible:
    """The sole reported file must be the exact staged input."""
    file = record(value)
    verify_fields(file)
    verify_identity(file, path, content)
    lines = nonnegative(file["lines"])
    tokens = nonnegative(file["tokens"])
    if not lines or not tokens:
        raise ValueError("duplication inventory reported empty source metrics")
    return Eligible(len(content), lines, tokens)


def inventory_files(value: object) -> list[object]:
    """Require one complete per-file native summary."""
    summary = record(record(value)["summary"])
    if summary["by"] not in ("tokens",):
        raise ValueError("duplication inventory summary measure changed")
    files = array(summary["files"])
    count = len(files)
    reported = nonnegative(summary["totalFiles"])
    if (count, reported) not in ((0, 0), (1, 1)):
        raise ValueError("duplication inventory totals disagree")
    return files


def empty_inventory(content: bytes) -> None:
    """Only a genuinely blank input may be absent from low-threshold output."""
    if content.decode("utf-8").strip():
        raise ValueError("duplication inventory omitted nonempty source")


def duplication_inventory(value: object, path: Path, content: bytes) -> Eligible | None:
    """A nonempty authored file cannot disappear from the native report."""
    files = inventory_files(value)
    if not files:
        empty_inventory(content)
        return None
    return item(next(iter(files)), path, content)
