"""Restricted native metadata. These values confer no scan or approval authority."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class NativeReportError(ValueError):
    """A constant-category failure that must not render native input."""


class ScanMode(StrEnum):
    """The two separately required native scan modes."""

    TREE = "dir"
    HISTORY = "git"


@dataclass(frozen=True, slots=True)
class NativeFinding:
    """Exact untrusted metadata, with source-controlled text hidden from repr."""

    mode: ScanMode
    native_commit: str = field(repr=False)
    path: str = field(repr=False)
    rule_id: str = field(repr=False)
    native_fingerprint: str = field(repr=False)
    start_line: int
    end_line: int
    start_column: int
    end_column: int
    tags: tuple[str, ...] = field(repr=False)


@dataclass(frozen=True, slots=True)
class NativeReport:
    """Parsed bytes and ordered findings; not authenticated execution or acceptance."""

    mode: ScanMode
    sha256: str
    findings: tuple[NativeFinding, ...] = field(repr=False)
