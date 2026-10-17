"""Qualified native8.30.1 schema. Text remains restricted identity data."""

from __future__ import annotations

import math
import re
import unicodedata
from typing import cast

from scripts._secret_models import NativeFinding, NativeReportError, ScanMode

TEXT_FIELDS = frozenset(
    {
        "RuleID",
        "Description",
        "Match",
        "Secret",
        "File",
        "SymlinkFile",
        "Commit",
        "Author",
        "Email",
        "Date",
        "Message",
        "Fingerprint",
    }
)
COORDINATE_FIELDS = frozenset({"StartLine", "EndLine", "StartColumn", "EndColumn"})
REQUIRED_FIELDS = TEXT_FIELDS | COORDINATE_FIELDS | {"Entropy", "Tags"}
ALLOWED_FIELDS = REQUIRED_FIELDS | {"Link"}
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise NativeReportError("finding object schema") from None
    data = cast(dict[str, object], value)
    if REQUIRED_FIELDS - data.keys() or data.keys() - ALLOWED_FIELDS:
        raise NativeReportError("finding field schema") from None
    return data


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise NativeReportError("finding text schema") from None
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise NativeReportError("finding text encoding") from None
    return value


def _integer(value: object) -> int:
    if type(value) is not int:
        raise NativeReportError("coordinate integer schema") from None
    if value < 1:
        raise NativeReportError("unsupported native coordinate") from None
    return value


def _entropy(value: object) -> None:
    if type(value) not in (int, float):
        raise NativeReportError("entropy number schema") from None
    try:
        finite = math.isfinite(cast(int | float, value))
    except OverflowError:
        raise NativeReportError("entropy number limit") from None
    if not finite:
        raise NativeReportError("nonfinite entropy") from None


def _tags(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise NativeReportError("tags array schema") from None
    return tuple(_text(tag) for tag in cast(list[object], value))


def _path(value: str) -> None:
    if value.startswith("/") or "\\" in value:
        raise NativeReportError("native relative path") from None
    if set(value.split("/")) & {"", ".", ".."}:
        raise NativeReportError("native path component") from None
    if any(unicodedata.category(char) == "Cc" for char in value):
        raise NativeReportError("native path control character") from None


def _coordinates(finding: NativeFinding) -> None:
    if finding.end_line < finding.start_line:
        raise NativeReportError("native line order") from None
    if finding.start_line == finding.end_line and finding.end_column < finding.start_column:
        raise NativeReportError("native column order") from None


def _commit(finding: NativeFinding) -> None:
    if finding.mode is ScanMode.HISTORY:
        if COMMIT_PATTERN.fullmatch(finding.native_commit) is None:
            raise NativeReportError("native history commit") from None
    elif finding.native_commit:
        raise NativeReportError("native tree commit") from None


def _identity(finding: NativeFinding) -> None:
    _commit(finding)
    prefix = f"{finding.native_commit}:" if finding.mode is ScanMode.HISTORY else ""
    expected = f"{prefix}{finding.path}:{finding.rule_id}:{finding.start_line}"
    if finding.native_fingerprint != expected:
        raise NativeReportError("native fingerprint mismatch") from None


def _native_text(data: dict[str, object]) -> dict[str, str]:
    text = {key: _text(data[key]) for key in TEXT_FIELDS}
    if "Link" in data:
        _text(data["Link"])
    _enrolled_text(text)
    return text


def _enrolled_text(text: dict[str, str]) -> None:
    if not text["RuleID"] or not text["Fingerprint"]:
        raise NativeReportError("empty native identity") from None
    if text["SymlinkFile"] or text["Secret"] != "REDACTED":
        raise NativeReportError("unsupported or unredacted finding") from None


def parse_finding(value: object, mode: ScanMode) -> NativeFinding:
    """Keep exact restricted metadata; discard all source prose/Match/Secret."""
    data = _mapping(value)
    text = _native_text(data)
    _entropy(data["Entropy"])
    finding = NativeFinding(
        mode=mode,
        native_commit=text["Commit"],
        path=text["File"],
        rule_id=text["RuleID"],
        native_fingerprint=text["Fingerprint"],
        start_line=_integer(data["StartLine"]),
        end_line=_integer(data["EndLine"]),
        start_column=_integer(data["StartColumn"]),
        end_column=_integer(data["EndColumn"]),
        tags=_tags(data["Tags"]),
    )
    _path(finding.path)
    _coordinates(finding)
    _identity(finding)
    return finding


def native_path(value: object) -> str:
    """Validate a direct source-reader path using the same native report contract."""
    text = _text(value)
    _path(text)
    return text


def _finding_mode(value: object) -> None:
    if not isinstance(value, ScanMode):
        raise NativeReportError("invalid native mode") from None


def _finding_tags(value: object) -> None:
    if not isinstance(value, tuple):
        raise NativeReportError("native identity tags") from None
    for tag in cast(tuple[object, ...], value):
        _text(tag)


def validate_native_finding(value: object) -> NativeFinding:
    """A nominal dataclass can be constructed directly; validate its exact metadata."""
    if type(value) is not NativeFinding:
        raise NativeReportError("native finding schema") from None
    _finding_mode(value.mode)
    for text in (value.native_commit, value.path, value.rule_id, value.native_fingerprint):
        _text(text)
    if not value.rule_id:
        raise NativeReportError("empty native identity") from None
    for coordinate in (value.start_line, value.end_line, value.start_column, value.end_column):
        _integer(coordinate)
    _finding_tags(value.tags)
    _path(value.path)
    _coordinates(value)
    _identity(value)
    return value
