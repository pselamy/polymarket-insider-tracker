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
        raise NativeReportError("finding object schema")
    data = cast(dict[str, object], value)
    if REQUIRED_FIELDS - data.keys() or data.keys() - ALLOWED_FIELDS:
        raise NativeReportError("finding field schema")
    return data


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise NativeReportError("finding text schema")
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise NativeReportError("finding text encoding") from None
    return value


def _integer(value: object) -> int:
    if type(value) is not int:
        raise NativeReportError("coordinate integer schema")
    if value < 1:
        raise NativeReportError("unsupported native coordinate")
    return value


def _entropy(value: object) -> None:
    if type(value) not in (int, float):
        raise NativeReportError("entropy number schema")
    try:
        finite = math.isfinite(cast(int | float, value))
    except OverflowError:
        raise NativeReportError("entropy number limit") from None
    if not finite:
        raise NativeReportError("nonfinite entropy")


def _tags(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise NativeReportError("tags array schema")
    return tuple(_text(tag) for tag in cast(list[object], value))


def _path(value: str) -> None:
    if value.startswith("/") or "\\" in value:
        raise NativeReportError("native relative path")
    if set(value.split("/")) & {"", ".", ".."}:
        raise NativeReportError("native path component")
    if any(unicodedata.category(char) == "Cc" for char in value):
        raise NativeReportError("native path control character")


def _coordinates(finding: NativeFinding) -> None:
    if finding.end_line < finding.start_line:
        raise NativeReportError("native line order")
    if finding.start_line == finding.end_line and finding.end_column < finding.start_column:
        raise NativeReportError("native column order")


def _commit(finding: NativeFinding) -> None:
    if finding.mode is ScanMode.HISTORY:
        if COMMIT_PATTERN.fullmatch(finding.native_commit) is None:
            raise NativeReportError("native history commit")
    elif finding.native_commit:
        raise NativeReportError("native tree commit")


def _identity(finding: NativeFinding) -> None:
    _commit(finding)
    prefix = f"{finding.native_commit}:" if finding.mode is ScanMode.HISTORY else ""
    expected = f"{prefix}{finding.path}:{finding.rule_id}:{finding.start_line}"
    if finding.native_fingerprint != expected:
        raise NativeReportError("native fingerprint mismatch")


def _native_text(data: dict[str, object]) -> dict[str, str]:
    text = {key: _text(data[key]) for key in TEXT_FIELDS}
    if "Link" in data:
        _text(data["Link"])
    _enrolled_text(text)
    return text


def _enrolled_text(text: dict[str, str]) -> None:
    if not text["RuleID"] or not text["Fingerprint"]:
        raise NativeReportError("empty native identity")
    if text["SymlinkFile"] or text["Secret"] != "REDACTED":
        raise NativeReportError("unsupported or unredacted finding")


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
