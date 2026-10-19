"""Bind a clean native clone report to all eligible frozen Python files."""

from datetime import datetime
from pathlib import Path

from quality.duplication_inventory import Eligible, nonnegative
from quality.report_data import array, record, text

ZERO_FIELDS = {
    "clones",
    "duplicatedLines",
    "duplicatedTokens",
    "newClones",
    "newDuplicatedLines",
    "percentage",
    "percentageTokens",
}
METRIC_FIELDS = {*ZERO_FIELDS, "lines", "sources", "tokens"}
FILE_FIELDS = {
    "bytes",
    "complexity",
    "duplicatedLines",
    "duplicatedTokens",
    "format",
    "lines",
    "path",
    "tokens",
}
FOLDER_FIELDS = {
    "bytes",
    "complexity",
    "duplicatedLines",
    "files",
    "lines",
    "path",
    "tokens",
}


def exact_paths(actual: list[str], expected: set[str]) -> None:
    """Neither omissions nor duplicate names can hide behind set comparison."""
    seen: set[str] = set()
    for name in actual:
        if name in seen:
            raise ValueError("duplication source or folder inventory is incomplete")
        seen.add(name)
    if seen != expected:
        raise ValueError("duplication source or folder inventory is incomplete")


def verify_zero_metrics(data: dict[str, object]) -> None:
    """Clones and duplicate percentages must all be numeric zero."""
    if set(data) != METRIC_FIELDS:
        raise ValueError("duplication statistics fields differ")
    for field in ZERO_FIELDS:
        if type(data[field]) not in {int, float} or data[field] != 0:
            raise ValueError("duplication statistics contain clones or invalid zero")


def metric(value: object, files: list[Eligible]) -> None:
    """A clean summary must agree with independent per-file receipts."""
    data = record(value)
    verify_zero_metrics(data)
    expected = (
        len(files),
        sum(item.lines for item in files),
        sum(item.tokens for item in files),
    )
    actual = (
        nonnegative(data["sources"]),
        nonnegative(data["lines"]),
        nonnegative(data["tokens"]),
    )
    if actual != expected:
        raise ValueError("duplication statistics differ from eligible source receipts")


def verify_file(value: object, expected: Eligible) -> None:
    """Require exact identity and source metrics in the combined report."""
    file = record(value)
    verify_file_identity(file)
    verify_file_counts(file, expected)


def verify_file_identity(file: dict[str, object]) -> None:
    """Reject a substituted format or report schema."""
    if set(file) != FILE_FIELDS or file["format"] not in ("python",):
        raise ValueError("duplication file fields or format differ")


def verify_file_counts(file: dict[str, object], expected: Eligible) -> None:
    """Compare native counts with the per-file low-threshold receipt."""
    for field in ("complexity", "duplicatedLines", "duplicatedTokens"):
        nonnegative(file[field])
    actual = (
        nonnegative(file["bytes"]),
        nonnegative(file["lines"]),
        nonnegative(file["tokens"]),
    )
    if actual != (expected.bytes, expected.lines, expected.tokens):
        raise ValueError("duplication file receipt differs from eligible source")
    if file["duplicatedLines"] or file["duplicatedTokens"]:
        raise ValueError("duplication file receipt contains cloned content")


def verify_folder(value: object, path: str, files: dict[str, Eligible]) -> None:
    """Folder totals must equal their constituent eligible source receipts."""
    folder = record(value)
    if set(folder) != FOLDER_FIELDS:
        raise ValueError("duplication folder fields or path differ")
    members = [item for name, item in files.items() if str(Path(name).parent) == path]
    verify_folder_counts(folder, members)


def verify_folder_counts(folder: dict[str, object], members: list[Eligible]) -> None:
    """Sum eligible bytes, lines and tokens for one directory."""
    expected = (
        len(members),
        sum(item.bytes for item in members),
        sum(item.lines for item in members),
        sum(item.tokens for item in members),
    )
    actual = tuple(nonnegative(folder[key]) for key in ("files", "bytes", "lines", "tokens"))
    nonnegative(folder["complexity"])
    if actual != expected:
        raise ValueError("duplication folder receipt differs from eligible source")
    verify_folder_zero(folder)


def verify_folder_zero(folder: dict[str, object]) -> None:
    """A clean folder may not report cloned lines."""
    if nonnegative(folder["duplicatedLines"]):
        raise ValueError("duplication folder receipt differs from eligible source")


def verify_summary(value: object, expected: dict[str, Eligible]) -> None:
    """Check exact per-file and per-directory inventory plus totals."""
    summary = record(value)
    if summary["by"] not in ("tokens",):
        raise ValueError("duplication summary measure changed")
    files = array(summary["files"])
    folders = array(summary["folders"])
    paths = {str(Path(name).parent) for name in expected}
    exact_paths([text(record(item)["path"]) for item in files], set(expected))
    exact_paths([text(record(item)["path"]) for item in folders], paths)
    verify_summary_totals(summary, len(files), len(folders))
    verify_summary_entries(files, folders, expected)


def verify_summary_totals(summary: dict[str, object], files: int, folders: int) -> None:
    """Reject truncated native totals despite complete-looking arrays."""
    if nonnegative(summary["totalFiles"]) != files:
        raise ValueError("duplication summary totals differ from eligible source")
    if nonnegative(summary["totalFolders"]) != folders:
        raise ValueError("duplication summary totals differ from eligible source")


def verify_summary_entries(
    files: list[object], folders: list[object], expected: dict[str, Eligible]
) -> None:
    """Check every file and folder receipt after inventory validation."""
    for item in files:
        file = record(item)
        verify_file(file, expected[text(file["path"])])
    for item in folders:
        folder = record(item)
        verify_folder(folder, text(folder["path"]), expected)


def verify_statistics(value: object, expected: dict[str, Eligible]) -> None:
    """Bind format and total statistics to the same eligible inventory."""
    statistics = record(value)
    when = text(statistics["detectionDate"])
    if datetime.fromisoformat(when.replace("Z", "+00:00")).tzinfo is None:
        raise ValueError("duplication report timestamp lacks timezone")
    formats = record(statistics["formats"])
    required_formats: set[str] = {"python"} if expected else set()
    if set(formats) != required_formats:
        raise ValueError("duplication report format inventory is incomplete")
    files = list(expected.values())
    if files:
        metric(formats["python"], files)
    metric(statistics["total"], files)


def verify_duplication_report(value: object, expected: dict[str, Eligible]) -> None:
    """No clone, missing report, or substituted source may pass."""
    report = record(value)
    if array(report["duplicates"]):
        raise ValueError("duplication report contains clones")
    verify_summary(report["summary"], expected)
    verify_statistics(report["statistics"], expected)
