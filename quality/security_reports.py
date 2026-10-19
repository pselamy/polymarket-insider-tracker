"""Validate scan completeness independently of scanner exit statuses."""

import json
import tomllib
from pathlib import Path

from quality.report_data import array, record, text, unique

PYPI = "PyPI"
LOCKFILE = "lockfile"


def read_report(path: Path) -> object:
    """A missing or malformed report is a failed check."""
    value: object = json.loads(path.read_text(encoding="utf-8"))
    return value


def verify_empty(value: object) -> None:
    """Findings, skipped rules and scanner errors must all be absent."""
    if array(value):
        raise ValueError("security findings, skipped rules or errors were reported")


def verify_sast(value: object, expected: set[str]) -> None:
    """Require exact coverage of the independently discovered authored files."""
    report = record(value)
    for field in ("results", "errors", "skipped_rules"):
        verify_empty(report[field])
    scanned = array(record(report["paths"])["scanned"])
    actual = {text(path) for path in scanned}
    if actual != expected:
        raise ValueError("static security scan source inventory is incomplete")


def locked_packages(path: Path) -> set[tuple[str, str, str]]:
    """Include every locked package, regardless of platform or dependency group."""
    value: object = tomllib.loads(path.read_text(encoding="utf-8"))
    packages = array(record(value)["package"])
    return unique([locked_package(record(item)) for item in packages])


def locked_package(item: dict[str, object]) -> tuple[str, str, str]:
    return (text(item["name"]), text(item["version"]), PYPI)


def scanned_package(value: object) -> tuple[str, str, str]:
    item = record(value)
    for field in ("vulnerabilities", "groups", "license_violations"):
        verify_empty(item.get(field, []))
    package = record(item["package"])
    return (
        text(package["name"]),
        text(package["version"]),
        text(package["ecosystem"]),
    )


def verify_audit(value: object, lockfile: Path) -> None:
    """A clean status cannot conceal missing, duplicate or substituted packages."""
    report = record(value)
    verify_empty(report.get("errors", []))
    (result,) = array(report["results"])
    group = record(result)
    source = record(group["source"])
    if source["type"] != LOCKFILE:
        raise ValueError("audit source must be the requested lockfile")
    if Path(text(source["path"])).resolve() != lockfile.resolve():
        raise ValueError("audit source must be the requested lockfile")
    actual = unique([scanned_package(item) for item in array(group["packages"])])
    expected = locked_packages(lockfile)
    if not expected or actual != expected:
        raise ValueError("dependency audit package inventory is incomplete")
