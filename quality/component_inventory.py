"""Bind every locked component to a reviewed SPDX and provenance record."""

import hashlib
import json
import re
import tomllib
from pathlib import Path

from quality.dependency_policy import verify as verify_origins
from quality.report_data import array, record, text

SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def digest(path: Path) -> str:
    """Bind reports to the exact local lock, policy and project notice."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def project(root: Path, policy: dict[str, object]) -> str:
    """Require a matching project SPDX declaration and unchanged notice."""
    metadata = record(tomllib.loads((root / "pyproject.toml").read_text()))
    declared = text(record(metadata["project"])["license"])
    if declared != text(policy["projectLicense"]):
        raise ValueError("project license differs from reviewed policy")
    notice = root / "LICENSE"
    if digest(notice) != text(policy["projectLicenseSha256"]):
        raise ValueError("project license notice differs from reviewed policy")
    return declared


def locked(root: Path) -> dict[str, dict[str, object]]:
    """Index each registry package by name and version without omission."""
    data = record(tomllib.loads((root / "uv.lock").read_text()))
    result: dict[str, dict[str, object]] = {}
    for value in array(data["package"]):
        add_locked(result, record(value))
    if not result:
        raise ValueError("locked component inventory is empty")
    return result


def add_locked(result: dict[str, dict[str, object]], item: dict[str, object]) -> None:
    """Skip only the editable project and reject duplicate identities."""
    if record(item["source"]) == {"editable": "."}:
        return
    key = text(item["name"]) + "==" + text(item["version"])
    if key in result:
        raise ValueError("duplicate locked component identity")
    result[key] = item


def evidence(entry: dict[str, object], item: dict[str, object]) -> None:
    """Require exact first-party metadata or locked source-file provenance."""
    kind = text(entry["evidenceKind"])
    url = text(entry["evidenceUrl"])
    name, version = text(item["name"]), text(item["version"])
    expected = f"https://pypi.org/pypi/{name}/{version}/json"
    if kind in {"pypi-release-metadata"} and url == expected:
        return
    source = text(record(item["sdist"])["url"])
    prefix = source + "#"
    if (
        kind in {"locked-sdist-license-file"}
        and url.startswith(prefix)
        and url.removeprefix(prefix)
    ):
        return
    raise ValueError("license provenance must bind to the locked release")


def component(
    key: str, item: dict[str, object], approved: set[str], policy: dict[str, object]
) -> dict[str, object]:
    """Reject unknown licenses and malformed reviewed evidence."""
    expression = text(policy["spdx"])
    if expression not in approved:
        raise ValueError("component SPDX expression requires review")
    proof = text(policy["evidenceSha256"])
    if not SHA256.fullmatch(proof):
        raise ValueError("component license evidence requires canonical SHA-256")
    evidence(policy, item)
    return {
        "identity": key,
        "name": text(item["name"]),
        "version": text(item["version"]),
        "spdx": expression,
        "evidenceKind": text(policy["evidenceKind"]),
        "evidenceSha256": proof,
        "evidenceUrl": text(policy["evidenceUrl"]),
        "sdist": record(item["sdist"]),
        "wheels": array(item.get("wheels", [])),
    }


def reviewed_components(
    packages: dict[str, dict[str, object]], policy: dict[str, object]
) -> list[dict[str, object]]:
    """Require one approved SPDX record per locked distribution."""
    reviewed = record(policy["packages"])
    if set(reviewed) != set(packages):
        raise ValueError("locked component lacks reviewed license provenance")
    approved = {text(value) for value in array(policy["approvedExpressions"])}
    rows = [
        component(key, packages[key], approved, record(reviewed[key])) for key in sorted(packages)
    ]
    if not approved.issubset({text(row["spdx"]) for row in rows}):
        raise ValueError("approved SPDX expressions differ from locked inventory")
    return rows


def inventory(root: Path) -> dict[str, object]:
    """Create a complete lock-bound inventory; policy edits need trusted review."""
    verify_origins(root)
    path = root / "quality/license-policy.json"
    policy = record(json.loads(path.read_text()))
    if policy["version"] != 1:
        raise ValueError("unsupported component license policy version")
    declared = project(root, policy)
    rows = reviewed_components(locked(root), policy)
    return {
        "projectLicense": declared,
        "projectLicenseSha256": digest(root / "LICENSE"),
        "lockSha256": digest(root / "uv.lock"),
        "policySha256": digest(path),
        "componentCount": len(rows),
        "components": rows,
    }
