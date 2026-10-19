"""Require reviewed upstream-supported Python branches and matching declarations."""

import hashlib
import re
import tomllib
from datetime import date
from pathlib import Path

from quality.report_data import array, record, text
from quality.runtime_data import fields, read_json, read_yaml

SOURCE = re.compile(
    r"https://raw\.githubusercontent\.com/python/devguide/[a-f0-9]{40}/versions\.rst"
)
RELEASE_SOURCE = "https://peps.python.org/api/release-cycle.json"
POLICY_KEYS = {
    "source",
    "releaseCycleSource",
    "snapshotSha256",
    "reviewedOn",
    "reviewBy",
    "versions",
}


def calendar(value: object) -> date:
    source = text(value)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", source) is None:
        raise ValueError("runtime dates must use YYYY-MM-DD")
    return date.fromisoformat(source)


def snapshot(root: Path, approved: dict[str, object]) -> dict[str, object]:
    if SOURCE.fullmatch(text(approved["source"])) is None:
        raise ValueError("runtime provenance requires a pinned upstream commit")
    if approved["releaseCycleSource"] != RELEASE_SOURCE:
        raise ValueError("runtime release-cycle source must be the primary PEP API")
    path = root / "quality" / "python-releases.json"
    snapshot_bytes = path.read_bytes().replace(b"\r\n", b"\n")
    if hashlib.sha256(snapshot_bytes).hexdigest() != approved["snapshotSha256"]:
        raise ValueError("runtime upstream snapshot digest differs from review")
    return record(read_json(path))


def branches(value: object) -> list[str]:
    versions = [text(item) for item in array(value)]
    if not versions:
        raise ValueError("runtime inventory must be nonempty")
    for version in versions:
        require_version(version)
    expected = [f"3.{int(versions[0][2:]) + index}" for index in range(len(versions))]
    if versions != expected:
        raise ValueError("runtime inventory must be ordered, unique and contiguous")
    return versions


def require_version(value: str) -> None:
    if re.fullmatch(r"3\.\d+", value) is None:
        raise ValueError("runtime branches must be Python 3 minor versions")


def end_of_support(value: object) -> date:
    source = text(value)
    if re.fullmatch(r"\d{4}-\d{2}", source) is not None:
        return calendar(source + "-01")
    return calendar(source)


def require_branch(version: str, release: object, today: date) -> None:
    item = record(release)
    if item["branch"] != version or text(item["status"]) not in {"bugfix", "security"}:
        raise ValueError("runtime branch is not an upstream stable supported release")
    if today < calendar(item["first_release"]) or today >= end_of_support(item["end_of_life"]):
        raise ValueError("runtime branch is outside its upstream support dates")


def verify_metadata(root: Path, versions: list[str]) -> None:
    metadata = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    expected = f">={versions[0]},<3.{int(versions[-1][2:]) + 1}"
    if record(metadata["project"])["requires-python"] != expected:
        raise ValueError("Python package metadata differs from reviewed runtime inventory")


def matrix(job: object) -> dict[str, object]:
    return record(record(record(job)["strategy"])["matrix"])


def verify_matrices(root: Path, versions: list[str]) -> None:
    jobs = record(record(read_yaml(root / ".github" / "workflows" / "ci.yml"))["jobs"])
    for name in ("analysis", "compatibility"):
        if matrix(jobs[name])["python"] != versions:
            raise ValueError("required Python matrix differs from reviewed runtime inventory")


def verify_runtimes(root: Path, today: date) -> None:
    approved = fields(read_json(root / "quality" / "runtime-support.json"), POLICY_KEYS)
    if today < calendar(approved["reviewedOn"]) or today >= calendar(approved["reviewBy"]):
        raise ValueError("runtime support requires a current upstream review")
    releases = snapshot(root, approved)
    versions = branches(approved["versions"])
    for version in versions:
        require_branch(version, releases[version], today)
    verify_metadata(root, versions)
    verify_matrices(root, versions)
