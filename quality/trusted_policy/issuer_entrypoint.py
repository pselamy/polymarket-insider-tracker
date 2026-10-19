"""Protected-base entrypoint for the dedicated trusted policy issuer."""

import os
import re
import shutil
import sys
from pathlib import Path

from quality.trusted_policy.check_publisher import GithubChecks, Target
from quality.trusted_policy.github_read import (
    MAX_NATIVE_JSON_BYTES,
    GithubAPI,
    decode_response,
)
from quality.trusted_policy.issuer_resolution import ReviewedPolicy
from quality.trusted_policy.metadata_collector import repository_route
from quality.trusted_policy.metadata_pages import ReadAPI
from quality.trusted_policy.review_policy import (
    PolicyFailure,
    digest,
    identifier,
    record,
    same_text,
    text,
)
from quality.trusted_policy.trusted_issuer import issue

WORKFLOW_PATH = ".github/workflows/ci.yml"
WORKFLOW_NAME = "Relentless CI"
PROFILES: dict[str, tuple[str, tuple[str, ...]]] = {
    "python": ("Python", ("3.11", "3.12", "3.13", "3.14")),
    "typescript": ("Node", ("22", "24", "26")),
}
SYSTEMS = ("ubuntu-24.04", "macos-15", "windows-2025")


def full_names(label: str, versions: tuple[str, ...]) -> set[str]:
    return {f"Full analysis ({label} {version})" for version in versions}


def installed_names(label: str, versions: tuple[str, ...]) -> set[str]:
    return {
        f"Installed behavior ({system}, {label} {version})"
        for system in SYSTEMS
        for version in versions
    }


def required_names(profile: str) -> frozenset[str]:
    if profile not in PROFILES:
        raise PolicyFailure("unknown protected template profile")
    label, versions = PROFILES[profile]
    names = full_names(label, versions)
    names.update(installed_names(label, versions))
    names.add("Relentless CI gate")
    return frozenset(names)


def reviewed_policy(api: ReadAPI, repository: str, base: str, profile: str) -> ReviewedPolicy:
    root = repository_route(repository)
    repository_id = current_base(api, root, repository, base)
    workflow_id = current_workflow(api, root)
    return ReviewedPolicy(
        repository,
        repository_id,
        base,
        workflow_id,
        required_names(profile),
    )


def current_base(api: ReadAPI, root: str, repository: str, base: str) -> int:
    native = record(api(root))
    branch = record(api(root + "/branches/main"))
    if native["full_name"] != repository or not same_text(native["default_branch"], "main"):
        raise PolicyFailure("issuer repository identity or default branch changed")
    if (
        not same_text(branch["name"], "main")
        or branch["protected"] is not True
        or digest(record(branch["commit"])["sha"]) != digest(base)
    ):
        raise PolicyFailure("issuer did not run from current protected main")
    return identifier(native["id"])


def current_workflow(api: ReadAPI, root: str) -> int:
    workflow = record(api(root + "/actions/workflows/ci.yml"))
    if (
        workflow["name"] != WORKFLOW_NAME
        or workflow["path"] != WORKFLOW_PATH
        or not same_text(workflow["state"], "active")
    ):
        raise PolicyFailure("required CI workflow identity differs")
    return identifier(workflow["id"])


def event_payload(path: Path) -> object:
    if not path.is_absolute() or not path.is_file() or path.stat().st_size > MAX_NATIVE_JSON_BYTES:
        raise PolicyFailure("trusted event file is missing or too large")
    return decode_response(path.read_bytes())


def app_id(value: str) -> int:
    if re.fullmatch(r"[1-9][0-9]*", value) is None:
        raise PolicyFailure("dedicated App ID is not a positive decimal identity")
    return identifier(int(value))


def executable() -> Path:
    located = shutil.which("gh")
    if located is None:
        raise PolicyFailure("trusted GitHub CLI is missing")
    path = Path(located)
    if not path.is_absolute():
        raise PolicyFailure("trusted GitHub CLI path is not absolute")
    return path


def main() -> int:
    if len(sys.argv) != 2:
        raise PolicyFailure("one protected template profile is required")
    profile = text(sys.argv[1])
    repository = text(os.environ["GITHUB_REPOSITORY"])
    base = digest(os.environ["GITHUB_SHA"])
    event_name = text(os.environ["GITHUB_EVENT_NAME"])
    event = event_payload(Path(os.environ["GITHUB_EVENT_PATH"]))
    identity = app_id(os.environ["RELENTLESS_POLICY_APP_ID"])
    if not os.environ.get("GH_TOKEN"):
        raise PolicyFailure("dedicated App installation token is missing")
    cli = executable()
    api = GithubAPI(cli, repository)
    reviewed = reviewed_policy(api, repository, base, profile)
    checks = GithubChecks(cli, Target(repository, identity))
    return issue(api, checks.create, event_name, event, reviewed)
