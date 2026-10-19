"""Publish one native check from a dedicated App token, then read it back."""

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

from quality.trusted_policy.github_read import GithubAPI, decode_response
from quality.trusted_policy.metadata_collector import repository_route
from quality.trusted_policy.review_policy import (
    PolicyFailure,
    digest,
    identifier,
    record,
    text,
)

CONTEXT = "Relentless trusted policy"


@dataclass(frozen=True)
class Target:
    repository: str
    app_id: int
    context: str = CONTEXT


def payload(target: Target, head: str, passed: object) -> dict[str, object]:
    if type(passed) is not bool or text(target.context) != CONTEXT:
        raise PolicyFailure("compiled trusted check decision required")
    return {
        "name": CONTEXT,
        "head_sha": digest(head),
        "status": "completed",
        "conclusion": "success" if passed else "failure",
        "output": {
            "title": ("Trusted policy qualified" if passed else "Trusted policy rejected"),
            "summary": (
                "Native run, review, and protected policy evidence qualified."
                if passed
                else "Required trusted policy evidence is absent or invalid."
            ),
        },
    }


def verify_response(value: object, target: Target, body: dict[str, object]) -> int:
    native = record(value)
    if any(native[key] != body[key] for key in ("name", "head_sha", "status", "conclusion")):
        raise PolicyFailure("native check differs from compiled decision")
    if identifier(record(native["app"])["id"]) != identifier(target.app_id):
        raise PolicyFailure("native check was not issued by the dedicated App")
    return identifier(native["id"])


@dataclass(frozen=True)
class GithubChecks:
    executable: Path
    target: Target

    def __post_init__(self) -> None:
        GithubAPI(self.executable, self.target.repository)
        identifier(self.target.app_id)
        if self.target.context != CONTEXT:
            raise PolicyFailure("trusted check context differs")

    def create(self, head: str, passed: bool) -> int:
        route = repository_route(self.target.repository) + "/check-runs"
        body = payload(self.target, head, passed)
        result = subprocess.run(
            [
                str(self.executable),
                "api",
                "--hostname",
                "github.com",
                "--method",
                "POST",
                "-H",
                "X-GitHub-Api-Version: 2026-03-10",
                "--input",
                "-",
                route,
            ],
            input=json.dumps(body, sort_keys=True).encode(),
            capture_output=True,
            timeout=30,
            check=False,
        )
        if result.returncode != 0:
            raise PolicyFailure("dedicated App check creation failed")
        identity = verify_response(decode_response(result.stdout), self.target, body)
        read = GithubAPI(self.executable, self.target.repository)
        observed = read(route + "/" + str(identity))
        if verify_response(observed, self.target, body) != identity:
            raise PolicyFailure("dedicated App check readback changed")
        return identity
