"""Read-only CLI transport bound to github.com and one trusted repository."""

import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from quality.trusted_policy.metadata_collector import repository_route
from quality.trusted_policy.review_policy import PolicyFailure

MAX_NATIVE_JSON_BYTES = 8 * 1024 * 1024


def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PolicyFailure("native metadata contains duplicate object keys")
        result[key] = value
    return result


def invalid_constant(_value: str) -> object:
    raise PolicyFailure("native metadata contains a nonfinite JSON number")


def endpoint_path(endpoint: str, repository: str) -> str:
    root = repository_route(repository)
    if endpoint == root:
        return endpoint
    prefix = root + "/"
    if not endpoint.startswith(prefix):
        raise PolicyFailure("native read must stay within the trusted repository")
    suffix = endpoint[len(prefix) :]
    if re.fullmatch(r"[A-Za-z0-9_./-]+(?:\?per_page=100&page=[1-9][0-9]*)?", suffix) is None:
        raise PolicyFailure("unsupported native metadata route or query")
    if any(part in {"", ".", ".."} for part in suffix.split("?")[0].split("/")):
        raise PolicyFailure("native route contains an unsafe path segment")
    return endpoint


def decode_response(value: bytes) -> object:
    if len(value) > MAX_NATIVE_JSON_BYTES:
        raise PolicyFailure("native metadata response exceeded byte budget")
    result: object = json.loads(
        value.decode("utf-8"),
        object_pairs_hook=unique_object,
        parse_constant=invalid_constant,
    )
    return result


@dataclass(frozen=True)
class GithubAPI:
    executable: Path
    repository: str

    def __post_init__(self) -> None:
        repository_route(self.repository)
        if not self.executable.is_absolute():
            raise PolicyFailure("trusted GitHub CLI needs an absolute executable path")

    def __call__(self, endpoint: str) -> object:
        route = endpoint_path(endpoint, self.repository)
        result = subprocess.run(
            [
                str(self.executable),
                "api",
                "--hostname",
                "github.com",
                "--method",
                "GET",
                "-H",
                "X-GitHub-Api-Version: 2026-03-10",
                route,
            ],
            capture_output=True,
            timeout=30,
            check=False,
        )
        if result.returncode != 0:
            raise PolicyFailure("native GitHub metadata request failed")
        return decode_response(result.stdout)
