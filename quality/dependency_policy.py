"""Reject unreviewed UV lockfile sources and unverified artifact locations."""

import re
import tomllib
from pathlib import Path
from typing import TypeGuard
from urllib.parse import urlsplit

REGISTRY = "https://pypi.org/simple"
ARTIFACT_HOST = "files.pythonhosted.org"
SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")


def is_record(value: object) -> TypeGuard[dict[str, object]]:
    """Retain unknown TOML values after identifying a table."""
    return isinstance(value, dict)


def is_array(value: object) -> TypeGuard[list[object]]:
    """Retain unknown TOML elements after identifying an array."""
    return isinstance(value, list)


def record(value: object) -> dict[str, object]:
    """Require TOML tables at policy boundaries."""
    if not is_record(value):
        raise ValueError("dependency lock entry must be a table")
    return value


def text(value: object) -> str:
    """Require nonempty text without coercion."""
    if not isinstance(value, str) or not value:
        raise ValueError("dependency lock field must be nonempty text")
    return value


def artifact(value: object) -> None:
    """Require an approved artifact URL, canonical digest and positive size."""
    item = record(value)
    url = urlsplit(text(item.get("url")))
    if (
        url.scheme not in {"https"}
        or url.netloc != ARTIFACT_HOST
        or not url.path.startswith("/packages/")
        or not url.path.endswith((".whl", ".tar.gz"))
        or url.query
        or url.fragment
    ):
        raise ValueError("dependency artifact must use approved PyPI origin")
    if not SHA256.fullmatch(text(item.get("hash"))):
        raise ValueError("dependency artifact requires canonical SHA-256")
    size = item.get("size")
    if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
        raise ValueError("dependency artifact requires positive size")


def verify_wheels(value: object) -> None:
    """Validate every locked wheel, including wheels for other platforms."""
    if not is_array(value):
        raise ValueError("dependency wheels must be an array")
    for wheel in value:
        artifact(wheel)


def package(item: dict[str, object], project: str) -> bool:
    """Validate one registry dependency or the single local project root."""
    name = text(item.get("name"))
    text(item.get("version"))
    source = record(item.get("source"))
    if source == {"editable": "."}:
        if name != project:
            raise ValueError("editable dependency must be the project root")
        return False
    if source != {"registry": REGISTRY}:
        raise ValueError("dependency source must use approved PyPI registry")
    artifact(item.get("sdist"))
    verify_wheels(item.get("wheels", []))
    return True


def verify(root: Path) -> tuple[int, int]:
    """Check the complete locked graph, including uninstalled platform wheels."""
    project = record(tomllib.loads((root / "pyproject.toml").read_text()))
    name = text(record(project["project"])["name"])
    lock = record(tomllib.loads((root / "uv.lock").read_text()))
    packages = lock.get("package")
    if not is_array(packages):
        raise ValueError("dependency lock inventory must be an array")
    if not packages:
        raise ValueError("dependency lock inventory must be nonempty")
    registry_count = sum(package(record(item), name) for item in packages)
    root_count = len(packages) - registry_count
    if root_count != 1:
        raise ValueError("dependency lock requires one editable project root")
    return registry_count, len(packages)
