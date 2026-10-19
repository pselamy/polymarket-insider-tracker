"""Shared ledger context, git/object primitives, and history binding."""

from __future__ import annotations

import json
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from typing import NamedTuple

BASELINE = "62f5b1e13aa88327b569acf99ff644c6c8e76c41"
REQUIRED_KEYS = frozenset(
    {
        "requirement",
        "scenario",
        "negative_scenario",
        "test",
        "run",
        "source",
        "config",
        "result",
        "artifact_sha256",
        "state",
    }
)
STATES = frozenset(
    {
        "specified",
        "implemented",
        "executed",
        "exact-head-passed",
        "merge-required",
        "post-merge-verified",
    }
)


class LedgerResult(NamedTuple):
    ok: bool
    errors: tuple[str, ...]


class LedgerContext(NamedTuple):
    """Expected revision comes from the operator, never from a receipt.

    Legacy test sets are accepted for API compatibility, but grant no credit.
    """

    repository_root: Path
    ledger_path: Path
    executed_tests: frozenset[str] = frozenset()
    skipped_tests: frozenset[str] = frozenset()
    expected_revision: str = ""


def object_map(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("expected an object")
    if not all(isinstance(key, str) for key in value):
        raise ValueError("object keys must be strings")
    return dict(value)


def object_list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError("expected a list")
    return list(value)


def strings(value: object) -> list[str]:
    values = object_list(value)
    result: list[str] = []
    for item in values:
        if not isinstance(item, str) or not item:
            raise ValueError("expected non-empty strings")
        result.append(item)
    return result


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def _relative(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def _artifact(root: Path, path: str) -> Path:
    candidate = root / path
    if _relative(candidate, root) != path or not candidate.is_file():
        raise ValueError(f"dangling artifact or non-canonical path {path!r}")
    return candidate


def source_identity(context: LedgerContext) -> dict[str, object]:
    """Bind the actual checkout commit/tree AND all committed input bytes.

    Ledger JSON is output, excluded to avoid self-reference. Nothing else is excluded.
    Identity binds working bytes. Exact-head credit additionally rejects dirty tracked
    bytes and runs the child from a validator-owned committed checkout, so ignored
    working files cannot change execution.
    """
    root = context.repository_root
    head = _git(root, "rev-parse", "HEAD")
    if context.expected_revision != head:
        raise ValueError("configured expected revision must equal actual full HEAD")
    files = _git(root, "ls-files", "--cached", "--others", "--exclude-standard", "-z")
    paths = sorted(set(files.rstrip("\0").split("\0")))
    inputs = {
        path: sha256(_artifact(root, path).read_bytes()).hexdigest()
        for path in paths
        if not path.endswith("/evidence/TRACEABILITY.json")
    }
    return {
        "head": head,
        "tree": _git(root, "rev-parse", "HEAD^{tree}"),
        "inputs_sha256": sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest(),
    }


def _baseline(context: LedgerContext, revision: str) -> dict[str, object]:
    path = _relative(context.ledger_path, context.repository_root)
    return object_map(json.loads(_git(context.repository_root, "show", f"{revision}:{path}")))


def _history(document: dict[str, object], context: LedgerContext) -> int:
    baseline = _baseline(context, BASELINE)
    rows = object_list(document.get("rows"))
    original = object_list(baseline.get("rows"))
    previous = object_list(_baseline(context, "HEAD").get("rows"))
    if rows[: len(previous)] != previous or rows[: len(original)] != original:
        raise ValueError("append-only history differs from immutable Git baseline/prior HEAD")
    if document.get("historical_manifest") != baseline.get("manifest"):
        raise ValueError("historical manifest must preserve the immutable baseline")
    return max(len(original), len(previous))


def _manifest(document: dict[str, object], context: LedgerContext) -> dict[str, object]:
    identity = source_identity(context)
    manifest = object_map(document.get("manifest"))
    expected = {
        **identity,
        "slice": context.ledger_path.parents[1].name,
        "history_revision": BASELINE,
    }
    if manifest != expected:
        raise ValueError("manifest source/head/tree does not match independently observed checkout")
    return identity


def _committed_bytes(root: Path, path: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(root), "show", f"HEAD:{path}"],
        check=True,
        capture_output=True,
    )
    return result.stdout


def execution_command(nodes: list[str]) -> list[str]:
    return [sys.executable, "scripts/traceability_harness.py", *nodes]
