"""Validator-owned execution of claimed test nodes against a committed revision."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from hashlib import sha256
from pathlib import Path

from scripts._traceability_primitives import (
    REQUIRED_KEYS,
    STATES,
    LedgerContext,
    _artifact,
    _committed_bytes,
    _git,
    _history,
    execution_command,
    object_list,
    object_map,
    strings,
)


def _test_path(entry: str, context: LedgerContext) -> None:
    path = entry.split("::")[0]
    if not path.startswith("tests/"):
        raise ValueError(f"dangling test path {entry!r}")
    _committed_test(path, context)


def _committed_test(path: str, context: LedgerContext) -> None:
    root = context.repository_root
    try:
        candidate = _artifact(root, path)
    except ValueError as exc:
        raise ValueError(f"dangling test path {path!r}") from exc
    entry = _git(root, "ls-tree", "HEAD", "--", path)
    if not entry.startswith("100644 blob ") and not entry.startswith("100755 blob "):
        raise ValueError(f"test {path!r} is not a committed tree entry")
    if candidate.read_bytes() != _committed_bytes(root, path):
        raise ValueError(f"test {path!r} differs from committed bytes")


def _artifacts(row: dict[str, object], context: LedgerContext) -> None:
    artifacts = object_map(row.get("artifact_sha256"))
    if not artifacts:
        raise ValueError("artifact_sha256 must be non-empty")
    for path, digest in artifacts.items():
        _artifact_digest(path, digest, context)


def _artifact_digest(path: str, digest: object, context: LedgerContext) -> None:
    if (context.repository_root / path).resolve() == context.ledger_path.resolve():
        raise ValueError("self-referential artifact")
    if sha256(_artifact(context.repository_root, path).read_bytes()).hexdigest() != digest:
        raise ValueError(f"artifact {path!r} digest mismatch")


def _row(row: dict[str, object], context: LedgerContext) -> None:
    if set(row) != REQUIRED_KEYS:
        raise ValueError("row keys differ from required keys")
    if not isinstance(row.get("state"), str) or row.get("state") not in STATES:
        raise ValueError("unknown state")
    tests = strings(row.get("test"))
    if not tests:
        raise ValueError("test must be non-empty")
    for entry in tests:
        _test_path(entry, context)
    _artifacts(row, context)


def _environment(scratch: Path, root: Path) -> dict[str, str]:
    # Only interpreter/tool discovery and the committed source path survive.
    # No application config or caller-controlled pytest injection is inherited.
    # Bytecode and pytest caches are redirected into validator-owned scratch so
    # working-tree cache inputs can neither change execution nor be written.
    return {
        "PATH": os.defpath,
        "HOME": str(scratch),
        "TMPDIR": str(scratch),
        "PYTHONPATH": os.pathsep.join((str(root), str(root / "src"))),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPYCACHEPREFIX": str(scratch / "pycache"),
        "PYTHONNOUSERSITE": "1",
        "PYTEST_ADDOPTS": f"-o cache_dir={scratch / 'pytest-cache'}",
    }


def _outcomes(value: object, nodes: list[str]) -> None:
    receipt = object_map(value)
    if strings(receipt.get("collected")) != nodes:
        raise ValueError("collected test IDs differ from claims")
    reports = object_map(receipt.get("reports"))
    expected = {node: {"setup": "passed", "call": "passed", "teardown": "passed"} for node in nodes}
    if reports != expected:
        raise ValueError("test was failed/skipped/not-run; no executed-run receipt")


def _require_committed_source(context: LedgerContext) -> None:
    _require_exact_bytes(context)


def _require_exact_bytes(context: LedgerContext) -> None:
    _require_revision_bytes(context.repository_root, context.expected_revision)


def _require_revision_bytes(root: Path, expected_revision: str) -> None:
    head = _git(root, "rev-parse", "HEAD")
    if expected_revision != head:
        raise ValueError("configured expected revision must equal actual full HEAD")
    changes = _git(
        root,
        "status",
        "--porcelain",
        "--untracked-files=all",
        "--",
        ".",
        ":(exclude)specs/002-reproducible-runtime/evidence/TRACEABILITY.json",
        ":(exclude)specs/003-safe-observable-operation/evidence/TRACEABILITY.json",
    )
    for line in changes.splitlines():
        if line.startswith("!!"):
            continue
        raise ValueError(
            "source changed or uncommitted inputs; exact-head execution requires clean source"
        )
    _require_tree_bytes(root)


def _require_tree_bytes(root: Path) -> None:
    listed = _git(root, "ls-tree", "-r", "-z", "--name-only", "HEAD")
    for path in listed.rstrip("\0").split("\0"):
        if path.endswith("/evidence/TRACEABILITY.json"):
            continue
        if (root / path).read_bytes() != _committed_bytes(root, path):
            raise ValueError(f"source {path!r} differs from committed bytes")


def _execute(nodes: list[str], context: LedgerContext, output: Path) -> None:
    root = context.repository_root.resolve()
    output.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix="traceability-", dir=output) as directory:
        scratch = Path(directory)
        execution_root = scratch / "source"
        _materialize_revision(root, execution_root, context.expected_revision)
        before = _committed_fingerprint(execution_root)
        _run_validated(nodes, output, execution_root, scratch)
        after = _committed_fingerprint(execution_root)
        if before != after:
            raise ValueError("source changed during execution")
        _require_revision_bytes(execution_root, context.expected_revision)


def _materialize_revision(root: Path, destination: Path, expected_revision: str) -> None:
    subprocess.run(
        [
            "git",
            "clone",
            "--quiet",
            "--no-hardlinks",
            "--no-recurse-submodules",
            str(root),
            str(destination),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    if _git(destination, "rev-parse", "HEAD") != expected_revision:
        raise ValueError("materialized execution revision differs from configured HEAD")


def _committed_fingerprint(root: Path) -> str:
    listed = _git(root, "ls-tree", "-r", "-z", "--name-only", "HEAD")
    digests = {
        path: sha256(_committed_bytes(root, path)).hexdigest()
        for path in listed.rstrip("\0").split("\0")
        if not path.endswith("/evidence/TRACEABILITY.json")
    }
    return sha256(json.dumps(digests, sort_keys=True).encode()).hexdigest()


def _run_validated(nodes: list[str], output: Path, root: Path, scratch: Path) -> None:
    command = execution_command(nodes)
    command[1] = str(root / command[1])
    with (output / "stdout").open("w") as stdout, (output / "stderr").open("w") as stderr:
        result = subprocess.run(
            command,
            cwd=scratch,
            env=_environment(scratch, root),
            stdout=stdout,
            stderr=stderr,
            timeout=300,
            check=False,
        )
    receipt = scratch / "receipt.json"
    (output / "command.json").write_text(json.dumps(command))
    (output / "exit").write_text(str(result.returncode))
    if not receipt.is_file():
        raise ValueError("no executed-run receipt from harness")
    raw = receipt.read_text()
    (output / "receipt.json").write_text(raw)
    _outcomes(json.loads(raw), nodes)
    if result.returncode != 0:
        raise ValueError("harness failed; no execution credit")


def _new_rows(document: dict[str, object], context: LedgerContext) -> list[dict[str, object]]:
    count = _history(document, context)
    rows = [object_map(row) for row in object_list(document.get("rows"))[count:]]
    if not rows:
        raise ValueError("no fresh execution rows; historical claims grant no current credit")
    for row in rows:
        _row(row, context)
    keys = [json.dumps([row.get("requirement"), sorted(strings(row.get("test")))]) for row in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate requirement/test pair")
    return rows
