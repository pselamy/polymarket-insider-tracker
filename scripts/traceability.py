"""Validate T5 history and fresh, validator-owned execution (see traceability contract)."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import tempfile
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
    """Bind the actual checkout commit/tree AND all working input bytes.

    Ledger JSON is output, excluded to avoid self-reference. Nothing else is excluded.
    The extra byte digest makes dirty execution explicit rather than calling it clean HEAD.
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


def _test_path(entry: str, context: LedgerContext) -> None:
    path = entry.split("::")[0]
    if not path.startswith("tests/"):
        raise ValueError(f"dangling test path {entry!r}")
    try:
        _artifact(context.repository_root, path)
    except ValueError as exc:
        raise ValueError(f"dangling test path {entry!r}") from exc


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


def _claim_nodes(rows: list[dict[str, object]]) -> list[str]:
    nodes: set[str] = set()
    for row in rows:
        nodes.update(strings(row.get("test")))
    if any("::" not in node for node in nodes):
        raise ValueError("execution credit requires exact collected node IDs, never files/gates")
    return sorted(nodes)


def _configuration(root: Path) -> dict[str, str]:
    return {
        path: sha256((root / path).read_bytes()).hexdigest()
        for path in ("pyproject.toml", "uv.lock")
    }


def execution_command(nodes: list[str]) -> list[str]:
    return [sys.executable, "scripts/traceability_harness.py", *nodes]


def _claims(rows: list[dict[str, object]], identity: dict[str, object], root: Path) -> list[str]:
    nodes = _claim_nodes(rows)
    expected: dict[str, object] = {
        "run": shlex.join(execution_command(nodes)),
        "source": identity,
        "config": _configuration(root),
        "result": "passed",
        "state": "exact-head-passed",
    }
    for row in rows:
        _execution_metadata(row, expected)
    return nodes


def _execution_metadata(row: dict[str, object], expected: dict[str, object]) -> None:
    if any(row.get(key) != value for key, value in expected.items()):
        raise ValueError("run/source/config/result/state differs from validator-owned execution")


def _environment(scratch: Path) -> dict[str, str]:
    # Only interpreter/tool discovery survives. No application config or pytest injection.
    return {"PATH": os.defpath, "HOME": str(scratch), "TMPDIR": str(scratch)}


def _outcomes(value: object, nodes: list[str]) -> None:
    receipt = object_map(value)
    if strings(receipt.get("collected")) != nodes:
        raise ValueError("collected test IDs differ from claims")
    reports = object_map(receipt.get("reports"))
    expected = {node: {"setup": "passed", "call": "passed", "teardown": "passed"} for node in nodes}
    if reports != expected:
        raise ValueError("test was failed/skipped/not-run; no executed-run receipt")


def _execute(nodes: list[str], context: LedgerContext, output: Path) -> None:
    root = context.repository_root.resolve()
    output.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix="traceability-", dir=output) as directory:
        scratch = Path(directory)
        command = execution_command(nodes)
        command[1] = str(root / command[1])
        with (output / "stdout").open("w") as stdout, (output / "stderr").open("w") as stderr:
            result = subprocess.run(
                command,
                cwd=scratch,
                env=_environment(scratch),
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


def validate_ledger_document(
    document: object, context: LedgerContext, *, execution_output: Path | None = None
) -> LedgerResult:
    """Only a newly executed child harness can grant credit; files/sets are not receipts.

    execution_output must not exist. Each validation runs again and retains both streams.
    Omitting it is read-only and always rejects execution claims.
    """
    try:
        parsed = object_map(document)
        identity = _manifest(parsed, context)
        rows = _new_rows(parsed, context)
        nodes = _claims(rows, identity, context.repository_root)
        if execution_output is None:
            raise ValueError("no executed-run receipt: validator-owned execution required")
        _execute(nodes, context, execution_output)
        if source_identity(context) != identity:
            raise ValueError("source changed during execution")
        (execution_output / "source.json").write_text(json.dumps(identity, sort_keys=True))
        (execution_output / "validated-document.json").write_text(
            json.dumps(parsed, indent=2) + "\n"
        )
        return LedgerResult(True, ())
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        return LedgerResult(False, (str(exc),))


def validate_ledger_file(
    ledger_path: Path,
    repository_root: Path,
    executed_tests: frozenset[str],
    skipped_tests: frozenset[str],
    *,
    expected_revision: str = "",
    execution_output: Path | None = None,
) -> LedgerResult:
    context = LedgerContext(
        repository_root, ledger_path, executed_tests, skipped_tests, expected_revision
    )
    try:
        document: object = json.loads(ledger_path.read_text())
    except (ValueError, OSError) as exc:
        return LedgerResult(False, (str(exc),))
    return validate_ledger_document(document, context, execution_output=execution_output)
