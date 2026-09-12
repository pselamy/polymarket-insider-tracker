"""Shared traceability-ledger validator (T5, slices 002/003 only).

The validator is intentionally stdlib-only (``json`` + ``hashlib``): the
maintained schema plus the contract test suffice, so no bespoke framework is
added. It checks one ledger file against the repository checkout:

- every row carries exactly the T5 schema keys;
- ``state`` is one of the six enumerated ledger states;
- every ``test`` entry names a real collected test file or a real
  ``scripts/verify.py`` gate id (no dangling refs, no unknown states);
- no row cites a skipped/not-run test as ``exact-head-passed``: the caller
  supplies the executed/skipped node-id sets from a real harness run, and a
  forged row fails validation;
- no row claims a self-referential hash of its own ledger file
  (``artifact_sha256`` must pin external artifacts, never the ledger itself).
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from hashlib import sha256
from pathlib import Path
from typing import Any, NamedTuple, cast

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
"""Exact T5 schema keys every ledger row must carry (see ``_check_keys``)."""

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
    """Outcome of validating one ledger file."""

    ok: bool
    errors: tuple[str, ...]


class LedgerContext(NamedTuple):
    """Executed-evidence inputs the validator checks rows against."""

    repository_root: Path
    ledger_path: Path
    executed_tests: frozenset[str]
    skipped_tests: frozenset[str]


def _is_ledger_artifact(path: str, context: LedgerContext) -> bool:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = context.repository_root / candidate
    try:
        return candidate.resolve() == context.ledger_path.resolve()
    except OSError:
        return False


def _check_keys(row: dict[str, Any], index: int) -> list[str]:
    keys = set(row.keys())
    required = set(REQUIRED_KEYS)
    errors: list[str] = []
    if keys != required:
        errors.append(f"row {index}: keys {sorted(keys)} != required {sorted(required)}")
    return errors


def _check_state(row: dict[str, Any], index: int) -> list[str]:
    state = row.get("state")
    if state not in STATES:
        return [f"row {index}: unknown state {state!r}"]
    return []


def _check_test_refs(row: dict[str, Any], index: int, context: LedgerContext) -> list[str]:
    tests: Any = row.get("test")
    if not isinstance(tests, list) or not tests:
        return [f"row {index}: test must be a non-empty list"]
    errors: list[str] = []
    claimed: Sequence[Any] = cast(Sequence[Any], tests)
    for entry in claimed:
        errors.extend(_check_one_test_ref(entry, index, context))
    return errors


def _check_one_test_ref(entry: Any, index: int, context: LedgerContext) -> list[str]:
    if not isinstance(entry, str) or not entry:
        return [f"row {index}: test entry must be a non-empty string"]
    if entry.startswith("gate:"):
        return _check_gate_ref(entry, index)
    return _check_file_ref(entry, index, context)


def _check_gate_ref(entry: str, index: int) -> list[str]:
    gate_id = entry.removeprefix("gate:")
    known = {
        "lock",
        "format",
        "lint",
        "strict-types",
        "pyright",
        "vulture",
        "complexipy",
        "imports",
        "tests",
        "services",
        "redis-contract",
        "migrations",
    }
    if gate_id not in known:
        return [f"row {index}: unknown verifier gate {entry!r}"]
    return []


def _check_file_ref(entry: str, index: int, context: LedgerContext) -> list[str]:
    file_part = entry.split("::")[0]
    candidate = context.repository_root / file_part
    if not candidate.is_file():
        return [f"row {index}: dangling test path {entry!r}"]
    return []


def _check_exact_head_evidence(
    row: dict[str, Any], index: int, context: LedgerContext
) -> list[str]:
    if row.get("state") != "exact-head-passed":
        return []
    errors: list[str] = []
    claimed: Any = row.get("test", [])
    if not isinstance(claimed, list):
        return [f"row {index}: exact-head-passed test must be a list"]
    entries: Sequence[Any] = cast(Sequence[Any], claimed)
    for entry in entries:
        errors.extend(_check_one_exact_head_entry(entry, index, context))
    return errors


def _check_one_exact_head_entry(entry: Any, index: int, context: LedgerContext) -> list[str]:
    if not isinstance(entry, str):
        return [f"row {index}: exact-head-passed test entry must be a string"]
    if entry.startswith("gate:"):
        return []
    return _check_file_exact_head_entry(entry, index, context)


def _check_file_exact_head_entry(entry: str, index: int, context: LedgerContext) -> list[str]:
    node_file = entry.split("::")[0]
    if entry in context.skipped_tests:
        return [f"row {index}: {entry!r} was skipped/not-run, not exact-head-passed"]
    if "::" in entry and entry not in context.executed_tests:
        return [f"row {index}: {entry!r} has no executed-run receipt"]
    if "::" not in entry and not (context.repository_root / node_file).is_file():
        return [f"row {index}: dangling test path {entry!r}"]
    return []


def _check_artifact_hash(row: dict[str, Any], index: int, context: LedgerContext) -> list[str]:
    digest: Any = row.get("artifact_sha256")
    if not isinstance(digest, dict) or not digest:
        return [f"row {index}: artifact_sha256 must be a non-empty mapping"]
    typed_digest: dict[str, str] = cast(dict[str, str], digest)
    pairs: Sequence[tuple[str, str]] = cast(Sequence[tuple[str, str]], list(typed_digest.items()))
    errors: list[str] = []
    for path, pinned in pairs:
        errors.extend(_check_one_artifact(path, pinned, index, context))
    return errors


def _check_one_artifact(path: Any, pinned: Any, index: int, context: LedgerContext) -> list[str]:
    if not isinstance(path, str) or not isinstance(pinned, str):
        return [f"row {index}: artifact entries must be string path/digest pairs"]
    if not pinned:
        return [f"row {index}: artifact {path!r} has an empty digest"]
    return _check_pinned_artifact(path, pinned, index, context)


def _check_pinned_artifact(path: str, pinned: str, index: int, context: LedgerContext) -> list[str]:
    if _is_ledger_artifact(path, context):
        return [f"row {index}: artifact {path!r} is self-referential"]
    candidate = context.repository_root / path
    if not candidate.is_file():
        return [f"row {index}: dangling artifact path {path!r}"]
    actual = sha256(candidate.read_bytes()).hexdigest()
    if actual != pinned:
        return [f"row {index}: artifact {path!r} digest mismatch"]
    return []


def _check_row(row: Any, index: int, context: LedgerContext) -> list[str]:
    if not isinstance(row, dict):
        return [f"row {index}: must be an object"]
    typed_row: dict[str, Any] = cast(dict[str, Any], row)
    errors: list[str] = []
    errors.extend(_check_keys(typed_row, index))
    errors.extend(_check_state(typed_row, index))
    errors.extend(_check_test_refs(typed_row, index, context))
    errors.extend(_check_exact_head_evidence(typed_row, index, context))
    errors.extend(_check_artifact_hash(typed_row, index, context))
    return errors


def validate_ledger_document(document: Any, context: LedgerContext) -> LedgerResult:
    """Validate an already-parsed ledger document."""
    if not isinstance(document, dict):
        return LedgerResult(ok=False, errors=("ledger must be a JSON object",))
    typed_document: dict[str, Any] = cast(dict[str, Any], document)
    manifest: Any = typed_document.get("manifest")
    rows: Any = typed_document.get("rows")
    errors: list[str] = []
    errors.extend(_check_manifest(manifest))
    if not isinstance(rows, list) or not rows:
        errors.append("ledger rows must be a non-empty list")
        return LedgerResult(ok=False, errors=tuple(errors))
    typed_rows: list[dict[str, Any]] = cast(list[dict[str, Any]], rows)
    for index, row in enumerate(typed_rows):
        errors.extend(_check_row(row, index, context))
    errors.extend(_check_no_duplicate_rows(typed_rows))
    return LedgerResult(ok=len(errors) == 0, errors=tuple(errors))


def _check_manifest(manifest: Any) -> list[str]:
    if not isinstance(manifest, dict):
        return ["manifest must be an object"]
    typed_manifest: dict[str, Any] = cast(dict[str, Any], manifest)
    missing = {"slice", "head", "tree"} - set(typed_manifest.keys())
    if missing:
        return [f"manifest missing keys {sorted(missing)}"]
    return []


def _row_sort_key(claimed: list[str]) -> str:
    return str(sorted(claimed))


def _check_no_duplicate_rows(rows: Sequence[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    errors: list[str] = []
    for index, row in enumerate(rows):
        claimed: Any = row.get("test", [])
        ordered = (
            _row_sort_key(cast(list[str], claimed)) if isinstance(claimed, list) else str(claimed)
        )
        key = f"{row.get('requirement')}|{ordered}"
        if key in seen:
            errors.append(f"row {index}: duplicate requirement/test pair")
        seen.add(key)
    return errors


def validate_ledger_file(
    ledger_path: Path,
    repository_root: Path,
    executed_tests: frozenset[str],
    skipped_tests: frozenset[str],
) -> LedgerResult:
    """Load and validate one ledger file."""
    context = LedgerContext(
        repository_root=repository_root,
        ledger_path=ledger_path,
        executed_tests=executed_tests,
        skipped_tests=skipped_tests,
    )
    try:
        document = json.loads(ledger_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return LedgerResult(ok=False, errors=(f"cannot load ledger: {exc}",))
    return validate_ledger_document(document, context)
