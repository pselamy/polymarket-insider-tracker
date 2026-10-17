"""Adversarial nominal models and primitive subclasses cannot change source meaning."""

from __future__ import annotations

import io
import logging
import traceback
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path, PosixPath
from typing import SupportsIndex, cast

import pytest
from scripts._secret_git import GitObjects, ObjectType, ResolutionBudget, validate_objects
from scripts._secret_identity import FindingIdentityV1, SourceScope, validate_scope
from scripts._secret_models import NativeFinding, NativeReportError, ScanMode
from scripts._secret_process import GitLocation, run_git, validate_location
from scripts._secret_report import parse_report
from scripts._secret_schema import validate_native_finding
from scripts._secret_source import bind_finding
from tests.tooling.secret_source_fixtures import finding, repository, single_file, source_scope
from tests.tooling.test_secret_process import program


class RedirectPath(str):
    def split(self, sep: str | None = None, maxsplit: SupportsIndex = -1) -> list[str]:
        return "note.py".split(sep, maxsplit)


class PermissiveCommits(frozenset[str]):
    def __contains__(self, item: object) -> bool:
        return item is not None


class NativeText(str):
    pass


class NativeTags(tuple[str, ...]):
    pass


class RedirectLocation(PosixPath):
    def __str__(self) -> str:
        raise AssertionError("untrusted path method invoked")


def assert_private_failure(operation: Callable[[], object]) -> None:
    private = "unrelated-" + "private-context-sentinel"
    output = io.StringIO()
    logger = logging.Logger("native-boundary")
    logger.addHandler(logging.StreamHandler(output))
    try:
        raise RuntimeError(private)
    except RuntimeError:
        with pytest.raises(NativeReportError) as raised:
            try:
                operation()
            except NativeReportError:
                logger.exception("native boundary failure")
                raise
    assert private not in "".join(traceback.format_exception(raised.value))
    assert private not in output.getvalue()
    assert raised.value.__suppress_context__


@pytest.mark.parametrize(
    "model",
    ["finding", "scope", "location", "objects", "identity", "location-kind", "objects-kind"],
)
def test_uninitialized_nominal_models_fail_privately(tmp_path: Path, model: str) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo)
    operations: dict[str, Callable[[], object]] = {
        "finding": lambda: validate_native_finding(object.__new__(NativeFinding)),
        "scope": lambda: validate_scope(object.__new__(SourceScope)),
        "location": lambda: run_git(object.__new__(GitLocation), ()),
        "objects": lambda: bind_finding(
            finding(head), source_scope(head), object.__new__(GitObjects)
        ),
        "identity": lambda: object.__new__(FindingIdentityV1).canonical_bytes(),
        "location-kind": lambda: validate_location("private"),
        "objects-kind": lambda: validate_objects("private"),
    }
    assert_private_failure(operations[model])


def test_path_subclass_cannot_redirect_committed_resolution(tmp_path: Path) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo)
    row = finding(head, path=RedirectPath("absent.py"))
    assert_private_failure(lambda: bind_finding(row, source_scope(head), GitObjects(repo.location)))


def test_membership_subclass_cannot_expand_retained_scope(tmp_path: Path) -> None:
    repo = repository(tmp_path / "repo.git")
    old = single_file(repo, b"old\n")
    head = single_file(repo, b"new\n")
    retained = PermissiveCommits((head,))
    assert old not in frozenset(retained)
    assert_private_failure(lambda: SourceScope(17, "a" * 64, head, retained))


@pytest.mark.parametrize("field", ["native_commit", "path", "rule_id", "native_fingerprint"])
def test_native_text_subclasses_are_not_exact_wire_metadata(field: str) -> None:
    row = finding("1" * 40)
    ordinary = cast(str, getattr(row, field))
    invalid = replace(row)
    object.__setattr__(invalid, field, NativeText(ordinary))
    assert_private_failure(lambda: validate_native_finding(invalid))


@pytest.mark.parametrize("tags", [NativeTags(("fixture",)), (NativeText("fixture"),)])
def test_tag_container_and_elements_are_exact(tags: tuple[str, ...]) -> None:
    row = replace(finding("1" * 40), tags=tags)
    assert_private_failure(lambda: validate_native_finding(row))


@pytest.mark.parametrize("field", ["scanner_policy_sha256", "head", "retained_commits"])
def test_scope_text_is_exact(field: str) -> None:
    scope = source_scope("1" * 40)
    invalid = replace(scope)
    if field == "retained_commits":
        value: object = frozenset({NativeText("1" * 40)})
    else:
        value = NativeText(cast(str, getattr(scope, field)))
    object.__setattr__(invalid, field, value)
    assert_private_failure(lambda: validate_scope(invalid))


@pytest.mark.parametrize("version", [7, True, 1.0])
def test_canonical_identity_revalidates_fixed_schema(version: object) -> None:
    identity = FindingIdentityV1(17, "a" * 64, finding("1" * 40), "1" * 40, "2" * 40, "3" * 64)
    object.__setattr__(identity, "schema_version", version)
    assert_private_failure(identity.canonical_bytes)


@pytest.mark.parametrize("field", ["mode", "start_line", "end_line", "start_column", "end_column"])
def test_even_invalid_unvalidated_finding_repr_hides_text(field: str) -> None:
    row = replace(finding("1" * 40))
    private = "private-" + "metadata-sentinel"
    object.__setattr__(row, field, private)
    assert private not in repr(row)


@pytest.mark.parametrize("payload", ["private", bytearray(b"private")])
def test_process_requires_exact_bytes(tmp_path: Path, payload: object) -> None:
    location = program(tmp_path, "pass")
    assert_private_failure(lambda: run_git(location, (), input_bytes=cast(bytes, payload)))


@pytest.mark.parametrize("capacity", [False, 0, -1, "private", 16 * 1024 * 1024 + 1])
def test_process_requires_bounded_integer_capacity(tmp_path: Path, capacity: object) -> None:
    location = program(tmp_path, "pass")
    assert_private_failure(lambda: run_git(location, (), limit=cast(int, capacity)))


@pytest.mark.parametrize("arguments", [list[str](), (NativeText("private"),), (False,)])
def test_process_requires_exact_argument_primitives(tmp_path: Path, arguments: object) -> None:
    location = program(tmp_path, "pass")
    assert_private_failure(lambda: run_git(location, cast(tuple[str, ...], arguments)))


@pytest.mark.parametrize(
    "deadline", [float("nan"), float("inf"), "private", pytest.param(10**10000, id="huge-integer")]
)
def test_process_requires_finite_numeric_deadline(tmp_path: Path, deadline: object) -> None:
    location = program(tmp_path, "pass")
    assert_private_failure(lambda: run_git(location, (), deadline=cast(float, deadline)))


def test_location_fields_require_native_path_values(tmp_path: Path) -> None:
    assert_private_failure(lambda: GitLocation(cast(Path, "private"), tmp_path))


def test_location_subclass_methods_never_run(tmp_path: Path) -> None:
    redirected = RedirectLocation(tmp_path)
    assert_private_failure(lambda: GitLocation(redirected, tmp_path))


class SkippedIdentityValidation(FindingIdentityV1):
    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        pass


class ReplacedIdentitySerialization(SkippedIdentityValidation):
    def canonical_bytes(self) -> bytes:
        return b"forged-noncanonical-payload"


@pytest.mark.parametrize(
    "entrypoint", ["canonical", "hash", "base-post", "overridden-serialization"]
)
def test_identity_subclass_cannot_bypass_base_entrypoints(entrypoint: str) -> None:
    subtype = (
        ReplacedIdentitySerialization
        if entrypoint == "overridden-serialization"
        else SkippedIdentityValidation
    )
    identity = subtype(True, "private", finding("1" * 40), "1" * 40, "private", "private")
    object.__setattr__(identity, "schema_version", 7)
    operations: dict[str, Callable[[], object]] = {
        "canonical": identity.canonical_bytes,
        "hash": lambda: identity.sha256,
        "base-post": lambda: FindingIdentityV1.__post_init__(identity),
        "overridden-serialization": lambda: identity.sha256,
    }
    assert_private_failure(operations[entrypoint])


@pytest.mark.parametrize("value", ["dir", "git", "private"])
@pytest.mark.parametrize("entrypoint", ["metadata", "report", "canonical"])
def test_uninitialized_enum_values_are_not_scan_modes(value: str, entrypoint: str) -> None:
    forged = str.__new__(ScanMode, value)
    row = replace(finding("1" * 40, ScanMode.TREE), mode=forged)
    operations: dict[str, Callable[[], object]] = {
        "metadata": lambda: validate_native_finding(row),
        "report": lambda: parse_report(b"[]", forged, 0),
        "canonical": lambda: FindingIdentityV1(
            17, "a" * 64, row, "1" * 40, "2" * 40, "3" * 64
        ).canonical_bytes(),
    }
    assert_private_failure(operations[entrypoint])


@pytest.mark.parametrize("kind", [True, None, NativeText("blob")])
def test_direct_reader_requires_exact_supported_kind(tmp_path: Path, kind: object) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo)
    blob = repo.git("rev-parse", f"{head}:note.py").decode().strip()
    objects = GitObjects(repo.location)
    assert_private_failure(lambda: objects.read(blob, cast(ObjectType, kind), ResolutionBudget()))


def test_partially_initialized_native_path_fails_privately(tmp_path: Path) -> None:
    partial = object.__new__(PosixPath)
    object.__setattr__(partial, "_root", "/")
    object.__setattr__(partial, "_drv", "")
    assert_private_failure(lambda: run_git(GitLocation(partial, tmp_path), ()))


class RedirectReportBytes(bytes):
    def decode(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        return b"[]".decode(encoding, errors)


class RaisingReportBytes(bytes):
    def decode(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        raise RuntimeError(f"candidate-byte-{encoding}-{errors}")


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(RedirectReportBytes(b"not-json"), id="non-json-subclass"),
        pytest.param(RedirectReportBytes(b"\xff"), id="invalid-utf8-subclass"),
        pytest.param(bytearray(b"[]"), id="mutable-bytearray"),
        pytest.param(RaisingReportBytes(b"[]"), id="decode-must-not-run"),
    ],
)
def test_report_bytes_are_exact_immutable_input(payload: object) -> None:
    assert_private_failure(lambda: parse_report(cast(bytes, payload), ScanMode.TREE, 0))
