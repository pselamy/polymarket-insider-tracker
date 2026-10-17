"""Direct native types and complete commit/tree/blob OIDs are all verified."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal

from scripts._secret_identity import hexadecimal
from scripts._secret_models import NativeReportError
from scripts._secret_process import OBJECT_BYTES, GitLocation, run_git
from scripts._secret_schema import native_path
from scripts._secret_tree import select_entry

RESOLUTION_BYTES = 64 * 1024 * 1024
RESOLUTION_COMMANDS = 1024
RESOLUTION_SECONDS = 30.0
PATH_COMPONENTS = 256
ObjectType = Literal["commit", "tree", "blob"]


@dataclass(slots=True)
class ResolutionBudget:
    """Per-resolution accountant, not a worker sandbox or authenticity claim."""

    deadline: float = field(default_factory=lambda: time.monotonic() + RESOLUTION_SECONDS)
    commands: int = 0
    raw_bytes: int = 0

    def command(self) -> float:
        self.commands += 1
        if self.commands > RESOLUTION_COMMANDS:
            raise NativeReportError("native resolution command limit") from None
        if time.monotonic() >= self.deadline:
            raise NativeReportError("native resolution timeout") from None
        return self.deadline

    def account(self, size: int) -> None:
        self.raw_bytes += size
        if self.raw_bytes > RESOLUTION_BYTES:
            raise NativeReportError("native resolution byte limit") from None


@dataclass(frozen=True, slots=True)
class SourceBlob:
    commit: str
    oid: str
    mode: bytes
    body: bytes = field(repr=False)


def commit_tree_oid(body: bytes) -> str:
    """Read the exact first tree header of an already hash-verified commit body."""
    header = body[:46]
    if not header.startswith(b"tree ") or not header.endswith(b"\n"):
        raise NativeReportError("native commit tree header") from None
    try:
        return hexadecimal(header[5:45].decode("ascii"), 40)
    except UnicodeError:
        raise NativeReportError("native commit tree encoding") from None


def _directory(mode: bytes) -> None:
    if mode != b"40000":
        raise NativeReportError("native source directory required") from None


def _regular(mode: bytes) -> None:
    if mode not in (b"100644", b"100755"):
        raise NativeReportError("native regular source required") from None


@dataclass(frozen=True, slots=True)
class GitObjects:
    """Read/hash a declared store; its ownership and execution provenance are external."""

    location: GitLocation = field(repr=False)

    def _run(
        self,
        args: tuple[str, ...],
        budget: ResolutionBudget,
        *,
        limit: int = 64,
        payload: bytes = b"",
    ) -> bytes:
        return run_git(
            self.location, args, limit=limit, input_bytes=payload, deadline=budget.command()
        )

    def _format(self, budget: ResolutionBudget) -> None:
        if self._run(("rev-parse", "--show-object-format=storage"), budget) != b"sha1\n":
            raise NativeReportError("native object format unsupported") from None

    def read(self, oid: str, kind: ObjectType, budget: ResolutionBudget) -> bytes:
        hexadecimal(oid, 40)
        if self._run(("cat-file", "-t", oid), budget) != kind.encode() + b"\n":
            raise NativeReportError("native direct object type") from None
        body = self._run(("cat-file", kind, oid), budget, limit=OBJECT_BYTES)
        budget.account(len(body))
        actual = self._run(
            ("hash-object", "--no-filters", "-t", kind, "--stdin"), budget, payload=body
        )
        if actual != oid.encode() + b"\n":
            raise NativeReportError("native object digest mismatch") from None
        return body

    def resolve(self, commit: str, path: str) -> SourceBlob:
        path = native_path(path)
        budget = ResolutionBudget()
        self._format(budget)
        tree = commit_tree_oid(self.read(commit, "commit", budget))
        components = path.split("/")
        if len(components) > PATH_COMPONENTS:
            raise NativeReportError("native source path depth") from None
        for component in components[:-1]:
            entry = select_entry(self.read(tree, "tree", budget), component.encode("utf-8"))
            _directory(entry.mode)
            tree = entry.oid
        entry = select_entry(self.read(tree, "tree", budget), components[-1].encode("utf-8"))
        _regular(entry.mode)
        return SourceBlob(commit, entry.oid, entry.mode, self.read(entry.oid, "blob", budget))
