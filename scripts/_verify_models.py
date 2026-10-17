"""Stable value types for repository verification."""

from __future__ import annotations

import shlex
from collections.abc import Callable
from typing import Literal, NamedTuple, Never, TypedDict

ProfileName = Literal["static", "compatibility", "services", "all"]


GateStatus = Literal["passed", "failed", "not-run"]


RedactionPolicy = Literal["configured-secrets"]


class Gate(NamedTuple):
    """One required command in a verification profile."""

    id: str
    command: tuple[str, ...]
    needs_services: bool = False
    redaction_policy: RedactionPolicy = "configured-secrets"
    environment: tuple[tuple[str, str], ...] = ()

    @property
    def command_text(self) -> str:
        """Render a copyable shell representation for diagnostics."""
        return shlex.join(self.command)


class CommandExecution(NamedTuple):
    """Captured result returned by an injected gate runner."""

    exit_code: int
    stdout: str = ""
    stderr: str = ""
    duration_seconds: float = 0.0


class GateResult(NamedTuple):
    """Stable evidence for one selected gate."""

    id: str
    command: str
    status: GateStatus
    exit_code: int | None
    duration_seconds: float
    summary: str
    stdout: str = ""
    stderr: str = ""


class VerificationResult(NamedTuple):
    """Aggregate outcome for one profile."""

    profile: ProfileName
    status: Literal["passed", "failed"]
    exit_code: int
    duration_seconds: float
    gates: tuple[GateResult, ...]
    first_failed_gate: str | None


Runner = Callable[[Gate], CommandExecution]


class InvocationErrorPayload(TypedDict):
    """An invalid invocation has no attempted gate rows."""

    status: Literal["error"]
    exit_code: int
    gates: list[Never]
    error: str
