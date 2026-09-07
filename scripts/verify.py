#!/usr/bin/env python3
"""Run one fail-closed repository verification profile."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Literal, NamedTuple, cast
from urllib.parse import urlsplit, urlunsplit

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ProfileName = Literal["static", "compatibility", "services", "all"]
GateStatus = Literal["passed", "failed", "not-run"]


class Gate(NamedTuple):
    """One required command in a verification profile."""

    id: str
    command: tuple[str, ...]

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

GATES: Mapping[str, Gate] = {
    "lock": Gate("lock", ("uv", "lock", "--check")),
    "support-contract": Gate(
        "support-contract", ("uv", "run", "python", "scripts/check_support_contract.py")
    ),
    "format": Gate("format", ("uv", "run", "ruff", "format", "--check", "src", "tests", "scripts")),
    "lint": Gate("lint", ("uv", "run", "ruff", "check", "src", "tests", "scripts")),
    "mypy": Gate(
        "mypy",
        (
            "uv",
            "run",
            "--isolated",
            "--locked",
            "--all-extras",
            "--python",
            "3.11",
            "mypy",
        ),
    ),
    "imports": Gate(
        "imports",
        (
            "uv",
            "run",
            "python",
            "-c",
            (
                "import alembic, greenlet, psycopg, redis, sqlalchemy; "
                "import polymarket_insider_tracker; "
                "from sqlalchemy.ext.asyncio import create_async_engine"
            ),
        ),
    ),
    "tests": Gate("tests", ("uv", "run", "pytest")),
    "services": Gate(
        "services",
        ("uv", "run", "python", "scripts/runtime_services.py", "--phase", "probe"),
    ),
    "migrations": Gate(
        "migrations",
        ("uv", "run", "python", "scripts/runtime_services.py", "--phase", "migrations"),
    ),
}

BASE_PROFILES: Mapping[str, tuple[str, ...]] = {
    "static": ("lock", "support-contract", "format", "lint", "mypy"),
    "compatibility": ("lock", "imports", "tests"),
    "services": ("services", "migrations"),
}
PROFILE_NAMES = ("static", "compatibility", "services", "all")
SENSITIVE_ENVIRONMENT_KEYS = (
    "DATABASE_URL",
    "REDIS_URL",
    "POLYMARKET_API_KEY",
    "DISCORD_WEBHOOK_URL",
    "TELEGRAM_BOT_TOKEN",
)


def gate_ids_for_profile(profile: str) -> tuple[str, ...]:
    """Return the exact ordered, de-duplicated gate IDs for ``profile``."""
    if profile in BASE_PROFILES:
        return BASE_PROFILES[profile]
    if profile != "all":
        raise ValueError(f"unknown profile {profile!r}")
    ordered: list[str] = []
    for profile_name in ("static", "compatibility", "services"):
        for gate_id in BASE_PROFILES[profile_name]:
            if gate_id not in ordered:
                ordered.append(gate_id)
    return tuple(ordered)


def _redacted_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return "***"
    if parsed.hostname is None:
        return "***"
    host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
    port = f":{parsed.port}" if parsed.port is not None else ""
    user = f"{parsed.username}:***@" if parsed.username is not None else ""
    return urlunsplit(
        (parsed.scheme, f"{user}{host}{port}", parsed.path, parsed.query, parsed.fragment)
    )


def redact_text(text: str, environment: Mapping[str, str] | None = None) -> str:
    """Redact configured credentials and complete sensitive values from diagnostics."""
    values = environment if environment is not None else os.environ
    redacted = text
    for key in SENSITIVE_ENVIRONMENT_KEYS:
        value = values.get(key)
        if not value:
            continue
        replacement = _redacted_url(value) if key.endswith("_URL") else "***"
        redacted = redacted.replace(value, replacement)
        if key.endswith("_URL"):
            try:
                password = urlsplit(value).password
            except ValueError:
                password = None
            if password:
                redacted = redacted.replace(password, "***")
        else:
            redacted = redacted.replace(value, "***")
    return redacted


def _run_command(gate: Gate) -> CommandExecution:
    started = time.monotonic()
    try:
        completed = subprocess.run(
            gate.command,
            cwd=REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        return CommandExecution(
            exit_code=127,
            stderr=str(exc),
            duration_seconds=time.monotonic() - started,
        )
    return CommandExecution(
        exit_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        duration_seconds=time.monotonic() - started,
    )


def _summary(execution: CommandExecution) -> str:
    for stream in (execution.stderr, execution.stdout):
        for line in stream.splitlines():
            if line.strip():
                return line.strip()
    return "command returned a nonzero exit status"


def run_verification(profile: str, *, runner: Runner | None = None) -> VerificationResult:
    """Execute ``profile`` until its first failure and mark later gates not-run."""
    selected = gate_ids_for_profile(profile)
    typed_profile = cast(ProfileName, profile)
    execute = runner or _run_command
    results: list[GateResult] = []
    first_failed_gate: str | None = None
    total_duration = 0.0

    for gate_id in selected:
        gate = GATES[gate_id]
        if first_failed_gate is not None:
            results.append(
                GateResult(
                    id=gate.id,
                    command=gate.command_text,
                    status="not-run",
                    exit_code=None,
                    duration_seconds=0.0,
                    summary=f"not run after {first_failed_gate} failed",
                )
            )
            continue
        try:
            execution = execute(gate)
        except Exception as exc:
            execution = CommandExecution(exit_code=1, stderr=f"runner error: {exc}")
        safe_execution = CommandExecution(
            exit_code=execution.exit_code,
            stdout=redact_text(execution.stdout),
            stderr=redact_text(execution.stderr),
            duration_seconds=execution.duration_seconds,
        )
        total_duration += safe_execution.duration_seconds
        if safe_execution.exit_code == 0:
            status: GateStatus = "passed"
            summary = next(
                (line.strip() for line in safe_execution.stdout.splitlines() if line.strip()),
                "passed",
            )
        else:
            status = "failed"
            summary = _summary(safe_execution)
            first_failed_gate = gate.id
        results.append(
            GateResult(
                id=gate.id,
                command=gate.command_text,
                status=status,
                exit_code=safe_execution.exit_code,
                duration_seconds=safe_execution.duration_seconds,
                summary=summary,
                stdout=safe_execution.stdout,
                stderr=safe_execution.stderr,
            )
        )

    aggregate_status: Literal["passed", "failed"] = (
        "failed" if first_failed_gate is not None else "passed"
    )
    return VerificationResult(
        profile=typed_profile,
        status=aggregate_status,
        exit_code=1 if first_failed_gate is not None else 0,
        duration_seconds=total_duration,
        gates=tuple(results),
        first_failed_gate=first_failed_gate,
    )


def render_human(result: VerificationResult) -> str:
    """Render copyable commands, captured diagnostics, and stable statuses."""
    lines: list[str] = []
    for gate in result.gates:
        if gate.status == "not-run":
            lines.append(f"[SKIP] {gate.id}: {gate.summary}")
            continue
        lines.append(f"[RUN ] {gate.id}: {gate.command}")
        if gate.stdout:
            lines.extend(gate.stdout.rstrip().splitlines())
        if gate.stderr:
            lines.extend(gate.stderr.rstrip().splitlines())
        if gate.status == "passed":
            lines.append(f"[PASS] {gate.id} ({gate.duration_seconds:.2f}s)")
        else:
            lines.append(
                f"[FAIL] {gate.id} (exit {gate.exit_code}, "
                f"{gate.duration_seconds:.2f}s): {gate.summary}"
            )
    lines.append(f"status: {result.status}")
    lines.append(f"duration: {result.duration_seconds:.2f}s")
    if result.first_failed_gate is not None:
        lines.append(f"first failed gate: {result.first_failed_gate}")
    return "\n".join(lines)


def render_json(result: VerificationResult) -> str:
    """Render exactly one stable JSON object without raw subprocess output."""
    payload = {
        "profile": result.profile,
        "status": result.status,
        "exit_code": result.exit_code,
        "duration_seconds": round(result.duration_seconds, 3),
        "gates": [
            {
                "id": gate.id,
                "status": gate.status,
                "exit_code": gate.exit_code,
                "duration_seconds": round(gate.duration_seconds, 3),
                "summary": gate.summary,
            }
            for gate in result.gates
        ],
    }
    return json.dumps(payload, separators=(",", ":"))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, exit_on_error=False)
    parser.add_argument("--profile", choices=PROFILE_NAMES, required=True)
    parser.add_argument("--json", action="store_true", help="emit exactly one JSON object")
    return parser


def main(argv: Sequence[str] | None = None, *, runner: Runner | None = None) -> int:
    """Parse one profile, execute it, and map the result to its stable exit code."""
    try:
        args = _parser().parse_args(argv)
    except argparse.ArgumentError as exc:
        print(f"verify.py: error: {exc}", file=sys.stderr)
        return 2
    result = run_verification(args.profile, runner=runner)
    print(render_json(result) if args.json else render_human(result))
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
