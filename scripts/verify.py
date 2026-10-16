#!/usr/bin/env python3
"""Run one fail-closed repository verification profile."""

from __future__ import annotations

import argparse
import json
import os
import re as re
import shlex as shlex
import subprocess
import sys
import time
from collections.abc import Callable as Callable
from collections.abc import Mapping as Mapping
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, NoReturn, cast
from typing import NamedTuple as NamedTuple
from urllib.parse import unquote as unquote

# Support the documented script path and arbitrary importlib loading without an installed project.
if not sys.path or sys.path[0] != str(Path(__file__).resolve().parents[1]):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts._verify_models import (
    CommandExecution,
    Gate,
    GateResult,
    GateStatus,
    ProfileName,
    Runner,
    VerificationResult,
)
from scripts._verify_models import InvocationErrorPayload as _InvocationErrorPayload
from scripts._verify_models import RedactionPolicy as RedactionPolicy
from scripts._verify_policy import (
    APPLY_MIGRATIONS_COMMAND,
    BASE_PROFILES,
    DIRECT_GATE_COMMANDS,
    GATES,
    PROFILE_NAMES,
)
from scripts._verify_redaction import SECRET_VALUE_KEYS as SECRET_VALUE_KEYS
from scripts._verify_redaction import SERVICE_URL_KEYS as SERVICE_URL_KEYS
from scripts._verify_redaction import redact_text

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
GATE_FAILURE_EXIT_CODE = 1
PREREQUISITE_EXIT_CODE = 2


TEST_CONFIGURATION_KEYS = frozenset(
    {"DATABASE_URL", "REDIS_URL", "LOG_LEVEL", "DRY_RUN", "HEALTH_PORT", "RUN_SERVICE_TESTS"}
)
TEST_CONFIGURATION_PREFIXES = (
    "POSTGRES_",
    "REDIS_",
    "POLYGON_",
    "POLYMARKET_",
    "DISCORD_",
    "TELEGRAM_",
)


def _all_profile_gate_ids() -> tuple[str, ...]:
    gates: list[str] = []
    for name in ("static", "compatibility", "services"):
        gates.extend(BASE_PROFILES[name])
    return tuple(dict.fromkeys(gates))


def gate_ids_for_profile(profile: str) -> tuple[str, ...]:
    """Return the exact ordered, de-duplicated gate IDs for ``profile``."""
    if profile in BASE_PROFILES:
        return BASE_PROFILES[profile]
    if profile == "all":
        return _all_profile_gate_ids()
    raise ValueError(f"unknown profile {profile!r}")


def _scrubbed_test_environment() -> dict[str, str]:
    """Return the caller's environment without any application or service configuration."""
    return {
        key: value
        for key, value in os.environ.items()
        if key not in TEST_CONFIGURATION_KEYS and not key.startswith(TEST_CONFIGURATION_PREFIXES)
    }


def _environment_for_gate(gate: Gate) -> dict[str, str] | None:
    """Return the environment a gate runs in.

    The deterministic ``tests`` gate is isolated from every application setting; a gate that
    declares ``environment`` overrides inherits the caller's environment plus those overrides; every
    other gate inherits the caller's environment unchanged.
    """
    if gate.id == "tests":
        return _scrubbed_test_environment()
    if gate.environment:
        return {**os.environ, **dict(gate.environment)}
    return None


def _run_command(gate: Gate) -> CommandExecution:
    started = time.monotonic()
    try:
        completed = subprocess.run(
            gate.command,
            cwd=REPOSITORY_ROOT,
            env=_environment_for_gate(gate),
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


def _first_nonempty_line(stream: str) -> str | None:
    for line in stream.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return None


def _summary(execution: CommandExecution) -> str:
    for stream in (execution.stderr, execution.stdout):
        line = _first_nonempty_line(stream)
        if line is not None:
            return line
    return "command returned a nonzero exit status"


def _aggregate_exit_code(failure: GateResult | None) -> int:
    """Map the first failed gate to the documented aggregate exit status.

    A gate that exits ``2`` reports an invocation or prerequisite problem before any destructive
    step began, so the aggregate preserves it; every other failure is an ordinary gate failure.
    """
    if failure is None:
        return 0
    if failure.exit_code == PREREQUISITE_EXIT_CODE:
        return PREREQUISITE_EXIT_CODE
    return GATE_FAILURE_EXIT_CODE


def _execute_gate(gate: Gate, execute: Runner) -> CommandExecution:
    try:
        execution = execute(gate)
    except Exception as exc:
        execution = CommandExecution(exit_code=1, stderr=f"runner error: {exc}")
    return CommandExecution(
        exit_code=execution.exit_code,
        stdout=redact_text(execution.stdout),
        stderr=redact_text(execution.stderr),
        duration_seconds=execution.duration_seconds,
    )


def _build_gate_result(gate: Gate, execution: CommandExecution) -> GateResult:
    if execution.exit_code == 0:
        status: GateStatus = "passed"
        summary = _first_nonempty_line(execution.stdout) or "passed"
    else:
        status = "failed"
        summary = _summary(execution)
    return GateResult(
        id=gate.id,
        command=gate.command_text,
        status=status,
        exit_code=execution.exit_code,
        duration_seconds=execution.duration_seconds,
        summary=summary,
        stdout=execution.stdout,
        stderr=execution.stderr,
    )


def _build_skipped_result(gate: Gate, failed_gate_id: str) -> GateResult:
    return GateResult(
        id=gate.id,
        command=gate.command_text,
        status="not-run",
        exit_code=None,
        duration_seconds=0.0,
        summary=f"not run after {failed_gate_id} failed",
    )


def _verification_outcome(
    failure: GateResult | None,
) -> tuple[Literal["passed", "failed"], str | None]:
    if failure is None:
        return "passed", None
    return "failed", failure.id


def _get_runner(runner: Runner | None) -> Runner:
    return runner if runner is not None else _run_command


def _step_gate(gate: Gate, execute: Runner) -> tuple[GateResult, float]:
    safe_execution = _execute_gate(gate, execute)
    gate_result = _build_gate_result(gate, safe_execution)
    return gate_result, safe_execution.duration_seconds


def run_verification(profile: str, *, runner: Runner | None = None) -> VerificationResult:
    """Execute ``profile`` until its first failure and mark later gates not-run."""
    selected = gate_ids_for_profile(profile)
    typed_profile = cast(ProfileName, profile)
    execute = _get_runner(runner)
    results: list[GateResult] = []
    failure: GateResult | None = None
    total_duration = 0.0

    for gate_id in selected:
        gate = GATES[gate_id]
        if failure is not None:
            results.append(_build_skipped_result(gate, failure.id))
            continue
        gate_result, duration = _step_gate(gate, execute)
        total_duration += duration
        results.append(gate_result)
        if gate_result.status == "failed":
            failure = gate_result

    status, first_failed = _verification_outcome(failure)
    return VerificationResult(
        profile=typed_profile,
        status=status,
        exit_code=_aggregate_exit_code(failure),
        duration_seconds=total_duration,
        gates=tuple(results),
        first_failed_gate=first_failed,
    )


def _render_gate_output(gate: GateResult) -> list[str]:
    lines: list[str] = []
    if gate.stdout:
        lines.extend(gate.stdout.rstrip().splitlines())
    if gate.stderr:
        lines.extend(gate.stderr.rstrip().splitlines())
    return lines


def _render_gate_status(gate: GateResult) -> str:
    if gate.status == "passed":
        return f"[PASS] {gate.id} ({gate.duration_seconds:.2f}s)"
    return (
        f"[FAIL] {gate.id} (exit {gate.exit_code}, "
        f"{gate.duration_seconds:.2f}s): {gate.summary}"
    )


def _render_gate(gate: GateResult) -> list[str]:
    if gate.status == "not-run":
        return [f"[SKIP] {gate.id}: {gate.summary}"]
    lines = [f"[RUN ] {gate.id}: {gate.command}"]
    lines.extend(_render_gate_output(gate))
    lines.append(_render_gate_status(gate))
    return lines


def render_human(result: VerificationResult) -> str:
    """Render copyable commands, captured diagnostics, and stable statuses."""
    lines: list[str] = []
    for gate in result.gates:
        lines.extend(_render_gate(gate))
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


def render_invocation_error(message: str) -> str:
    """Render the single JSON object emitted when ``--json`` accompanies an invalid invocation."""
    payload: _InvocationErrorPayload = {
        "status": "error",
        "exit_code": PREREQUISITE_EXIT_CODE,
        "gates": [],
        "error": message,
    }
    return json.dumps(payload, separators=(",", ":"))


def _help_epilog() -> str:
    width = max(len(gate_id) for gate_id, _ in DIRECT_GATE_COMMANDS)
    lines = ["direct gate commands (every aggregate gate remains directly runnable):"]
    lines.extend(f"  {gate_id:<{width}}  {command}" for gate_id, command in DIRECT_GATE_COMMANDS)
    lines.append("")
    lines.append("related commands:")
    lines.append(f"  {'apply-migrations':<{width}}  {APPLY_MIGRATIONS_COMMAND}")
    lines.append("")
    lines.append(
        "services and migrations read DATABASE_URL and REDIS_URL; load them with --env-file .env."
    )
    lines.append(
        "exit status: 0 every selected gate passed; 1 a gate ran and failed; "
        "2 invocation or prerequisite error."
    )
    return "\n".join(lines)


class _ArgumentParser(argparse.ArgumentParser):
    """Raise every usage error instead of exiting.

    Python 3.11 and 3.12 call ``error()`` directly for missing required and unrecognized
    arguments even with ``exit_on_error=False``; routing them through ``ArgumentError`` keeps
    the ``--json`` invocation-error contract identical on every supported interpreter.
    """

    def error(self, message: str) -> NoReturn:
        raise argparse.ArgumentError(None, message)


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="verify.py",
        description=__doc__,
        epilog=_help_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
        exit_on_error=False,
    )
    parser.add_argument("--profile", choices=PROFILE_NAMES, required=True)
    parser.add_argument("--json", action="store_true", help="emit exactly one JSON object")
    return parser


def _handle_invocation_error(
    parser: argparse.ArgumentParser, exc: Exception, json_mode: bool
) -> None:
    if json_mode:
        print(render_invocation_error(str(exc)))
    else:
        parser.print_usage(sys.stderr)
        print(f"{parser.prog}: error: {exc}", file=sys.stderr)


def main(argv: Sequence[str] | None = None, *, runner: Runner | None = None) -> int:
    """Parse one profile, execute it, and map the result to its stable exit code."""
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    parser = _parser()
    try:
        args = parser.parse_args(arguments)
    except argparse.ArgumentError as exc:
        _handle_invocation_error(parser, exc, "--json" in arguments)
        return PREREQUISITE_EXIT_CODE
    result = run_verification(args.profile, runner=runner)
    output = render_json(result) if args.json else render_human(result)
    print(output)
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
