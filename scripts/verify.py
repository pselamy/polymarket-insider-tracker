#!/usr/bin/env python3
"""Run one fail-closed repository verification profile."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Literal, NamedTuple, NoReturn, cast
from urllib.parse import unquote

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ProfileName = Literal["static", "compatibility", "services", "all"]
GateStatus = Literal["passed", "failed", "not-run"]
RedactionPolicy = Literal["configured-secrets"]
GATE_FAILURE_EXIT_CODE = 1
PREREQUISITE_EXIT_CODE = 2


class Gate(NamedTuple):
    """One required command in a verification profile."""

    id: str
    command: tuple[str, ...]
    needs_services: bool = False
    redaction_policy: RedactionPolicy = "configured-secrets"

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
    "format": Gate("format", (sys.executable, "-m", "black", "--check", ".")),
    "lint": Gate("lint", (sys.executable, "-m", "ruff", "check", "src", "tests", "scripts")),
    "strict-types": Gate(
        "strict-types",
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
    "pyright": Gate(
        "pyright",
        (
            "uv",
            "run",
            "--isolated",
            "--locked",
            "--all-extras",
            "--python",
            "3.11",
            "pyright",
            "src/polymarket_insider_tracker",
        ),
    ),
    "vulture": Gate(
        "vulture",
        (
            "uv",
            "run",
            "--isolated",
            "--locked",
            "--all-extras",
            "--python",
            "3.11",
            "vulture",
            "src",
            "tests",
            "scripts",
            "alembic",
            "conftest.py",
        ),
    ),
    "complexipy": Gate(
        "complexipy",
        (
            "uv",
            "run",
            "--isolated",
            "--locked",
            "--all-extras",
            "--python",
            "3.11",
            "python",
            "scripts/complexipy_gate.py",
            "src",
            "tests",
            "scripts",
            "alembic",
            "conftest.py",
            "--max-complexity-allowed",
            "5",
            "--no-ignore",
            "--ignore-complexity=false",
            "--snapshot-ignore=true",
            "--snapshot-create=false",
            "--exclude=.",
            "--check-script=true",
        ),
    ),
    "imports": Gate(
        "imports",
        (
            sys.executable,
            "-c",
            (
                "import alembic, greenlet, psycopg, redis, sqlalchemy; "
                "import polymarket_insider_tracker; "
                "from sqlalchemy.ext.asyncio import create_async_engine"
            ),
        ),
    ),
    "tests": Gate("tests", (sys.executable, "-m", "pytest")),
    "services": Gate(
        "services",
        (sys.executable, "scripts/runtime_services.py", "--phase", "probe"),
        needs_services=True,
    ),
    "migrations": Gate(
        "migrations",
        (sys.executable, "scripts/runtime_services.py", "--phase", "migrations"),
        needs_services=True,
    ),
}

BASE_PROFILES: Mapping[str, tuple[str, ...]] = {
    "static": ("lock", "format", "lint", "strict-types", "pyright", "vulture", "complexipy"),
    "compatibility": ("lock", "imports", "tests"),
    "services": ("services", "migrations"),
}
PROFILE_NAMES = ("static", "compatibility", "services", "all")

# Service URLs are rendered with their credential hidden so diagnostics stay actionable; every
# other configured secret is replaced completely because its value may live anywhere in the string.
SERVICE_URL_KEYS = ("DATABASE_URL", "REDIS_URL")
SECRET_VALUE_KEYS = ("POLYMARKET_API_KEY", "DISCORD_WEBHOOK_URL", "TELEGRAM_BOT_TOKEN")
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

# scheme://authority[path][?query][#fragment]; the authority ends at the first "/", "?", or "#".
_URL_PATTERN = re.compile(
    r"^(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*)://(?P<authority>[^/?#]*)(?P<path>[^?#]*)(?P<tail>[?#].*)?$",
    re.DOTALL,
)

# Direct equivalents of every aggregate gate, kept literal so contributors can copy them verbatim.
DIRECT_GATE_COMMANDS: tuple[tuple[str, str], ...] = (
    ("lock", "uv lock --check"),
    ("format", "uv run black --check ."),
    ("lint", "uv run ruff check src tests scripts"),
    ("strict-types", "uv run --isolated --locked --all-extras --python 3.11 mypy"),
    (
        "pyright",
        "uv run --isolated --locked --all-extras --python 3.11 "
        "pyright src/polymarket_insider_tracker",
    ),
    (
        "vulture",
        "uv run --isolated --locked --all-extras --python 3.11 vulture "
        "src tests scripts alembic conftest.py",
    ),
    (
        "complexipy",
        "uv run --isolated --locked --all-extras --python 3.11 python scripts/complexipy_gate.py "
        "src tests scripts alembic conftest.py --max-complexity-allowed 5 --no-ignore "
        "--ignore-complexity=false --snapshot-ignore=true --snapshot-create=false --exclude=. "
        "--check-script=true",
    ),
    (
        "imports",
        'uv run python -c "import alembic, greenlet, psycopg, redis, sqlalchemy; '
        "import polymarket_insider_tracker; "
        'from sqlalchemy.ext.asyncio import create_async_engine"',
    ),
    ("tests", "uv run pytest"),
    ("services", "uv run --env-file .env python scripts/runtime_services.py --phase probe"),
    (
        "migrations",
        "uv run --env-file .env python scripts/runtime_services.py --phase migrations",
    ),
)
APPLY_MIGRATIONS_COMMAND = "uv run --env-file .env alembic upgrade head"


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


def _split_userinfo(authority: str) -> tuple[str | None, str | None, str]:
    """Split an authority into (username, password, hostinfo) exactly like URL consumers do."""
    userinfo, separator, hostinfo = authority.rpartition("@")
    if not separator:
        return None, None, hostinfo
    username, separator, password = userinfo.partition(":")
    return username, (password if separator else None), hostinfo


def _extract_query_password(pair: str) -> str | None:
    name, separator, raw_value = pair.partition("=")
    if not separator or not raw_value:
        return None
    return raw_value if name.casefold() == "password" else None


def _query_credentials(tail: str) -> list[str]:
    if not tail.startswith("?"):
        return []
    query = tail[1:].partition("#")[0]
    results: list[str] = []
    for pair in query.split("&"):
        pwd = _extract_query_password(pair)
        if pwd:
            results.append(pwd)
    return results


def _url_credentials(value: str) -> tuple[str, ...]:
    """Return the raw credentials a consumer would read from ``value``.

    Both the userinfo password and any ``password`` query parameter count, because redis-py and
    libpq accept either spelling. Parsing never requires a well-formed URL.
    """
    match = _URL_PATTERN.match(value)
    if match is None:
        return ()
    credentials: list[str] = []
    password = _split_userinfo(match.group("authority"))[1]
    if password:
        credentials.append(password)
    credentials.extend(_query_credentials(match.group("tail") or ""))
    return tuple(credentials)


def _credential_forms(value: str) -> tuple[str, ...]:
    """Return every encoded or decoded credential spelling a tool might echo from ``value``."""
    raw_forms: list[str] = []
    for credential in _url_credentials(value):
        raw_forms.extend((credential, unquote(credential)))
    return tuple(dict.fromkeys(raw_forms))


def _redacted_url(value: str) -> str:
    """Render a service URL with its credential and query hidden, or ``***`` when unparseable.

    The host, port, and path are copied verbatim, so an out-of-range or non-numeric port can
    never break redaction. Query strings are hidden because Redis and libpq accept passwords there.
    """
    match = _URL_PATTERN.match(value)
    if match is None:
        return "***"
    username, password, hostinfo = _split_userinfo(match.group("authority"))
    if username is None:
        credential = ""
    elif password is None:
        credential = f"{username}@"
    else:
        credential = f"{username}:***@"
    query = "?***" if match.group("tail") else ""
    return f"{match.group('scheme')}://{credential}{hostinfo}{match.group('path')}{query}"


def _redact_service_url(text: str, value: str) -> str:
    redacted = text.replace(value, _redacted_url(value))
    for form in _credential_forms(value):
        redacted = redacted.replace(form, "***")
    return redacted


def _redact_urls(text: str, values: Mapping[str, str]) -> str:
    redacted = text
    for key in SERVICE_URL_KEYS:
        value = values.get(key)
        if value:
            redacted = _redact_service_url(redacted, value)
    return redacted


def _redact_secrets(text: str, values: Mapping[str, str]) -> str:
    redacted = text
    for key in SECRET_VALUE_KEYS:
        value = values.get(key)
        if value:
            redacted = redacted.replace(value, "***")
    return redacted


def redact_text(text: str, environment: Mapping[str, str] | None = None) -> str:
    """Redact configured credentials and complete sensitive values from diagnostics."""
    values = environment if environment is not None else os.environ
    return _redact_secrets(_redact_urls(text, values), values)


def _environment_for_gate(gate: Gate) -> dict[str, str] | None:
    """Return an isolated environment for deterministic tests; other gates inherit the caller."""
    if gate.id != "tests":
        return None
    return {
        key: value
        for key, value in os.environ.items()
        if key not in TEST_CONFIGURATION_KEYS and not key.startswith(TEST_CONFIGURATION_PREFIXES)
    }


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
    payload = {
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
