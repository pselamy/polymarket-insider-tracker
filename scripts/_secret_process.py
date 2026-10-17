"""Bounded native Git transport. A chosen executable/store is not authentication."""

from __future__ import annotations

import math
import os
import select
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, cast

from scripts._secret_models import NativeReportError

OBJECT_BYTES = 16 * 1024 * 1024
METADATA_BYTES = 64
COMMAND_SECONDS = 10.0


def _absolute(value: object) -> Path:
    if not isinstance(value, Path) or type(value) is not type(Path()):
        raise NativeReportError("native absolute location") from None
    path = Path(str(value))
    if not path.is_absolute():
        raise NativeReportError("native absolute location") from None
    return path


@dataclass(frozen=True, slots=True)
class GitLocation:
    """The protected caller must independently qualify and own both locations."""

    executable: Path = field(repr=False)
    store: Path = field(repr=False)

    def __post_init__(self) -> None:
        validate_location(self)


def validate_location(value: object) -> GitLocation:
    if type(value) is not GitLocation:
        raise NativeReportError("native location schema") from None
    try:
        _absolute(value.executable)
        _absolute(value.store)
    except (AttributeError, TypeError):
        raise NativeReportError("native location schema") from None
    return value


def _arguments(value: object) -> tuple[str, ...]:
    if type(value) is not tuple:
        raise NativeReportError("native argument schema") from None
    for argument in cast(tuple[object, ...], value):
        if type(argument) is not str:
            raise NativeReportError("native argument schema") from None
    return cast(tuple[str, ...], value)


def _capacity(value: object) -> int:
    if type(value) is not int or value < 1 or value > OBJECT_BYTES:
        raise NativeReportError("native output capacity") from None
    return value


def _input_bytes(value: object) -> bytes:
    if type(value) is not bytes:
        raise NativeReportError("native input schema") from None
    if len(value) > OBJECT_BYTES:
        raise NativeReportError("native input limit") from None
    return value


def _finite_deadline(value: object) -> float:
    if type(value) not in (int, float):
        raise NativeReportError("native deadline schema") from None
    try:
        number = float(cast(int | float, value))
    except OverflowError:
        raise NativeReportError("native deadline schema") from None
    if not math.isfinite(number):
        raise NativeReportError("native deadline schema") from None
    return number


def _environment() -> dict[str, str]:
    return {
        "PATH": os.defpath,
        "LC_ALL": "C",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_SYSTEM": os.devnull,
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_COUNT": "0",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ALLOW_PROTOCOL": "",
        "GIT_NO_LAZY_FETCH": "1",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_GRAFT_FILE": os.devnull,
    }


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise NativeReportError("native command timeout") from None
    return remaining


def _chunk(descriptor: int, deadline: float) -> bytes:
    readable, _, _ = select.select([descriptor], [], [], _remaining(deadline))
    if not readable:
        raise NativeReportError("native command timeout") from None
    return os.read(descriptor, 65536)


def _append(output: bytearray, chunk: bytes, limit: int) -> None:
    if len(output) + len(chunk) > limit:
        raise NativeReportError("native output limit") from None
    output.extend(chunk)


def _capture(process: subprocess.Popen[bytes], limit: int, deadline: float) -> bytes:
    stream = process.stdout
    if stream is None:
        raise NativeReportError("native output unavailable") from None
    output = bytearray()
    chunk = _chunk(stream.fileno(), deadline)
    while chunk:
        _append(output, chunk, limit)
        chunk = _chunk(stream.fileno(), deadline)
    code = process.wait(timeout=_remaining(deadline))
    if code != 0:
        raise NativeReportError("native command failed") from None
    return bytes(output)


def _stop(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.kill()
    process.wait()


def _execute(
    location: GitLocation, arguments: tuple[str, ...], source: BinaryIO, limit: int, deadline: float
) -> bytes:
    argv = [
        str(_absolute(location.executable)),
        "--no-replace-objects",
        "--no-lazy-fetch",
        "--git-dir",
        str(_absolute(location.store)),
        *arguments,
    ]
    with subprocess.Popen(
        argv, stdin=source, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=_environment()
    ) as process:
        try:
            return _capture(process, limit, deadline)
        finally:
            _stop(process)


def _command_deadline(declared: float | None) -> float:
    end = time.monotonic() + COMMAND_SECONDS
    if declared is None:
        return end
    return min(end, _finite_deadline(declared))


def run_git(
    location: GitLocation,
    arguments: tuple[str, ...],
    *,
    limit: int = METADATA_BYTES,
    input_bytes: bytes = b"",
    deadline: float | None = None,
) -> bytes:
    """Return complete bounded stdout or a constant failure; never emit native stderr."""
    location = validate_location(location)
    arguments = _arguments(arguments)
    limit = _capacity(limit)
    input_bytes = _input_bytes(input_bytes)
    end = _command_deadline(deadline)
    try:
        with tempfile.TemporaryFile() as source:
            source.write(input_bytes)
            source.seek(0)
            return _execute(location, arguments, source, limit, end)
    except subprocess.TimeoutExpired:
        raise NativeReportError("native command timeout") from None
    except (OSError, ValueError, AttributeError, TypeError) as error:
        if isinstance(error, NativeReportError):
            raise error from None
        raise NativeReportError("native transport failure") from None
