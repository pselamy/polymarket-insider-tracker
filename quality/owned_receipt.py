"""Require a fresh complete native cleanup receipt before interpreting tool status."""

import json
import math
import subprocess
from pathlib import Path

from quality.owned_process import Cleanup, Completion
from quality.report_data import array, record

MINIMUM_PID = 0
MINIMUM_TIMEOUT = 0


def positive_timeout(value: float) -> None:
    if isinstance(value, bool) or not math.isfinite(value) or value <= MINIMUM_TIMEOUT:
        raise ValueError("supervised timeout must be positive and finite")


def integer(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError("supervision integer field is malformed")
    return value


def positive_pid(value: object) -> int:
    pid = integer(value)
    if pid <= MINIMUM_PID:
        raise ValueError("supervision PID must be positive")
    return pid


def boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError("supervision boolean field is malformed")
    return value


def reaped_pair(value: object) -> tuple[int, int]:
    pair = array(value)
    pid, status = pair
    return positive_pid(pid), integer(status)


def cleanup(value: object) -> Cleanup:
    data = record(value)
    if set(data) != {"killed", "reaped"}:
        raise ValueError("cleanup receipt fields are incomplete")
    killed = [positive_pid(item) for item in array(data["killed"])]
    reaped = [reaped_pair(item) for item in array(data["reaped"])]
    verify_reaped(killed, reaped)
    return Cleanup(killed, reaped)


def verify_reaped(killed: list[int], reaped: list[tuple[int, int]]) -> None:
    pids: set[int] = set()
    for pid, _status in reaped:
        if pid in pids:
            raise ValueError("duplicate reaped PID")
        pids.add(pid)
    if not set(killed).issubset(pids):
        raise ValueError("terminated children were not completely reaped")


def parse_completion(value: object, nonce: str) -> Completion:
    data = record(value)
    if set(data) != {"nonce", "returncode", "timed_out", "cleanup"}:
        raise ValueError("supervision receipt fields are incomplete")
    if data["nonce"] != nonce:
        raise ValueError("supervision receipt is stale or belongs to another run")
    return Completion(
        integer(data["returncode"]),
        boolean(data["timed_out"]),
        cleanup(data["cleanup"]),
    )


def no_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate supervision receipt key")
        result[key] = value
    return result


def read_completion(path: Path, nonce: str) -> Completion:
    value: object = json.loads(
        path.read_text(encoding="utf-8"), object_pairs_hook=no_duplicate_keys
    )
    return parse_completion(value, nonce)


def require_success(result: Completion, command: list[str], timeout: float) -> None:
    if result.timed_out:
        raise subprocess.TimeoutExpired(command, timeout)
    if result.returncode:
        raise subprocess.CalledProcessError(result.returncode, command)
    if result.cleanup.killed:
        raise RuntimeError("successful tool leaked owned descendants")
