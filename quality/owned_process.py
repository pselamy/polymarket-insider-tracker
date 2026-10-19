"""Linux supervision runs only inside a dedicated, single-threaded child process."""

import contextlib
import ctypes
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

MINIMUM_PID = 0
LINUX = "linux"
SIGNAL_STATUS: dict[int, int] = {signal.SIGINT: 130, signal.SIGTERM: 143}


@dataclass(frozen=True)
class Cleanup:
    """Retain actual owned termination and signed native reaping evidence."""

    killed: list[int]
    reaped: list[tuple[int, int]]


@dataclass(frozen=True)
class Completion:
    """A timed-out or resource-leaking command cannot be accepted as a clean check."""

    returncode: int
    timed_out: bool
    cleanup: Cleanup


def prctl(option: int, argument: int) -> None:
    """Use the native varargs ABI with pointer-sized arguments and errno handling."""
    libc = ctypes.CDLL(None, use_errno=True)
    native = libc.prctl
    native.argtypes = [ctypes.c_int] + [ctypes.c_ulong] * 4
    native.restype = ctypes.c_int
    if int(native(option, argument, 0, 0, 0)):
        raise OSError(ctypes.get_errno(), "native process supervision failed")


def readback(option: int, expected: int) -> None:
    """Require the kernel's actual setting, not merely a successful setter call."""
    value = ctypes.c_int()
    prctl(option, ctypes.addressof(value))
    if value.value != expected:
        raise RuntimeError("native supervision readback disagrees")


def interrupt(signum: int, _frame: object) -> None:
    """Raise through the owned cleanup finally instead of abruptly abandoning it."""
    raise SystemExit(SIGNAL_STATUS[signum])


def enable() -> None:
    """Adopt orphans and clean them when the creating caller dies."""
    if sys.platform != LINUX:
        raise RuntimeError("owned subreaper supervision requires Linux")
    owner = os.getppid()
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    prctl(36, 1)
    readback(37, 1)
    prctl(1, int(signal.SIGTERM))
    readback(2, int(signal.SIGTERM))
    if os.getppid() != owner:
        raise RuntimeError("supervisor creator changed during registration")


def children() -> list[int]:
    """Unreaped direct children cannot have their PIDs reused by unrelated tasks."""
    path = Path(f"/proc/self/task/{os.getpid()}/children")
    return [int(pid) for pid in path.read_text(encoding="utf-8").split()]


def validate_owned(pids: list[int]) -> None:
    """Reject process-group selectors and any PID outside current direct children."""
    if any(pid <= MINIMUM_PID for pid in pids):
        raise ValueError("owned process identifiers must be positive")
    if not set(pids).issubset(children()):
        raise ValueError("process identifier is outside owned children")


def kill_children(pids: list[int]) -> None:
    """Only terminate direct children owned by this dedicated supervisor."""
    validate_owned(pids)
    for pid in pids:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)


def reap_available(results: list[tuple[int, int]]) -> bool:
    """Return whether living owned children remain; retain every signed outcome."""
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return False
        if not pid:
            return True
        results.append((pid, os.waitstatus_to_exitcode(status)))


def drain(timeout: float) -> Cleanup:
    """Keep cleanup running through repeated interrupts, with a bounded failure."""
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    deadline = time.monotonic() + timeout
    killed: list[int] = []
    reaped: list[tuple[int, int]] = []
    while True:
        if not reap_available(reaped):
            return Cleanup(killed, reaped)
        owned = children()
        kill_children(owned)
        killed.extend(owned)
        if time.monotonic() >= deadline:
            raise TimeoutError("owned descendants did not reap before cleanup deadline")
        time.sleep(0.001)


def execute(command: list[str], root: Path, timeout: float, env: dict[str, str]) -> Completion:
    """Retain the original tool deadline; always reap before returning or raising."""
    enable()
    try:
        process = subprocess.Popen(command, cwd=root, env=env, start_new_session=True)
    except BaseException:
        drain(20)
        raise
    return wait_owned(process, timeout)


def wait_owned(process: subprocess.Popen[bytes], timeout: float) -> Completion:
    """Update the native handle before propagating interruptions through cleanup."""
    timed_out = False
    status: int | None = None
    try:
        try:
            status = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
    finally:
        cleanup = drain(20)
        if status is None:
            status = dict(cleanup.reaped)[process.pid]
        process.returncode = status
    return Completion(status, timed_out, cleanup)
