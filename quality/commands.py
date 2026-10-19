"""Run tools with explicit arguments, no shell, and a bounded execution time."""

import contextlib
import os
import signal
import subprocess
from pathlib import Path

POSIX_IDENTIFIER = "posix"


def is_posix(name: str) -> bool:
    """Classify only the exact native POSIX identifier."""
    return name == POSIX_IDENTIFIER


POSIX = is_posix(os.name)


def kill_group(pid: int) -> None:
    """A reaped process group is already clean; otherwise terminate its children."""
    with contextlib.suppress(ProcessLookupError):
        os.killpg(pid, signal.SIGKILL)


def stop_tree(process: subprocess.Popen[bytes]) -> None:
    """Terminate the owned POSIX group or native Windows process tree."""
    if POSIX:
        kill_group(process.pid)
    elif process.poll() is None:
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            check=True,
            timeout=20,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )


def stop(process: subprocess.Popen[bytes]) -> None:
    """Bound reaping even when the platform tree-termination operation fails."""
    try:
        stop_tree(process)
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=20)


def run(
    command: list[str],
    root: Path,
    timeout: float,
    env: dict[str, str],
) -> None:
    """A missing tool, nonzero exit or timeout is a verification failure."""
    with subprocess.Popen(command, cwd=root, env=env, start_new_session=POSIX) as process:
        try:
            status = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            stop(process)
            raise
        if status:
            raise subprocess.CalledProcessError(status, command)
