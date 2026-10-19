"""Launch dedicated Linux supervision; never kill its process before cleanup."""

import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from tempfile import mkdtemp
from uuid import uuid4

from quality.owned_receipt import positive_timeout, read_completion, require_success


@dataclass(frozen=True)
class OwnedCommand:
    """Keep the immutable request binding and live dedicated process handle."""

    command: tuple[str, ...]
    timeout: float
    process: subprocess.Popen[bytes]
    directory: Path
    nonce: str
    receipt: Path


class SupervisionUnproven(RuntimeError):
    """Retain metadata/workspaces when supervisor cleanup cannot be verified."""

    def __init__(self, pid: int, directory: Path, reason: str) -> None:
        self.pid = pid
        self.directory = directory
        super().__init__(f"supervisor PID {pid}, retained {directory}: {reason}")


def wait_supervisor(process: subprocess.Popen[bytes], timeout: float, directory: Path) -> None:
    """The tool deadline is internal; the outer allowance only bounds cleanup."""
    try:
        process.wait(timeout=timeout + 21)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=21)
        except subprocess.TimeoutExpired as error:
            raise SupervisionUnproven(process.pid, directory, "cleanup remains live") from error
    verify_supervisor(process, directory)


def verify_supervisor(process: subprocess.Popen[bytes], directory: Path) -> None:
    status = process.poll()
    if status is None:
        raise SupervisionUnproven(process.pid, directory, "supervisor remains live")
    if status:
        raise SupervisionUnproven(process.pid, directory, "supervisor failed")


def cancel_supervisor(
    process: subprocess.Popen[bytes], directory: Path, cause: BaseException
) -> None:
    """An interrupted caller requests graceful cleanup and retains uncertain proof."""
    try:
        process.terminate()
        process.wait(timeout=21)
    except BaseException as error:
        raise SupervisionUnproven(
            process.pid, directory, "interrupted cleanup is unproven"
        ) from error
    raise SupervisionUnproven(process.pid, directory, "caller interrupted supervision") from cause


def start_owned(
    command: list[str], root: Path, timeout: float, env: dict[str, str]
) -> OwnedCommand:
    """Retain fresh metadata for success, failure and uncertain cleanup."""
    positive_timeout(timeout)
    parent = root / ".quality-results" / "owned"
    parent.mkdir(parents=True, exist_ok=True)
    directory = Path(mkdtemp(prefix="run-", dir=parent))
    nonce = uuid4().hex
    receipt = directory / "completion.json"
    request = directory / "request.json"
    request.write_text(
        json.dumps(
            {
                "command": command,
                "root": str(root.resolve()),
                "timeout": str(timeout),
                "nonce": nonce,
                "receipt": str(receipt.resolve()),
            }
        ),
        encoding="utf-8",
    )
    process = subprocess.Popen(
        [sys.executable, "-m", "quality.owned_main", str(request.resolve())],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        start_new_session=True,
    )
    return OwnedCommand(tuple(command), timeout, process, directory, nonce, receipt)


def finish_owned(owned: OwnedCommand, wait_timeout: float) -> None:
    """Require terminal cleanup and a fresh receipt after an asynchronous start."""
    positive_timeout(wait_timeout)
    try:
        wait_supervisor(owned.process, wait_timeout, owned.directory)
    except SupervisionUnproven:
        raise
    except BaseException as error:
        cancel_supervisor(owned.process, owned.directory, error)
    try:
        result = read_completion(owned.receipt, owned.nonce)
    except (OSError, ValueError) as error:
        raise SupervisionUnproven(
            owned.process.pid, owned.directory, "receipt is incomplete"
        ) from error
    require_success(result, list(owned.command), owned.timeout)


def run_owned(command: list[str], root: Path, timeout: float, env: dict[str, str]) -> None:
    """Use the same ownership and receipt protocol for synchronous commands."""
    finish_owned(start_owned(command, root, timeout, env), timeout)
