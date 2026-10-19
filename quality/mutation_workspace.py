"""Preserve completed mutation evidence before deletion; retain uncertain owners."""

import json
import shutil
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from tempfile import mkdtemp
from typing import cast

from quality.owned_commands import SupervisionUnproven


def cleanup_unproven(error: BaseException) -> bool:
    if isinstance(error, SupervisionUnproven):
        return True
    if isinstance(error, BaseExceptionGroup):
        group = cast(BaseExceptionGroup[BaseException], error)
        return any(cleanup_unproven(item) for item in group.exceptions)
    return False


def special_artifact(path: Path) -> bool:
    """FIFOs and sockets have no copyable byte contents."""
    return not (path.is_symlink() or path.is_file() or path.is_dir())


def copy_workspace(source: Path, destination: Path) -> list[str]:
    """Copy raw evidence and record special test artifacts that lack contents."""
    skipped: list[str] = []

    def unsupported(directory: str, names: list[str]) -> list[str]:
        excluded: list[str] = []
        for name in names:
            path = Path(directory) / name
            if special_artifact(path):
                excluded.append(name)
                skipped.append(path.relative_to(source).as_posix())
        return excluded

    shutil.copytree(source, destination, symlinks=True, ignore=unsupported)
    return sorted(skipped)


def preserve_failure(root: Path, target: Path, error: BaseException) -> None:
    output = root / ".quality-results"
    output.mkdir(exist_ok=True)
    retained = Path(mkdtemp(prefix="mutation-failure-", dir=output))
    skipped = copy_workspace(target, retained / "workspace")
    (retained / "failure.json").write_text(
        json.dumps(
            {
                "errorType": type(error).__name__,
                "message": str(error),
                "originalWorkspace": str(target),
                "cleanupUnproven": cleanup_unproven(error),
                "skippedSpecialEntries": skipped,
                "completeMutationPass": False,
            }
        ),
        encoding="utf-8",
    )


def remove_workspace(target: Path, retain: bool) -> None:
    """Leave uncertain owners and failed archives intact."""
    if retain:
        return
    shutil.rmtree(target)


def preserve_success(root: Path, target: Path) -> None:
    """Retain raw journals, receipts, source and configuration from this run."""
    output = root / ".quality-results"
    output.mkdir(exist_ok=True)
    retained = Path(mkdtemp(prefix="mutation-success-", dir=output))
    skipped = copy_workspace(target, retained / "workspace")
    (retained / "skipped-special-entries.json").write_text(json.dumps(skipped))


@contextmanager
def workspace(root: Path) -> Generator[Path]:
    target = Path(mkdtemp(prefix="relentless-mutation-"))
    retain = True
    try:
        yield target
    except BaseException as error:
        preserve_failure(root, target, error)
        retain = cleanup_unproven(error)
        raise
    else:
        preserve_success(root, target)
        retain = False
    finally:
        remove_workspace(target, retain)
