"""Real child processes exercise caps, deadlines, exits and environment isolation."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import BinaryIO, cast

import pytest
from scripts import _secret_process
from scripts._secret_models import NativeReportError
from scripts._secret_process import GitLocation, run_git


def program(directory: Path, body: str) -> GitLocation:
    executable = directory / "fixture-git"
    executable.write_text(f"#!{sys.executable}\n{body}\n")
    executable.chmod(0o700)
    return GitLocation(executable, directory)


@pytest.mark.parametrize("length", [0, 1, 1024])
def test_complete_exact_cap_output(tmp_path: Path, length: int) -> None:
    location = program(tmp_path, f"import os; os.write(1, b'x' * {length})")
    assert run_git(location, (), limit=1024) == b"x" * length


@pytest.mark.parametrize(
    "body",
    ["import os; os.write(1, b'x' * 1025)", "import os\nwhile True: os.write(1, b'x' * 1024)"],
)
def test_overflow_fails_without_partial_result(tmp_path: Path, body: str) -> None:
    location = program(tmp_path, body)
    with pytest.raises(NativeReportError, match="native output limit"):
        run_git(location, (), limit=1024)


@pytest.mark.parametrize("prefix", ["", "os.close(1); "])
def test_silent_and_eof_hang_terminate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prefix: str
) -> None:
    location = program(tmp_path, f"import os, time; {prefix}time.sleep(60)")
    monkeypatch.setattr(_secret_process, "COMMAND_SECONDS", 0.5 if prefix else 0.12)
    started = time.monotonic()
    with pytest.raises(NativeReportError, match="native command timeout"):
        run_git(location, ())
    assert time.monotonic() - started < 3


def test_stderr_is_discarded_and_input_is_complete(tmp_path: Path) -> None:
    location = program(
        tmp_path,
        "import os, sys; os.write(2, b'z' * 1000000); "
        "os.write(1, str(len(sys.stdin.buffer.read())).encode())",
    )
    assert run_git(location, (), input_bytes=b"x" * 2000000) == b"2000000"


def test_nonzero_and_spawn_errors_are_private(tmp_path: Path) -> None:
    private = "private-" + "process-sentinel"
    location = program(tmp_path, f"import sys; sys.stderr.write({private!r}); sys.exit(9)")
    missing = GitLocation(tmp_path / private, tmp_path)
    for target in (location, missing):
        try:
            raise RuntimeError(private)
        except RuntimeError:
            with pytest.raises(NativeReportError) as error:
                run_git(target, ())
        rendered = "".join(traceback.format_exception(error.value))
        assert private not in rendered
        assert error.value.__suppress_context__
        assert private not in repr(target)


def test_only_fixed_environment_reaches_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hostile = {
        "GIT_DIR": "private-dir",
        "GIT_OBJECT_DIRECTORY": "private-objects",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "core.pager",
        "GIT_CONFIG_VALUE_0": "private-helper",
        "GIT_TRACE": "private-trace",
        "LD_PRELOAD": "private-loader",
        "DYLD_INSERT_LIBRARIES": "private-loader",
        "GIT_ASKPASS": "private-askpass",
        "PRIVATE_CREDENTIAL": "private-value",
    }
    for key, value in hostile.items():
        monkeypatch.setenv(key, value)
    location = program(tmp_path, "import json, os; print(json.dumps(dict(os.environ)))")
    child = cast(dict[str, str], json.loads(run_git(location, (), limit=4096)))
    assert all(child.get(key) != value for key, value in hostile.items())
    assert child["GIT_ALLOW_PROTOCOL"] == ""
    assert child["GIT_CONFIG_GLOBAL"] == child["GIT_CONFIG_SYSTEM"] == os.devnull
    assert child["GIT_CONFIG_COUNT"] == "0"
    assert child["GIT_TERMINAL_PROMPT"] == "0"


def test_relative_location_rejected() -> None:
    with pytest.raises(NativeReportError):
        GitLocation(Path("git"), Path("store"))


def test_input_cap_and_expired_deadline_fail(tmp_path: Path) -> None:
    location = program(tmp_path, "pass")
    with pytest.raises(NativeReportError, match="native input limit"):
        run_git(location, (), input_bytes=b"x" * (16 * 1024 * 1024 + 1))
    with pytest.raises(NativeReportError, match="native command timeout"):
        run_git(location, (), deadline=0.0)


def test_missing_stdout_boundary_fails_and_reaps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = subprocess.Popen

    def without_stdout(
        args: list[str], *, stdin: BinaryIO, stdout: int, stderr: int, env: dict[str, str]
    ) -> subprocess.Popen[bytes]:
        assert stdout == subprocess.PIPE
        return original(args, stdin=stdin, stdout=subprocess.DEVNULL, stderr=stderr, env=env)

    location = program(tmp_path, "pass")
    monkeypatch.setattr(subprocess, "Popen", without_stdout)
    with pytest.raises(NativeReportError, match="native output unavailable"):
        run_git(location, ())


def test_native_guards_are_real_argv(tmp_path: Path) -> None:
    location = program(tmp_path, "import json, sys; print(json.dumps(sys.argv[1:]))")
    argv = cast(list[str], json.loads(run_git(location, ("cat-file", "-t", "1" * 40), limit=4096)))
    assert argv == [
        "--no-replace-objects",
        "--no-lazy-fetch",
        "--git-dir",
        str(tmp_path),
        "cat-file",
        "-t",
        "1" * 40,
    ]
