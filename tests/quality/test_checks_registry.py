"""The protected check registry cannot be silently emptied or malformed."""

import json
import os
import runpy
import subprocess
from pathlib import Path

import pytest

from quality import pipeline


def registry(root: Path, value: object) -> None:
    """Write one candidate registry and a valid deadline beside it."""
    (root / "quality").mkdir(exist_ok=True)
    (root / "quality" / "checks.json").write_text(json.dumps(value))
    (root / "quality" / "timeout.json").write_text("5")


@pytest.mark.parametrize("value", [None, {}, [], True, "text"])
def test_rejects_invalid_registries(tmp_path: Path, value: object) -> None:
    registry(tmp_path, value)
    with pytest.raises(ValueError, match="nonempty array"):
        pipeline.checks(tmp_path)


@pytest.mark.parametrize("value", [None, {}, [], True, "text"])
def test_rejects_invalid_commands(tmp_path: Path, value: object) -> None:
    registry(tmp_path, [value])
    with pytest.raises(ValueError, match="argument array"):
        pipeline.checks(tmp_path)


@pytest.mark.parametrize("value", [None, {}, [], True, "", 1])
def test_rejects_invalid_arguments(tmp_path: Path, value: object) -> None:
    registry(tmp_path, [["tool", value]])
    with pytest.raises(ValueError, match="nonempty strings"):
        pipeline.checks(tmp_path)


def test_missing_registry_is_a_failed_check(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        pipeline.checks(tmp_path)


@pytest.mark.parametrize("timeout", [True, None, "5", 0, -1, float("inf"), float("nan")])
def test_rejects_unbounded_or_invalid_timeouts(tmp_path: Path, timeout: object) -> None:
    registry(tmp_path, [["first"]])
    (tmp_path / "quality" / "timeout.json").write_text(json.dumps(timeout))
    with pytest.raises(ValueError, match="timeout must be"):
        pipeline.verify(tmp_path)


def test_runs_enrolled_hooks_then_commands_in_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry(tmp_path, [["first", "argument with spaces"], ["second"]])
    received: list[object] = []

    def hook(root: Path) -> None:
        received.append(("hook", root))

    def command(arguments: list[str], root: Path, timeout: float, env: dict[str, str]) -> None:
        received.append((arguments, root, timeout, env))

    monkeypatch.setattr(pipeline, "ENROLLED_HOOKS", {"recorded": hook})
    monkeypatch.setattr(pipeline, "run", command)
    pipeline.verify(tmp_path)
    assert received == [
        ("hook", tmp_path),
        (["first", "argument with spaces"], tmp_path, 5.0, dict(os.environ)),
        (["second"], tmp_path, 5.0, dict(os.environ)),
    ]


def test_failed_command_prevents_later_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry(tmp_path, [["first"], ["second"]])
    received: list[list[str]] = []

    def failure(arguments: list[str], _root: Path, _timeout: float, _env: dict[str, str]) -> None:
        received.append(arguments)
        raise subprocess.CalledProcessError(2, arguments)

    monkeypatch.setattr(pipeline, "ENROLLED_HOOKS", {})
    monkeypatch.setattr(pipeline, "run", failure)
    with pytest.raises(subprocess.CalledProcessError):
        pipeline.verify(tmp_path)
    assert received == [["first"]]


def test_module_entry_point_invokes_the_current_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: list[Path] = []
    monkeypatch.setattr(pipeline, "verify", received.append)
    runpy.run_module("quality.verify", run_name="__main__")
    assert received == [Path.cwd()]
