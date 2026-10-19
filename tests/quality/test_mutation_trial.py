"""The native mutation trial entry point retains signed streams and status."""

import subprocess
from types import SimpleNamespace

import pytest
from quality.mutation_trial import run_trial


def _result(returncode: int, stderr: str = "") -> SimpleNamespace:
    return SimpleNamespace(returncode=returncode, stdout="output\n", stderr=stderr)


def test_successful_trial_returns_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subprocess, "run", lambda *_args, **_kwargs: _result(0))
    assert run_trial() == 0


def test_failed_trial_propagates_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subprocess, "run", lambda *_args, **_kwargs: _result(1))
    assert run_trial() == 1


def test_trial_with_stderr_is_a_retained_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subprocess, "run", lambda *_args, **_kwargs: _result(0, "boom\n"))
    assert run_trial() == 2
