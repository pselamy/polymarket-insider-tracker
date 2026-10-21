"""Narrow failure injection for a fakeredis client: chosen commands raise a chosen error."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import NoReturn

import pytest
from fakeredis import FakeAsyncRedis


def _failing_command(error: Exception) -> Callable[..., Awaitable[NoReturn]]:
    async def fail(*_args: object, **_kwargs: object) -> NoReturn:
        raise error

    return fail


def fail_commands(
    monkeypatch: pytest.MonkeyPatch, client: FakeAsyncRedis, **errors: Exception
) -> None:
    """Make each named command of ``client`` raise its error (``get=ConnectionError(...)``)."""
    for command, error in errors.items():
        monkeypatch.setattr(client, command, _failing_command(error))


def fail_pipeline(
    monkeypatch: pytest.MonkeyPatch, client: FakeAsyncRedis, error: Exception
) -> None:
    """Make opening a pipeline on ``client`` raise ``error``."""

    def fail(*_args: object, **_kwargs: object) -> NoReturn:
        raise error

    monkeypatch.setattr(client, "pipeline", fail)
