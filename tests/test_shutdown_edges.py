"""Edge behaviour of graceful shutdown: bounded waits, platform dispatch, failing installs."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import threading
from collections.abc import Callable
from types import FrameType

import pytest

from polymarket_insider_tracker.shutdown import (
    SHUTDOWN_SIGNALS,
    GracefulShutdown,
    run_with_graceful_shutdown,
    wait_bounded,
)
from tests.fakes import resist_cancellation_for


async def _fail_after_resisting(seconds: float) -> None:
    await resist_cancellation_for(seconds)
    raise RuntimeError("late failure")


async def test_wait_bounded_logs_failure_of_abandoned_work(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.ERROR):
        assert await wait_bounded(_fail_after_resisting(0.1), timeout=0.02) is False
        await asyncio.sleep(0.2)

    assert "Abandoned shutdown work failed after its deadline: late failure" in caplog.text


async def test_wait_bounded_stays_quiet_when_abandoned_work_is_cancelled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.ERROR):
        assert await wait_bounded(asyncio.sleep(30), timeout=0.02) is False
        await asyncio.sleep(0.05)

    assert caplog.text == ""


async def test_wait_bounded_propagates_failure_of_work_finishing_in_time() -> None:
    async def fail() -> None:
        raise ValueError("in time")

    with pytest.raises(ValueError, match="in time"):
        await wait_bounded(fail(), timeout=1.0)


def test_remove_signal_handlers_before_install_is_a_noop() -> None:
    shutdown = GracefulShutdown()

    shutdown.remove_signal_handlers()

    assert shutdown.is_shutdown_requested is False


def _installed_handler(sig: signal.Signals) -> Callable[[int, FrameType | None], object]:
    handler = signal.getsignal(sig)
    assert callable(handler)
    return handler


async def test_windows_platform_installs_handlers_that_request_then_force_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {sig: signal.getsignal(sig) for sig in SHUTDOWN_SIGNALS}
    monkeypatch.setattr(sys, "platform", "win32")
    shutdown = GracefulShutdown()

    shutdown.install_signal_handlers()
    try:
        handler = _installed_handler(signal.SIGINT)
        assert _installed_handler(signal.SIGTERM) == handler

        handler(int(signal.SIGINT), None)
        assert shutdown.is_shutdown_requested is True
        assert shutdown.is_force_exit_requested is False

        with pytest.raises(SystemExit) as exit_info:
            handler(int(signal.SIGINT), None)
        assert exit_info.value.code == 128 + signal.SIGINT.value
        assert shutdown.is_force_exit_requested is True
    finally:
        shutdown.remove_signal_handlers()

    assert {sig: signal.getsignal(sig) for sig in SHUTDOWN_SIGNALS} == before


def _install_off_main_thread(shutdown: GracefulShutdown) -> None:
    async def install() -> None:
        shutdown.install_signal_handlers()

    asyncio.run(install())


def test_windows_install_off_the_main_thread_warns_per_signal_instead_of_raising(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Handlers can only be installed from the main thread; each refusal is only logged."""
    monkeypatch.setattr(sys, "platform", "win32")
    shutdown = GracefulShutdown()

    with caplog.at_level(logging.WARNING):
        worker = threading.Thread(target=_install_off_main_thread, args=(shutdown,))
        worker.start()
        worker.join()

    warned = [r.getMessage() for r in caplog.records if "Could not install handler" in r.message]
    assert len(warned) == len(SHUTDOWN_SIGNALS)


@pytest.mark.skipif(sys.platform == "win32", reason="Unix signal delivery")
async def test_shutdown_signal_cancels_the_running_coroutine_and_returns() -> None:
    cancelled = asyncio.Event()

    async def long_running() -> None:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    asyncio.get_running_loop().call_later(0.05, os.kill, os.getpid(), signal.SIGTERM)
    await run_with_graceful_shutdown(long_running(), timeout=1.0)

    assert cancelled.is_set()


class _RefusingLoop(asyncio.SelectorEventLoop):
    """A real event loop whose OS refuses every signal registration."""

    def add_signal_handler(self, sig: int, callback: Callable[..., object], *args: object) -> None:
        _ = (sig, callback, args)
        raise OSError("signal registration refused")


def test_unix_install_logs_a_warning_per_refused_signal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    shutdown = GracefulShutdown()
    loop = _RefusingLoop()

    async def install() -> None:
        shutdown.install_signal_handlers()

    try:
        with caplog.at_level(logging.WARNING):
            loop.run_until_complete(install())
    finally:
        loop.close()

    assert caplog.text.count("signal registration refused") == len(SHUTDOWN_SIGNALS)


async def test_windows_restore_failure_off_the_main_thread_is_suppressed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {sig: signal.getsignal(sig) for sig in SHUTDOWN_SIGNALS}
    monkeypatch.setattr(sys, "platform", "win32")
    shutdown = GracefulShutdown()
    shutdown.install_signal_handlers()
    installed = [signal.getsignal(sig) for sig in SHUTDOWN_SIGNALS]
    try:
        worker = threading.Thread(target=shutdown.remove_signal_handlers)
        worker.start()
        worker.join()

        assert [signal.getsignal(sig) for sig in SHUTDOWN_SIGNALS] == installed
    finally:
        for sig, handler in before.items():
            signal.signal(sig, handler)
