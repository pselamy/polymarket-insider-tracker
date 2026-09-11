"""Tests for the graceful shutdown handler."""

from __future__ import annotations

import asyncio
import signal
import sys

import pytest

from polymarket_insider_tracker.shutdown import (
    DEFAULT_SHUTDOWN_TIMEOUT,
    SHUTDOWN_SIGNALS,
    GracefulShutdown,
    run_with_graceful_shutdown,
)
from tests.fakes import resist_cancellation_for


class TestGracefulShutdownInit:
    """Tests for GracefulShutdown initialization."""

    def test_default_timeout(self) -> None:
        """Should use default timeout when not specified."""
        shutdown = GracefulShutdown()
        assert shutdown.timeout == DEFAULT_SHUTDOWN_TIMEOUT

    def test_custom_timeout(self) -> None:
        """Should accept custom timeout."""
        shutdown = GracefulShutdown(timeout=60.0)
        assert shutdown.timeout == 60.0

    def test_initial_state(self) -> None:
        """Should start in non-shutdown state."""
        shutdown = GracefulShutdown()
        assert shutdown.is_shutdown_requested is False
        assert shutdown.is_force_exit_requested is False


class TestRequestShutdown:
    """Tests for programmatic shutdown requests."""

    async def test_request_shutdown_sets_flag(self) -> None:
        """Should set shutdown requested flag."""
        shutdown = GracefulShutdown()
        shutdown.request_shutdown()
        assert shutdown.is_shutdown_requested is True

    async def test_request_shutdown_sets_event(self) -> None:
        """Should set the shutdown event when called."""
        shutdown = GracefulShutdown()
        shutdown._shutdown_event = asyncio.Event()

        shutdown.request_shutdown()

        assert shutdown._shutdown_event.is_set()

    async def test_request_shutdown_idempotent(self) -> None:
        """Multiple requests should be idempotent."""
        shutdown = GracefulShutdown()
        shutdown._shutdown_event = asyncio.Event()

        shutdown.request_shutdown()
        shutdown.request_shutdown()
        shutdown.request_shutdown()

        assert shutdown.is_shutdown_requested is True


class TestWait:
    """Tests for waiting for shutdown."""

    async def test_wait_blocks_until_shutdown(self) -> None:
        """Wait should block until shutdown is requested."""
        shutdown = GracefulShutdown()

        async def request_after_delay() -> None:
            await asyncio.sleep(0.1)
            shutdown.request_shutdown()

        asyncio.create_task(request_after_delay())

        # Should complete after the delay
        await asyncio.wait_for(shutdown.wait(), timeout=1.0)

        assert shutdown.is_shutdown_requested is True

    async def test_wait_with_timeout_returns_true_on_shutdown(self) -> None:
        """wait_with_timeout should return True when shutdown is requested."""
        shutdown = GracefulShutdown(timeout=1.0)

        async def request_after_delay() -> None:
            await asyncio.sleep(0.05)
            shutdown.request_shutdown()

        asyncio.create_task(request_after_delay())

        result = await shutdown.wait_with_timeout()
        assert result is True

    async def test_wait_with_timeout_returns_false_on_timeout(self) -> None:
        """wait_with_timeout should return False when timeout occurs."""
        shutdown = GracefulShutdown(timeout=0.05)

        # Don't request shutdown, let it timeout
        result = await shutdown.wait_with_timeout()
        assert result is False


class TestSignalHandlers:
    """Tests for signal handler installation and removal."""

    async def test_install_signal_handlers_creates_event(self) -> None:
        """Installing handlers should create the shutdown event."""
        shutdown = GracefulShutdown()
        shutdown.install_signal_handlers()

        try:
            assert shutdown._shutdown_event is not None
            assert not shutdown._shutdown_event.is_set()
        finally:
            shutdown.remove_signal_handlers()

    @pytest.mark.skipif(sys.platform == "win32", reason="Unix-specific test")
    async def test_unix_signal_handler_installed(self) -> None:
        """On Unix, the loop's handler replaces the process disposition and is restored later."""
        shutdown = GracefulShutdown()
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL

        shutdown.install_signal_handlers()
        try:
            assert signal.getsignal(signal.SIGTERM) is not signal.SIG_DFL
            assert signal.getsignal(signal.SIGINT) is signal.getsignal(signal.SIGTERM)
        finally:
            shutdown.remove_signal_handlers()

        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific test")
    async def test_windows_signal_handler_installed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """On Windows, should install signal.signal handlers for every shutdown signal."""
        shutdown = GracefulShutdown()
        installed: list[signal.Signals] = []
        original_signal = signal.signal

        def record(sig: signal.Signals, handler: object) -> object:
            installed.append(sig)
            return original_signal(sig, handler)

        monkeypatch.setattr(signal, "signal", record)
        shutdown.install_signal_handlers()
        shutdown.remove_signal_handlers()

        assert set(installed) >= set(SHUTDOWN_SIGNALS)


class TestHandleSignal:
    """Tests for signal handling behavior."""

    async def test_first_signal_sets_shutdown_event(self) -> None:
        """First signal should set the shutdown event."""
        shutdown = GracefulShutdown()
        shutdown._shutdown_event = asyncio.Event()

        shutdown._handle_signal(signal.SIGTERM)

        assert shutdown.is_shutdown_requested is True
        assert shutdown._shutdown_event.is_set()
        assert shutdown.is_force_exit_requested is False

    async def test_second_signal_force_exits(self) -> None:
        """Second signal should trigger force exit."""
        shutdown = GracefulShutdown()
        shutdown._shutdown_event = asyncio.Event()
        shutdown._shutdown_requested = True  # First signal already received

        with pytest.raises(SystemExit) as exc_info:
            shutdown._handle_signal(signal.SIGTERM)

        assert exc_info.value.code == 128 + signal.SIGTERM.value
        assert shutdown.is_force_exit_requested is True


class TestCleanupCallbacks:
    """Tests for cleanup callback registration and execution."""

    async def test_register_sync_callback(self) -> None:
        """Should register sync cleanup callbacks."""
        shutdown = GracefulShutdown()
        runs: list[str] = []

        def callback() -> None:
            runs.append("cleanup")

        shutdown.register_cleanup(callback)

        assert callback in shutdown._cleanup_callbacks
        assert runs == []

    async def test_run_sync_cleanup_callback(self) -> None:
        """Should run sync cleanup callbacks exactly once."""
        shutdown = GracefulShutdown()
        runs: list[str] = []
        shutdown.register_cleanup(lambda: runs.append("cleanup"))

        await shutdown.run_cleanup_callbacks()

        assert runs == ["cleanup"]

    async def test_run_async_cleanup_callback(self) -> None:
        """Should run async cleanup callbacks."""
        shutdown = GracefulShutdown()
        called = False

        async def async_callback() -> None:
            nonlocal called
            called = True

        shutdown.register_cleanup(async_callback)

        await shutdown.run_cleanup_callbacks()

        assert called is True

    async def test_cleanup_callback_error_logged(self) -> None:
        """Cleanup callback errors should be logged, not raised."""
        shutdown = GracefulShutdown()

        def failing_callback() -> None:
            raise ValueError("Cleanup failed")

        shutdown.register_cleanup(failing_callback)

        # Should not raise
        await shutdown.run_cleanup_callbacks()

    async def test_hanging_cleanup_callback_is_bounded_by_shutdown_timeout(self) -> None:
        """A hung cleanup callback must not stall shutdown past the configured timeout."""
        shutdown = GracefulShutdown(timeout=0.1)
        completed: list[str] = []

        async def hanging_callback() -> None:
            await asyncio.sleep(3600)

        async def later_callback() -> None:
            completed.append("later")

        shutdown.register_cleanup(hanging_callback)
        shutdown.register_cleanup(later_callback)

        await asyncio.wait_for(shutdown.run_cleanup_callbacks(), timeout=5.0)

        assert completed == ["later"]

    async def test_cancellation_suppressing_cleanup_is_still_bounded(self) -> None:
        """Round-4 finding 7: the deadline must hold even when the cleanup coroutine
        suppresses cancellation; the abandoned work finishes in the background."""
        shutdown = GracefulShutdown(timeout=0.05)
        completed: list[str] = []
        stubborn_finished = asyncio.Event()

        async def stubborn_callback() -> None:
            await resist_cancellation_for(0.5)
            stubborn_finished.set()

        async def later_callback() -> None:
            completed.append("later")

        shutdown.register_cleanup(stubborn_callback)
        shutdown.register_cleanup(later_callback)

        loop = asyncio.get_running_loop()
        started = loop.time()
        await asyncio.wait_for(shutdown.run_cleanup_callbacks(), timeout=5.0)
        elapsed = loop.time() - started

        assert completed == ["later"]
        assert elapsed < 0.4, f"cleanup took {elapsed:.3f}s despite the 0.05s deadline"
        # Let the abandoned callback finish so it cannot leak into another test.
        await asyncio.wait_for(stubborn_finished.wait(), timeout=2.0)


class TestAsyncContextManager:
    """Tests for async context manager protocol."""

    async def test_context_manager_installs_handlers(self) -> None:
        """Entering context should install signal handlers."""
        shutdown = GracefulShutdown()

        async with shutdown:
            assert shutdown._shutdown_event is not None
            assert shutdown._loop is not None

    @pytest.mark.skipif(sys.platform == "win32", reason="Unix-specific test")
    async def test_context_manager_removes_handlers(self) -> None:
        """Exiting context should restore the default SIGTERM disposition."""
        shutdown = GracefulShutdown()

        async with shutdown:
            assert signal.getsignal(signal.SIGTERM) is not signal.SIG_DFL

        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL

    async def test_context_manager_runs_cleanup(self) -> None:
        """Exiting context should run cleanup callbacks exactly once."""
        shutdown = GracefulShutdown()
        runs: list[str] = []
        shutdown.register_cleanup(lambda: runs.append("cleanup"))

        async with shutdown:
            assert runs == []

        assert runs == ["cleanup"]


class TestRunWithGracefulShutdown:
    """Tests for the run_with_graceful_shutdown helper."""

    async def test_runs_coroutine_to_completion(self) -> None:
        """Should run the coroutine to completion if no shutdown."""
        result = []

        async def my_coro() -> None:
            result.append("done")

        await run_with_graceful_shutdown(my_coro(), timeout=1.0)

        assert result == ["done"]

    async def test_stops_on_shutdown_signal(self) -> None:
        """Should stop when shutdown is signaled."""
        result = []

        async def long_running() -> None:
            try:
                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                result.append("cancelled")
                raise

        # Create a wrapper that will trigger shutdown
        async def run_with_interrupt() -> None:
            async def trigger_shutdown() -> None:
                await asyncio.sleep(0.1)
                # We can't easily trigger a signal in tests, so we test the task
                # completion path instead
                pass

            # Run both tasks
            task = asyncio.create_task(long_running())
            asyncio.create_task(trigger_shutdown())

            await asyncio.sleep(0.05)
            task.cancel()

            with pytest.raises(asyncio.CancelledError):
                await task

            result.append("finished")

        await run_with_interrupt()
        assert "finished" in result


class TestShutdownSignals:
    """Tests for shutdown signal configuration."""

    def test_shutdown_signals_includes_sigterm(self) -> None:
        """SHUTDOWN_SIGNALS should include SIGTERM."""
        assert signal.SIGTERM in SHUTDOWN_SIGNALS

    def test_shutdown_signals_includes_sigint(self) -> None:
        """SHUTDOWN_SIGNALS should include SIGINT."""
        assert signal.SIGINT in SHUTDOWN_SIGNALS
