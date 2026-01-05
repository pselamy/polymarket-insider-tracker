"""Graceful shutdown handler for Polymarket Insider Tracker.

This module provides signal handling and graceful shutdown coordination
for the async pipeline components.

Usage:
    ```python
    async def main():
        shutdown = GracefulShutdown()

        async with shutdown:
            pipeline = Pipeline(settings)
            await pipeline.start()

            # Wait for shutdown signal
            await shutdown.wait()

            # Graceful cleanup
            await pipeline.stop()
    ```
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from collections.abc import Callable
from contextlib import suppress
from types import FrameType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any

logger = logging.getLogger(__name__)

# Default shutdown timeout in seconds
DEFAULT_SHUTDOWN_TIMEOUT = 30.0

# Signals to trap for graceful shutdown
SHUTDOWN_SIGNALS = (signal.SIGTERM, signal.SIGINT)


class ShutdownTimeoutError(Exception):
    """Raised when graceful shutdown exceeds timeout."""


class GracefulShutdown:
    """Graceful shutdown handler with signal trapping.

    This class provides coordinated shutdown handling for async applications.
    It traps SIGTERM and SIGINT signals and provides an async event that
    can be awaited to detect shutdown requests.

    Features:
        - SIGTERM and SIGINT signal trapping
        - Async event-based shutdown coordination
        - Configurable shutdown timeout
        - Async context manager support
        - Cleanup callback registration
        - Force exit on second signal or timeout

    Example:
        ```python
        shutdown = GracefulShutdown(timeout=30.0)

        async with shutdown:
            await some_long_running_task()
            # Automatically handles cleanup on signals
        ```
    """

    def __init__(
        self,
        timeout: float = DEFAULT_SHUTDOWN_TIMEOUT,
        *,
        exit_on_timeout: bool = True,
    ) -> None:
        """Initialize the shutdown handler.

        Args:
            timeout: Maximum time in seconds to wait for graceful shutdown.
            exit_on_timeout: If True, force exit when timeout is exceeded.
        """
        self._timeout = timeout
        self._exit_on_timeout = exit_on_timeout

        self._shutdown_event: asyncio.Event | None = None
        self._shutdown_requested = False
        self._force_exit_requested = False
        self._original_handlers: dict[signal.Signals, Any] = {}
        self._cleanup_callbacks: list[Callable[[], Any]] = []
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def timeout(self) -> float:
        """Shutdown timeout in seconds."""
        return self._timeout

    @property
    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        return self._shutdown_requested

    @property
    def is_force_exit_requested(self) -> bool:
        """Check if force exit has been requested (second signal received)."""
        return self._force_exit_requested

    def register_cleanup(self, callback: Callable[[], Any]) -> None:
        """Register a cleanup callback to run during shutdown.

        Args:
            callback: A callable (sync or async) to run during shutdown.
        """
        self._cleanup_callbacks.append(callback)

    def request_shutdown(self) -> None:
        """Programmatically request shutdown.

        This can be used to trigger shutdown from application code
        instead of waiting for a signal.
        """
        if not self._shutdown_requested:
            self._shutdown_requested = True
            logger.info("Shutdown requested programmatically")
            if self._shutdown_event:
                self._shutdown_event.set()

    async def wait(self) -> None:
        """Wait for a shutdown signal.

        This coroutine blocks until a shutdown signal is received
        or request_shutdown() is called.
        """
        if self._shutdown_event is None:
            self._shutdown_event = asyncio.Event()

        await self._shutdown_event.wait()

    async def wait_with_timeout(self) -> bool:
        """Wait for shutdown with timeout.

        Returns:
            True if shutdown was requested, False if timeout occurred.
        """
        if self._shutdown_event is None:
            self._shutdown_event = asyncio.Event()

        try:
            await asyncio.wait_for(
                self._shutdown_event.wait(),
                timeout=self._timeout,
            )
            return True
        except TimeoutError:
            return False

    def install_signal_handlers(self) -> None:
        """Install signal handlers for graceful shutdown.

        Traps SIGTERM and SIGINT to trigger graceful shutdown.
        On Windows, only SIGINT is trapped as SIGTERM is not available.
        """
        self._loop = asyncio.get_running_loop()
        self._shutdown_event = asyncio.Event()

        # Platform-specific signal handling
        if sys.platform == "win32":
            # Windows: use signal.signal for SIGINT only
            self._install_windows_handlers()
        else:
            # Unix: use loop.add_signal_handler for both signals
            self._install_unix_handlers()

        logger.debug("Signal handlers installed")

    def _install_unix_handlers(self) -> None:
        """Install Unix signal handlers using the event loop."""
        if self._loop is None:
            return

        for sig in SHUTDOWN_SIGNALS:
            try:
                self._loop.add_signal_handler(
                    sig,
                    self._handle_signal,
                    sig,
                )
                logger.debug("Installed handler for %s", sig.name)
            except (ValueError, OSError) as e:
                logger.warning("Could not install handler for %s: %s", sig.name, e)

    def _install_windows_handlers(self) -> None:
        """Install Windows signal handlers using signal.signal."""
        for sig in SHUTDOWN_SIGNALS:
            try:
                self._original_handlers[sig] = signal.signal(
                    sig,
                    self._handle_signal_sync,
                )
                logger.debug("Installed handler for %s", sig.name)
            except (ValueError, OSError) as e:
                logger.warning("Could not install handler for %s: %s", sig.name, e)

    def remove_signal_handlers(self) -> None:
        """Remove installed signal handlers and restore originals."""
        if sys.platform == "win32":
            self._remove_windows_handlers()
        else:
            self._remove_unix_handlers()

        logger.debug("Signal handlers removed")

    def _remove_unix_handlers(self) -> None:
        """Remove Unix signal handlers."""
        if self._loop is None:
            return

        for sig in SHUTDOWN_SIGNALS:
            with suppress(ValueError, OSError):
                self._loop.remove_signal_handler(sig)

    def _remove_windows_handlers(self) -> None:
        """Remove Windows signal handlers and restore originals."""
        for sig, original in self._original_handlers.items():
            with suppress(ValueError, OSError):
                signal.signal(sig, original)
        self._original_handlers.clear()

    def _handle_signal(self, sig: signal.Signals) -> None:
        """Handle shutdown signal (Unix version).

        Args:
            sig: The signal that was received.
        """
        if self._shutdown_requested:
            # Second signal - force exit
            self._force_exit_requested = True
            logger.warning("Received %s again - forcing exit!", sig.name)
            sys.exit(128 + sig.value)
        else:
            self._shutdown_requested = True
            logger.info("Received %s - initiating graceful shutdown...", sig.name)
            if self._shutdown_event:
                self._shutdown_event.set()

    def _handle_signal_sync(self, sig: int, _frame: FrameType | None) -> None:
        """Handle shutdown signal (Windows version).

        Args:
            sig: The signal number that was received.
            _frame: The current stack frame (unused).
        """
        sig_enum = signal.Signals(sig)
        self._handle_signal(sig_enum)

    async def run_cleanup_callbacks(self) -> None:
        """Run all registered cleanup callbacks."""
        for callback in self._cleanup_callbacks:
            try:
                result = callback()
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error("Cleanup callback failed: %s", e)

    async def __aenter__(self) -> GracefulShutdown:
        """Async context manager entry - install signal handlers."""
        self.install_signal_handlers()
        return self

    async def __aexit__(self, *_args: Any) -> None:
        """Async context manager exit - cleanup."""
        self.remove_signal_handlers()
        await self.run_cleanup_callbacks()


async def run_with_graceful_shutdown(
    coro: Any,
    *,
    timeout: float = DEFAULT_SHUTDOWN_TIMEOUT,
) -> None:
    """Run an async coroutine with graceful shutdown handling.

    This is a convenience function that wraps a coroutine with
    signal handling and timeout-based cleanup.

    Args:
        coro: The coroutine to run.
        timeout: Maximum time to wait for graceful shutdown.

    Example:
        ```python
        async def my_app():
            await asyncio.sleep(3600)  # Run for an hour

        # Will handle SIGTERM/SIGINT gracefully
        await run_with_graceful_shutdown(my_app())
        ```
    """
    shutdown = GracefulShutdown(timeout=timeout)

    async with shutdown:
        task = asyncio.create_task(coro)

        # Wait for either task completion or shutdown signal
        shutdown_wait = asyncio.create_task(shutdown.wait())

        done, pending = await asyncio.wait(
            [task, shutdown_wait],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Cancel pending tasks
        for pending_task in pending:
            pending_task.cancel()
            with suppress(asyncio.CancelledError):
                await pending_task

        # If the main task is in done, get any exception
        if task in done:
            task.result()
