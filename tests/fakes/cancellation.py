"""A working fake for dependencies that resist cancellation during shutdown/delivery."""

from __future__ import annotations

import asyncio
import contextlib


async def resist_cancellation_for(seconds: float) -> None:
    """Sleep ``seconds`` of wall time, suppressing every cancellation attempt.

    Models an HTTP stack or cleanup routine that swallows ``CancelledError`` and
    keeps running, so deadline enforcement cannot rely on cooperative cancellation.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    while loop.time() < deadline:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(deadline - loop.time())
