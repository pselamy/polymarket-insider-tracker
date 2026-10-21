"""Serve a ``HealthMonitor`` over its real HTTP server on a free loopback port."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from aiohttp import ClientSession

from polymarket_insider_tracker.ingestor.health import HealthMonitor


@asynccontextmanager
async def served(monitor: HealthMonitor) -> AsyncIterator[ClientSession]:
    """A client bound to the monitor's started HTTP server; the server stops on exit."""
    await monitor.start_http_server(port=0)
    try:
        port = monitor.http_addresses[0][1]
        async with ClientSession(f"http://127.0.0.1:{port}") as client:
            yield client
    finally:
        await monitor.stop_http_server()
