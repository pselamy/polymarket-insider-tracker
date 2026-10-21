"""Give the pinned native HTTP distributor an explicit current event loop."""

import asyncio

from quality.native_api import native_callable


def execute(arguments: list[str]) -> None:
    """Run the unchanged native CLI and close the owned loop even on failure."""
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        cli = native_callable("cosmic_ray.cli", "cli")
        cli(arguments, standalone_mode=False)
    finally:
        asyncio.set_event_loop(None)
        loop.close()
