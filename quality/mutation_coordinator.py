"""Give the pinned native HTTP distributor an explicit current event loop."""

import asyncio
import importlib
from collections.abc import Callable
from typing import cast


def execute(arguments: list[str]) -> None:
    """Run the unchanged native CLI and close the owned loop even on failure."""
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        module = importlib.import_module("cosmic_ray.cli")
        cli = cast(Callable[..., None], module.cli)
        cli(arguments, standalone_mode=False)
    finally:
        asyncio.set_event_loop(None)
        loop.close()
