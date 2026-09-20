"""Terminal-relevant lifecycle values shared with the terminal error home.

Kept in its own module so both ``pipeline`` and ``terminal_errors`` can import
it at runtime without a circular import.
"""

from __future__ import annotations

from enum import StrEnum


class TerminalLifecycle(StrEnum):
    """Terminal-relevant lifecycle values mirrored from PipelineState."""

    STARTING = "starting"
    RUNNING = "running"
    ERROR = "error"
