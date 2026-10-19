"""Capture what the production console log handler writes."""

from __future__ import annotations

import io
import logging
from collections.abc import Callable
from typing import TextIO, TypeGuard

from polymarket_insider_tracker.__main__ import configure_logging


def _is_stream_handler(handler: logging.Handler) -> TypeGuard[logging.StreamHandler[TextIO]]:
    return isinstance(handler, logging.StreamHandler)


def capture_console_output(emit: Callable[[], None]) -> str:
    """Emit one record and return exactly what the production console handler wrote."""
    configure_logging("INFO")
    console = next(
        handler for handler in logging.getLogger().handlers if _is_stream_handler(handler)
    )
    capture = io.StringIO()
    previous = console.setStream(capture)
    try:
        emit()
    finally:
        if previous is not None:
            console.setStream(previous)
    return capture.getvalue()
