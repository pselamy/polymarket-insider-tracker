"""Capture what the production console log handler writes."""

from __future__ import annotations

import io
import logging
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import TextIO, TypeGuard

from polymarket_insider_tracker.__main__ import configure_logging


def _is_stream_handler(handler: logging.Handler) -> TypeGuard[logging.StreamHandler[TextIO]]:
    return isinstance(handler, logging.StreamHandler)


def production_console_handler(level: str = "INFO") -> logging.StreamHandler[TextIO]:
    """The console handler ``configure_logging`` installs, redaction filter included."""
    configure_logging(level)
    return next(handler for handler in logging.getLogger().handlers if _is_stream_handler(handler))


@contextmanager
def console_capture(level: str = "INFO") -> Generator[io.StringIO]:
    """Redirect the production console handler into a buffer for the block."""
    console = production_console_handler(level)
    capture = io.StringIO()
    previous = console.setStream(capture)
    try:
        yield capture
    finally:
        if previous is not None:
            console.setStream(previous)


def capture_console_output(emit: Callable[[], None]) -> str:
    """Emit one record and return exactly what the production console handler wrote."""
    with console_capture() as capture:
        emit()
    return capture.getvalue()
