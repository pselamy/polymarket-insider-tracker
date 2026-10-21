"""Characterisation of how the failsafe log filter carries ``status`` and exc_info.

These pin the behaviour of ``_RedactingLogFilter`` on ``origin/main`` so the
escape-free rewrite is proven behaviour-preserving for the shapes the review
named: instance-attribute status (set in ``__init__`` or after construction),
class-level-only status, no status, and ``exc_info`` with no exception value.
"""

from __future__ import annotations

import logging

from polymarket_insider_tracker.__main__ import _RedactingLogFilter


class InitStatusError(Exception):
    """Sets ``status`` in ``__init__`` from a fixed value, like an HTTP client error."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.status = 503


class LateStatusError(Exception):
    """Has no ``status`` until the raiser assigns one after construction."""


class ClassStatusError(Exception):
    """Exposes ``status`` only as a class-level attribute."""

    status = 418


class PlainError(Exception):
    """Carries no ``status`` at all."""


def _filtered(exc_info: tuple[object, object, object]) -> logging.LogRecord:
    record = logging.LogRecord("t", logging.ERROR, __file__, 1, "boom", None, None)
    setattr(record, "exc_info", exc_info)
    _RedactingLogFilter().filter(record)
    return record


def _clone_of(exc: BaseException) -> BaseException:
    record = _filtered((type(exc), exc, None))
    assert record.exc_info is not None
    clone = record.exc_info[1]
    assert isinstance(clone, BaseException)
    return clone


def test_init_status_reaches_the_sanitized_clone_with_the_original_value() -> None:
    exc = InitStatusError("x")
    exc.status = 429
    clone = _clone_of(exc)
    assert type(clone) is InitStatusError
    assert getattr(clone, "status") == 429


def test_status_assigned_after_construction_is_carried_to_the_clone() -> None:
    exc = LateStatusError("x")
    setattr(exc, "status", 502)
    clone = _clone_of(exc)
    assert type(clone) is LateStatusError
    assert getattr(clone, "status") == 502


def test_class_level_status_stays_visible_on_the_clone() -> None:
    clone = _clone_of(ClassStatusError("x"))
    assert type(clone) is ClassStatusError
    assert clone.status == 418


def test_exception_without_status_gets_none_added() -> None:
    clone = _clone_of(PlainError("x"))
    assert type(clone) is PlainError
    assert not hasattr(clone, "status")


def test_exc_info_with_type_but_no_value_is_left_untouched() -> None:
    record = _filtered((ValueError, None, None))
    assert record.exc_info == (ValueError, None, None)
