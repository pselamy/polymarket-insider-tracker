"""Bounded strict JSON decoding without input excerpts in errors."""

from __future__ import annotations

import json
from typing import NoReturn, cast

from scripts._secret_models import NativeReportError

MAX_REPORT_BYTES = 16 * 1024 * 1024
MAX_REPORT_ROWS = 100_000


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise NativeReportError("duplicate JSON key")
        result[key] = value
    return result


def _nonfinite(_value: str) -> NoReturn:
    raise NativeReportError("nonfinite JSON number")


def decode_array(payload: bytes) -> list[object]:
    """Reject invalid/budget-exceeding input rather than return partial rows."""
    if len(payload) > MAX_REPORT_BYTES:
        raise NativeReportError("report size limit")
    try:
        value: object = json.loads(
            payload.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_nonfinite
        )
    except NativeReportError:
        raise
    except (ValueError, UnicodeError, RecursionError):
        raise NativeReportError("invalid report JSON") from None
    if not isinstance(value, list):
        raise NativeReportError("report root schema")
    rows = cast(list[object], value)
    if len(rows) > MAX_REPORT_ROWS:
        raise NativeReportError("report row limit")
    return rows
