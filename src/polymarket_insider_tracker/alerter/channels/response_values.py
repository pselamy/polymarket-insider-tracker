"""Fail-closed handling of untrusted provider response values.

A rejected webhook or bot-API response is server-controlled and can echo the
credential-bearing request URL in arbitrarily re-encoded spellings
(percent-encoded, JSON ``\\uXXXX`` escapes, split components) that no
replacement list can enumerate. Channels therefore never log response text:
only values validated here — plain integers and finite non-negative retry
delays — may reach a log line or a sleep, and anything else collapses to a
fixed placeholder or default.
"""

from __future__ import annotations

import math
from typing import cast

import httpx

DEFAULT_RETRY_AFTER_SECONDS = 1.0


def validated_integer(value: object) -> int | None:
    """``value`` only when it is a plain integer (``bool`` is not one)."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def validated_retry_delay(value: object) -> float:
    """A finite non-negative numeric delay, anything else the fixed default.

    The value drives both a log line and a sleep, so a string, boolean,
    negative, or non-finite shape must not survive: JSON can smuggle
    ``Infinity`` through a lenient parser and an unbounded sleep would hang
    the delivery attempt forever.
    """
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _bounded_delay(float(value))
    return DEFAULT_RETRY_AFTER_SECONDS


def _bounded_delay(delay: float) -> float:
    if math.isfinite(delay) and delay >= 0:
        return delay
    return DEFAULT_RETRY_AFTER_SECONDS


def response_json_object(response: httpx.Response) -> dict[str, object]:
    """The response body as a JSON object, or empty when it is not one.

    Parsing failures are part of the untrusted surface: they must produce
    the empty fail-closed shape, never an exception that could carry body
    bytes into an unguarded error path.
    """
    try:
        payload: object = response.json()
    except Exception:
        return {}
    if isinstance(payload, dict):
        return cast(dict[str, object], payload)
    return {}
