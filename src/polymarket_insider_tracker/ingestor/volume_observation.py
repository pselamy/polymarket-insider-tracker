"""One Gamma market-volume observation and its stored record form.

Contract: ``specs/004-truthful-detection-contract/data-model.md`` sections 1.1 and 1.1a. An
observation is one market object received in one successful Gamma page response; it carries the
decoded ``volume24hr`` value untouched (bounded only by the section 1.1a storage rule) and the
instant the page body was received. Nothing here interprets the value; classification into the
eight volume states lives in ``detector.volume_state``.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TypeAlias

JsonValue: TypeAlias = "None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]"

GAMMA_SOURCE = "gamma-markets-v1"
RAW_BOUND_BYTES = 256
RESERVED_KEYS = frozenset({"$truncated", "$nonfinite"})
# data-model section 1.1b: a numeric string is exactly one RFC 8259 JSON number token.
NUMERIC_TOKEN = re.compile(r"-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?")
_JSON_TYPE_NAMES: dict[type, str] = {
    bool: "boolean",
    int: "number",
    float: "number",
    str: "string",
    list: "array",
}
_CANONICAL_ENCODER = json.JSONEncoder(sort_keys=True, separators=(",", ":"), ensure_ascii=False)
_STRICT_ENCODER = json.JSONEncoder(allow_nan=False)


def _canonical(value: object) -> bytes:
    """Canonical UTF-8 bytes; non-finite floats keep Python's ``NaN``/``Infinity`` tokens."""
    return _CANONICAL_ENCODER.encode(value).encode()


def _has_nonfinite(value: object) -> bool:
    """True when any float inside ``value`` is NaN or infinite (strict JSON refuses it)."""
    try:
        _STRICT_ENCODER.encode(value)
    except ValueError:
        return True
    return False


def _has_reserved_key(value: object) -> bool:
    return isinstance(value, dict) and any(key in value for key in RESERVED_KEYS)


def _truncation_reason(value: object, size: int) -> str | None:
    if _has_nonfinite(value):
        return "nested_nonfinite"
    if _has_reserved_key(value):
        return "reserved_key"
    return "oversize" if size > RAW_BOUND_BYTES else None


def _nonfinite_tag(value: float) -> str:
    """Python's JSON token for a non-finite float: ``NaN``, ``Infinity`` or ``-Infinity``."""
    return json.dumps(value)


def bound_raw(value: object) -> object:
    """The stored form of a decoded ``volume24hr`` value (data-model section 1.1a)."""
    if isinstance(value, float) and not math.isfinite(value):
        return {"$nonfinite": _nonfinite_tag(value)}
    body = _canonical(value)
    reason = _truncation_reason(value, len(body))
    if reason is None:
        return value
    return {
        "$truncated": True,
        "bytes": len(body),
        "reason": reason,
        "sha256": hashlib.sha256(body).hexdigest(),
        "type": "object" if isinstance(value, dict) else _JSON_TYPE_NAMES.get(type(value), "null"),
    }


@dataclass(frozen=True)
class VolumeObservation:
    """The acquisition facts of one observation; ``acquired_at`` is the stored ISO-8601 text.

    ``source`` is ``None`` for a legacy record that has a value but no provenance.
    ``oversize_numeric`` survives bounding: a truncated string marker no longer shows whether the
    text was a numeric token, and that decides the reason (``out_of_range`` or not a number).
    """

    present: bool
    raw: JsonValue
    acquired_at: str | None
    source: str | None
    oversize_numeric: bool = False

    @classmethod
    def from_page(
        cls, *, present: bool, raw: JsonValue, acquired_at: datetime
    ) -> VolumeObservation:
        """Observation of a market object received in a Gamma page at ``acquired_at``."""
        return cls(present, raw, acquired_at.astimezone(UTC).isoformat(), GAMMA_SOURCE)

    def to_record(self) -> dict[str, object]:
        """The Redis record body; the raw value is bounded by the section 1.1a rule."""
        stored = bound_raw(self.raw)
        oversize = isinstance(self.raw, str) and isinstance(stored, dict)
        return {
            "present": self.present,
            "raw": stored,
            "acquired_at": self.acquired_at,
            "source": self.source,
            "oversize_numeric": self.oversize_numeric
            or (oversize and NUMERIC_TOKEN.fullmatch(str(self.raw)) is not None),
        }

    @classmethod
    def from_record(cls, record: JsonValue) -> VolumeObservation | None:
        """Read a stored record body; ``None`` when it is not a record (a corrupt entry)."""
        if not isinstance(record, dict):
            return None
        present, acquired_at, source = (record.get(k) for k in ("present", "acquired_at", "source"))
        if not isinstance(present, bool) or not isinstance(acquired_at, str | None):
            return None
        if not isinstance(source, str | None):
            return None
        oversize = record.get("oversize_numeric") is True
        return cls(present, record.get("raw"), acquired_at, source, oversize)

    @classmethod
    def from_cached(cls, data: Mapping[str, JsonValue]) -> VolumeObservation | None:
        """The observation of a cached metadata dict; a bare ``daily_volume`` is a legacy record.

        A legacy value has no source and no stamp, so it classifies as ``unqualified`` /
        ``no_provenance`` and is never interpreted.
        """
        if "volume" in data:
            return cls.from_record(data["volume"])
        legacy = data.get("daily_volume")
        return None if legacy is None else cls(True, legacy, None, None)
