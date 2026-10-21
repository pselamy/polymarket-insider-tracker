"""Checked shapes for the decoded dictionaries the ingestor models read and write."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TypedDict

from polymarket_insider_tracker.ingestor._json_value import json_value
from polymarket_insider_tracker.ingestor.volume_observation import JsonValue
from polymarket_insider_tracker.shapes import object_sequence, string_keyed


class TokenRecord(TypedDict):
    """The cached form of one ``Token``."""

    token_id: str
    outcome: str
    price: str | None


class MarketMetadataRecord(TypedDict):
    """The cached (Redis, JSON) form of a ``MarketMetadata``."""

    condition_id: str
    question: str
    description: str
    tokens: list[TokenRecord]
    end_date: str | None
    active: bool
    closed: bool
    category: str
    volume: dict[str, object] | None
    last_updated: str


def token_entries(raw: object) -> list[dict[str, object]]:
    """The objects of a decoded ``tokens`` list; any other shape is rejected with ``TypeError``."""
    entries = object_sequence(raw)
    if entries is None:
        raise TypeError("tokens must be a list")
    return [_token_entry(entry) for entry in entries]


def _token_entry(entry: object) -> dict[str, object]:
    fields = string_keyed(entry)
    if fields is None:
        raise TypeError("token must be an object")
    return fields


def int_field(value: object) -> int:
    """``int(value)`` for the decoded scalar kinds; any other kind is a ``TypeError``."""
    if isinstance(value, (int, float, str)):
        return int(value)
    raise TypeError(f"expected an integer-like value, not {type(value).__name__}")


def cached_volume_fields(data: Mapping[str, object]) -> dict[str, JsonValue]:
    """The volume keys of a cached record, as the JSON values ``VolumeObservation`` reads."""
    return {key: json_value(data[key]) for key in ("volume", "daily_volume") if key in data}


def iso_text(value: object) -> str:
    """The text of a stored timestamp; any other kind is a ``TypeError``."""
    if not isinstance(value, str):
        raise TypeError(f"expected an ISO timestamp string, not {type(value).__name__}")
    return value
