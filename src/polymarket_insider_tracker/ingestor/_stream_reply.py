"""Typed reading of a redis-py ``XREADGROUP`` reply.

redis-py types every command reply as ``Any``; the reply is validated here shape by shape so the
rest of the publisher only sees precisely typed rows. A row that is not ``(id, fields)`` with
``bytes``/``str`` content is reported as ``None`` and the caller skips it like any other entry it
cannot deserialize.
"""

from __future__ import annotations

from polymarket_insider_tracker.shapes import object_mapping, object_sequence

StreamRow = tuple[bytes | str, dict[bytes | str, bytes | str] | None]


def reply_batches(results: object) -> list[list[object]] | None:
    """The per-stream entry lists of a reply, or ``None`` when the reply is not well formed."""
    pairs = object_sequence(results)
    if pairs is None:
        return None
    batches: list[list[object]] = []
    for pair in pairs:
        entries = _batch_entries(pair)
        if entries is None:
            return None
        batches.append(entries)
    return batches


def _batch_entries(pair: object) -> list[object] | None:
    parts = object_sequence(pair)
    if parts is None or len(parts) != 2:
        return None
    return object_sequence(parts[1])


def stream_row(entry: object) -> StreamRow | None:
    """``(entry_id, fields)`` when ``entry`` has that shape; ``None`` fields are a tombstone."""
    parts = object_sequence(entry)
    if parts is None or len(parts) != 2:
        return None
    entry_id, data = parts
    if not isinstance(entry_id, (bytes, str)):
        return None
    if data is None:
        return entry_id, None
    fields = _text_mapping(data)
    return None if fields is None else (entry_id, fields)


def _text_mapping(data: object) -> dict[bytes | str, bytes | str] | None:
    fields = object_mapping(data)
    return None if fields is None else _text_fields(fields)


def _text_fields(fields: dict[object, object]) -> dict[bytes | str, bytes | str] | None:
    row: dict[bytes | str, bytes | str] = {}
    for key, value in fields.items():
        if not isinstance(key, (bytes, str)) or not isinstance(value, (bytes, str)):
            return None
        row[key] = value
    return row
