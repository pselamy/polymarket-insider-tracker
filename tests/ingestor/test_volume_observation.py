"""Raw-value bounding and the stored record form of a volume observation (data-model 1.1a)."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

from polymarket_insider_tracker.ingestor.models import MarketMetadata
from polymarket_insider_tracker.ingestor.volume_observation import (
    GAMMA_SOURCE,
    JsonValue,
    VolumeObservation,
    bound_raw,
)

OVERSIZE_300: dict[str, JsonValue] = {
    "$truncated": True,
    "bytes": 302,
    "reason": "oversize",
    "sha256": "d24923841d98dcd5a081ba3b99596483edeeca895b41ded64c8aa77a3c438fa7",
    "type": "string",
}
AT = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "value",
    [None, True, 0, 5, 5.0, "5", "", "NaN", [1, "a"], {"a": [1]}, "x" * 254],
)
def test_ordinary_values_are_stored_verbatim(value: object) -> None:
    stored = bound_raw(value)

    assert stored == value
    assert type(stored) is type(value)


@pytest.mark.parametrize(
    ("value", "tag"),
    [(float("nan"), "NaN"), (float("inf"), "Infinity"), (float("-inf"), "-Infinity")],
)
def test_bare_non_finite_float_is_a_tag(value: float, tag: str) -> None:
    assert bound_raw(value) == {"$nonfinite": tag}


def test_oversize_string_is_the_contract_marker() -> None:
    assert bound_raw("x" * 300) == OVERSIZE_300


@pytest.mark.parametrize(
    ("value", "reason", "kind"),
    [
        ([1.0, float("nan")], "nested_nonfinite", "array"),
        ({"a": [float("inf")]}, "nested_nonfinite", "object"),
        ({"$truncated": False}, "reserved_key", "object"),
        ({"$nonfinite": "NaN"}, "reserved_key", "object"),
        (int("9" * 300), "oversize", "number"),
        (["x" * 300], "oversize", "array"),
    ],
)
def test_each_truncation_reason_and_type(value: object, reason: str, kind: str) -> None:
    stored = bound_raw(value)

    assert isinstance(stored, dict)
    assert (stored["$truncated"], stored["reason"], stored["type"]) == (True, reason, kind)


def test_marker_hash_and_length_are_measured_on_the_original() -> None:
    value = {"$truncated": False}
    body = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()

    stored = bound_raw(value)

    assert isinstance(stored, dict)
    assert stored["bytes"] == len(body)
    assert stored["sha256"] == hashlib.sha256(body).hexdigest()


def test_boundary_of_256_canonical_bytes() -> None:
    assert bound_raw("x" * 254) == "x" * 254  # 256 bytes with the quotes
    assert isinstance(bound_raw("x" * 255), dict)


def test_from_page_stamps_gamma_source_in_utc() -> None:
    zone = timezone(timedelta(hours=2))

    observation = VolumeObservation.from_page(
        present=True, raw="5", acquired_at=AT.astimezone(zone)
    )

    assert observation == VolumeObservation(True, "5", "2026-10-19T12:00:00+00:00", GAMMA_SOURCE)


def test_record_round_trip_bounds_the_raw_value() -> None:
    observation = VolumeObservation.from_page(present=True, raw="x" * 300, acquired_at=AT)

    restored = VolumeObservation.from_record(json.loads(json.dumps(observation.to_record())))

    assert restored == VolumeObservation(
        True, OVERSIZE_300, "2026-10-19T12:00:00+00:00", GAMMA_SOURCE
    )


@pytest.mark.parametrize(
    "record",
    [
        None,
        "x",
        [],
        {"present": "yes"},
        {"present": True, "acquired_at": 5},
        {"present": True, "acquired_at": None, "source": 1},
    ],
)
def test_a_corrupt_record_is_not_an_observation(record: object) -> None:
    assert VolumeObservation.from_record(json.loads(json.dumps(record))) is None


def test_record_without_stamp_or_source_keeps_both_null() -> None:
    assert VolumeObservation.from_record({"present": False}) == VolumeObservation(
        False, None, None, None
    )


class TestCachedMetadataProvenance:
    """T012c: legacy records read as unqualified; the generic last_updated is never a stamp."""

    @staticmethod
    def _cached(**extra: object) -> dict[str, object]:
        return {"condition_id": "0xa", "last_updated": AT.isoformat(), **extra}

    def test_legacy_value_has_no_source_and_no_stamp(self) -> None:
        metadata = MarketMetadata.from_dict(self._cached(daily_volume="30000"))

        assert metadata.volume == VolumeObservation(True, "30000", None, None)

    def test_no_value_and_no_volume_key_is_no_observation(self) -> None:
        assert MarketMetadata.from_dict(self._cached(daily_volume=None)).volume is None

    def test_provenance_round_trips_through_the_cache_dict(self) -> None:
        observation = VolumeObservation.from_page(present=True, raw=30000, acquired_at=AT)
        metadata = MarketMetadata.from_dict(self._cached(volume=observation.to_record()))

        assert metadata.volume == observation
        assert MarketMetadata.from_dict(metadata.to_dict()).volume == observation

    def test_volume_null_key_overrides_the_legacy_value(self) -> None:
        assert MarketMetadata.from_dict(self._cached(volume=None, daily_volume="5")).volume is None


def test_the_marker_hash_uses_sorted_compact_unescaped_utf8() -> None:
    value = {"z": 1, "é": "é", "$truncated": False}
    body = '{"$truncated":false,"z":1,"é":"é"}'.encode()

    stored = bound_raw(value)

    assert isinstance(stored, dict)
    assert (stored["bytes"], stored["sha256"]) == (len(body), hashlib.sha256(body).hexdigest())


def test_a_value_of_an_unknown_json_type_is_marked_null() -> None:
    stored = bound_raw(("x" * 300,))

    assert isinstance(stored, dict)
    assert (stored["reason"], stored["type"]) == ("oversize", "null")


@pytest.mark.parametrize(
    ("raw", "oversize_numeric"),
    [("9" * 300, True), ("x" * 300, False), (int("9" * 300), False), ("9", False)],
)
def test_only_an_oversize_numeric_string_is_remembered_as_numeric(
    raw: JsonValue, oversize_numeric: bool
) -> None:
    record = VolumeObservation.from_page(present=True, raw=raw, acquired_at=AT).to_record()

    assert record["oversize_numeric"] is oversize_numeric


def test_a_remembered_numeric_flag_is_read_back_from_a_record() -> None:
    record: JsonValue = {"present": True, "raw": OVERSIZE_300, "oversize_numeric": True}

    restored = VolumeObservation.from_record(record)

    assert restored == VolumeObservation(True, OVERSIZE_300, None, None, oversize_numeric=True)
