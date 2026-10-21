"""Exact serialization contract of :class:`AlertRecord` (dict round trip and defaults)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from polymarket_insider_tracker.alerter._history_records import AlertRecord

CREATED_AT = datetime(2026, 10, 21, 12, 30, 15, tzinfo=UTC)


def _record() -> AlertRecord:
    return AlertRecord(
        alert_id="a-1",
        wallet_address="0xwallet",
        market_id="m-1",
        risk_score=0.75,
        signals_triggered=["size_anomaly", "niche_market"],
        channels_attempted=["discord", "telegram"],
        channels_succeeded=["discord"],
        dedup_key="0xwallet:m-1:2026102112",
        feedback_useful=True,
        created_at=CREATED_AT,
    )


def _minimal() -> dict[str, Any]:
    return {
        "alert_id": "a-1",
        "wallet_address": "0xwallet",
        "market_id": "m-1",
        "risk_score": "0.5",
        "dedup_key": "k",
    }


def test_to_dict_uses_exact_storage_keys() -> None:
    assert _record().to_dict() == {
        "alert_id": "a-1",
        "wallet_address": "0xwallet",
        "market_id": "m-1",
        "risk_score": 0.75,
        "signals_triggered": ["size_anomaly", "niche_market"],
        "channels_attempted": ["discord", "telegram"],
        "channels_succeeded": ["discord"],
        "dedup_key": "0xwallet:m-1:2026102112",
        "feedback_useful": True,
        "created_at": "2026-10-21T12:30:15+00:00",
    }


def test_from_dict_round_trips_every_field() -> None:
    assert AlertRecord.from_dict(_record().to_dict()) == _record()


def test_from_dict_defaults_missing_optional_fields() -> None:
    record = AlertRecord.from_dict(_minimal())

    assert record.risk_score == 0.5
    assert record.dedup_key == "k"
    assert record.signals_triggered == []
    assert record.channels_attempted == []
    assert record.channels_succeeded == []
    assert record.feedback_useful is None


def test_from_dict_without_created_at_stamps_an_aware_utc_time() -> None:
    before = datetime.now(UTC)

    record = AlertRecord.from_dict(_minimal())

    assert record.created_at.tzinfo is UTC
    assert before <= record.created_at <= datetime.now(UTC)


_NOT_TEXT = "alert record field {} is not a string"
_NOT_TEXT_LIST = "alert record field {} is not a list of strings"


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("alert_id", 7, _NOT_TEXT.format("alert_id")),
        ("wallet_address", None, _NOT_TEXT.format("wallet_address")),
        ("market_id", ["m"], _NOT_TEXT.format("market_id")),
        ("dedup_key", 1.5, _NOT_TEXT.format("dedup_key")),
        ("risk_score", [0.5], "alert record field risk_score is not numeric"),
        ("risk_score", None, "alert record field risk_score is not numeric"),
        ("signals_triggered", "size_anomaly", _NOT_TEXT_LIST.format("signals_triggered")),
        ("channels_attempted", [1], _NOT_TEXT_LIST.format("channels_attempted")),
        ("channels_succeeded", ["discord", None], _NOT_TEXT_LIST.format("channels_succeeded")),
        (
            "feedback_useful",
            "yes",
            "alert record field feedback_useful is not a boolean or null",
        ),
        ("created_at", 1_700_000_000, "alert record field created_at is not a timestamp"),
    ],
)
def test_from_dict_rejects_a_value_of_the_wrong_type(key: str, value: object, message: str) -> None:
    data: dict[str, object] = {**_minimal(), key: value}

    with pytest.raises(ValueError) as raised:
        AlertRecord.from_dict(data)

    assert str(raised.value) == message


@pytest.mark.parametrize("key", ["alert_id", "wallet_address", "market_id", "risk_score"])
def test_from_dict_still_raises_key_error_for_a_missing_required_field(key: str) -> None:
    data = _minimal()
    del data[key]

    with pytest.raises(KeyError):
        AlertRecord.from_dict(data)


def test_from_dict_accepts_an_already_parsed_created_at() -> None:
    record = AlertRecord.from_dict({**_minimal(), "created_at": CREATED_AT})

    assert record.created_at == CREATED_AT
