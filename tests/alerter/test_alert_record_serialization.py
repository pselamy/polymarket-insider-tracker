"""Exact serialization contract of :class:`AlertRecord` (dict round trip and defaults)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from polymarket_insider_tracker.alerter._history_records import AlertRecord

CREATED_AT = datetime(2026, 10, 21, 12, 30, 15, tzinfo=UTC)


def _record() -> AlertRecord:
    return AlertRecord(
        alert_id="a-1",
        wallet_address="0xwallet",
        market_id="m-1",
        risk_score=0.75,
        signals_triggered=["fresh_wallet", "size_anomaly"],
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
        "signals_triggered": ["fresh_wallet", "size_anomaly"],
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
