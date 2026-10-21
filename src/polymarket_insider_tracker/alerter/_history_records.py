"""Alert record model and pure helpers for alert history.

This module holds the :class:`AlertRecord` value object and the side-effect
free helpers used by :mod:`polymarket_insider_tracker.alerter.history` to
build hour-bucketed deduplication keys and to summarise the signals that
triggered a risk assessment. The names are re-exported from the historical
module so existing import paths, qualnames and pickle references keep
resolving.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from polymarket_insider_tracker.shapes import object_list

if TYPE_CHECKING:
    from polymarket_insider_tracker.detector.models import RiskAssessment


@dataclass
class AlertRecord:
    """Record of a sent alert.

    Attributes:
        alert_id: Unique identifier for this alert.
        wallet_address: Trader's wallet address.
        market_id: Market condition ID.
        risk_score: Final weighted risk score.
        signals_triggered: List of signal names that triggered.
        channels_attempted: List of channels we tried to send to.
        channels_succeeded: List of channels that succeeded.
        dedup_key: Key used for deduplication.
        feedback_useful: User feedback on alert usefulness.
        created_at: When the alert was sent.
    """

    alert_id: str
    wallet_address: str
    market_id: str
    risk_score: float
    signals_triggered: list[str]
    channels_attempted: list[str]
    channels_succeeded: list[str]
    dedup_key: str
    feedback_useful: bool | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, object]:
        """Serialize to dictionary for storage."""
        return {
            "alert_id": self.alert_id,
            "wallet_address": self.wallet_address,
            "market_id": self.market_id,
            "risk_score": self.risk_score,
            "signals_triggered": self.signals_triggered,
            "channels_attempted": self.channels_attempted,
            "channels_succeeded": self.channels_succeeded,
            "dedup_key": self.dedup_key,
            "feedback_useful": self.feedback_useful,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> AlertRecord:
        """Deserialize from dictionary; a missing required key raises ``KeyError`` and a
        value of the wrong type raises ``ValueError``."""
        return cls(
            alert_id=_text(data, "alert_id"),
            wallet_address=_text(data, "wallet_address"),
            market_id=_text(data, "market_id"),
            risk_score=_score(data["risk_score"]),
            signals_triggered=_text_list(data, "signals_triggered"),
            channels_attempted=_text_list(data, "channels_attempted"),
            channels_succeeded=_text_list(data, "channels_succeeded"),
            dedup_key=_text(data, "dedup_key"),
            feedback_useful=_feedback(data.get("feedback_useful")),
            created_at=_created_at(data.get("created_at")),
        )


def _text(data: Mapping[str, object], key: str) -> str:
    value = data[key]
    if not isinstance(value, str):
        raise ValueError(f"alert record field {key} is not a string")
    return value


def _text_list(data: Mapping[str, object], key: str) -> list[str]:
    items = object_list(data.get(key, []))
    if items is None:
        raise ValueError(f"alert record field {key} is not a list of strings")
    names = [item for item in items if isinstance(item, str)]
    if len(names) != len(items):
        raise ValueError(f"alert record field {key} is not a list of strings")
    return names


def _score(value: object) -> float:
    if not isinstance(value, (int, float, str)):
        raise ValueError("alert record field risk_score is not numeric")
    return float(value)


def _feedback(value: object) -> bool | None:
    if value is None or isinstance(value, bool):
        return value
    raise ValueError("alert record field feedback_useful is not a boolean or null")


def _created_at(value: object) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise ValueError("alert record field created_at is not a timestamp")


def generate_dedup_key(wallet_address: str, market_id: str, hour: datetime) -> str:
    """Generate deduplication key for wallet/market/hour combination."""
    hour_str = hour.strftime("%Y%m%d%H")
    return f"{wallet_address}:{market_id}:{hour_str}"


def get_signals_from_assessment(assessment: RiskAssessment) -> list[str]:
    """Extract triggered signal names from assessment."""
    signals: list[str] = []
    if assessment.size_anomaly_signal:
        signals.append("size_anomaly")
        if assessment.size_anomaly_signal.is_niche_market:
            signals.append("niche_market")
    return signals
