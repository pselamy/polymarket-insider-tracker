"""Alert record model and pure helpers for alert history.

This module holds the :class:`AlertRecord` value object and the side-effect
free helpers used by :mod:`polymarket_insider_tracker.alerter.history` to
build hour-bucketed deduplication keys and to summarise the signals that
triggered a risk assessment. The names are re-exported from the historical
module so existing import paths, qualnames and pickle references keep
resolving.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

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

    def to_dict(self) -> dict[str, Any]:
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
    def from_dict(cls, data: dict[str, Any]) -> AlertRecord:
        """Deserialize from dictionary."""
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.now(UTC)

        return cls(
            alert_id=data["alert_id"],
            wallet_address=data["wallet_address"],
            market_id=data["market_id"],
            risk_score=float(data["risk_score"]),
            signals_triggered=data.get("signals_triggered", []),
            channels_attempted=data.get("channels_attempted", []),
            channels_succeeded=data.get("channels_succeeded", []),
            dedup_key=data["dedup_key"],
            feedback_useful=data.get("feedback_useful"),
            created_at=created_at,
        )


def generate_dedup_key(wallet_address: str, market_id: str, hour: datetime) -> str:
    """Generate deduplication key for wallet/market/hour combination."""
    hour_str = hour.strftime("%Y%m%d%H")
    return f"{wallet_address}:{market_id}:{hour_str}"


def get_signals_from_assessment(assessment: RiskAssessment) -> list[str]:
    """Extract triggered signal names from assessment."""
    signals: list[str] = []
    if assessment.fresh_wallet_signal:
        signals.append("fresh_wallet")
    if assessment.size_anomaly_signal:
        signals.append("size_anomaly")
        if assessment.size_anomaly_signal.is_niche_market:
            signals.append("niche_market")
    return signals
