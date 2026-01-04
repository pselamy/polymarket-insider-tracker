"""Alert history tracking and deduplication.

This module provides alert history management with deduplication
to prevent spam and enable analytics on alert patterns.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from polymarket_insider_tracker.detector.models import RiskAssessment

logger = logging.getLogger(__name__)


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


def _generate_dedup_key(wallet_address: str, market_id: str, hour: datetime) -> str:
    """Generate deduplication key for wallet/market/hour combination."""
    hour_str = hour.strftime("%Y%m%d%H")
    return f"{wallet_address}:{market_id}:{hour_str}"


def _get_signals_from_assessment(assessment: RiskAssessment) -> list[str]:
    """Extract triggered signal names from assessment."""
    signals = []
    if assessment.fresh_wallet_signal:
        signals.append("fresh_wallet")
    if assessment.size_anomaly_signal:
        signals.append("size_anomaly")
        if assessment.size_anomaly_signal.is_niche_market:
            signals.append("niche_market")
    return signals


class AlertHistory:
    """Tracks alert history and provides deduplication.

    Uses Redis for storage with configurable dedup window.
    """

    # Redis key prefixes
    KEY_PREFIX_DEDUP = "alert:dedup:"
    KEY_PREFIX_ALERT = "alert:record:"
    KEY_PREFIX_FEEDBACK = "alert:feedback:"
    KEY_INDEX_TIME = "alert:index:time"
    KEY_INDEX_WALLET = "alert:index:wallet:"
    KEY_INDEX_MARKET = "alert:index:market:"

    def __init__(
        self,
        redis: Any,
        *,
        dedup_window_hours: int = 1,
        retention_days: int = 30,
    ) -> None:
        """Initialize alert history.

        Args:
            redis: Redis client (async).
            dedup_window_hours: Hours to deduplicate alerts for same wallet/market.
            retention_days: Days to retain alert history.
        """
        self.redis = redis
        self.dedup_window_hours = dedup_window_hours
        self.retention_days = retention_days
        self._dedup_ttl = dedup_window_hours * 3600
        self._retention_ttl = retention_days * 86400

    def _get_dedup_key(self, assessment: RiskAssessment) -> str:
        """Get deduplication key for an assessment."""
        now = datetime.now(UTC)
        return _generate_dedup_key(
            assessment.wallet_address,
            assessment.market_id,
            now,
        )

    async def should_send(self, assessment: RiskAssessment) -> bool:
        """Check if alert should be sent (not a duplicate).

        Args:
            assessment: Risk assessment to check.

        Returns:
            True if alert should be sent, False if duplicate.
        """
        dedup_key = self._get_dedup_key(assessment)
        redis_key = f"{self.KEY_PREFIX_DEDUP}{dedup_key}"

        # Check if key exists
        exists = await self.redis.exists(redis_key)
        if exists:
            logger.debug(f"Duplicate alert for {dedup_key}")
            return False

        return True

    async def record_sent(
        self,
        assessment: RiskAssessment,
        channels_attempted: list[str],
        channels_succeeded: dict[str, bool],
    ) -> str:
        """Record that an alert was sent.

        Args:
            assessment: The risk assessment that was alerted.
            channels_attempted: List of channels we tried to send to.
            channels_succeeded: Dict of channel name -> success status.

        Returns:
            The alert_id for this record.
        """
        alert_id = str(uuid.uuid4())
        dedup_key = self._get_dedup_key(assessment)
        now = datetime.now(UTC)

        # Create record
        record = AlertRecord(
            alert_id=alert_id,
            wallet_address=assessment.wallet_address,
            market_id=assessment.market_id,
            risk_score=assessment.weighted_score,
            signals_triggered=_get_signals_from_assessment(assessment),
            channels_attempted=channels_attempted,
            channels_succeeded=[ch for ch, success in channels_succeeded.items() if success],
            dedup_key=dedup_key,
            created_at=now,
        )

        # Store in Redis with pipeline
        async with self.redis.pipeline() as pipe:
            # Store dedup key with TTL
            dedup_redis_key = f"{self.KEY_PREFIX_DEDUP}{dedup_key}"
            pipe.set(dedup_redis_key, "1", ex=self._dedup_ttl)

            # Store alert record
            alert_redis_key = f"{self.KEY_PREFIX_ALERT}{alert_id}"
            pipe.set(
                alert_redis_key,
                json.dumps(record.to_dict()),
                ex=self._retention_ttl,
            )

            # Add to time index (sorted set with timestamp as score)
            timestamp_score = now.timestamp()
            pipe.zadd(self.KEY_INDEX_TIME, {alert_id: timestamp_score})

            # Add to wallet index
            wallet_index_key = f"{self.KEY_INDEX_WALLET}{assessment.wallet_address}"
            pipe.zadd(wallet_index_key, {alert_id: timestamp_score})
            pipe.expire(wallet_index_key, self._retention_ttl)

            # Add to market index
            market_index_key = f"{self.KEY_INDEX_MARKET}{assessment.market_id}"
            pipe.zadd(market_index_key, {alert_id: timestamp_score})
            pipe.expire(market_index_key, self._retention_ttl)

            await pipe.execute()

        logger.info(f"Recorded alert {alert_id} for {assessment.wallet_address}")
        return alert_id

    async def record_feedback(self, alert_id: str, useful: bool) -> bool:
        """Record user feedback on alert usefulness.

        Args:
            alert_id: The alert to provide feedback on.
            useful: Whether the alert was useful.

        Returns:
            True if feedback was recorded, False if alert not found.
        """
        alert_redis_key = f"{self.KEY_PREFIX_ALERT}{alert_id}"

        # Get existing record
        data = await self.redis.get(alert_redis_key)
        if not data:
            logger.warning(f"Alert {alert_id} not found for feedback")
            return False

        # Update record
        record_dict = json.loads(data)
        record_dict["feedback_useful"] = useful

        # Get remaining TTL
        ttl = await self.redis.ttl(alert_redis_key)
        if ttl < 0:
            ttl = self._retention_ttl

        # Store updated record
        await self.redis.set(alert_redis_key, json.dumps(record_dict), ex=ttl)
        logger.info(f"Recorded feedback for alert {alert_id}: useful={useful}")
        return True

    async def get_alert(self, alert_id: str) -> AlertRecord | None:
        """Get a specific alert record.

        Args:
            alert_id: The alert ID to retrieve.

        Returns:
            AlertRecord if found, None otherwise.
        """
        alert_redis_key = f"{self.KEY_PREFIX_ALERT}{alert_id}"
        data = await self.redis.get(alert_redis_key)
        if not data:
            return None
        return AlertRecord.from_dict(json.loads(data))

    async def get_alerts(
        self,
        start: datetime,
        end: datetime,
        wallet: str | None = None,
        market: str | None = None,
        limit: int = 100,
    ) -> list[AlertRecord]:
        """Query alert history.

        Args:
            start: Start of time range.
            end: End of time range.
            wallet: Optional wallet address filter.
            market: Optional market ID filter.
            limit: Maximum number of results.

        Returns:
            List of matching AlertRecord objects.
        """
        start_score = start.timestamp()
        end_score = end.timestamp()

        # Determine which index to use
        if wallet:
            index_key = f"{self.KEY_INDEX_WALLET}{wallet}"
        elif market:
            index_key = f"{self.KEY_INDEX_MARKET}{market}"
        else:
            index_key = self.KEY_INDEX_TIME

        # Get alert IDs from index
        alert_ids = await self.redis.zrangebyscore(
            index_key,
            start_score,
            end_score,
            start=0,
            num=limit,
        )

        if not alert_ids:
            return []

        # Fetch all records
        records = []
        for alert_id in alert_ids:
            if isinstance(alert_id, bytes):
                alert_id = alert_id.decode()
            record = await self.get_alert(alert_id)
            if record:
                # Apply additional filters if needed
                if wallet and record.wallet_address != wallet:
                    continue
                if market and record.market_id != market:
                    continue
                records.append(record)

        return records

    async def get_recent_count(
        self,
        hours: int = 24,
        wallet: str | None = None,
    ) -> int:
        """Get count of alerts in recent hours.

        Args:
            hours: Number of hours to look back.
            wallet: Optional wallet address filter.

        Returns:
            Number of alerts in time period.
        """
        end = datetime.now(UTC)
        start = end - timedelta(hours=hours)

        index_key = (
            f"{self.KEY_INDEX_WALLET}{wallet}" if wallet else self.KEY_INDEX_TIME
        )

        count = await self.redis.zcount(
            index_key,
            start.timestamp(),
            end.timestamp(),
        )
        return count

    async def cleanup_old_alerts(self) -> int:
        """Remove alerts older than retention period.

        Returns:
            Number of alerts removed.
        """
        cutoff = datetime.now(UTC) - timedelta(days=self.retention_days)
        cutoff_score = cutoff.timestamp()

        # Get old alert IDs
        old_ids = await self.redis.zrangebyscore(
            self.KEY_INDEX_TIME,
            "-inf",
            cutoff_score,
        )

        if not old_ids:
            return 0

        # Remove from time index
        removed = await self.redis.zremrangebyscore(
            self.KEY_INDEX_TIME,
            "-inf",
            cutoff_score,
        )

        # Note: Individual alert records will expire via TTL
        # Wallet/market indexes will also expire via TTL
        logger.info(f"Cleaned up {removed} old alert references")
        return removed
