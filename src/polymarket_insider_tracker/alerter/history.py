"""Alert history tracking and deduplication.

This module provides alert history management with deduplication
to prevent spam and enable analytics on alert patterns.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from . import _channel_delivery
from ._history_records import AlertRecord as AlertRecord
from ._history_records import generate_dedup_key as _generate_dedup_key
from ._history_records import get_signals_from_assessment as _get_signals_from_assessment

if TYPE_CHECKING:
    from polymarket_insider_tracker.detector.models import RiskAssessment

logger = logging.getLogger(__name__)

__all__ = ["AlertHistory", "AlertRecord", "_generate_dedup_key", "_get_signals_from_assessment"]

# Compatibility: the record type and pure helpers now live in
# ``_history_records``; keep the historical ``history.<name>`` module/qualname
# identity so existing import paths and pickle references keep resolving.
AlertRecord.__module__ = __name__
AlertRecord.__qualname__ = "AlertRecord"
_generate_dedup_key.__module__ = __name__
_generate_dedup_key.__qualname__ = "_generate_dedup_key"
_get_signals_from_assessment.__module__ = __name__
_get_signals_from_assessment.__qualname__ = "_get_signals_from_assessment"


class AlertHistory:
    """Tracks alert history and provides deduplication.

    Uses Redis for storage with configurable dedup window. The authoritative delivery
    deduplication is the channel-scoped API (``get_channel_dedup_key``,
    ``is_channel_suppressed``, ``record_channel_delivery``, ``record_channel_ambiguous``);
    the assessment-hour methods below it are a non-operational legacy analytics path.
    """

    # Redis key prefixes
    KEY_PREFIX_DEDUP = "alert:dedup:"
    KEY_PREFIX_ALERT = "alert:record:"
    KEY_INDEX_TIME = "alert:index:time"
    KEY_INDEX_WALLET = "alert:index:wallet:"
    KEY_INDEX_MARKET = "alert:index:market:"

    def __init__(
        self,
        redis: Any,
        *,
        dedup_window_hours: int = 1,
        retention_days: int = 30,
        dedup_window_seconds: int | None = None,
    ) -> None:
        """Initialize alert history.

        Args:
            redis: Redis client (async).
            dedup_window_hours: Hours to deduplicate alerts for same wallet/market.
            retention_days: Days to retain alert history.
            dedup_window_seconds: Exact deduplication TTL in seconds; when provided it
                takes precedence over ``dedup_window_hours`` so the configured
                ``DETECTOR_DEDUP_WINDOW_SECONDS`` value is honored without rounding.
        """
        self.redis = redis
        self.dedup_window_hours = dedup_window_hours
        self.retention_days = retention_days
        self._dedup_ttl = (
            dedup_window_seconds if dedup_window_seconds is not None else dedup_window_hours * 3600
        )
        self._retention_ttl = retention_days * 86400

    KEY_PREFIX_AMBIGUOUS = "alert:ambiguous:"

    @property
    def dedup_ttl(self) -> int:
        """Effective channel delivery deduplication TTL in seconds."""
        return self._dedup_ttl

    @property
    def retention_ttl(self) -> int:
        """Retention TTL in seconds for stored alert records and their indexes."""
        return self._retention_ttl

    @classmethod
    def get_channel_dedup_key(cls, channel: str, wallet: str, market: str) -> str:
        """Get channel-scoped delivery deduplication Redis key."""
        return f"{cls.KEY_PREFIX_DEDUP}{channel}:{wallet.lower()}:{market}"

    @classmethod
    def get_channel_ambiguous_key(cls, channel: str, wallet: str, market: str) -> str:
        """Get channel-scoped ambiguity temporary Redis key."""
        return f"{cls.KEY_PREFIX_AMBIGUOUS}{channel}:{wallet.lower()}:{market}"

    # Channel-scoped delivery deduplication lives in ``_channel_delivery`` and
    # is bound here so ``AlertHistory.<name>`` keeps its historical method
    # identity for import and introspection. The claim lifecycle internally
    # dispatches to the module-level ``compare_and_delete``/``compare_and_expire``
    # (Pyright strict forbids cross-module access to the protected attributes).
    is_channel_suppressed = _channel_delivery.is_channel_suppressed
    is_channel_delivered = _channel_delivery.is_channel_delivered
    record_channel_delivery = _channel_delivery.record_channel_delivery
    claim_channel_send = _channel_delivery.claim_channel_send
    release_channel_claim = _channel_delivery.release_channel_claim
    _compare_and_delete = _channel_delivery.compare_and_delete
    extend_channel_claim = _channel_delivery.extend_channel_claim
    _compare_and_expire = _channel_delivery.compare_and_expire
    record_channel_ambiguous = _channel_delivery.record_channel_ambiguous

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

        Non-operational legacy path: the pipeline does not call this method. The
        authoritative delivery deduplication is the channel-scoped API
        (``is_channel_suppressed`` / ``record_channel_delivery``) per FR-011.

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

        Non-operational legacy path: the pipeline does not call this method, and its
        hour-bucket dedup key does not participate in delivery suppression. The
        authoritative delivery deduplication is the channel-scoped API per FR-011.

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

    def _select_index_key(self, wallet: str | None, market: str | None) -> str:
        if wallet:
            return f"{self.KEY_INDEX_WALLET}{wallet}"
        if market:
            return f"{self.KEY_INDEX_MARKET}{market}"
        return self.KEY_INDEX_TIME

    @staticmethod
    def _matches_filters(
        record: AlertRecord | None,
        wallet: str | None,
        market: str | None,
    ) -> bool:
        if record is None:
            return False
        if wallet and record.wallet_address != wallet:
            return False
        return not (market and record.market_id != market)

    async def _fetch_alert_record(self, raw_id: str | bytes) -> AlertRecord | None:
        alert_id = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
        return await self.get_alert(alert_id)

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
        index_key = self._select_index_key(wallet, market)

        alert_ids = await self.redis.zrangebyscore(
            index_key,
            start.timestamp(),
            end.timestamp(),
            start=0,
            num=limit,
        )

        if not alert_ids:
            return []

        records: list[AlertRecord] = []
        for raw_id in alert_ids:
            record = await self._fetch_alert_record(raw_id)
            if self._matches_filters(record, wallet, market):
                assert record is not None
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

        index_key = f"{self.KEY_INDEX_WALLET}{wallet}" if wallet else self.KEY_INDEX_TIME

        count = await self.redis.zcount(
            index_key,
            start.timestamp(),
            end.timestamp(),
        )
        return int(count)

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
        return int(removed)
