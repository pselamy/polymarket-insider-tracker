"""Redis volume records, kept apart from the CLOB metadata records.

Contract: ``specs/004-truthful-detection-contract/data-model.md`` section 1.4. Only the Gamma
refresh writes these records; the CLOB paths never do, so a CLOB write can never replace a
stamped volume with a volume-less record. A record outlives the 600 s recency rule (TTL 1,200 s)
so an old value stays readable as ``stale`` instead of vanishing.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Literal

from redis.asyncio import Redis

from polymarket_insider_tracker.ingestor.gamma_client import GammaCrawl, GammaMarketStats
from polymarket_insider_tracker.ingestor.models import MarketMetadata
from polymarket_insider_tracker.ingestor.volume_observation import JsonValue, VolumeObservation
from polymarket_insider_tracker.redaction import redact_exception_message

logger = logging.getLogger(__name__)

DEFAULT_VOLUME_KEY_PREFIX = "polymarket:volume:"
VOLUME_RECORD_TTL_SECONDS = 1200
CRAWL_STATUS_KEY = "_crawl"

CrawlOutcome = Literal["ok", "truncated", "failed"]
# P1 reason for a market with no volume record, by the outcome of the last crawl.
_UNAVAILABLE_BY_OUTCOME: dict[str, str] = {
    "ok": "absent_from_complete_crawl",
    "truncated": "crawl_truncated",
    "failed": "gamma_fetch_failed",
}
NO_VOLUME_RECORD = "no_volume_record"


def _decimal_or_none(value: JsonValue) -> Decimal | None:
    if not isinstance(value, str):
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


@dataclass(frozen=True)
class VolumeRecord:
    """A stored observation plus the lossy decimals older consumers read from the metadata."""

    observation: VolumeObservation
    daily_volume: Decimal | None
    weekly_volume: Decimal | None
    liquidity: Decimal | None

    @classmethod
    def from_stats(cls, stats: GammaMarketStats) -> VolumeRecord:
        return cls(
            stats.volume_observation(), stats.daily_volume, stats.weekly_volume, stats.liquidity
        )

    def to_json(self) -> str:
        return json.dumps(
            {
                "observation": self.observation.to_record(),
                "daily_volume": _decimal_text(self.daily_volume),
                "weekly_volume": _decimal_text(self.weekly_volume),
                "liquidity": _decimal_text(self.liquidity),
            }
        )

    @classmethod
    def from_json(cls, text: str | bytes) -> VolumeRecord | None:
        """Read a stored record; ``None`` when it is not a volume record."""
        try:
            data: JsonValue = json.loads(text)
        except ValueError:
            return None
        if not isinstance(data, dict):
            return None
        observation = VolumeObservation.from_record(data.get("observation"))
        if observation is None:
            return None
        return cls(
            observation,
            _decimal_or_none(data.get("daily_volume")),
            _decimal_or_none(data.get("weekly_volume")),
            _decimal_or_none(data.get("liquidity")),
        )


def crawl_outcome(crawl: GammaCrawl | None) -> CrawlOutcome:
    """``failed`` when no page was read (or the crawl raised), ``truncated`` on a partial read."""
    if crawl is None or crawl.pages_ok == 0:
        return "failed"
    return "truncated" if crawl.truncated else "ok"


class VolumeStore:
    """Reads and writes the per-market volume records and the last-crawl status."""

    def __init__(
        self,
        redis: Redis,
        *,
        key_prefix: str = DEFAULT_VOLUME_KEY_PREFIX,
        ttl_seconds: int = VOLUME_RECORD_TTL_SECONDS,
    ) -> None:
        self._redis = redis
        self._prefix = key_prefix
        self._ttl = ttl_seconds

    async def _write_one(self, stats: GammaMarketStats) -> bool:
        record = VolumeRecord.from_stats(stats)
        try:
            await self._redis.setex(self._prefix + stats.condition_id, self._ttl, record.to_json())
        except Exception as e:
            logger.warning(
                "Failed to store volume record %s: %s",
                stats.condition_id,
                redact_exception_message(e),
            )
            return False
        return True

    async def write_crawl(self, crawl: GammaCrawl | None, finished: datetime) -> int:
        """Store every observation of ``crawl`` and the crawl outcome; return records written.

        A failed crawl (``None``) writes no record, so it never refreshes an older observation.
        """
        written = 0
        if crawl is not None:
            for stats in crawl.stats.values():
                written += await self._write_one(stats)
        status = json.dumps(
            {"outcome": crawl_outcome(crawl), "finished_at": finished.astimezone(UTC).isoformat()}
        )
        await self._redis.setex(self._prefix + CRAWL_STATUS_KEY, self._ttl, status)
        return written

    async def _unavailable_reason(self) -> str:
        raw = await self._redis.get(self._prefix + CRAWL_STATUS_KEY)
        return _UNAVAILABLE_BY_OUTCOME.get(self._outcome_text(raw), NO_VOLUME_RECORD)

    @staticmethod
    def _outcome_text(raw: str | bytes | None) -> str:
        try:
            status: JsonValue = json.loads(raw) if raw else None
        except ValueError:
            return ""
        return str(status.get("outcome")) if isinstance(status, dict) else ""

    async def read(self, condition_id: str) -> VolumeRecord | None:
        raw = await self._redis.get(self._prefix + condition_id)
        return VolumeRecord.from_json(raw) if raw else None

    async def _read_or_none(self, condition_id: str) -> VolumeRecord | None:
        """The stored record, or ``None`` when Redis cannot be read: the volume is then simply
        unavailable and the CLOB metadata it would have been merged into stays usable."""
        try:
            return await self.read(condition_id)
        except Exception as e:
            logger.warning(
                "Failed to read volume record %s: %s", condition_id, redact_exception_message(e)
            )
            return None

    async def _reason_or_default(self) -> str:
        try:
            return await self._unavailable_reason()
        except Exception:
            return NO_VOLUME_RECORD

    async def merge(
        self, metadata: MarketMetadata, *, no_record_reason: str | None = None
    ) -> MarketMetadata:
        """``metadata`` with its stored volume record merged in.

        With no record the metadata keeps any legacy volume it carried; otherwise it is marked
        unavailable with ``no_record_reason`` or the reason the last crawl implies.
        """
        record = await self._read_or_none(metadata.condition_id)
        if record is None:
            if metadata.volume is not None:
                return metadata
            reason = no_record_reason or await self._reason_or_default()
            return replace(metadata, volume_unavailable_reason=reason)
        return replace(
            metadata,
            volume=record.observation,
            daily_volume=record.daily_volume,
            weekly_volume=record.weekly_volume,
            liquidity=record.liquidity,
        )
