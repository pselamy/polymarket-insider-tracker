"""Durable complete-through boundary, identity window, and loss events in Redis.

All keys share ``polymarket:ingest:<source-id>:`` where the source id derives from the trades URL
and coverage mode, so changing either starts a fresh boundary (except the official v1/v2 pair). The identity window is mirrored in
memory for duplicate checks and reloaded on start; every boundary advance writes the checkpoint,
new identities, the trim, and any loss events in one ``MULTI``/``EXEC`` pipeline so a failed write
leaves the previous checkpoint intact. See ``contracts/observation-boundary.md``.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from enum import StrEnum

from polymarket_insider_tracker.alerter._redis_port import (
    BoundaryPipeline,
    BoundaryRedis,
    reply_byte_hash,
    reply_bytes,
    reply_ids,
    reply_scored_members,
    settle,
)
from polymarket_insider_tracker.endpoints import OFFICIAL_V1_TRADES_URL, OFFICIAL_V2_TRADES_URL
from polymarket_insider_tracker.shapes import string_keyed

SCHEMA_VERSION = 2
LOSS_EVENT_RETENTION = 50
KEY_PREFIX = "polymarket:ingest:"
_REQUIRED_FIELDS = (
    "schema_version",
    "boundary_time",
    "emission_floor",
    "boundary_origin",
    "written_at",
    "coverage",
    "last_request_end",
)


class BoundaryOrigin(StrEnum):
    """How the durable boundary was established."""

    FIRST_START = "first-start"
    PROVEN = "proven"
    RE_ANCHORED = "re-anchored"


class LossReason(StrEnum):
    """Why an interval could not be proven covered."""

    HORIZON_EXPIRED = "horizon-expired"
    RESTART_BEYOND_HORIZON = "restart-beyond-horizon"
    CONTINUITY_MISMATCH = "continuity-mismatch"


class BoundarySchemaError(Exception):
    """The durable checkpoint has an unknown schema or the boundary is unusable; terminal."""


@dataclass(frozen=True)
class Checkpoint:
    """The durable complete-through boundary."""

    boundary_time: int
    emission_floor: int
    boundary_origin: BoundaryOrigin
    written_at: int
    coverage: str
    last_request_end: int

    def to_mapping(self) -> dict[str, str | int]:
        return {
            "schema_version": SCHEMA_VERSION,
            "boundary_time": self.boundary_time,
            "emission_floor": self.emission_floor,
            "boundary_origin": self.boundary_origin.value,
            "written_at": self.written_at,
            "coverage": self.coverage,
            "last_request_end": self.last_request_end,
        }

    @classmethod
    def from_mapping(cls, raw: Mapping[bytes, bytes]) -> Checkpoint:
        """Decode a checkpoint hash; any unknown or missing field is a schema error."""
        fields = {key.decode(): value.decode() for key, value in raw.items()}
        missing = [name for name in _REQUIRED_FIELDS if name not in fields]
        if missing:
            raise BoundarySchemaError(f"checkpoint is missing {', '.join(missing)}")
        try:
            return cls._decode(fields)
        except ValueError as exc:
            raise BoundarySchemaError(f"checkpoint field is malformed: {exc}") from exc

    @classmethod
    def _decode(cls, fields: Mapping[str, str]) -> Checkpoint:
        if int(fields["schema_version"]) != SCHEMA_VERSION:
            raise BoundarySchemaError(
                f"checkpoint schema_version {fields['schema_version']} is unknown"
            )
        return cls(
            boundary_time=int(fields["boundary_time"]),
            emission_floor=int(fields["emission_floor"]),
            boundary_origin=BoundaryOrigin(fields["boundary_origin"]),
            written_at=int(fields["written_at"]),
            coverage=fields["coverage"],
            last_request_end=int(fields["last_request_end"]),
        )


def _json_object(payload: bytes | str) -> dict[str, object]:
    decoded = string_keyed(json.loads(payload))
    if decoded is None:
        raise ValueError("loss event is not a JSON object")
    return decoded


def _str_field(data: Mapping[str, object], name: str) -> str:
    value = data[name]
    if not isinstance(value, str):
        raise ValueError(f"loss event field {name} is not a string")
    return value


def _int_field(data: Mapping[str, object], name: str) -> int:
    value = data[name]
    if not isinstance(value, int | str):
        raise ValueError(f"loss event field {name} is not an integer")
    return int(value)


@dataclass(frozen=True)
class LossEvent:
    """An interval the tracker could not prove it covered."""

    from_time: int
    to_time: int
    reason: LossReason
    detected_at: int
    recorded_at: int
    pages_examined: int

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def from_json(cls, payload: bytes | str) -> LossEvent:
        data = _json_object(payload)
        return cls(
            from_time=_int_field(data, "from_time"),
            to_time=_int_field(data, "to_time"),
            reason=LossReason(_str_field(data, "reason")),
            detected_at=_int_field(data, "detected_at"),
            recorded_at=_int_field(data, "recorded_at"),
            pages_examined=_int_field(data, "pages_examined"),
        )


@dataclass(frozen=True)
class ProofResult:
    """Outcome of the reach-plus-continuity rule for one page."""

    reach: bool
    continuity: bool
    oldest: int | None
    missing_identities: tuple[str, ...] = ()

    @property
    def missing(self) -> int:
        return len(self.missing_identities)

    @property
    def proven(self) -> bool:
        return self.reach and self.continuity


def source_id(trades_url: str, coverage: str) -> str:
    """First 12 hex characters of ``sha256("<logical source url>|<coverage>")``.

    The exact official v1/v2 default endpoint pair with the same coverage mode shares the
    existing v1 namespace so a saved checkpoint and identity window survive the upgrade;
    every other URL keeps its own identity. See contract item 4.
    """
    logical = OFFICIAL_V1_TRADES_URL if trades_url == OFFICIAL_V2_TRADES_URL else trades_url
    return hashlib.sha256(f"{logical}|{coverage}".encode()).hexdigest()[:12]


class ObservationBoundary:
    """Redis-backed boundary with an in-memory identity mirror."""

    def __init__(
        self,
        redis: BoundaryRedis,
        *,
        trades_url: str,
        coverage: str,
        horizon_seconds: int,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._redis = redis
        self._coverage = coverage
        self._horizon = horizon_seconds
        self._clock = clock
        prefix = f"{KEY_PREFIX}{source_id(trades_url, coverage)}:"
        self.checkpoint_key = f"{prefix}checkpoint"
        self.identities_key = f"{prefix}identities"
        self.loss_events_key = f"{prefix}loss-events"
        self._checkpoint: Checkpoint | None = None
        self._window: dict[str, int] = {}
        self._loss_events: tuple[LossEvent, ...] = ()

    @property
    def checkpoint(self) -> Checkpoint | None:
        return self._checkpoint

    @property
    def window(self) -> Mapping[str, int]:
        return self._window

    @property
    def loss_events(self) -> tuple[LossEvent, ...]:
        return self._loss_events

    def contains(self, identity: str) -> bool:
        return identity in self._window

    async def load(self) -> Checkpoint | None:
        """Read the checkpoint; reload the identity window and loss events when it is usable."""
        raw = reply_byte_hash(await settle(self._redis.hgetall(self.checkpoint_key)))
        if not raw:
            return None
        checkpoint = Checkpoint.from_mapping(raw)
        if checkpoint.coverage != self._coverage:
            return None
        self._checkpoint = checkpoint
        await self._reload_window()
        await self._reload_loss_events()
        return checkpoint

    async def _reload_window(self) -> None:
        withscores = True
        members = reply_scored_members(
            await self._redis.zrangebyscore(
                self.identities_key, "-inf", "+inf", withscores=withscores
            )
        )
        self._window = {member.decode(): int(score) for member, score in members}

    async def _reload_loss_events(self) -> None:
        stored = reply_ids(
            await settle(self._redis.lrange(self.loss_events_key, 0, LOSS_EVENT_RETENTION - 1))
        )
        self._loss_events = tuple(LossEvent.from_json(item) for item in stored)

    def prove(
        self,
        *,
        page_identities: Collection[str],
        oldest: int | None,
        boundary_time: int | None = None,
        ignored: Collection[str] = (),
    ) -> ProofResult:
        """Apply the reach and continuity rule against the durable or an explicit boundary.

        ``ignored`` names retained identities already attributed to an open gap; a provisional
        continuity check skips them so one provider rewrite is recorded as one gap.
        """
        target = boundary_time if boundary_time is not None else self._durable_time()
        if oldest is None:
            return ProofResult(reach=False, continuity=True, oldest=None)
        missing = self._missing_identities(page_identities, oldest, ignored)
        return ProofResult(
            reach=oldest < target,
            continuity=not missing,
            oldest=oldest,
            missing_identities=missing,
        )

    def _missing_identities(
        self, page_identities: Collection[str], oldest: int, ignored: Collection[str]
    ) -> tuple[str, ...]:
        """Retained identities newer than ``oldest`` that the page does not contain."""
        expected = [identity for identity, score in self._window.items() if score > oldest]
        return tuple(
            identity
            for identity in expected
            if identity not in page_identities and identity not in ignored
        )

    def _durable_time(self) -> int:
        if self._checkpoint is None:
            raise BoundarySchemaError("no boundary is loaded; anchor a first start before proving")
        return self._checkpoint.boundary_time

    async def record_identity(self, identity: str, timestamp: int) -> None:
        """Retain one delivered observation immediately, after its callback returned."""
        await self._redis.zadd(self.identities_key, {identity: float(timestamp)})
        self._window[identity] = timestamp

    async def advance(
        self,
        *,
        boundary_time: int,
        origin: BoundaryOrigin,
        last_request_end: int,
        identities: Mapping[str, int],
        loss_events: Sequence[LossEvent] = (),
    ) -> Checkpoint:
        """Move the durable boundary in one transaction; memory changes only after success."""
        emission_floor = (
            self._checkpoint.emission_floor
            if origin is BoundaryOrigin.PROVEN and self._checkpoint is not None
            else boundary_time
        )
        checkpoint = Checkpoint(
            boundary_time=boundary_time,
            emission_floor=emission_floor,
            boundary_origin=origin,
            written_at=int(self._clock()),
            coverage=self._coverage,
            last_request_end=last_request_end,
        )
        floor = boundary_time - self._horizon
        async with self._redis.pipeline(transaction=True) as pipe:
            await self._watch_and_validate(pipe, require_checkpoint=False)
            pipe.multi()
            pipe.hset(self.checkpoint_key, mapping=checkpoint.to_mapping())
            self._queue_identities(pipe, identities, floor)
            self._queue_loss_events(pipe, loss_events)
            await pipe.execute()
        self._checkpoint = checkpoint
        self._apply_identities(identities, floor)
        self._loss_events = (*reversed(loss_events), *self._loss_events)[:LOSS_EVENT_RETENTION]
        return checkpoint

    async def retain(
        self, *, identities: Mapping[str, int], trim_floor: int, last_request_end: int
    ) -> None:
        """Retain identities and record the request cursor without moving the boundary."""
        async with self._redis.pipeline(transaction=True) as pipe:
            await self._watch_and_validate(pipe, require_checkpoint=True)
            pipe.multi()
            pipe.hset(self.checkpoint_key, mapping={"last_request_end": last_request_end})
            self._queue_identities(pipe, identities, trim_floor)
            await pipe.execute()
        if self._checkpoint is not None:
            self._checkpoint = replace(self._checkpoint, last_request_end=last_request_end)
        self._apply_identities(identities, trim_floor)

    async def _watch_and_validate(
        self, pipe: BoundaryPipeline, *, require_checkpoint: bool
    ) -> None:
        """Validate watched key types before queuing an all-or-nothing write."""
        keys = (self.checkpoint_key, self.identities_key, self.loss_events_key)
        await pipe.watch(*keys)
        expected = ((b"hash", not require_checkpoint), (b"zset", True), (b"list", True))
        for key, (kind, allow_none) in zip(keys, expected, strict=True):
            self._validate_key_type(key, reply_bytes(await pipe.type(key)), kind, allow_none)

    @staticmethod
    def _validate_key_type(key: str, actual: bytes, expected: bytes, allow_none: bool) -> None:
        if actual == expected or (actual == b"none" and allow_none):
            return
        rendered = actual.decode(errors="replace")
        raise BoundarySchemaError(
            f"boundary key {key} has type {rendered}; expected {expected.decode()}"
        )

    def _queue_identities(
        self, pipe: BoundaryPipeline, identities: Mapping[str, int], floor: int
    ) -> None:
        if identities:
            pipe.zadd(self.identities_key, {key: float(score) for key, score in identities.items()})
        pipe.zremrangebyscore(self.identities_key, "-inf", f"({floor}")

    def _queue_loss_events(self, pipe: BoundaryPipeline, loss_events: Sequence[LossEvent]) -> None:
        if not loss_events:
            return
        pipe.lpush(self.loss_events_key, *[event.to_json() for event in loss_events])
        pipe.ltrim(self.loss_events_key, 0, LOSS_EVENT_RETENTION - 1)

    def _apply_identities(self, identities: Mapping[str, int], floor: int) -> None:
        self._window.update(identities)
        self._window = {key: score for key, score in self._window.items() if score >= floor}
