"""Checkpoint decoding and retention edges of the observation boundary."""

from __future__ import annotations

import json

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.ingestor.observation_boundary import (
    SCHEMA_VERSION,
    BoundaryOrigin,
    BoundarySchemaError,
    LossEvent,
    LossReason,
    ObservationBoundary,
)
from tests.fakes import FakeClock
from tests.fakes.redis_await import as_bytes, write_hash

URL = "https://trades.invalid/trades"
NOW = 1_788_983_720


def _boundary(redis: FakeAsyncRedis) -> ObservationBoundary:
    return ObservationBoundary(
        redis, trades_url=URL, coverage="all", horizon_seconds=600, clock=FakeClock(NOW)
    )


def _complete(**overrides: str) -> dict[str, str]:
    fields = {
        "schema_version": str(SCHEMA_VERSION),
        "boundary_time": str(NOW),
        "emission_floor": str(NOW),
        "boundary_origin": "proven",
        "written_at": str(NOW),
        "coverage": "all",
        "last_request_end": str(NOW),
    }
    return {**fields, **overrides}


async def test_a_complete_checkpoint_of_another_schema_version_is_terminal(
    fake_redis: FakeAsyncRedis,
) -> None:
    boundary = _boundary(fake_redis)
    await write_hash(
        fake_redis, boundary.checkpoint_key, _complete(schema_version=str(SCHEMA_VERSION + 1))
    )

    with pytest.raises(
        BoundarySchemaError, match=f"schema_version {SCHEMA_VERSION + 1} is unknown"
    ):
        await boundary.load()


async def test_a_complete_checkpoint_with_a_non_integer_field_is_terminal(
    fake_redis: FakeAsyncRedis,
) -> None:
    boundary = _boundary(fake_redis)
    await write_hash(fake_redis, boundary.checkpoint_key, _complete(boundary_time="soon"))

    with pytest.raises(BoundarySchemaError, match="field is malformed"):
        await boundary.load()


async def test_retaining_before_the_checkpoint_is_loaded_leaves_it_unloaded(
    fake_redis: FakeAsyncRedis,
) -> None:
    writer = _boundary(fake_redis)
    await writer.advance(
        boundary_time=1_000,
        origin=BoundaryOrigin.PROVEN,
        last_request_end=1_000,
        identities={"anchor": 1_000},
    )
    fresh = _boundary(fake_redis)

    await fresh.retain(identities={"late": 1_500}, trim_floor=0, last_request_end=1_500)

    assert fresh.checkpoint is None
    assert dict(fresh.window) == {"late": 1_500}
    assert await as_bytes(fake_redis.hget(fresh.checkpoint_key, "last_request_end")) == b"1500"


_LOSS_EVENT = {
    "from_time": 1,
    "to_time": 2,
    "reason": "horizon-expired",
    "detected_at": 3,
    "recorded_at": 4,
    "pages_examined": 5,
}


def test_a_well_formed_loss_event_round_trips() -> None:
    event = LossEvent.from_json(json.dumps(_LOSS_EVENT))

    assert event.reason is LossReason.HORIZON_EXPIRED
    assert LossEvent.from_json(event.to_json()) == event


@pytest.mark.parametrize(
    "payload",
    [
        json.dumps([1, 2]),
        json.dumps({**_LOSS_EVENT, "from_time": None}),
        json.dumps({**_LOSS_EVENT, "pages_examined": [5]}),
        json.dumps({**_LOSS_EVENT, "reason": 7}),
    ],
)
def test_a_loss_event_of_the_wrong_shape_is_rejected(payload: str) -> None:
    with pytest.raises(ValueError, match="loss event"):
        LossEvent.from_json(payload)


@pytest.mark.parametrize(
    "payload",
    [b"[1]", b'"text"', b"null"],
)
def test_loss_event_rejects_a_non_object_document(payload: bytes) -> None:
    with pytest.raises(ValueError, match="not a JSON object"):
        LossEvent.from_json(payload)
