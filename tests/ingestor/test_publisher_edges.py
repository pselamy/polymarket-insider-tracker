"""Publisher behaviour for decoded-response clients and unexpected Redis failures."""

from collections.abc import AsyncIterator

import pytest
from fakeredis import FakeAsyncRedis
from redis.exceptions import ResponseError

from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.ingestor.publisher import DEFAULT_STREAM_NAME, EventPublisher


@pytest.fixture
async def decoded_redis() -> AsyncIterator[FakeAsyncRedis]:
    """A client configured to return ``str`` instead of ``bytes`` entry IDs."""
    client = FakeAsyncRedis(decode_responses=True)
    yield client
    await client.aclose()


@pytest.mark.asyncio
async def test_publish_returns_the_entry_id_from_a_decoding_client(
    decoded_redis: FakeAsyncRedis, sample_trade_event: TradeEvent
) -> None:
    entry_id = await EventPublisher(decoded_redis).publish(sample_trade_event)

    stored = await decoded_redis.xrange(DEFAULT_STREAM_NAME)
    assert isinstance(stored[0][0], str)
    assert entry_id == stored[0][0]


@pytest.mark.asyncio
async def test_create_consumer_group_propagates_errors_other_than_an_existing_group(
    fake_redis: FakeAsyncRedis,
) -> None:
    await fake_redis.set(DEFAULT_STREAM_NAME, "not-a-stream")

    with pytest.raises(ResponseError) as exc_info:
        await EventPublisher(fake_redis).create_consumer_group("g")

    assert "BUSYGROUP" not in str(exc_info.value)
