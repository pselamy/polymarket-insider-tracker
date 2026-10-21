"""Reply narrowing for the Redis surface AlertHistory depends on."""

from __future__ import annotations

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.alerter._redis_port import (
    HistoryRedis,
    reply_ids,
    reply_int,
    reply_text,
)
from polymarket_insider_tracker.alerter.history import AlertHistory


def test_reply_text_accepts_str_and_bytes_only() -> None:
    assert reply_text("a") == "a"
    assert reply_text(b"a") == b"a"
    with pytest.raises(TypeError, match="text Redis reply, got int"):
        reply_text(1)


def test_reply_int_accepts_integers_but_not_bool_or_text() -> None:
    assert reply_int(3) == 3
    assert reply_int(-1) == -1
    with pytest.raises(TypeError, match="integer Redis reply, got bool"):
        reply_int(True)
    with pytest.raises(TypeError, match="integer Redis reply, got str"):
        reply_int("3")


def test_reply_ids_accepts_lists_of_text_only() -> None:
    assert reply_ids([b"a", "b"]) == [b"a", "b"]
    assert reply_ids(()) == []
    with pytest.raises(TypeError, match="list Redis reply, got str"):
        reply_ids("ab")
    with pytest.raises(TypeError, match="text Redis reply, got int"):
        reply_ids([b"a", 1])


def test_fakeredis_satisfies_the_history_surface() -> None:
    redis: HistoryRedis = FakeAsyncRedis()
    assert AlertHistory(redis).redis is redis


@pytest.mark.asyncio
async def test_history_rejects_a_non_integer_count_reply() -> None:
    class WrongCount(FakeAsyncRedis):
        async def zcount(self, *_args: object, **_kwargs: object) -> str:
            return "many"

    with pytest.raises(TypeError, match="integer Redis reply"):
        await AlertHistory(WrongCount()).get_recent_count()


@pytest.mark.asyncio
async def test_history_rejects_a_non_text_alert_record() -> None:
    class WrongRecord(FakeAsyncRedis):
        async def get(self, *_args: object, **_kwargs: object) -> int:
            return 7

    with pytest.raises(TypeError, match="text Redis reply"):
        await AlertHistory(WrongRecord()).get_alert("a1")
