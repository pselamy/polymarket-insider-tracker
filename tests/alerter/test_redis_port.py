"""Reply narrowing for the Redis surface AlertHistory depends on."""

from __future__ import annotations

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.alerter._redis_port import (
    HistoryRedis,
    reply_byte_hash,
    reply_bytes,
    reply_ids,
    reply_int,
    reply_scan,
    reply_scored_members,
    reply_text,
    scan_page,
    settle,
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


def test_reply_bytes_accepts_bytes_only() -> None:
    assert reply_bytes(b"a") == b"a"
    with pytest.raises(TypeError, match="bytes Redis reply, got str"):
        reply_bytes("a")


def test_reply_byte_hash_requires_bytes_fields_and_values() -> None:
    assert reply_byte_hash({b"k": b"v"}) == {b"k": b"v"}
    with pytest.raises(TypeError, match="hash Redis reply, got list"):
        reply_byte_hash([b"k", b"v"])
    with pytest.raises(TypeError, match="bytes Redis reply, got int"):
        reply_byte_hash({b"k": 1})
    with pytest.raises(TypeError, match="bytes Redis reply, got str"):
        reply_byte_hash({"k": b"v"})


def test_reply_scored_members_accepts_pairs_with_numeric_scores() -> None:
    assert reply_scored_members([(b"a", 1.0), [b"b", 2]]) == [(b"a", 1.0), (b"b", 2.0)]
    with pytest.raises(TypeError, match="list Redis reply, got bytes"):
        reply_scored_members(b"a")
    with pytest.raises(TypeError, match=r"\(member, score\) Redis pair"):
        reply_scored_members([b"a"])
    with pytest.raises(TypeError, match=r"\(member, score\) Redis pair"):
        reply_scored_members([(b"a", 1.0, 2.0)])
    with pytest.raises(TypeError, match="numeric Redis score, got bool"):
        reply_scored_members([(b"a", True)])
    with pytest.raises(TypeError, match="numeric Redis score, got str"):
        reply_scored_members([(b"a", "1")])
    with pytest.raises(TypeError, match="bytes Redis reply, got str"):
        reply_scored_members([("a", 1.0)])


def test_reply_scan_accepts_cursor_and_keys_only() -> None:
    assert reply_scan((0, [b"a", "b"])) == (0, [b"a", "b"])
    with pytest.raises(TypeError, match="cursor, keys"):
        reply_scan(b"0")
    with pytest.raises(TypeError, match="cursor, keys"):
        reply_scan((0,))
    with pytest.raises(TypeError, match="integer Redis reply, got str"):
        reply_scan(("0", [b"a"]))
    with pytest.raises(TypeError, match="list Redis reply, got str"):
        reply_scan((0, "a"))


async def _awaitable_hash() -> dict[object, object]:
    return {b"f": b"v"}


async def test_scan_page_and_settle_read_a_live_client() -> None:
    redis = FakeAsyncRedis()
    await redis.set("p:a", "1")
    assert await scan_page(redis, 0, "p:*", 10) == (0, [b"p:a"])
    assert await settle(_awaitable_hash()) == {b"f": b"v"}
    assert await settle({b"f": b"v"}) == {b"f": b"v"}
    assert await settle([b"a"]) == [b"a"]
