"""Characterization tests for the compare-and-delete / compare-and-expire CAS.

Cluster-25 dedup card, US-001: these tests pin the current behaviour of BOTH
call sites in :mod:`polymarket_insider_tracker.alerter._channel_delivery` and
the exact Redis command sequence each one issues, before the duplicated
WATCH/MULTI compare-owner skeleton is given a single home:

- ``compare_and_delete`` (owned by ``release_channel_claim``) issues
  WATCH, GET, then MULTI+DELETE+EXEC when the stored token still matches,
- ``compare_and_expire`` (owned by ``extend_channel_claim``) issues
  WATCH, GET, then MULTI+EXPIRE+EXEC when the stored token still matches,
- a foreign or missing value aborts with UNWATCH, no transaction, and False,
- neither wrapper retries: a WatchError becomes False on the first occurrence,
- against fakeredis, a rival owner re-acquiring the key between WATCH and EXEC
  makes EXEC raise WatchError, which the wrappers translate to False while the
  rival's token stays in place.

The raw WATCH/GET/MULTI/EXEC and abort sequences are additionally pinned
against fakeredis and a real loopback Redis by tests/integration/
test_redis_contract.py (``test_compare_and_delete_*``/``test_compare_and_expire_*``).
"""

from __future__ import annotations

from typing import Any

import pytest
from fakeredis import FakeAsyncRedis, FakeServer
from redis.exceptions import WatchError

from polymarket_insider_tracker.alerter import _channel_delivery
from polymarket_insider_tracker.alerter.history import AlertHistory

CHANNEL = "discord"
WALLET = "0xW"
MARKET = "0xM"
TOKEN = "owner-token-hex"
RIVAL = "reacquired-by-other-owner"


class RecordingPipeline:
    """Transparent proxy over a fakeredis pipeline that records issued commands."""

    def __init__(self, pipe: Any, commands: list[tuple[str, tuple[Any, ...]]]) -> None:
        self._pipe = pipe
        self._commands = commands

    async def watch(self, key: str) -> None:
        self._commands.append(("watch", (key,)))
        await self._pipe.watch(key)

    async def get(self, key: str) -> Any:
        self._commands.append(("get", (key,)))
        return await self._pipe.get(key)

    async def unwatch(self) -> None:
        self._commands.append(("unwatch", ()))
        await self._pipe.unwatch()

    def multi(self) -> None:
        self._commands.append(("multi", ()))
        self._pipe.multi()

    def delete(self, key: str) -> None:
        self._commands.append(("delete", (key,)))
        self._pipe.delete(key)

    def expire(self, key: str, ttl: int) -> None:
        self._commands.append(("expire", (key, ttl)))
        self._pipe.expire(key, ttl)

    async def execute(self) -> Any:
        self._commands.append(("execute", ()))
        return await self._pipe.execute()

    async def __aenter__(self) -> RecordingPipeline:
        await self._pipe.__aenter__()
        return self

    async def __aexit__(self, *exc: Any) -> Any:
        return await self._pipe.__aexit__(*exc)


class RecordingRedis:
    """Fake redis delegating to fakeredis while recording pipeline commands."""

    def __init__(self, seed: FakeAsyncRedis) -> None:
        self._seed = seed
        self.commands: list[tuple[str, tuple[Any, ...]]] = []

    def pipeline(self, transaction: bool = True) -> RecordingPipeline:
        return RecordingPipeline(self._seed.pipeline(transaction=transaction), self.commands)


class RacingPipeline:
    """Proxy pipeline where a rival owner re-acquires the key after the READ.

    After the CAS helper's WATCH+GET, a second connection sharing the same
    server sets the watched key to the rival token, so EXEC must raise
    ``WatchError`` exactly as the real Redis contract describes.
    """

    def __init__(self, pipe: Any, server: FakeServer, key: str, rival: str) -> None:
        self._pipe = pipe
        self._server = server
        self._key = key
        self._rival = rival

    def __getattr__(self, name: str) -> Any:
        return getattr(self._pipe, name)

    async def get(self, key: str) -> Any:
        result = await self._pipe.get(key)
        rival = FakeAsyncRedis(server=self._server)
        try:
            await rival.set(self._key, self._rival, ex=60)
        finally:
            await rival.aclose()
        return result

    async def __aenter__(self) -> RacingPipeline:
        await self._pipe.__aenter__()
        return self

    async def __aexit__(self, *exc: Any) -> Any:
        return await self._pipe.__aexit__(*exc)


class RacingRedis:
    """Fake redis delegating to fakeredis with the rival-owner pipeline above."""

    def __init__(self, seed: FakeAsyncRedis, server: FakeServer, key: str, rival: str) -> None:
        self._seed = seed
        self._server = server
        self._key = key
        self._rival = rival

    def pipeline(self, transaction: bool = True) -> RacingPipeline:
        return RacingPipeline(
            self._seed.pipeline(transaction=transaction),
            self._server,
            self._key,
            self._rival,
        )


async def raise_watch_error(*args: Any, **kwargs: Any) -> bool:
    """Failure-injection stub CAS: always loses the watch race."""
    _ = args, kwargs
    raise WatchError("watch invalidated by a concurrent owner")


def _ambiguous_key() -> str:
    return AlertHistory.get_channel_ambiguous_key(CHANNEL, WALLET, MARKET)


class TestCompareAndDeleteSequence:
    """The release sequence: WATCH, GET, compare, then MULTI DELETE on match."""

    @pytest.mark.asyncio
    async def test_owned_value_issues_watch_get_multi_delete_execute(self) -> None:
        fake = FakeAsyncRedis()
        await fake.set(_ambiguous_key(), TOKEN, ex=60)
        redis = RecordingRedis(fake)
        history = AlertHistory(redis, dedup_window_hours=1)

        result = await _channel_delivery.compare_and_delete(history, _ambiguous_key(), TOKEN)

        assert result is True
        assert redis.commands == [
            ("watch", (_ambiguous_key(),)),
            ("get", (_ambiguous_key(),)),
            ("multi", ()),
            ("delete", (_ambiguous_key(),)),
            ("execute", ()),
        ]
        assert await fake.exists(_ambiguous_key()) == 0

    @pytest.mark.asyncio
    async def test_foreign_value_aborts_with_unwatch_and_no_transaction(self) -> None:
        fake = FakeAsyncRedis()
        await fake.set(_ambiguous_key(), TOKEN, ex=1200)
        redis = RecordingRedis(fake)
        history = AlertHistory(redis, dedup_window_hours=1)

        result = await _channel_delivery.compare_and_delete(history, _ambiguous_key(), "stale")

        assert result is False
        assert redis.commands == [
            ("watch", (_ambiguous_key(),)),
            ("get", (_ambiguous_key(),)),
            ("unwatch", ()),
        ]
        assert await fake.get(_ambiguous_key()) == TOKEN.encode()
        assert await fake.ttl(_ambiguous_key()) > 1000

    @pytest.mark.asyncio
    async def test_missing_key_aborts_with_unwatch_and_no_transaction(self) -> None:
        fake = FakeAsyncRedis()
        redis = RecordingRedis(fake)
        history = AlertHistory(redis, dedup_window_hours=1)

        result = await _channel_delivery.compare_and_delete(history, _ambiguous_key(), TOKEN)

        assert result is False
        assert redis.commands == [
            ("watch", (_ambiguous_key(),)),
            ("get", (_ambiguous_key(),)),
            ("unwatch", ()),
        ]
        assert await fake.dbsize() == 0


class TestCompareAndExpireSequence:
    """The renewal sequence: WATCH, GET, compare, then MULTI EXPIRE on match."""

    @pytest.mark.asyncio
    async def test_owned_value_issues_watch_get_multi_expire_execute(self) -> None:
        fake = FakeAsyncRedis()
        ttl = 120
        await fake.set(_ambiguous_key(), TOKEN, ex=2)
        redis = RecordingRedis(fake)
        history = AlertHistory(redis, dedup_window_hours=1)

        result = await _channel_delivery.compare_and_expire(history, _ambiguous_key(), TOKEN, ttl)

        assert result is True
        assert redis.commands == [
            ("watch", (_ambiguous_key(),)),
            ("get", (_ambiguous_key(),)),
            ("multi", ()),
            ("expire", (_ambiguous_key(), ttl)),
            ("execute", ()),
        ]
        assert await fake.get(_ambiguous_key()) == TOKEN.encode()
        assert await fake.ttl(_ambiguous_key()) >= ttl

    @pytest.mark.asyncio
    async def test_foreign_value_aborts_with_unwatch_and_no_transaction(self) -> None:
        fake = FakeAsyncRedis()
        await fake.set(_ambiguous_key(), TOKEN, ex=1200)
        redis = RecordingRedis(fake)
        history = AlertHistory(redis, dedup_window_hours=1)

        result = await _channel_delivery.compare_and_expire(history, _ambiguous_key(), "stale", 30)

        assert result is False
        assert redis.commands == [
            ("watch", (_ambiguous_key(),)),
            ("get", (_ambiguous_key(),)),
            ("unwatch", ()),
        ]
        assert await fake.get(_ambiguous_key()) == TOKEN.encode()
        assert await fake.ttl(_ambiguous_key()) > 1000

    @pytest.mark.asyncio
    async def test_missing_key_aborts_with_unwatch_and_no_transaction(self) -> None:
        fake = FakeAsyncRedis()
        redis = RecordingRedis(fake)
        history = AlertHistory(redis, dedup_window_hours=1)

        result = await _channel_delivery.compare_and_expire(history, _ambiguous_key(), TOKEN, 60)

        assert result is False
        assert redis.commands == [
            ("watch", (_ambiguous_key(),)),
            ("get", (_ambiguous_key(),)),
            ("unwatch", ()),
        ]
        assert await fake.dbsize() == 0


class TestWatchErrorIsNotRetried:
    """Both call sites translate a WatchError into False on the first occurrence."""

    @pytest.mark.asyncio
    async def test_release_channel_claim_returns_false_without_retry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        history = AlertHistory(FakeAsyncRedis(), dedup_window_hours=1)
        monkeypatch.setattr(_channel_delivery, "compare_and_delete", raise_watch_error)

        assert (
            await _channel_delivery.release_channel_claim(history, CHANNEL, WALLET, MARKET, TOKEN)
            is False
        )

    @pytest.mark.asyncio
    async def test_extend_channel_claim_returns_false_without_retry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        history = AlertHistory(FakeAsyncRedis(), dedup_window_hours=1)
        monkeypatch.setattr(_channel_delivery, "compare_and_expire", raise_watch_error)

        assert (
            await _channel_delivery.extend_channel_claim(
                history, CHANNEL, WALLET, MARKET, TOKEN, 60
            )
            is False
        )


class TestRivalOwnerBetweenWatchAndExec:
    """A fakeredis rival connection turns the race into a real WatchError."""

    @pytest.mark.asyncio
    async def test_release_loss_returns_false_and_keeps_the_rival_token(self) -> None:
        server = FakeServer()
        fake = FakeAsyncRedis(server=server)
        key = _ambiguous_key()
        await fake.set(key, TOKEN, ex=60)
        history = AlertHistory(RacingRedis(fake, server, key, RIVAL), dedup_window_hours=1)

        assert await history.release_channel_claim(CHANNEL, WALLET, MARKET, TOKEN) is False

        assert await fake.get(key) == RIVAL.encode()

    @pytest.mark.asyncio
    async def test_extend_loss_returns_false_and_keeps_the_rival_lease(self) -> None:
        server = FakeServer()
        fake = FakeAsyncRedis(server=server)
        key = _ambiguous_key()
        await fake.set(key, TOKEN, ex=60)
        history = AlertHistory(RacingRedis(fake, server, key, RIVAL), dedup_window_hours=1)

        assert await history.extend_channel_claim(CHANNEL, WALLET, MARKET, TOKEN, 600) is False

        assert await fake.get(key) == RIVAL.encode()
        assert 0 < await fake.ttl(key) <= 60
