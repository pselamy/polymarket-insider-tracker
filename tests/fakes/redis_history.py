"""Redis doubles that satisfy ``HistoryRedis`` and delegate every command to ``fakeredis``.

They exist only to observe or interleave with a pipeline; every command still runs on the real
``fakeredis`` server, so the Redis semantics under test are not re-implemented.
"""

from __future__ import annotations

from collections.abc import Awaitable, Mapping
from contextlib import AbstractAsyncContextManager
from types import TracebackType

from fakeredis import FakeAsyncRedis, FakeServer

from polymarket_insider_tracker.alerter._redis_port import HistoryPipeline, HistoryRedis

Command = tuple[str, tuple[object, ...]]


class DelegatingPipeline:
    """A ``HistoryPipeline`` over a fakeredis pipeline; subclasses hook single commands."""

    def __init__(self, opened: AbstractAsyncContextManager[HistoryPipeline]) -> None:
        self._opened = opened
        self._pipe: HistoryPipeline | None = None

    @property
    def _live(self) -> HistoryPipeline:
        if self._pipe is None:
            raise RuntimeError("pipeline used outside its async with block")
        return self._pipe

    def set(self, name: str, value: str, *, ex: int | None = None) -> object:
        return self._live.set(name, value, ex=ex)

    def zadd(self, name: str, mapping: Mapping[str, float]) -> object:
        return self._live.zadd(name, mapping)

    def expire(self, name: str, time: int) -> object:
        return self._live.expire(name, time)

    def delete(self, *names: str) -> object:
        return self._live.delete(*names)

    def multi(self) -> None:
        self._live.multi()

    async def watch(self, *names: str) -> object:
        return await self._live.watch(*names)

    async def unwatch(self) -> object:
        return await self._live.unwatch()

    async def get(self, name: str) -> object:
        return await self._live.get(name)

    async def execute(self) -> object:
        return await self._live.execute()

    async def __aenter__(self) -> HistoryPipeline:
        self._pipe = await self._opened.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return await self._opened.__aexit__(exc_type, exc, traceback)


class DelegatingRedis:
    """A ``HistoryRedis`` over a fakeredis client; subclasses override single commands."""

    def __init__(self, backend: HistoryRedis) -> None:
        self._backend = backend

    def pipeline(self, transaction: bool = True) -> DelegatingPipeline:
        return DelegatingPipeline(self._backend.pipeline(transaction=transaction))

    def exists(self, *names: str) -> Awaitable[object]:
        return self._backend.exists(*names)

    def get(self, name: str) -> Awaitable[object]:
        return self._backend.get(name)

    def set(
        self, name: str, value: str, *, ex: int | None = None, nx: bool = False
    ) -> Awaitable[object]:
        return self._backend.set(name, value, ex=ex, nx=nx)

    def ttl(self, name: str) -> Awaitable[object]:
        return self._backend.ttl(name)

    def zrangebyscore(
        self,
        name: str,
        min: float | str,
        max: float | str,
        start: int | None = None,
        num: int | None = None,
    ) -> Awaitable[object]:
        return self._backend.zrangebyscore(name, min, max, start=start, num=num)

    def zcount(self, name: str, min: float | str, max: float | str) -> Awaitable[object]:
        return self._backend.zcount(name, min, max)

    def zremrangebyscore(self, name: str, min: float | str, max: float | str) -> Awaitable[object]:
        return self._backend.zremrangebyscore(name, min, max)


class RecordingPipeline(DelegatingPipeline):
    """Records the CAS commands issued, in order."""

    def __init__(
        self, pipe: AbstractAsyncContextManager[HistoryPipeline], commands: list[Command]
    ) -> None:
        super().__init__(pipe)
        self._commands = commands

    async def watch(self, *names: str) -> object:
        self._commands.append(("watch", names))
        return await super().watch(*names)

    async def get(self, name: str) -> object:
        self._commands.append(("get", (name,)))
        return await super().get(name)

    async def unwatch(self) -> object:
        self._commands.append(("unwatch", ()))
        return await super().unwatch()

    def multi(self) -> None:
        self._commands.append(("multi", ()))
        super().multi()

    def delete(self, *names: str) -> object:
        self._commands.append(("delete", names))
        return super().delete(*names)

    def expire(self, name: str, time: int) -> object:
        self._commands.append(("expire", (name, time)))
        return super().expire(name, time)

    async def execute(self) -> object:
        self._commands.append(("execute", ()))
        return await super().execute()


class RecordingRedis(DelegatingRedis):
    """Records the commands of every pipeline it opens."""

    def __init__(self, backend: HistoryRedis) -> None:
        super().__init__(backend)
        self.commands: list[Command] = []

    def pipeline(self, transaction: bool = True) -> RecordingPipeline:
        return RecordingPipeline(self._backend.pipeline(transaction=transaction), self.commands)


class RacingPipeline(DelegatingPipeline):
    """After the CAS helper's WATCH+GET a second connection re-acquires the watched key.

    EXEC must then raise ``WatchError`` exactly as the real Redis contract describes.
    """

    def __init__(
        self,
        pipe: AbstractAsyncContextManager[HistoryPipeline],
        server: FakeServer,
        key: str,
        rival: str,
    ) -> None:
        super().__init__(pipe)
        self._server = server
        self._key = key
        self._rival = rival

    async def get(self, name: str) -> object:
        result = await super().get(name)
        rival = FakeAsyncRedis(server=self._server)
        try:
            await rival.set(self._key, self._rival, ex=60)
        finally:
            await rival.aclose()
        return result


class RacingRedis(DelegatingRedis):
    """Opens pipelines whose watched key is taken by a rival owner after the read."""

    def __init__(self, backend: HistoryRedis, server: FakeServer, key: str, rival: str) -> None:
        super().__init__(backend)
        self._server = server
        self._key = key
        self._rival = rival

    def pipeline(self, transaction: bool = True) -> RacingPipeline:
        pipe = self._backend.pipeline(transaction=transaction)
        return RacingPipeline(pipe, self._server, self._key, self._rival)
