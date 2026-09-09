"""Lightweight, in-memory fake Redis client with working semantics."""

from __future__ import annotations

import fnmatch
import math
import time
from collections.abc import Callable, Coroutine, Mapping
from typing import Any

from redis.exceptions import ResponseError


def _to_bytes(val: Any) -> bytes:
    """Convert string or scalar to bytes."""
    if isinstance(val, bytes):
        return val
    if isinstance(val, str):
        return val.encode("utf-8")
    return str(val).encode("utf-8")


def _to_str(val: Any) -> str:
    """Convert bytes or scalar to string."""
    if isinstance(val, bytes):
        return val.decode("utf-8")
    return str(val)


def _parse_score_bound(val: float | str, default_val: float) -> float:
    """Parse score boundary including inf."""
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if s in ("-inf", "-infinity"):
        return float("-inf")
    if s in ("+inf", "+infinity", "inf", "infinity"):
        return float("inf")
    try:
        return float(s)
    except ValueError:
        return default_val


def _filter_matching_keys(keys: list[bytes], match: str | None) -> list[bytes]:
    """Filter keys by fnmatch pattern if given."""
    if not match:
        return keys
    return [k for k in keys if fnmatch.fnmatch(_to_str(k), match)]


def _apply_mapping(target: dict[bytes, bytes], mapping: Mapping[str | bytes, Any]) -> int:
    """Apply mapping pairs to hash target dict."""
    added = 0
    for k, v in mapping.items():
        kb = _to_bytes(k)
        if kb not in target:
            added += 1
        target[kb] = _to_bytes(v)
    return added


def _apply_single(target: dict[bytes, bytes], key: str | bytes, value: Any) -> int:
    """Apply single key-value to hash target dict."""
    kb = _to_bytes(key)
    added = 1 if kb not in target else 0
    target[kb] = _to_bytes(value)
    return added


def _filter_zset_items(
    z: dict[bytes, float], min_s: float, max_s: float
) -> list[tuple[bytes, float]]:
    """Filter sorted set items within inclusive score bounds."""
    items = [(m, s) for m, s in z.items() if min_s <= s <= max_s]
    items.sort(key=lambda x: x[1])
    return items


def _slice_items(items: list[Any], start: int | None, num: int | None) -> list[Any]:
    """Slice items by offset and count if specified."""
    if start is not None and num is not None:
        return items[start : start + num]
    return items


def _read_new_entries(
    st: list[tuple[bytes, dict[bytes, bytes]]],
    grp: dict[str, Any],
    consumer_b: bytes,
    count: int | None,
) -> list[tuple[bytes, dict[bytes, bytes]]]:
    """Read unread entries from stream for group."""
    last_idx: int = grp.get("last_idx", 0)
    available = st[last_idx:]
    if count is not None:
        available = available[:count]
    grp["last_idx"] = last_idx + len(available)
    pending: dict[bytes, bytes] = grp.setdefault("pending", {})
    for eid, _ in available:
        pending[eid] = consumer_b
    return available


def _read_pending_entries(
    st: list[tuple[bytes, dict[bytes, bytes]]],
    grp: dict[str, Any],
    consumer_b: bytes,
    count: int | None,
) -> list[tuple[bytes, dict[bytes, bytes]]]:
    """Read pending unacknowledged entries for consumer."""
    pending = grp.get("pending", {})
    entries = [item for item in st if item[0] in pending and pending[item[0]] == consumer_b]
    if count is not None:
        return entries[:count]
    return entries


def _read_stream_for_group(
    st: list[tuple[bytes, dict[bytes, bytes]]],
    grp: dict[str, Any],
    consumer_b: bytes,
    id_spec: str,
    count: int | None,
) -> list[tuple[bytes, dict[bytes, bytes]]]:
    """Dispatch stream read based on ID specification."""
    if id_spec == ">":
        return _read_new_entries(st, grp, consumer_b, count)
    return _read_pending_entries(st, grp, consumer_b, count)


class FakePipeline:
    """In-memory Redis pipeline supporting async context and command queueing."""

    def __init__(self, redis: FakeRedis) -> None:
        self._redis = redis
        self._commands: list[Callable[[], Coroutine[Any, Any, Any]]] = []

    async def __aenter__(self) -> FakePipeline:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass

    def xadd(
        self,
        name: str | bytes,
        fields: Mapping[Any, Any],
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> FakePipeline:
        self._commands.append(
            lambda: self._redis.xadd(name, fields, id=id, maxlen=maxlen, approximate=approximate)
        )
        return self

    def set(
        self,
        key: str | bytes,
        value: Any,
        ex: int | float | None = None,
        px: int | float | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> FakePipeline:
        self._commands.append(lambda: self._redis.set(key, value, ex=ex, px=px, nx=nx, xx=xx))
        return self

    def setex(self, key: str | bytes, time_sec: int | float, value: Any) -> FakePipeline:
        self._commands.append(lambda: self._redis.setex(key, time_sec, value))
        return self

    def get(self, key: str | bytes) -> FakePipeline:
        self._commands.append(lambda: self._redis.get(key))
        return self

    def delete(self, *keys: str | bytes) -> FakePipeline:
        self._commands.append(lambda: self._redis.delete(*keys))
        return self

    def exists(self, *keys: str | bytes) -> FakePipeline:
        self._commands.append(lambda: self._redis.exists(*keys))
        return self

    def expire(self, key: str | bytes, time_sec: int | float) -> FakePipeline:
        self._commands.append(lambda: self._redis.expire(key, time_sec))
        return self

    def ttl(self, key: str | bytes) -> FakePipeline:
        self._commands.append(lambda: self._redis.ttl(key))
        return self

    def zadd(self, name: str | bytes, mapping: Mapping[Any, float]) -> FakePipeline:
        self._commands.append(lambda: self._redis.zadd(name, mapping))
        return self

    def zrangebyscore(
        self,
        name: str | bytes,
        min_score: float | str,
        max_score: float | str,
        withscores: bool = False,
    ) -> FakePipeline:
        self._commands.append(
            lambda: self._redis.zrangebyscore(name, min_score, max_score, withscores=withscores)
        )
        return self

    def zremrangebyscore(
        self, name: str | bytes, min_score: float | str, max_score: float | str
    ) -> FakePipeline:
        self._commands.append(lambda: self._redis.zremrangebyscore(name, min_score, max_score))
        return self

    async def execute(self) -> list[Any]:
        results: list[Any] = []
        for cmd in self._commands:
            results.append(await cmd())
        self._commands.clear()
        return results


class FakeRedis:
    """Lightweight in-memory working Redis fake for pipeline, alerter, and detectors."""

    def __init__(self) -> None:
        self._data: dict[bytes, bytes] = {}
        self._hashes: dict[bytes, dict[bytes, bytes]] = {}
        self._sets: dict[bytes, set[bytes]] = {}
        self._zsets: dict[bytes, dict[bytes, float]] = {}
        self._streams: dict[bytes, list[tuple[bytes, dict[bytes, bytes]]]] = {}
        self._stream_groups: dict[bytes, dict[bytes, dict[str, Any]]] = {}
        self._expirations: dict[bytes, float] = {}
        self._seq: int = 0

    def _is_expired(self, key: bytes) -> bool:
        exp = self._expirations.get(key)
        if exp is None:
            return False
        if time.time() >= exp:
            self._delete_key(key)
            return True
        return False

    def _delete_key(self, key: bytes) -> bool:
        removed = False
        removed = self._data.pop(key, None) is not None or removed
        removed = self._hashes.pop(key, None) is not None or removed
        removed = self._sets.pop(key, None) is not None or removed
        removed = self._zsets.pop(key, None) is not None or removed
        removed = self._streams.pop(key, None) is not None or removed
        self._expirations.pop(key, None)
        return removed

    def _key_exists(self, key: bytes) -> bool:
        if self._is_expired(key):
            return False
        return (
            key in self._data
            or key in self._hashes
            or key in self._sets
            or key in self._zsets
            or key in self._streams
        )

    def _unexpired_data_keys(self) -> list[bytes]:
        return [k for k in self._data if not self._is_expired(k)]

    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        pass

    async def close(self) -> None:
        pass

    def pipeline(self, transaction: bool = True) -> FakePipeline:
        _ = transaction
        return FakePipeline(self)

    async def get(self, key: str | bytes) -> bytes | None:
        k = _to_bytes(key)
        if self._is_expired(k):
            return None
        return self._data.get(k)

    def _set_expiration(self, k: bytes, ex: int | float | None, px: int | float | None) -> None:
        if ex is not None:
            self._expirations[k] = time.time() + float(ex)
        elif px is not None:
            self._expirations[k] = time.time() + float(px) / 1000.0

    async def set(
        self,
        key: str | bytes,
        value: Any,
        ex: int | float | None = None,
        px: int | float | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool | None:
        k = _to_bytes(key)
        exists = self._key_exists(k)
        if nx and exists:
            return None
        if xx and not exists:
            return None
        self._data[k] = _to_bytes(value)
        self._set_expiration(k, ex, px)
        return True

    async def setex(self, key: str | bytes, time_sec: int | float, value: Any) -> bool:
        res = await self.set(key, value, ex=time_sec)
        return bool(res)

    async def delete(self, *keys: str | bytes) -> int:
        count = sum(1 for k in keys if self._delete_key(_to_bytes(k)))
        return count

    async def exists(self, *keys: str | bytes) -> int:
        return sum(1 for k in keys if self._key_exists(_to_bytes(k)))

    async def expire(self, key: str | bytes, time_sec: int | float) -> bool:
        k = _to_bytes(key)
        if not self._key_exists(k):
            return False
        self._expirations[k] = time.time() + float(time_sec)
        return True

    async def ttl(self, key: str | bytes) -> int:
        k = _to_bytes(key)
        if not self._key_exists(k):
            return -2
        exp = self._expirations.get(k)
        if exp is None:
            return -1
        rem = int(math.ceil(exp - time.time()))
        return max(0, rem)

    async def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, list[bytes]]:
        _ = (cursor, count)
        keys = self._unexpired_data_keys()
        return 0, _filter_matching_keys(keys, match)

    async def hget(self, name: str | bytes, key: str | bytes) -> bytes | None:
        n, k = _to_bytes(name), _to_bytes(key)
        if self._is_expired(n):
            return None
        return self._hashes.get(n, {}).get(k)

    async def hset(
        self,
        name: str | bytes,
        key: str | bytes | None = None,
        value: Any = None,
        mapping: Mapping[str | bytes, Any] | None = None,
    ) -> int:
        n = _to_bytes(name)
        h = self._hashes.setdefault(n, {})
        added = 0
        if mapping:
            added += _apply_mapping(h, mapping)
        if key is not None:
            added += _apply_single(h, key, value)
        return added

    async def hgetall(self, name: str | bytes) -> dict[bytes, bytes]:
        n = _to_bytes(name)
        if self._is_expired(n):
            return {}
        return dict(self._hashes.get(n, {}))

    async def hdel(self, name: str | bytes, *keys: str | bytes) -> int:
        n = _to_bytes(name)
        h = self._hashes.get(n, {})
        count = sum(1 for k in keys if h.pop(_to_bytes(k), None) is not None)
        return count

    async def sadd(self, name: str | bytes, *values: Any) -> int:
        n = _to_bytes(name)
        s = self._sets.setdefault(n, set())
        added = 0
        for val in values:
            vb = _to_bytes(val)
            if vb not in s:
                added += 1
                s.add(vb)
        return added

    async def srem(self, name: str | bytes, *values: Any) -> int:
        n = _to_bytes(name)
        s = self._sets.get(n, set())
        removed = 0
        for val in values:
            vb = _to_bytes(val)
            if vb in s:
                removed += 1
                s.remove(vb)
        return removed

    async def smembers(self, name: str | bytes) -> set[bytes]:
        n = _to_bytes(name)
        if self._is_expired(n):
            return set()
        return set(self._sets.get(n, set()))

    async def sismember(self, name: str | bytes, value: Any) -> bool:
        n, vb = _to_bytes(name), _to_bytes(value)
        if self._is_expired(n):
            return False
        return vb in self._sets.get(n, set())

    async def zadd(self, name: str | bytes, mapping: Mapping[Any, float]) -> int:
        n = _to_bytes(name)
        z = self._zsets.setdefault(n, {})
        added = sum(1 for member in mapping if _to_bytes(member) not in z)
        for member, score in mapping.items():
            z[_to_bytes(member)] = float(score)
        return added

    async def zcount(self, name: str | bytes, min_val: float | str, max_val: float | str) -> int:
        n = _to_bytes(name)
        if self._is_expired(n):
            return 0
        min_s = _parse_score_bound(min_val, float("-inf"))
        max_s = _parse_score_bound(max_val, float("inf"))
        z = self._zsets.get(n, {})
        return sum(1 for score in z.values() if min_s <= score <= max_s)

    async def zrangebyscore(
        self,
        name: str | bytes,
        min_val: float | str,
        max_val: float | str,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[Any]:
        n = _to_bytes(name)
        if self._is_expired(n):
            return []
        min_s = _parse_score_bound(min_val, float("-inf"))
        max_s = _parse_score_bound(max_val, float("inf"))
        items = _filter_zset_items(self._zsets.get(n, {}), min_s, max_s)
        sliced = _slice_items(items, start, num)
        if withscores:
            return sliced
        return [m for m, _ in sliced]

    async def zremrangebyscore(
        self,
        name: str | bytes,
        min_val: float | str,
        max_val: float | str,
    ) -> int:
        n = _to_bytes(name)
        if self._is_expired(n):
            return 0
        min_s = _parse_score_bound(min_val, float("-inf"))
        max_s = _parse_score_bound(max_val, float("inf"))
        z = self._zsets.get(n, {})
        to_del = [m for m, s in z.items() if min_s <= s <= max_s]
        for m in to_del:
            del z[m]
        return len(to_del)

    async def xadd(
        self,
        name: str | bytes,
        fields: Mapping[Any, Any],
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        _ = approximate
        n = _to_bytes(name)
        st = self._streams.setdefault(n, [])
        self._seq += 1
        entry_id = f"{int(time.time() * 1000)}-{self._seq}" if id == "*" else str(id)
        entry_id_b = entry_id.encode("utf-8")
        fields_b = {_to_bytes(k): _to_bytes(v) for k, v in fields.items()}
        st.append((entry_id_b, fields_b))
        if maxlen is not None and len(st) > maxlen:
            trim_count = len(st) - maxlen
            self._streams[n] = st[trim_count:]
        return entry_id

    async def xgroup_create(
        self,
        name: str | bytes,
        groupname: str | bytes,
        id: str = "$",
        mkstream: bool = False,
    ) -> bool:
        n, g = _to_bytes(name), _to_bytes(groupname)
        if mkstream and n not in self._streams:
            self._streams[n] = []
        groups = self._stream_groups.setdefault(n, {})
        if g in groups:
            raise ResponseError("BUSYGROUP Consumer Group name already exists")
        st = self._streams.get(n, [])
        start_idx = 0 if id in ("0", "0-0") else len(st)
        groups[g] = {"last_idx": start_idx, "pending": {}}
        return True

    async def xreadgroup(
        self,
        groupname: str | bytes,
        consumername: str | bytes,
        streams: Mapping[str | bytes, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> list[list[Any]]:
        _ = (block, noack)
        g, c = _to_bytes(groupname), _to_bytes(consumername)
        results: list[list[Any]] = []
        for s_name, id_spec in streams.items():
            s_b = _to_bytes(s_name)
            st = self._streams.get(s_b, [])
            grp = self._stream_groups.get(s_b, {}).get(g, {"last_idx": 0, "pending": {}})
            entries = _read_stream_for_group(st, grp, c, id_spec, count)
            if entries:
                results.append([s_b, entries])
        return results

    async def xack(self, name: str | bytes, groupname: str | bytes, *entry_ids: str | bytes) -> int:
        n, g = _to_bytes(name), _to_bytes(groupname)
        grp = self._stream_groups.get(n, {}).get(g)
        if not grp:
            return 0
        pending: dict[bytes, bytes] = grp.get("pending", {})
        acked = sum(1 for eid in entry_ids if pending.pop(_to_bytes(eid), None) is not None)
        return acked

    async def xlen(self, name: str | bytes) -> int:
        n = _to_bytes(name)
        return len(self._streams.get(n, []))

    async def xtrim(self, name: str | bytes, maxlen: int, approximate: bool = True) -> int:
        _ = approximate
        n = _to_bytes(name)
        st = self._streams.get(n, [])
        if len(st) <= maxlen:
            return 0
        diff = len(st) - maxlen
        self._streams[n] = st[diff:]
        return diff

    async def xinfo_stream(self, name: str | bytes) -> dict[str, Any]:
        n = _to_bytes(name)
        if n not in self._streams:
            raise ResponseError("no such key")
        return {"length": len(self._streams[n])}
