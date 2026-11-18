"""Asynchronous framework-neutral cache store across memory, SHM, Valkey, Redis, and object storage."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Self

from vertebra.cache._sync import (
    _OBJECT_SCHEMES,
    _REDIS_SCHEMES,
    _SHM_MAX_KEY_BYTES,
    _SHM_MAX_VALUE_BYTES,
    _VALKEY_SCHEMES,
    SyncCacheStore,
    _decode_envelope,
    _encode_envelope,
    _encode_shm_key,
    _escape_redis_glob,
    _is_redis_int_error,
    _MemoryState,
    _object_store_item_hex,
    _object_store_key,
    _object_store_ns_dir,
    _parse_int_bytes,
    _parse_url_scheme,
    _resolve_px_ms,
    _shm_namespace_prefix,
    _shm_segment_name,
    _validate_key_prefix,
)
from vertebra.cache._types import CacheStats
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from vertebra._vertebra import ShmCache
    from vertebra.db.adapters.redis._async import AsyncRedisDriver
    from vertebra.db.adapters.valkey._async import AsyncValkeyDriver
    from vertebra.storage._store import AsyncObjectStore

__all__ = ("AsyncCacheStore",)


class AsyncCacheStore:
    """Framework-neutral asynchronous cache store supporting memory, SHM, Valkey, Redis, and object storage."""

    __slots__ = (
        "_capacity",
        "_clock",
        "_default_ttl",
        "_driver",
        "_expirations",
        "_hits",
        "_key_prefix",
        "_l1",
        "_l1_url",
        "_misses",
        "_ns_dir",
        "_raw_clock",
        "_scheme",
        "_shm",
        "_shm_name",
        "_shm_ns_prefix",
        "_state",
        "_store",
        "_url",
    )

    def __init__(
        self,
        url: str = "memory://",
        *,
        l1_url: str | None = None,
        capacity: int = 2048,
        default_ttl: float | None = None,
        key_prefix: str = "vtb:cache:",
        _clock: Callable[[], float] | None = None,
    ) -> None:
        """Initialize an asynchronous cache store for the configured backend URL and optional L1 tier.

        Args:
            url: Backend URL (``memory://``, ``shm://``, ``valkey(s)://``, ``redis(s)://``,
                ``s3://``, ``gs://``, ``az://``, ``r2://``, ``oci://``, or ``file://``).
            l1_url: Optional L1 cache URL paired with an L2 backend (including ``memory://``).
            capacity: Maximum entry capacity (must be at least 1).
            default_ttl: Default TTL in seconds applied when per-call ``ttl`` is omitted.
            key_prefix: Non-empty namespace prefix without glob wildcards.
            _clock: Optional injectable clock callable returning seconds as ``float``.

        Raises:
            ValueError: When ``url``, ``l1_url``, ``capacity``, or ``key_prefix`` is invalid.
            MissingDependencyError: When required optional native modules are unavailable.
        """
        self._initialize(
            url=url,
            l1_url=l1_url,
            capacity=capacity,
            default_ttl=default_ttl,
            key_prefix=key_prefix,
            _clock=_clock,
            shared_state=None,
            l1_store=None,
        )

    @classmethod
    def from_sync(cls, sync_store: SyncCacheStore) -> Self:
        """Create an ``AsyncCacheStore`` sharing in-process or SHM state with ``sync_store``.

        Memory stores share the underlying locked ``OrderedDict`` and counters directly.
        SHM stores attach a new handle to the same POSIX shared-memory segment.
        Remote and object-storage backends construct independent async drivers/stores.
        """
        instance = cls.__new__(cls)
        l1_store = cls.from_sync(sync_store._l1) if sync_store._l1 is not None else None
        instance._initialize(
            url=sync_store._url,
            l1_url=sync_store._l1_url,
            capacity=sync_store._capacity,
            default_ttl=sync_store._default_ttl,
            key_prefix=sync_store._key_prefix,
            _clock=sync_store._raw_clock,
            shared_state=sync_store._state,
            l1_store=l1_store,
        )
        return instance

    async def get(self, key: str) -> bytes | None:
        """Retrieve cached bytes for ``key`` or return ``None`` on miss or expiration."""
        if self._l1 is not None:
            l1_val = await self._l1.get(key)
            if l1_val is not None:
                self._record_hit()
                return l1_val

        hit = await self._get_l2_with_ttl(key)
        if hit is None:
            if self._scheme != "memory":
                self._misses += 1
            return None

        value, remaining_ttl = hit
        if self._scheme != "memory":
            self._hits += 1
        if self._l1 is not None and (self._l1._scheme != "shm" or len(value) <= _SHM_MAX_VALUE_BYTES):
            await self._l1.set(key, value, ttl=remaining_ttl)
        return value

    async def set(self, key: str, value: bytes, *, ttl: float | None = None) -> None:
        """Store ``value`` under ``key`` with an optional TTL in seconds."""
        effective_ttl = self._effective_ttl(ttl)
        if effective_ttl is not None and effective_ttl <= 0:
            await self.delete(key)
            return

        if self._l1 is not None:
            if self._l1._scheme == "shm" and len(value) > _SHM_MAX_VALUE_BYTES:
                await self._l1.delete(key)
            else:
                await self._l1.set(key, value, ttl=ttl)

        if self._state is not None:
            self._state.set(key, value, effective_ttl)
            return
        if self._shm is not None:
            shm_key = _encode_shm_key(self._key_prefix, key)
            if len(value) > _SHM_MAX_VALUE_BYTES:
                self._shm.delete(shm_key)
            else:
                self._shm.set(shm_key, value, ttl=effective_ttl)
            return
        if self._driver is not None:
            dkey = self._driver_key(key)
            if effective_ttl is not None:
                await self._driver.set(dkey, value, px=_resolve_px_ms(effective_ttl))
            else:
                await self._driver.set(dkey, value)
            return
        if self._store is not None:
            await self._store_put(key, value, effective_ttl)

    async def add(self, key: str, value: bytes, *, ttl: float | None = None) -> bool:
        """Store ``value`` only if ``key`` is absent or expired, returning True when inserted."""
        effective_ttl = self._effective_ttl(ttl)
        added = await self._add_l2(key, value, effective_ttl)
        if added and self._l1 is not None:
            if (
                effective_ttl is not None
                and effective_ttl <= 0
                or self._l1._scheme == "shm"
                and len(value) > _SHM_MAX_VALUE_BYTES
            ):
                await self._l1.delete(key)
            else:
                await self._l1.set(key, value, ttl=ttl)
        return added

    async def delete(self, key: str) -> bool:
        """Delete ``key`` from L1 and L2, returning True if a live entry was removed."""
        removed_l1 = await self._l1.delete(key) if self._l1 is not None else False
        removed_l2 = await self._delete_l2(key)
        return removed_l2 or removed_l1

    async def has(self, key: str) -> bool:
        """Return True if ``key`` exists and has not expired."""
        if self._l1 is not None and await self._l1.has(key):
            return True
        if self._state is not None:
            return self._state.has(key)
        if self._shm is not None:
            return bool(self._shm.has_key(_encode_shm_key(self._key_prefix, key)))
        if self._driver is not None:
            return bool(await self._driver.execute_command("EXISTS", self._driver_key(key)))
        if self._store is not None:
            return await self._store_get(key) is not None
        return False

    async def touch(self, key: str, *, ttl: float | None = None) -> bool:
        """Refresh the expiration timestamp for ``key``, returning True if a live key existed."""
        effective_ttl = self._effective_ttl(ttl)
        if self._l1 is not None:
            await self._l1.touch(key, ttl=ttl)
        if self._state is not None:
            return self._state.touch(key, effective_ttl)
        if self._shm is not None:
            shm_key = _encode_shm_key(self._key_prefix, key)
            if effective_ttl is not None and effective_ttl <= 0:
                return bool(self._shm.delete(shm_key))
            return bool(self._shm.touch(shm_key, ttl=effective_ttl))
        if self._driver is not None:
            dkey = self._driver_key(key)
            if effective_ttl is not None and effective_ttl <= 0:
                return bool(await self._driver.delete(dkey))
            if effective_ttl is not None:
                return bool(await self._driver.execute_command("PEXPIRE", dkey, _resolve_px_ms(effective_ttl)))
            if not await self._driver.execute_command("EXISTS", dkey):
                return False
            await self._driver.execute_command("PERSIST", dkey)
            return True
        if self._store is not None:
            found = await self._store_get(key)
            if found is None:
                return False
            _, payload = found
            if effective_ttl is not None and effective_ttl <= 0:
                await self._store.delete(_object_store_key(self._ns_dir, key))
                return True
            await self._store_put(key, payload, effective_ttl)
            return True
        return False

    async def incr(self, key: str, delta: int = 1) -> int:
        """Increment an integer value stored at ``key`` or initialize missing keys to ``delta``."""
        new_val, remaining_ttl = await self._incr_l2(key, delta)
        if self._l1 is not None:
            encoded = str(new_val).encode("utf-8")
            if (
                remaining_ttl is not None
                and remaining_ttl <= 0
                or self._l1._scheme == "shm"
                and len(encoded) > _SHM_MAX_VALUE_BYTES
            ):
                await self._l1.delete(key)
            else:
                await self._l1.set(key, encoded, ttl=remaining_ttl)
        return new_val

    async def get_many(self, keys: Sequence[str]) -> dict[str, bytes]:
        """Retrieve all live entries among ``keys`` as a key-to-bytes dictionary."""
        result: dict[str, bytes] = {}
        for key in keys:
            val = await self.get(key)
            if val is not None:
                result[key] = val
        return result

    async def set_many(self, entries: Mapping[str, bytes], *, ttl: float | None = None) -> None:
        """Store multiple ``key -> value`` pairs with an optional shared TTL."""
        for key, value in entries.items():
            await self.set(key, value, ttl=ttl)

    async def delete_many(self, keys: Sequence[str]) -> int:
        """Delete all ``keys`` from the cache and return the count of live keys removed."""
        unique_keys = list(dict.fromkeys(keys))
        if not unique_keys:
            return 0
        if self._l1 is None and self._driver is not None:
            dkeys = [self._driver_key(key) for key in unique_keys]
            return int(await self._driver.unlink(*dkeys))
        removed = 0
        for key in unique_keys:
            if await self.delete(key):
                removed += 1
        return removed

    async def delete_prefix(self, prefix: str) -> int:
        """Delete all keys matching ``key_prefix + prefix`` and return the count removed."""
        if self._l1 is not None:
            await self._l1.delete_prefix(prefix)
        if self._state is not None:
            return self._state.delete_prefix(prefix)
        if self._shm is not None:
            pattern = self._shm_ns_prefix + prefix.encode("utf-8")
            if len(pattern) > _SHM_MAX_KEY_BYTES:
                return 0
            return int(self._shm.invalidate_prefix(pattern))
        if self._driver is not None:
            full_prefix = self._driver_key(prefix)
            match = f"{_escape_redis_glob(full_prefix)}*"
            cursor = 0
            removed = 0
            while True:
                cursor, batch = await self._driver.scan(cursor, match=match, count=500)
                matched = [item for item in batch if item.startswith(full_prefix)]
                if matched:
                    removed += int(await self._driver.unlink(*matched))
                if cursor == 0:
                    break
            return removed
        if self._store is not None:
            return await self._store_delete_prefix(prefix)
        return 0

    async def clear(self) -> None:
        """Clear all keys within this store's ``key_prefix`` namespace."""
        if self._l1 is not None:
            await self._l1.clear()
        if self._state is not None:
            self._state.clear()
            return
        if self._shm is not None:
            self._shm.invalidate_prefix(self._shm_ns_prefix)
            return
        if self._driver is not None:
            await self.delete_prefix("")
            return
        if self._store is not None:
            await self._store_delete_prefix("")

    async def stats(self) -> CacheStats:
        """Return a snapshot of hit, miss, eviction, expiration, size, and capacity metrics."""
        if self._state is not None:
            return self._state.stats()
        if self._shm is not None:
            raw = self._shm.stats()
            return CacheStats(
                hits=raw.hits,
                misses=raw.misses,
                evictions=raw.evictions,
                expirations=raw.expirations,
                size=0,
                capacity=self._capacity,
            )
        return CacheStats(
            hits=self._hits,
            misses=self._misses,
            evictions=0,
            expirations=self._expirations,
            size=0,
            capacity=self._capacity,
        )

    async def aclose(self) -> None:
        """Asynchronously release underlying backend connections or shared-memory handles."""
        if self._l1 is not None:
            await self._l1.aclose()
        if self._shm is not None:
            self._shm.close()
        if self._driver is not None:
            await self._driver.close()
        if self._store is not None:
            self._store.close()

    def _initialize(
        self,
        *,
        url: str,
        l1_url: str | None,
        capacity: int,
        default_ttl: float | None,
        key_prefix: str,
        _clock: Callable[[], float] | None,
        shared_state: _MemoryState | None,
        l1_store: AsyncCacheStore | None,
    ) -> None:
        if capacity < 1:
            msg = "capacity must be >= 1"
            raise ValueError(msg)
        scheme = _parse_url_scheme(url)
        if l1_url is not None:
            _parse_url_scheme(l1_url)
        validated_prefix = _validate_key_prefix(key_prefix)

        self._url = url
        self._l1_url = l1_url
        self._scheme = scheme
        self._capacity = capacity
        self._default_ttl = default_ttl
        self._key_prefix = validated_prefix
        self._raw_clock = _clock
        default_clock = time.time if scheme in _OBJECT_SCHEMES else time.monotonic
        self._clock: Callable[[], float] = _clock if _clock is not None else default_clock
        self._hits = 0
        self._misses = 0
        self._expirations = 0
        self._state: _MemoryState | None = None
        self._shm: ShmCache | None = None
        self._shm_name: str | None = None
        self._shm_ns_prefix = _shm_namespace_prefix(validated_prefix)
        self._driver: AsyncValkeyDriver | AsyncRedisDriver | None = None
        self._store: AsyncObjectStore | None = None
        self._ns_dir = _object_store_ns_dir(validated_prefix)
        self._l1: AsyncCacheStore | None = l1_store

        if scheme == "memory":
            self._state = (
                shared_state if shared_state is not None else _MemoryState(capacity=capacity, clock=self._clock)
            )
        elif scheme == "shm":
            from vertebra._vertebra import ShmCache

            segment = _shm_segment_name(url)
            self._shm_name = segment
            self._shm = ShmCache(segment, capacity_bytes=max(65536, capacity * 1024), max_entries=capacity)
        elif scheme in _VALKEY_SCHEMES:
            ensure_module("vertebra._valkey", package="vertebra-core[valkey]", extra="valkey")
            from vertebra.db.adapters.valkey import AsyncValkeyDriver, ValkeyConfig

            self._driver = AsyncValkeyDriver(ValkeyConfig(url=url))
        elif scheme in _REDIS_SCHEMES:
            ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
            from vertebra.db.adapters.redis import AsyncRedisDriver, RedisConfig

            self._driver = AsyncRedisDriver(RedisConfig(url=url))
        elif scheme in _OBJECT_SCHEMES:
            ensure_module("vertebra._storage", package="vertebra-core[storage]", extra="storage")
            from vertebra.storage import AsyncObjectStore

            self._store = AsyncObjectStore(url)

        if l1_url is not None and self._l1 is None:
            self._l1 = AsyncCacheStore(
                l1_url,
                capacity=capacity,
                default_ttl=default_ttl,
                key_prefix=validated_prefix,
                _clock=_clock,
            )

    def _effective_ttl(self, ttl: float | None) -> float | None:
        return self._default_ttl if ttl is None else ttl

    def _driver_key(self, key: str) -> str:
        return f"{self._key_prefix}{key}"

    def _record_hit(self) -> None:
        if self._state is not None:
            self._state.record_hit()
        else:
            self._hits += 1

    async def _get_l2_with_ttl(self, key: str) -> tuple[bytes, float | None] | None:
        if self._state is not None:
            hit = self._state.get_with_expiry(key, record_stats=True)
            if hit is None:
                return None
            value, expires_at = hit
            remaining = max(0.0, expires_at - self._clock()) if expires_at is not None else None
            return value, remaining
        if self._shm is not None:
            raw = self._shm.get(_encode_shm_key(self._key_prefix, key))
            return (raw, None) if raw is not None else None
        if self._driver is not None:
            dkey = self._driver_key(key)
            raw = await self._driver.get(dkey)
            if raw is None:
                return None
            remaining = None
            if self._l1 is not None:
                pttl_ms = await self._driver.pttl(dkey)
                if pttl_ms > 0:
                    remaining = pttl_ms / 1000.0
            return raw, remaining
        if self._store is not None:
            found = await self._store_get(key)
            if found is None:
                return None
            deadline, payload = found
            remaining = max(0.0, deadline - self._clock()) if deadline is not None else None
            return payload, remaining
        return None

    async def _add_l2(self, key: str, value: bytes, effective_ttl: float | None) -> bool:
        if self._state is not None:
            return self._state.add(key, value, effective_ttl)
        if self._shm is not None:
            shm_key = _encode_shm_key(self._key_prefix, key)
            if effective_ttl is not None and effective_ttl <= 0:
                return not self._shm.has_key(shm_key)
            if len(value) > _SHM_MAX_VALUE_BYTES:
                return False
            return bool(self._shm.add(shm_key, value, ttl=effective_ttl))
        if self._driver is not None:
            dkey = self._driver_key(key)
            if effective_ttl is not None and effective_ttl <= 0:
                return not bool(await self._driver.execute_command("EXISTS", dkey))
            if effective_ttl is not None:
                res = await self._driver.execute_command("SET", dkey, value, "PX", _resolve_px_ms(effective_ttl), "NX")
            else:
                res = await self._driver.execute_command("SET", dkey, value, "NX")
            return res is not None and res is not False
        if self._store is not None:
            if await self._store_get(key) is not None:
                return False
            if effective_ttl is not None and effective_ttl <= 0:
                return True
            await self._store_put(key, value, effective_ttl)
            return True
        return False

    async def _delete_l2(self, key: str) -> bool:
        if self._state is not None:
            return self._state.delete(key)
        if self._shm is not None:
            return bool(self._shm.delete(_encode_shm_key(self._key_prefix, key)))
        if self._driver is not None:
            return bool(await self._driver.delete(self._driver_key(key)))
        if self._store is not None:
            existed = await self._store_get(key) is not None
            if existed:
                await self._store.delete(_object_store_key(self._ns_dir, key))
            return existed
        return False

    async def _incr_l2(self, key: str, delta: int) -> tuple[int, float | None]:
        if self._state is not None:
            new_val, expires_at = self._state.incr(key, delta, self._default_ttl)
            remaining = max(0.0, expires_at - self._clock()) if expires_at is not None else None
            return new_val, remaining
        if self._shm is not None:
            return self._incr_shm(key, delta), self._default_ttl
        if self._driver is not None:
            return await self._incr_driver(key, delta), self._default_ttl
        if self._store is not None:
            return await self._incr_store(key, delta)
        return delta, self._default_ttl

    def _incr_shm(self, key: str, delta: int) -> int:
        assert self._shm is not None
        dkey = _encode_shm_key(self._key_prefix, key)
        while True:
            existing = self._shm.get(dkey)
            if existing is not None:
                _parse_int_bytes(existing, key)
                return int(self._shm.incr(dkey, delta))
            if self._default_ttl is not None and self._default_ttl <= 0:
                return delta
            if self._default_ttl is None:
                return int(self._shm.incr(dkey, delta))
            encoded = str(delta).encode("utf-8")
            if self._shm.add(dkey, encoded, ttl=self._default_ttl):
                return delta

    async def _incr_driver(self, key: str, delta: int) -> int:
        assert self._driver is not None
        dkey = self._driver_key(key)
        if self._default_ttl is not None and self._default_ttl > 0:
            created = await self._driver.execute_command(
                "SET", dkey, str(delta).encode("utf-8"), "PX", _resolve_px_ms(self._default_ttl), "NX"
            )
            if created is not None and created is not False:
                return delta
        elif self._default_ttl is not None and self._default_ttl <= 0:
            existing = await self._driver.get(dkey)
            if existing is None:
                return delta
            _parse_int_bytes(existing, key)
        try:
            return int(await self._driver.execute_command("INCRBY", dkey, delta))
        except Exception as exc:
            if _is_redis_int_error(exc):
                msg = f"Cache value for key {key!r} is not a valid integer"
                raise ValueError(msg) from exc
            raise

    async def _incr_store(self, key: str, delta: int) -> tuple[int, float | None]:
        assert self._store is not None
        found = await self._store_get(key)
        if found is None:
            if self._default_ttl is not None and self._default_ttl <= 0:
                return delta, None
            await self._store_put(key, str(delta).encode("utf-8"), self._default_ttl)
            return delta, self._default_ttl
        deadline, payload = found
        cur = _parse_int_bytes(payload, key)
        new_val = cur + delta
        await self._store_put_deadline(key, str(new_val).encode("utf-8"), deadline)
        remaining = max(0.0, deadline - self._clock()) if deadline is not None else None
        return new_val, remaining

    async def _store_get(self, key: str) -> tuple[float | None, bytes] | None:
        assert self._store is not None
        store_key = _object_store_key(self._ns_dir, key)
        try:
            raw = await self._store.get(store_key)
        except FileNotFoundError:
            return None
        deadline, payload = _decode_envelope(raw)
        if deadline is not None and self._clock() >= deadline:
            await self._store.delete(store_key)
            self._expirations += 1
            return None
        return deadline, payload

    async def _store_put(self, key: str, value: bytes, effective_ttl: float | None) -> None:
        deadline = (self._clock() + effective_ttl) if effective_ttl is not None else None
        await self._store_put_deadline(key, value, deadline)

    async def _store_put_deadline(self, key: str, value: bytes, deadline: float | None) -> None:
        assert self._store is not None
        store_key = _object_store_key(self._ns_dir, key)
        await self._store.put(store_key, _encode_envelope(value, deadline))

    async def _store_delete_prefix(self, prefix: str) -> int:
        assert self._store is not None
        prefix_hex = prefix.encode("utf-8").hex()
        cursor: str | None = None
        removed = 0
        while True:
            items, cursor = await self._store.list_page(f"{self._ns_dir}/", cursor=cursor, limit=1000)
            for item in items:
                item_hex = _object_store_item_hex(self._ns_dir, item.path)
                if item_hex is not None and item_hex.startswith(prefix_hex):
                    await self._store.delete(item.path)
                    removed += 1
            if cursor is None:
                break
        return removed
