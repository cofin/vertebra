"""Unified sync and async cache backend, CacheManager, and service mixin for SQLAlchemy and Advanced Alchemy."""

from __future__ import annotations

import base64
import collections
import datetime as dt
import decimal
import inspect
import math
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator, Mapping, MutableMapping, Sequence
from datetime import timedelta
from functools import partial
from typing import TYPE_CHECKING, Any, Literal, Self, TypeVar, cast

import msgspec
from sqlalchemy import inspect as sa_inspect

from vertebra.db.redis import AsyncRedisDriver, RedisConfig, SyncRedisDriver
from vertebra.db.valkey import AsyncValkeyDriver, SyncValkeyDriver, ValkeyConfig
from vertebra.typing import MissingDependencyError
from vertebra.utils.serializers import (
    ZeroCopyCodecRegistry,
    decode_json,
    decode_msgpack,
    encode_json_bytes,
    encode_msgpack_bytes,
)
from vertebra.utils.text import validate_identifier

try:
    from advanced_alchemy.cache import CacheConfig, CacheManager, setup_cache_listeners
    from advanced_alchemy.cache._null import NO_VALUE
    from advanced_alchemy.cache.manager import DOGPILE_NO_VALUE
    from advanced_alchemy.cache.serializers import default_deserializer, default_serializer
except ImportError as exc:
    raise MissingDependencyError(
        "advanced-alchemy",
        install_package="advanced-alchemy",
        extra="alchemy",
    ) from exc

if TYPE_CHECKING:
    pass

__all__ = (
    "AsyncCacheManager",
    "CacheConfig",
    "CacheEntry",
    "CacheStats",
    "SQLAlchemyCacheBackend",
    "ZeroCopyServiceMixin",
    "create_cache_config",
    "msgspec_model_deserializer",
    "msgspec_model_serializer",
    "setup_cache_listeners",
)

T = TypeVar("T")

_MODEL_KEY = "__aa_model__"
_TABLE_KEY = "__aa_table__"
_MSGPACK_MAGIC = b"\x00vtb_mp\x00"


class CacheStats(msgspec.Struct, frozen=True, gc=False):
    """Runtime cache performance metrics and counters."""

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    size: int = 0
    capacity: int = 0


class CacheEntry:
    """Internal cache entry storing value and optional monotonic expiration."""

    __slots__ = ("expires_at", "value")

    def __init__(self, value: Any, expires_at: float | None = None) -> None:
        """Initialize cache entry with stored value and optional expiration timestamp."""
        self.value = value
        self.expires_at = expires_at

    def is_expired(self, now: float) -> bool:
        """Return True if entry has an expiration timestamp that has elapsed."""
        return self.expires_at is not None and now >= self.expires_at


class SQLAlchemyCacheBackend(MutableMapping[Any, Any]):
    """Thread-safe unified sync and async cache backend for SQLAlchemy and Advanced Alchemy.

    Implements Python's MutableMapping protocol for SQLAlchemy execution_options compiled_cache,
    as well as Advanced Alchemy's SyncCacheRegionProtocol and AsyncCacheRegionProtocol over
    L1 local/SHM LRU memory and optional L2 SyncValkeyDriver/AsyncValkeyDriver or Redis drivers.
    """

    __slots__ = (
        "_async_driver",
        "_capacity",
        "_default_ttl",
        "_driver_prefix",
        "_entries",
        "_evictions",
        "_expirations",
        "_hits",
        "_key_prefix",
        "_lock",
        "_misses",
        "_region_mode",
        "_shm_name",
        "_sync_driver",
    )

    def __init__(
        self,
        *,
        capacity: int | None = 1024,
        default_ttl: float | timedelta | None = None,
        shm_name: str | None = None,
        sync_driver: SyncValkeyDriver | SyncRedisDriver | None = None,
        async_driver: AsyncValkeyDriver | AsyncRedisDriver | None = None,
        region_mode: bool = False,
        key_prefix: str = "vtb:aa:",
    ) -> None:
        """Initialize unified cache backend with L1 capacity, TTL, key prefix, and optional L2 Valkey/Redis drivers."""
        normalized_prefix, driver_prefix = self._normalize_key_prefix(key_prefix)
        self._entries: collections.OrderedDict[Any, CacheEntry] = collections.OrderedDict()
        self._capacity = capacity
        self._default_ttl = default_ttl.total_seconds() if isinstance(default_ttl, timedelta) else default_ttl
        self._shm_name = shm_name
        self._sync_driver = sync_driver
        self._async_driver = async_driver
        self._region_mode = region_mode
        self._key_prefix = normalized_prefix
        self._driver_prefix = driver_prefix
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._expirations = 0
        if isinstance(sync_driver, SyncValkeyDriver):
            sync_driver.add_invalidation_listener(self._on_driver_invalidate)
        if isinstance(async_driver, AsyncValkeyDriver):
            async_driver.add_invalidation_listener(self._on_driver_invalidate)

    @classmethod
    def create_shm_cache_backend(
        cls,
        name: str | None = None,
        capacity: int = 1024,
        default_ttl: float | timedelta | None = None,
        *,
        key_prefix: str = "vtb:aa:",
    ) -> Self:
        """Create a shared-memory backed cache backend instance."""
        resolved_name = name or f"vtb-cache-{id(cls)}-{int(time.time())}"
        return cls(capacity=capacity, default_ttl=default_ttl, shm_name=resolved_name, key_prefix=key_prefix)

    @classmethod
    def from_valkey(
        cls,
        config_or_url: ValkeyConfig | str = "valkey://localhost:6379/0",
        *,
        capacity: int | None = 2048,
        default_ttl: float | timedelta | None = 3600.0,
        shm_name: str | None = None,
        region_mode: bool = False,
        key_prefix: str = "vtb:aa:",
    ) -> Self:
        """Create a cache backend backed by both SyncValkeyDriver and AsyncValkeyDriver."""
        cfg = ValkeyConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(
            capacity=capacity,
            default_ttl=default_ttl,
            shm_name=shm_name,
            sync_driver=SyncValkeyDriver(cfg),
            async_driver=AsyncValkeyDriver(cfg),
            region_mode=region_mode,
            key_prefix=key_prefix,
        )

    @classmethod
    def from_redis(
        cls,
        config_or_url: RedisConfig | str = "redis://localhost:6379/0",
        *,
        capacity: int | None = 2048,
        default_ttl: float | timedelta | None = 3600.0,
        shm_name: str | None = None,
        region_mode: bool = False,
        key_prefix: str = "vtb:aa:",
    ) -> Self:
        """Create a cache backend backed by both SyncRedisDriver and AsyncRedisDriver."""
        cfg = RedisConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(
            capacity=capacity,
            default_ttl=default_ttl,
            shm_name=shm_name,
            sync_driver=SyncRedisDriver(cfg),
            async_driver=AsyncRedisDriver(cfg),
            region_mode=region_mode,
            key_prefix=key_prefix,
        )

    @classmethod
    def from_url(
        cls,
        url: str | None = None,
        *,
        capacity: int | None = 2048,
        default_ttl: float | timedelta | None = 3600.0,
        shm_name: str | None = None,
        region_mode: bool = False,
        key_prefix: str = "vtb:aa:",
    ) -> Self:
        """Create a cache backend from a URI (`valkey://`, `redis://`, `shm://`, or `memory://`)."""
        if not url or url.startswith("memory://"):
            return cls(
                capacity=capacity,
                default_ttl=default_ttl,
                shm_name=shm_name,
                region_mode=region_mode,
                key_prefix=key_prefix,
            )
        if url.startswith("shm://"):
            segment = url.removeprefix("shm://").strip("/") or shm_name or "vtb-aa-cache"
            return cls(
                capacity=capacity,
                default_ttl=default_ttl,
                shm_name=segment,
                region_mode=region_mode,
                key_prefix=key_prefix,
            )
        if url.startswith(("valkey://", "valkeys://")):
            return cls.from_valkey(
                url,
                capacity=capacity,
                default_ttl=default_ttl,
                shm_name=shm_name,
                region_mode=region_mode,
                key_prefix=key_prefix,
            )
        if url.startswith(("redis://", "rediss://")):
            return cls.from_redis(
                url,
                capacity=capacity,
                default_ttl=default_ttl,
                shm_name=shm_name,
                region_mode=region_mode,
                key_prefix=key_prefix,
            )
        return cls(
            capacity=capacity,
            default_ttl=default_ttl,
            shm_name=shm_name,
            region_mode=region_mode,
            key_prefix=key_prefix,
        )

    @property
    def capacity(self) -> int | None:
        """Maximum configured L1 capacity of the cache."""
        return self._capacity

    @property
    def default_ttl(self) -> float | None:
        """Default TTL in seconds for entries without an explicit expiration."""
        return self._default_ttl

    @property
    def shm_name(self) -> str | None:
        """Shared-memory segment name when configured."""
        return self._shm_name

    @property
    def key_prefix(self) -> str:
        """Namespace prefix applied to keys written to the L2 Valkey/Redis driver."""
        return self._key_prefix

    @property
    def sync_driver(self) -> SyncValkeyDriver | SyncRedisDriver | None:
        """Optional synchronous L2 Valkey or Redis driver."""
        return self._sync_driver

    @property
    def async_driver(self) -> AsyncValkeyDriver | AsyncRedisDriver | None:
        """Optional asynchronous L2 Valkey or Redis driver."""
        return self._async_driver

    def stats(self) -> CacheStats:
        """Return snapshot of hit, miss, eviction, and size statistics."""
        with self._lock:
            return CacheStats(
                hits=self._hits,
                misses=self._misses,
                evictions=self._evictions,
                expirations=self._expirations,
                size=len(self._entries),
                capacity=self._capacity or 0,
            )

    def configure(
        self,
        backend: str = "vertebra.memory",
        expiration_time: int | None = None,
        arguments: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Self:
        """Configure this backend as an Advanced Alchemy SyncCacheRegionProtocol region."""
        _ = kwargs
        self._region_mode = True
        if expiration_time is not None:
            self._default_ttl = None if expiration_time < 0 else float(expiration_time)
        args = dict(arguments or {})
        if "capacity" in args and args["capacity"] is not None:
            self._capacity = int(args["capacity"])
        if "shm_name" in args and args["shm_name"] is not None:
            self._shm_name = str(args["shm_name"])
        if "key_prefix" in args and args["key_prefix"] is not None:
            normalized_prefix, driver_prefix = self._normalize_key_prefix(str(args["key_prefix"]))
            self._key_prefix = normalized_prefix
            self._driver_prefix = driver_prefix
        url = args.get("url")
        if isinstance(url, str):
            if url.startswith(("valkey://", "valkeys://")) and self._sync_driver is None:
                cfg = ValkeyConfig(url=url)
                self._sync_driver = SyncValkeyDriver(cfg)
                self._async_driver = AsyncValkeyDriver(cfg)
            elif url.startswith(("redis://", "rediss://")) and self._sync_driver is None:
                cfg_r = RedisConfig(url=url)
                self._sync_driver = SyncRedisDriver(cfg_r)
                self._async_driver = AsyncRedisDriver(cfg_r)
        elif "redis" in backend and self._sync_driver is None and ("host" in args or "port" in args):
            host = str(args.get("host", "localhost"))
            port = int(args.get("port", 6379))
            db = int(args.get("db", 0))
            cfg_r = RedisConfig(url=f"redis://{host}:{port}/{db}")
            self._sync_driver = SyncRedisDriver(cfg_r)
            self._async_driver = AsyncRedisDriver(cfg_r)
        return self

    async def configure_async(
        self,
        backend: str = "vertebra.memory",
        expiration_time: int | None = None,
        arguments: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Self:
        """Asynchronously configure this backend as an AsyncCacheRegionProtocol region."""
        return self.configure(backend, expiration_time=expiration_time, arguments=arguments, **kwargs)

    def __getitem__(self, key: Any) -> Any:
        """Retrieve value for key, raising KeyError if missing or expired."""
        with self._lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            if entry is None:
                self._misses += 1
                raise KeyError(key)
            if entry.is_expired(now):
                del self._entries[key]
                self._expirations += 1
                self._misses += 1
                raise KeyError(key)
            self._entries.move_to_end(key)
            self._hits += 1
            return entry.value

    def __setitem__(self, key: Any, value: Any) -> None:
        """Store key and value with default TTL."""
        self.set(key, value)

    def __delitem__(self, key: Any) -> None:
        """Remove key from cache, raising KeyError if absent."""
        with self._lock:
            try:
                del self._entries[key]
            except KeyError:
                self._misses += 1
                raise

    def __contains__(self, key: Any) -> bool:
        """Return True if unexpired key is present in cache."""
        return self.exists(key)

    def __iter__(self) -> Iterator[Any]:
        """Iterate over currently unexpired keys."""
        with self._lock:
            now = time.monotonic()
            unexpired_keys = [k for k, v in self._entries.items() if not v.is_expired(now)]
            return iter(unexpired_keys)

    def __len__(self) -> int:
        """Return count of unexpired entries."""
        with self._lock:
            now = time.monotonic()
            return sum(1 for v in self._entries.values() if not v.is_expired(now))

    def get(
        self,
        key: Any,
        default: Any = None,
        *,
        expiration_time: int | float | None = None,
    ) -> Any:
        """Retrieve value for key if present and unexpired, else return default (or NO_VALUE in region mode)."""
        _ = expiration_time
        miss_sentinel = NO_VALUE if (default is None and self._region_mode) else default
        with self._lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            if entry is not None:
                if entry.is_expired(now):
                    del self._entries[key]
                    self._expirations += 1
                else:
                    self._entries.move_to_end(key)
                    self._hits += 1
                    return entry.value

        if self._sync_driver is not None and isinstance(key, str):
            raw = self._sync_driver.get(self._driver_key(key))
            if raw is not None:
                decoded = self._decode_driver_payload(raw)
                with self._lock:
                    self._entries[key] = CacheEntry(
                        value=decoded,
                        expires_at=self._resolve_expires_at(expiration_time),
                    )
                    self._entries.move_to_end(key)
                    self._evict_if_needed()
                    self._hits += 1
                return decoded

        with self._lock:
            self._misses += 1
        return miss_sentinel

    def get_or_create(
        self,
        key: str,
        creator: Callable[[], T],
        expiration_time: int | None = None,
    ) -> T:
        """Retrieve cached value for key or compute and store it synchronously."""
        marker = object()
        cached = self.get(key, default=marker, expiration_time=expiration_time)
        if cached is not marker and cached is not NO_VALUE and cached is not DOGPILE_NO_VALUE:
            return cast("T", cached)
        created = creator()
        self.set(key, created, ttl=expiration_time)
        return created

    def set(
        self,
        key: Any,
        value: Any,
        ttl: float | int | timedelta | None = None,
    ) -> None:
        """Store value for key with optional expiration duration."""
        expires_at = self._resolve_expires_at(ttl)
        with self._lock:
            self._entries[key] = CacheEntry(value=value, expires_at=expires_at)
            self._entries.move_to_end(key)
            self._evict_if_needed()
        if self._sync_driver is not None and isinstance(key, str):
            px_ms = self._resolve_driver_px(ttl)
            self._sync_driver.set(self._driver_key(key), self._encode_driver_payload(value), px=px_ms)

    def delete(self, key: Any) -> bool:
        """Remove key from cache. Return True if key was present, False otherwise."""
        with self._lock:
            entry = self._entries.pop(key, None)
            removed = entry is not None
        if self._sync_driver is not None and isinstance(key, str):
            deleted_count = self._sync_driver.delete(self._driver_key(key))
            removed = removed or (deleted_count > 0)
        return removed

    def get_many(self, keys: Sequence[str], *, default: Any = None) -> dict[str, Any]:
        """Retrieve multiple keys in a single batch dictionary synchronously."""
        return {key: self.get(key, default=default) for key in keys}

    def set_many(
        self,
        mapping: Mapping[str, Any],
        *,
        ttl: float | int | timedelta | None = None,
    ) -> None:
        """Store multiple key-value pairs synchronously."""
        for key, value in mapping.items():
            self.set(key, value, ttl=ttl)

    def delete_many(self, keys: Sequence[str]) -> int:
        """Delete multiple keys synchronously and return count of removed keys."""
        return sum(1 for key in keys if self.delete(key))

    def invalidate(self) -> None:
        """Invalidate all entries in the cache region synchronously."""
        self.clear()

    def exists(self, key: Any) -> bool:
        """Return True if key exists and has not expired."""
        with self._lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            if entry is not None:
                if entry.is_expired(now):
                    del self._entries[key]
                    self._expirations += 1
                else:
                    return True
        if self._sync_driver is not None and isinstance(key, str):
            return self._sync_driver.get(self._driver_key(key)) is not None
        return False

    def expires_in(self, key: Any) -> float | None:
        """Return remaining seconds until key expiration, or None if key has no TTL or does not exist."""
        with self._lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            if entry is not None:
                if entry.is_expired(now):
                    return None
                if entry.expires_at is None:
                    return None
                return max(0.0, entry.expires_at - now)
        if self._sync_driver is not None and isinstance(key, str):
            pttl_ms = self._sync_driver.pttl(self._driver_key(key))
            if pttl_ms <= 0:
                return None
            return pttl_ms / 1000.0
        return None

    def pop(self, key: Any, default: Any = None) -> Any:
        """Remove key and return its value if present and unexpired, else default."""
        with self._lock:
            now = time.monotonic()
            entry = self._entries.pop(key, None)
            if entry is None:
                return default
            if entry.is_expired(now):
                self._expirations += 1
                return default
            return entry.value

    def clear(self) -> None:
        """Remove all entries under this backend's key_prefix from L1 and optional L2 driver."""
        with self._lock:
            self._entries.clear()
        if self._sync_driver is not None:
            pattern = f"{self._driver_prefix}*"
            cursor = 0
            while True:
                cursor, keys = self._sync_driver.scan(cursor, match=pattern, count=500)
                if keys:
                    self._sync_driver.unlink(*keys)
                if cursor == 0:
                    break

    async def get_async(
        self,
        key: Any,
        default: Any = None,
        *,
        expiration_time: int | float | None = None,
    ) -> Any:
        """Asynchronously retrieve value for key without blocking the event loop."""
        _ = expiration_time
        miss_sentinel = NO_VALUE if (default is None and self._region_mode) else default
        with self._lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            if entry is not None:
                if entry.is_expired(now):
                    del self._entries[key]
                    self._expirations += 1
                else:
                    self._entries.move_to_end(key)
                    self._hits += 1
                    return entry.value

        if self._async_driver is not None and isinstance(key, str):
            raw = await self._async_driver.get(self._driver_key(key))
            if raw is not None:
                decoded = self._decode_driver_payload(raw)
                with self._lock:
                    self._entries[key] = CacheEntry(
                        value=decoded,
                        expires_at=self._resolve_expires_at(expiration_time),
                    )
                    self._entries.move_to_end(key)
                    self._evict_if_needed()
                    self._hits += 1
                return decoded

        with self._lock:
            self._misses += 1
        return miss_sentinel

    async def get_or_create_async(
        self,
        key: str,
        creator: Callable[[], T | Awaitable[T]],
        expiration_time: int | None = None,
    ) -> T:
        """Asynchronously retrieve cached value for key or await/call creator on miss."""
        marker = object()
        cached = await self.get_async(key, default=marker, expiration_time=expiration_time)
        if cached is not marker and cached is not NO_VALUE and cached is not DOGPILE_NO_VALUE:
            return cast("T", cached)
        candidate = creator()
        created: T = await candidate if inspect.isawaitable(candidate) else candidate
        await self.set_async(key, created, ttl=expiration_time)
        return created

    async def set_async(
        self,
        key: Any,
        value: Any,
        ttl: float | int | timedelta | None = None,
    ) -> None:
        """Asynchronously store value for key with optional expiration duration."""
        expires_at = self._resolve_expires_at(ttl)
        with self._lock:
            self._entries[key] = CacheEntry(value=value, expires_at=expires_at)
            self._entries.move_to_end(key)
            self._evict_if_needed()
        if self._async_driver is not None and isinstance(key, str):
            px_ms = self._resolve_driver_px(ttl)
            await self._async_driver.set(self._driver_key(key), self._encode_driver_payload(value), px=px_ms)

    async def delete_async(self, key: Any) -> bool:
        """Asynchronously remove key from cache."""
        with self._lock:
            entry = self._entries.pop(key, None)
            removed = entry is not None
        if self._async_driver is not None and isinstance(key, str):
            deleted_count = await self._async_driver.delete(self._driver_key(key))
            removed = removed or (deleted_count > 0)
        return removed

    async def get_many_async(self, keys: Sequence[str], *, default: Any = None) -> dict[str, Any]:
        """Asynchronously retrieve multiple keys."""
        result: dict[str, Any] = {}
        for key in keys:
            result[key] = await self.get_async(key, default=default)
        return result

    async def set_many_async(
        self,
        mapping: Mapping[str, Any],
        *,
        ttl: float | int | timedelta | None = None,
    ) -> None:
        """Asynchronously store multiple key-value pairs."""
        for key, value in mapping.items():
            await self.set_async(key, value, ttl=ttl)

    async def delete_many_async(self, keys: Sequence[str]) -> int:
        """Asynchronously delete multiple keys and return count of removed keys."""
        removed = 0
        for key in keys:
            if await self.delete_async(key):
                removed += 1
        return removed

    async def invalidate_async(self) -> None:
        """Asynchronously invalidate all entries in the cache region."""
        await self.clear_async()

    async def exists_async(self, key: Any) -> bool:
        """Asynchronously check if key exists and has not expired."""
        with self._lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            if entry is not None:
                if entry.is_expired(now):
                    del self._entries[key]
                    self._expirations += 1
                else:
                    return True
        if self._async_driver is not None and isinstance(key, str):
            return (await self._async_driver.get(self._driver_key(key))) is not None
        return False

    async def expires_in_async(self, key: Any) -> float | None:
        """Asynchronously return remaining seconds until key expiration."""
        with self._lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            if entry is not None:
                if entry.is_expired(now):
                    return None
                if entry.expires_at is None:
                    return None
                return max(0.0, entry.expires_at - now)
        if self._async_driver is not None and isinstance(key, str):
            pttl_ms = await self._async_driver.pttl(self._driver_key(key))
            if pttl_ms <= 0:
                return None
            return pttl_ms / 1000.0
        return None

    async def clear_async(self) -> None:
        """Asynchronously remove all entries under this backend's key_prefix from L1 and optional L2 driver."""
        with self._lock:
            self._entries.clear()
        if self._async_driver is not None:
            pattern = f"{self._driver_prefix}*"
            cursor = 0
            while True:
                cursor, keys = await self._async_driver.scan(cursor, match=pattern, count=500)
                if keys:
                    await self._async_driver.unlink(*keys)
                if cursor == 0:
                    break

    def _driver_key(self, key: str) -> str:
        """Return the prefix-qualified key sent to the L2 Valkey/Redis driver."""
        return key if key.startswith(self._driver_prefix) else f"{self._driver_prefix}{key}"

    @staticmethod
    def _normalize_key_prefix(key_prefix: str) -> tuple[str, str]:
        """Validate key_prefix and return (normalized_prefix, trailing_colon_driver_prefix)."""
        stripped = key_prefix.strip(":")
        if not stripped:
            msg = f"Invalid key_prefix: {key_prefix!r}"
            raise ValueError(msg)
        for segment in stripped.split(":"):
            if not segment:
                msg = f"Invalid key_prefix: {key_prefix!r}"
                raise ValueError(msg)
            validate_identifier(segment, "key_prefix", allow_hyphens=True)
        driver_prefix = key_prefix if key_prefix.endswith(":") else f"{key_prefix}:"
        return key_prefix, driver_prefix

    def _resolve_expires_at(self, ttl: float | int | timedelta | None) -> float | None:
        """Compute monotonic expiration timestamp from provided or default TTL."""
        effective_ttl: float | None = None
        if ttl is not None:
            effective_ttl = ttl.total_seconds() if isinstance(ttl, timedelta) else float(ttl)
        elif self._default_ttl is not None:
            effective_ttl = self._default_ttl
        if effective_ttl is None or effective_ttl < 0:
            return None
        return time.monotonic() + effective_ttl

    def _resolve_driver_px(self, ttl: float | int | timedelta | None) -> int | None:
        """Resolve integer TTL in milliseconds for Valkey/Redis SET PX."""
        effective_ttl: float | None = None
        if ttl is not None:
            effective_ttl = ttl.total_seconds() if isinstance(ttl, timedelta) else float(ttl)
        elif self._default_ttl is not None:
            effective_ttl = self._default_ttl
        if effective_ttl is None or effective_ttl <= 0:
            return None
        return max(1, math.ceil(effective_ttl * 1000))

    def _evict_if_needed(self) -> None:
        """Evict oldest entries when capacity is exceeded."""
        if self._capacity is None:
            return
        while len(self._entries) > self._capacity:
            self._entries.popitem(last=False)
            self._evictions += 1

    def _on_driver_invalidate(self, keys: Any) -> None:
        """Evict L1 cache entries when Valkey RESP3 BCAST client-tracking pushes invalidations."""
        with self._lock:
            for key in keys:
                self._entries.pop(key, None)
                if isinstance(key, str) and key.startswith(self._driver_prefix):
                    self._entries.pop(key.removeprefix(self._driver_prefix), None)

    @staticmethod
    def _encode_driver_payload(value: Any) -> bytes:
        """Serialize arbitrary Python cache values into bytes for L2 storage."""
        if isinstance(value, bytes):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        return _MSGPACK_MAGIC + bytes(encode_msgpack_bytes(value))

    @staticmethod
    def _decode_driver_payload(raw: bytes) -> Any:
        """Deserialize bytes retrieved from L2 storage back into the original value."""
        if raw.startswith(_MSGPACK_MAGIC):
            return decode_msgpack(raw[len(_MSGPACK_MAGIC) :])
        return raw


def _encode_column_value(value: Any) -> Any:
    """Convert non-primitive column values into msgspec-serializable markers."""
    if isinstance(value, uuid.UUID):
        return {"__vtb_uuid__": str(value)}
    if isinstance(value, dt.datetime):
        return {"__vtb_dt__": value.isoformat()}
    if isinstance(value, dt.date):
        return {"__vtb_date__": value.isoformat()}
    if isinstance(value, dt.time):
        return {"__vtb_time__": value.isoformat()}
    if isinstance(value, decimal.Decimal):
        return {"__vtb_dec__": str(value)}
    return value


def _decode_column_value(value: Any) -> Any:
    """Restore special column markers back to Python domain types."""
    if isinstance(value, dict) and len(value) == 1:
        if "__vtb_uuid__" in value:
            return uuid.UUID(value["__vtb_uuid__"])
        if "__vtb_dt__" in value:
            return dt.datetime.fromisoformat(value["__vtb_dt__"])
        if "__vtb_date__" in value:
            return dt.date.fromisoformat(value["__vtb_date__"])
        if "__vtb_time__" in value:
            return dt.time.fromisoformat(value["__vtb_time__"])
        if "__vtb_dec__" in value:
            return decimal.Decimal(value["__vtb_dec__"])
    return value


def msgspec_model_serializer(
    model: Any,
    *,
    format: Literal["msgpack", "json"] = "msgpack",
) -> bytes:
    """Serialize a SQLAlchemy model instance to MessagePack or JSON bytes via msgspec."""
    mapper = sa_inspect(model.__class__)
    data: dict[str, Any] = {
        _MODEL_KEY: model.__class__.__name__,
        _TABLE_KEY: getattr(model.__class__, "__tablename__", model.__class__.__name__),
    }
    for column in mapper.columns:
        if getattr(column, "_insert_sentinel", False):
            continue
        data[column.key] = _encode_column_value(getattr(model, column.key))
    if format == "json":
        return bytes(encode_json_bytes(data))
    return bytes(encode_msgpack_bytes(data))


def msgspec_model_deserializer[T](
    data: bytes,
    model_class: type[T],
    *,
    format: Literal["msgpack", "json"] = "msgpack",
) -> T:
    """Deserialize MessagePack or JSON bytes into a detached SQLAlchemy model or schema instance."""
    raw_dict: dict[str, Any] = decode_json(data) if format == "json" else decode_msgpack(data)
    parsed: dict[str, Any] = {k: _decode_column_value(v) for k, v in raw_dict.items()}
    serialized_model = parsed.pop(_MODEL_KEY, None)
    parsed.pop(_TABLE_KEY, None)

    try:
        sa_inspect(model_class)
        is_sa_model = True
    except Exception:
        is_sa_model = False

    if is_sa_model:
        if serialized_model and serialized_model != model_class.__name__:
            msg = f"Cannot deserialize {serialized_model} data as {model_class.__name__}"
            raise ValueError(msg)
        instance: T = model_class(**parsed)
        return instance

    plan = ZeroCopyCodecRegistry.compile(model_class)
    if plan.kind in ("msgspec", "dataclass", "attrs"):
        return cast("T", plan.hydrate_one(parsed))
    return msgspec.convert(parsed, type=model_class)


class AsyncCacheManager(CacheManager):
    """Event-loop native CacheManager subclass executing async operations without threadpool hopping."""

    __slots__ = ("_backend",)

    def __init__(
        self,
        config: CacheConfig,
        *,
        backend: SQLAlchemyCacheBackend | None = None,
    ) -> None:
        """Initialize AsyncCacheManager with CacheConfig and optional SQLAlchemyCacheBackend."""
        super().__init__(config)
        if backend is not None:
            backend._region_mode = True
            self._backend: SQLAlchemyCacheBackend | None = backend
            self._region = cast("Any", backend)
        else:
            region = self.region
            self._backend = region if isinstance(region, SQLAlchemyCacheBackend) else None

    @property
    def backend(self) -> SQLAlchemyCacheBackend | None:
        """Return the underlying SQLAlchemyCacheBackend when configured."""
        return self._backend

    async def get_async(self, key: str) -> object:
        """Get a value from the cache natively on the event loop."""
        if not self.config.enabled:
            return DOGPILE_NO_VALUE
        if self._backend is not None:
            return await self._backend.get_async(self._make_key(key), default=DOGPILE_NO_VALUE)
        return await super().get_async(key)

    async def set_async(self, key: str, value: Any) -> None:
        """Set a value in the cache natively on the event loop."""
        if not self.config.enabled:
            return
        if self._backend is not None:
            await self._backend.set_async(
                self._make_key(key),
                value,
                ttl=self.config.expiration_time if self.config.expiration_time > 0 else None,
            )
            return
        await super().set_async(key, value)

    async def delete_async(self, key: str) -> None:
        """Delete a value from the cache natively on the event loop."""
        if self._backend is not None:
            await self._backend.delete_async(self._make_key(key))
            return
        await super().delete_async(key)

    async def get_or_create_async(
        self,
        key: str,
        creator: Callable[[], T],
        expiration_time: int | None = None,
    ) -> T:
        """Get a value from cache or create it natively on the event loop."""
        if not self.config.enabled:
            res = creator()
            return await res if inspect.isawaitable(res) else res
        if self._backend is not None:
            return await self._backend.get_or_create_async(
                self._make_key(key),
                creator,
                expiration_time=expiration_time or self.config.expiration_time,
            )
        return await super().get_or_create_async(key, creator, expiration_time)

    async def get_entity_async(
        self,
        model_name: str,
        entity_id: Any,
        model_class: type[T],
        bind_group: str | None = None,
    ) -> T | None:
        """Get a cached entity by model name and ID natively on the event loop."""
        key = f"{model_name}:{bind_group}:get:{entity_id}" if bind_group else f"{model_name}:get:{entity_id}"
        cached = await self.get_async(key)
        if cached is DOGPILE_NO_VALUE or cached is NO_VALUE:
            return None
        if not isinstance(cached, (bytes, bytearray)):
            await self.delete_async(key)
            return None
        deserializer = self.config.deserializer or default_deserializer
        try:
            payload = bytes(cached) if isinstance(cached, bytearray) else cached
            return cast("T", deserializer(payload, model_class))
        except Exception:
            await self.delete_async(key)
            return None

    async def set_entity_async(
        self,
        model_name: str,
        entity_id: Any,
        entity: Any,
        bind_group: str | None = None,
    ) -> None:
        """Cache an entity natively on the event loop."""
        key = f"{model_name}:{bind_group}:get:{entity_id}" if bind_group else f"{model_name}:get:{entity_id}"
        serializer = self.config.serializer or default_serializer
        serialized = serializer(entity)
        await self.set_async(key, serialized)

    async def invalidate_entity_async(
        self,
        model_name: str,
        entity_id: Any,
        bind_group: str | None = None,
    ) -> None:
        """Invalidate the cache for a specific entity natively on the event loop."""
        key = f"{model_name}:{bind_group}:get:{entity_id}" if bind_group else f"{model_name}:get:{entity_id}"
        await self.delete_async(key)

    async def bump_model_version_async(self, model_name: str) -> str:
        """Bump the version token for a model natively on the event loop."""
        token = uuid.uuid4().hex
        self._model_versions[model_name] = token
        await self.set_async(f"{model_name}:version", token)
        return token

    async def get_model_version_async(self, model_name: str) -> str:
        """Get the current version token for a model natively on the event loop."""
        if model_name in self._model_versions:
            return self._model_versions[model_name]
        cached = await self.get_async(f"{model_name}:version")
        if cached is not DOGPILE_NO_VALUE and cached is not NO_VALUE and isinstance(cached, str):
            self._model_versions[model_name] = cached
            return cached
        if isinstance(cached, bytes):
            decoded = cached.decode("utf-8", errors="replace")
            self._model_versions[model_name] = decoded
            return decoded
        return "0"

    async def get_many_async(self, key: str, model_class: type[T]) -> list[T] | None:
        """Get a cached list of entities natively on the event loop."""
        cached = await self.get_async(key)
        if cached is DOGPILE_NO_VALUE or cached is NO_VALUE or not isinstance(cached, list):
            return None
        deserializer = self.config.deserializer or default_deserializer
        results: list[T] = []
        try:
            for item in cached:
                if not isinstance(item, str):
                    return None
                raw = base64.b64decode(item.encode("ascii"))
                results.append(deserializer(raw, model_class))
        except Exception:
            await self.delete_async(key)
            return None
        return results

    async def set_many_async(self, key: str, items: list[Any]) -> None:
        """Cache a list of entities natively on the event loop."""
        serializer = self.config.serializer or default_serializer
        payload = [base64.b64encode(serializer(item)).decode("ascii") for item in items]
        await self.set_async(key, payload)

    async def get_many_and_count_async(
        self,
        key: str,
        model_class: type[T],
    ) -> tuple[list[T], int] | None:
        """Get a cached list+count payload natively on the event loop."""
        cached = await self.get_async(key)
        if cached is DOGPILE_NO_VALUE or cached is NO_VALUE or not isinstance(cached, dict):
            return None
        items_raw = cached.get("items")
        count_raw = cached.get("count")
        if not isinstance(items_raw, list) or not isinstance(count_raw, int):
            return None
        deserializer = self.config.deserializer or default_deserializer
        results: list[T] = []
        try:
            for item in items_raw:
                if not isinstance(item, str):
                    return None
                raw = base64.b64decode(item.encode("ascii"))
                results.append(deserializer(raw, model_class))
        except Exception:
            await self.delete_async(key)
            return None
        return results, count_raw

    async def set_many_and_count_async(self, key: str, items: list[Any], count: int) -> None:
        """Cache a list+count payload natively on the event loop."""
        serializer = self.config.serializer or default_serializer
        payload = {
            "items": [base64.b64encode(serializer(item)).decode("ascii") for item in items],
            "count": count,
        }
        await self.set_async(key, payload)

    async def invalidate_all_async(self) -> None:
        """Invalidate all cached values natively on the event loop."""
        if self._backend is not None:
            await self._backend.invalidate_async()
            self._model_versions.clear()
            return
        await super().invalidate_all_async()

    def get_schema_sync(
        self,
        model_name: str,
        entity_id: Any,
        schema_type: type[T],
        *,
        bind_group: str | None = None,
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> T | None:
        """Decode a cached entity directly into `schema_type` without instantiating an ORM model."""
        key = f"{model_name}:{bind_group}:get:{entity_id}" if bind_group else f"{model_name}:get:{entity_id}"
        cached = self.get_sync(key)
        if cached is DOGPILE_NO_VALUE or cached is NO_VALUE or not isinstance(cached, (bytes, bytearray)):
            return None
        return msgspec_model_deserializer(bytes(cached), schema_type, format=format)

    async def get_schema_async(
        self,
        model_name: str,
        entity_id: Any,
        schema_type: type[T],
        *,
        bind_group: str | None = None,
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> T | None:
        """Asynchronously decode a cached entity directly into `schema_type` without ORM instantiation."""
        key = f"{model_name}:{bind_group}:get:{entity_id}" if bind_group else f"{model_name}:get:{entity_id}"
        cached = await self.get_async(key)
        if cached is DOGPILE_NO_VALUE or cached is NO_VALUE or not isinstance(cached, (bytes, bytearray)):
            return None
        return msgspec_model_deserializer(bytes(cached), schema_type, format=format)

    def get_many_schema_sync(
        self,
        key: str,
        schema_type: type[T],
        *,
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> list[T] | None:
        """Decode a cached entity list directly into `list[schema_type]` without ORM instantiation."""
        cached = self.get_sync(key)
        if cached is DOGPILE_NO_VALUE or cached is NO_VALUE or not isinstance(cached, list):
            return None
        results: list[T] = []
        for item in cached:
            if not isinstance(item, str):
                return None
            raw = base64.b64decode(item.encode("ascii"))
            results.append(msgspec_model_deserializer(raw, schema_type, format=format))
        return results

    async def get_many_schema_async(
        self,
        key: str,
        schema_type: type[T],
        *,
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> list[T] | None:
        """Asynchronously decode a cached entity list directly into `list[schema_type]`."""
        cached = await self.get_async(key)
        if cached is DOGPILE_NO_VALUE or cached is NO_VALUE or not isinstance(cached, list):
            return None
        results: list[T] = []
        for item in cached:
            if not isinstance(item, str):
                return None
            raw = base64.b64decode(item.encode("ascii"))
            results.append(msgspec_model_deserializer(raw, schema_type, format=format))
        return results


class ZeroCopyServiceMixin:
    """Mixin for Advanced Alchemy repository services accelerating `to_schema` via C-API vectorcall."""

    __slots__ = ()

    def to_schema(
        self,
        data: Any,
        *args: Any,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Convert ORM model instance(s) into `schema_type` using `orm_to_struct_vectorcall` when eligible."""
        if schema_type is not None and not args and not kwargs:
            plan = ZeroCopyCodecRegistry.compile(schema_type)
            if plan.kind in ("msgspec", "dataclass", "attrs"):
                if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
                    return plan.orm_to_struct(list(data))
                if not isinstance(data, Mapping) and hasattr(data, "__dict__"):
                    return plan.orm_to_struct(data)
        super_to_schema = getattr(super(), "to_schema", None)
        if callable(super_to_schema):
            return super_to_schema(data, *args, schema_type=schema_type, **kwargs)
        if schema_type is not None:
            return msgspec.convert(data, type=schema_type, from_attributes=True)
        return data


def create_cache_config(
    *,
    url: str | None = None,
    expiration_time: int = 3600,
    key_prefix: str = "vtb:aa:",
    capacity: int = 2048,
    shm_name: str | None = None,
    format: Literal["msgpack", "json"] = "msgpack",
    backend: SQLAlchemyCacheBackend | None = None,
) -> CacheConfig:
    """Create a pre-wired Advanced Alchemy CacheConfig backed by SQLAlchemyCacheBackend and msgspec."""
    resolved_backend = (
        backend
        if backend is not None
        else SQLAlchemyCacheBackend.from_url(
            url,
            capacity=capacity,
            default_ttl=float(expiration_time) if expiration_time > 0 else None,
            shm_name=shm_name,
            region_mode=True,
            key_prefix=key_prefix,
        )
    )
    resolved_backend._region_mode = True

    def _region_factory(_cfg: CacheConfig) -> SQLAlchemyCacheBackend:
        return resolved_backend

    return CacheConfig(
        backend=url or "vertebra.memory",
        expiration_time=expiration_time,
        arguments={
            "url": url,
            "capacity": capacity,
            "shm_name": shm_name,
            "key_prefix": key_prefix,
        },
        key_prefix=key_prefix,
        enabled=True,
        serializer=partial(msgspec_model_serializer, format=format),
        deserializer=partial(msgspec_model_deserializer, format=format),
        region_factory=_region_factory,
    )
