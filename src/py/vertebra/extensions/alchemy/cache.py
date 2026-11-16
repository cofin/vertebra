"""Unified sync and async cache backend, CacheManager, and service mixin for SQLAlchemy and Advanced Alchemy."""

from __future__ import annotations

import base64
import collections
import contextlib
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
from typing import Any, Literal, Self, cast

import msgspec
from sqlalchemy import inspect as sa_inspect

from vertebra._vertebra import ShmCache
from vertebra.db.adapters.redis import AsyncRedisDriver, RedisConfig, SyncRedisDriver
from vertebra.db.adapters.valkey import AsyncValkeyDriver, SyncValkeyDriver, ValkeyConfig
from vertebra.storage import AsyncObjectStore, SyncObjectStore
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

try:
    import dogpile.cache as _dogpile_cache
    import dogpile.cache.api as _dogpile_api

    register_backend: Any = _dogpile_cache.register_backend
    DogpileBackendApi: Any = _dogpile_api.CacheBackend
    _HAS_DOGPILE = True
except ImportError:
    register_backend = None
    DogpileBackendApi = object
    _HAS_DOGPILE = False

__all__ = (
    "AsyncCacheBackend",
    "AsyncCacheManager",
    "CacheConfig",
    "CacheEntry",
    "CacheStats",
    "DogpileMemoryBackend",
    "DogpileRedisBackend",
    "DogpileShmBackend",
    "DogpileStorageBackend",
    "DogpileValkeyBackend",
    "NO_VALUE",
    "SQLAlchemyCacheBackend",
    "SyncCacheBackend",
    "ZeroCopyServiceMixin",
    "create_cache_config",
    "msgspec_model_deserializer",
    "msgspec_model_serializer",
    "register_dogpile_backends",
    "setup_cache_listeners",
)

_MODEL_KEY = "__aa_model__"
_TABLE_KEY = "__aa_table__"
_MSGPACK_MAGIC = b"\x00vtb_mp\x00"
_BYTES_MAGIC = b"\x00vtb_by\x00"
_STR_MAGIC = b"\x00vtb_st\x00"
_REMOTE_OR_STORAGE_PREFIXES = (
    "valkey://",
    "valkeys://",
    "redis://",
    "rediss://",
    "s3://",
    "gs://",
    "az://",
    "r2://",
    "oci://",
    "file://",
)


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


class SyncCacheBackend:
    """Thread-safe synchronous cache backend for Advanced Alchemy and Vertebra.

    Implements Advanced Alchemy's SyncCacheRegionProtocol over POSIX shared memory (shm://),
    in-process LRU memory (memory://), Valkey/Redis drivers, or ObjectStore.
    """

    __slots__ = (
        "_capacity",
        "_default_ttl",
        "_driver_prefix",
        "_entries",
        "_evictions",
        "_expirations",
        "_format",
        "_hits",
        "_key_prefix",
        "_l1",
        "_l1_url",
        "_lock",
        "_misses",
        "_region_mode",
        "_sets",
        "_shm",
        "_shm_name",
        "_store",
        "_sync_driver",
        "_url",
    )

    def __init__(
        self,
        url: str = "shm://vertebra-alchemy-cache",
        *,
        l1_url: str | None = None,
        capacity: int | None = 2048,
        default_ttl: float | timedelta | None = None,
        shm_name: str | None = None,
        key_prefix: str = "vtb:aa:",
        format: Literal["msgpack", "json"] = "msgpack",
        sync_driver: SyncValkeyDriver | SyncRedisDriver | None = None,
        region_mode: bool = False,
    ) -> None:
        """Initialize synchronous cache backend with URL scheme dispatch and optional L1 tier."""
        normalized_prefix, driver_prefix = self._normalize_key_prefix(key_prefix)
        self._url = url
        self._l1_url = l1_url
        self._capacity = capacity or 2048
        self._default_ttl = default_ttl.total_seconds() if isinstance(default_ttl, timedelta) else default_ttl
        self._shm_name = shm_name
        self._key_prefix = normalized_prefix
        self._driver_prefix = driver_prefix
        self._format = format
        self._region_mode = region_mode
        self._entries: collections.OrderedDict[Any, CacheEntry] = collections.OrderedDict()
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._expirations = 0
        self._sets = 0
        self._sync_driver = sync_driver
        self._store: Any = None
        self._shm: ShmCache | None = None

        if url.startswith("shm://"):
            segment = url.removeprefix("shm://").strip("/") or shm_name or "vertebra-alchemy-cache"
            self._shm = ShmCache(segment, capacity_bytes=self._capacity * 1024, max_entries=self._capacity)
        elif url.startswith(("valkey://", "valkeys://")) and self._sync_driver is None:
            self._sync_driver = SyncValkeyDriver(ValkeyConfig(url=url))
        elif url.startswith(("redis://", "rediss://")) and self._sync_driver is None:
            self._sync_driver = SyncRedisDriver(RedisConfig(url=url))
        elif url.startswith(("s3://", "gs://", "az://", "r2://", "oci://", "file://")):
            self._store = SyncObjectStore(url)

        if isinstance(self._sync_driver, SyncValkeyDriver):
            self._sync_driver.add_invalidation_listener(self._on_driver_invalidate)

        if l1_url is not None and not url.startswith(("shm://", "memory://")):
            self._l1: SyncCacheBackend | None = SyncCacheBackend(
                url=l1_url,
                capacity=self._capacity,
                default_ttl=self._default_ttl,
                key_prefix=key_prefix,
                format=format,
            )
        else:
            self._l1 = None

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
        """Namespace prefix applied to keys written to the L2 driver."""
        return self._key_prefix

    @property
    def sync_driver(self) -> SyncValkeyDriver | SyncRedisDriver | None:
        """Optional synchronous L2 driver."""
        return self._sync_driver

    @property
    def size(self) -> int:
        """Return the count of currently unexpired in-process cache entries."""
        with self._lock:
            now = time.monotonic()
            expired = [k for k, v in self._entries.items() if v.is_expired(now)]
            for k in expired:
                del self._entries[k]
                self._expirations += 1
            return len(self._entries)

    def keys(self) -> list[Any]:
        """Return an ordered snapshot of currently unexpired in-process cache keys."""
        with self._lock:
            now = time.monotonic()
            expired = [k for k, v in self._entries.items() if v.is_expired(now)]
            for k in expired:
                del self._entries[k]
                self._expirations += 1
            return list(self._entries.keys())

    def __len__(self) -> int:
        """Return the count of currently unexpired in-process cache entries."""
        return self.size

    def __iter__(self) -> Iterator[Any]:
        """Iterate over currently unexpired in-process cache keys."""
        return iter(self.keys())

    def stats(self) -> CacheStats:
        """Return snapshot of hit, miss, eviction, and size statistics."""
        with self._lock:
            current_size = self.size
            return CacheStats(
                hits=self._hits,
                misses=self._misses,
                evictions=self._evictions,
                expirations=self._expirations,
                size=current_size,
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
                self._sync_driver = SyncValkeyDriver(ValkeyConfig(url=url))
            elif url.startswith(("redis://", "rediss://")) and self._sync_driver is None:
                self._sync_driver = SyncRedisDriver(RedisConfig(url=url))
        return self

    def get(
        self,
        key: Any,
        default: Any = None,
        *,
        expiration_time: int | float | None = None,
    ) -> Any:
        """Retrieve value for key if present and unexpired, else return default (or NO_VALUE in region mode)."""
        miss_sentinel = NO_VALUE if (default is None and self._region_mode) else default

        if self._l1 is not None and isinstance(key, str):
            l1_val = self._l1.get(key, default=NO_VALUE)
            if l1_val is not NO_VALUE and l1_val is not DOGPILE_NO_VALUE:
                return l1_val

        if self._shm is not None and isinstance(key, str):
            raw = self._shm.get(self._driver_key(key))
            if raw is not None:
                decoded = self._decode_driver_payload(raw)
                with self._lock:
                    self._hits += 1
                if self._l1 is not None:
                    self._l1.set(key, decoded, expiration_time=expiration_time)
                return decoded

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
                if self._l1 is not None:
                    self._l1.set(key, decoded, expiration_time=expiration_time)
                return decoded

        if self._store is not None and isinstance(key, str):
            store_key = f"{self._driver_prefix}~{key.encode('utf-8').hex()}"
            try:
                raw_bytes = self._store.get(store_key)
                if raw_bytes.startswith(b"VTBSTORE\x01"):
                    deadline_ms, payload = decode_msgpack(raw_bytes[9:], type=tuple[int | None, bytes])
                    now_ms = math.floor(time.time() * 1000)
                    if deadline_ms is not None and now_ms >= deadline_ms:
                        self._store.delete(store_key)
                    else:
                        decoded = self._decode_driver_payload(payload)
                        with self._lock:
                            self._hits += 1
                        if self._l1 is not None:
                            self._l1.set(key, decoded, expiration_time=expiration_time)
                        return decoded
            except Exception:
                pass

        with self._lock:
            self._misses += 1
        return miss_sentinel

    def get_or_create[T](
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
        self.set(key, created, expiration_time=expiration_time)
        return created

    def set(
        self,
        key: Any,
        value: Any,
        *,
        ttl: float | int | timedelta | None = None,
        expiration_time: int | float | timedelta | None = None,
    ) -> None:
        """Store key and value with optional expiration time or TTL."""
        effective_ttl = ttl if ttl is not None else expiration_time
        expires_at = self._resolve_expires_at(effective_ttl)

        with self._lock:
            self._entries[key] = CacheEntry(value=value, expires_at=expires_at)
            self._entries.move_to_end(key)
            self._evict_if_needed()
            self._sets += 1

        if self._shm is not None and isinstance(key, str):
            dkey = self._driver_key(key)
            if len(dkey) <= 128:
                payload = self._encode_driver_payload(value)
                if len(payload) <= 856:
                    shm_ttl = None
                    if effective_ttl is not None:
                        sec = (
                            effective_ttl.total_seconds()
                            if isinstance(effective_ttl, timedelta)
                            else float(effective_ttl)
                        )
                        shm_ttl = sec if sec > 0 else None
                    self._shm.set(dkey, payload, ttl=shm_ttl)
                else:
                    self._shm.delete(dkey)

        if self._sync_driver is not None and isinstance(key, str):
            px_ms = self._resolve_driver_px(effective_ttl)
            dkey = self._driver_key(key)
            payload = self._encode_driver_payload(value)
            if px_ms is not None:
                self._sync_driver.set(dkey, payload, px=px_ms)
            else:
                self._sync_driver.set(dkey, payload)

        if self._store is not None and isinstance(key, str):
            store_key = f"{self._driver_prefix}~{key.encode('utf-8').hex()}"
            deadline_ms = None
            if effective_ttl is not None:
                sec = effective_ttl.total_seconds() if isinstance(effective_ttl, timedelta) else float(effective_ttl)
                if sec > 0:
                    deadline_ms = math.floor((time.time() + sec) * 1000)
            payload = self._encode_driver_payload(value)
            envelope = b"VTBSTORE\x01" + bytes(encode_msgpack_bytes((deadline_ms, payload)))
            attrs = {"vertebra-expires-at-ms": str(deadline_ms)} if deadline_ms is not None else {}
            self._store.put(store_key, envelope, attributes=attrs)

        if self._l1 is not None and isinstance(key, str):
            self._l1.set(key, value, expiration_time=effective_ttl)

    def delete(self, key: Any) -> bool:
        """Remove key from cache and return True if removed."""
        with self._lock:
            entry = self._entries.pop(key, None)
            removed = entry is not None

        if self._shm is not None and isinstance(key, str):
            shm_del = self._shm.delete(self._driver_key(key))
            removed = removed or shm_del

        if self._sync_driver is not None and isinstance(key, str):
            deleted_count = self._sync_driver.delete(self._driver_key(key))
            removed = removed or (deleted_count > 0)

        if self._store is not None and isinstance(key, str):
            store_key = f"{self._driver_prefix}~{key.encode('utf-8').hex()}"
            try:
                existed = bool(self._store.exists(store_key))
                self._store.delete(store_key)
                removed = removed or existed
            except Exception:
                pass

        if self._l1 is not None and isinstance(key, str):
            self._l1.delete(key)

        return removed

    def invalidate(self, key: str) -> None:
        """Invalidate key in accordance with SyncCacheRegionProtocol."""
        self.delete(key)

    def get_multi(
        self,
        keys: Sequence[str],
        expiration_time: int | None = None,
    ) -> list[Any]:
        """Retrieve multiple keys, returning NO_VALUE for missing entries."""
        return [self.get(key, default=NO_VALUE, expiration_time=expiration_time) for key in keys]

    def set_multi(
        self,
        mapping: Mapping[str, Any],
        expiration_time: int | float | timedelta | None = None,
    ) -> None:
        """Store multiple key-value pairs."""
        for key, value in mapping.items():
            self.set(key, value, expiration_time=expiration_time)

    def delete_multi(self, keys: Sequence[str]) -> None:
        """Delete multiple keys from the cache."""
        for key in keys:
            self.delete(key)

    def add(
        self,
        key: str,
        value: Any,
        expiration_time: int | float | timedelta | None = None,
    ) -> bool:
        """Store key only if it does not already exist."""
        if self.has_key(key):
            return False
        self.set(key, value, expiration_time=expiration_time)
        return True

    def incr(self, key: str, delta: int = 1) -> int:
        """Atomically increment integer value associated with key."""
        if self._sync_driver is not None:
            res = self._sync_driver.execute_command("INCRBY", self._driver_key(key), delta)
            return int(res)
        if self._shm is not None:
            return self._shm.incr(self._driver_key(key), delta)
        with self._lock:
            cur = self.get(key, default=0)
            try:
                new_val = int(cur) + delta
            except (ValueError, TypeError):
                new_val = delta
            self.set(key, new_val)
            return new_val

    def decr(self, key: str, delta: int = 1) -> int:
        """Atomically decrement integer value associated with key."""
        return self.incr(key, -delta)

    def touch(
        self,
        key: str,
        expiration_time: int | float | timedelta | None = None,
    ) -> bool:
        """Update expiration timestamp for key without modifying value."""
        if self._shm is not None:
            sec = None
            if expiration_time is not None:
                raw_sec = (
                    expiration_time.total_seconds()
                    if isinstance(expiration_time, timedelta)
                    else float(expiration_time)
                )
                sec = raw_sec if raw_sec > 0 else None
            return self._shm.touch(self._driver_key(key), ttl=sec)
        if self._sync_driver is not None:
            px_ms = self._resolve_driver_px(expiration_time)
            if px_ms is not None:
                return bool(self._sync_driver.execute_command("PEXPIRE", self._driver_key(key), px_ms))
            if expiration_time is not None:
                raw_sec = (
                    expiration_time.total_seconds()
                    if isinstance(expiration_time, timedelta)
                    else float(expiration_time)
                )
                if raw_sec < 0:
                    dkey = self._driver_key(key)
                    if self._sync_driver.get(dkey) is None:
                        return False
                    self._sync_driver.execute_command("PERSIST", dkey)
                    return True
        with self._lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            if entry is not None and not entry.is_expired(now):
                entry.expires_at = self._resolve_expires_at(expiration_time)
                return True
        return False

    def has_key(self, key: str) -> bool:
        """Return True if unexpired key exists in cache."""
        val = self.get(key, default=NO_VALUE)
        return val is not NO_VALUE and val is not DOGPILE_NO_VALUE

    def invalidate_prefix(self, prefix: str) -> int:
        """Invalidate all entries matching prefix and return count removed."""
        count = 0
        if self._shm is not None:
            count += self._shm.invalidate_prefix(self._driver_key(prefix))

        if self._sync_driver is not None:
            driver_pref = self._driver_key(prefix)
            cursor = 0
            keys: list[str] = []
            while True:
                cursor, batch = self._sync_driver.scan(cursor, match=f"{driver_pref}*")
                keys.extend(batch)
                if cursor == 0:
                    break
            if keys:
                for k in keys:
                    self._sync_driver.delete(k)
                count += len(keys)

        with self._lock:
            to_remove = [k for k in self._entries if isinstance(k, str) and k.startswith(prefix)]
            for k in to_remove:
                del self._entries[k]
                count += 1

        if self._l1 is not None:
            count += self._l1.invalidate_prefix(prefix)

        return count

    def clear(self) -> None:
        """Clear all entries matching backend key_prefix."""
        with self._lock:
            self._entries.clear()

        if self._shm is not None:
            self._shm.clear()

        if self._sync_driver is not None:
            cursor = 0
            keys = []
            while True:
                cursor, batch = self._sync_driver.scan(cursor, match=f"{self._driver_prefix}*")
                keys.extend(batch)
                if cursor == 0:
                    break
            for k in keys:
                self._sync_driver.delete(k)

        if self._store is not None:
            try:
                for obj in self._store.list():
                    obj_path = str(getattr(obj, "path", obj))
                    if obj_path.startswith(self._driver_prefix):
                        self._store.delete(obj_path)
            except Exception:
                pass

        if self._l1 is not None:
            self._l1.clear()

    def close(self) -> None:
        """Close driver connections."""
        if self._sync_driver is not None and hasattr(self._sync_driver, "close"):
            self._sync_driver.close()

    def _driver_key(self, key: str) -> str:
        """Return prefix-qualified key for L2 driver."""
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
        """Serialize arbitrary Python cache values into framed bytes for L2 and SHM storage."""
        if isinstance(value, bytes):
            return _BYTES_MAGIC + value
        if isinstance(value, bytearray):
            return _BYTES_MAGIC + bytes(value)
        if isinstance(value, str):
            return _STR_MAGIC + value.encode("utf-8")
        return _MSGPACK_MAGIC + bytes(encode_msgpack_bytes(value))

    @staticmethod
    def _decode_driver_payload(raw: bytes) -> Any:
        """Deserialize framed bytes retrieved from L2 or SHM storage back into the original value."""
        if raw.startswith(_BYTES_MAGIC):
            return raw[len(_BYTES_MAGIC) :]
        if raw.startswith(_STR_MAGIC):
            return raw[len(_STR_MAGIC) :].decode("utf-8")
        if raw.startswith(_MSGPACK_MAGIC):
            return decode_msgpack(raw[len(_MSGPACK_MAGIC) :])
        return raw


class AsyncCacheBackend:
    """Thread-safe asynchronous cache backend for Advanced Alchemy and Vertebra.

    Executes operations directly on the event loop over async Valkey/Redis drivers,
    async ObjectStore, in-process LRU, or POSIX shared memory.
    """

    __slots__ = (
        "_async_driver",
        "_async_store",
        "_capacity",
        "_default_ttl",
        "_driver_prefix",
        "_format",
        "_key_prefix",
        "_l1",
        "_l1_url",
        "_shm_name",
        "_sync_backend",
        "_url",
    )

    def __init__(
        self,
        url: str = "shm://vertebra-alchemy-cache",
        *,
        sync_backend: SyncCacheBackend | None = None,
        l1_url: str | None = None,
        capacity: int | None = 2048,
        default_ttl: float | timedelta | None = None,
        shm_name: str | None = None,
        key_prefix: str = "vtb:aa:",
        format: Literal["msgpack", "json"] = "msgpack",
        async_driver: AsyncValkeyDriver | AsyncRedisDriver | None = None,
        async_store: Any | None = None,
    ) -> None:
        """Initialize asynchronous cache backend with async drivers and sync fallback."""
        normalized_prefix, driver_prefix = SyncCacheBackend._normalize_key_prefix(key_prefix)
        self._url = url
        self._l1_url = l1_url
        self._capacity = capacity or 2048
        self._default_ttl = default_ttl.total_seconds() if isinstance(default_ttl, timedelta) else default_ttl
        self._shm_name = shm_name
        self._key_prefix = normalized_prefix
        self._driver_prefix = driver_prefix
        self._format = format
        self._async_driver = async_driver
        self._async_store = async_store
        is_remote_or_storage = url.startswith(_REMOTE_OR_STORAGE_PREFIXES)
        sync_fallback_url = (l1_url or "memory://") if is_remote_or_storage else url
        self._sync_backend = (
            sync_backend
            if sync_backend is not None
            else SyncCacheBackend(
                url=sync_fallback_url,
                capacity=capacity,
                default_ttl=default_ttl,
                shm_name=shm_name,
                key_prefix=key_prefix,
                format=format,
            )
        )

        if url.startswith(("valkey://", "valkeys://")) and self._async_driver is None:
            self._async_driver = AsyncValkeyDriver(ValkeyConfig(url=url))
        elif url.startswith(("redis://", "rediss://")) and self._async_driver is None:
            self._async_driver = AsyncRedisDriver(RedisConfig(url=url))
        elif url.startswith(("s3://", "gs://", "az://", "r2://", "oci://", "file://")) and self._async_store is None:
            self._async_store = AsyncObjectStore(url)

        if l1_url is not None and not url.startswith(("shm://", "memory://")):
            if sync_backend is None and self._sync_backend._url == l1_url:
                self._l1: SyncCacheBackend | None = self._sync_backend
            else:
                self._l1 = SyncCacheBackend(
                    url=l1_url,
                    capacity=self._capacity,
                    default_ttl=self._default_ttl,
                    key_prefix=key_prefix,
                    format=format,
                )
        else:
            self._l1 = None

    @property
    def key_prefix(self) -> str:
        """Namespace prefix applied to keys written to the L2 driver."""
        return self._key_prefix

    @property
    def async_driver(self) -> AsyncValkeyDriver | AsyncRedisDriver | None:
        """Optional asynchronous L2 driver."""
        return self._async_driver

    def stats(self) -> CacheStats:
        """Return snapshot of hit, miss, eviction, and size statistics."""
        return self._sync_backend.stats()

    async def get(
        self,
        key: str,
        default: Any = NO_VALUE,
        *,
        expiration_time: int | None = None,
    ) -> Any:
        """Retrieve value for key if present and unexpired, else return default."""
        if self._l1 is not None:
            l1_val = self._l1.get(key, default=NO_VALUE)
            if l1_val is not NO_VALUE and l1_val is not DOGPILE_NO_VALUE:
                return l1_val

        if self._async_driver is not None:
            raw = await self._async_driver.get(self._sync_backend._driver_key(key))
            if raw is not None:
                decoded = self._sync_backend._decode_driver_payload(raw)
                if self._l1 is not None:
                    self._l1.set(key, decoded, expiration_time=expiration_time)
                return decoded
            return default

        if self._async_store is not None:
            store_key = f"{self._driver_prefix}~{key.encode('utf-8').hex()}"
            try:
                raw_bytes = await self._async_store.get(store_key)
                if raw_bytes.startswith(b"VTBSTORE\x01"):
                    deadline_ms, payload = decode_msgpack(raw_bytes[9:], type=tuple[int | None, bytes])
                    now_ms = math.floor(time.time() * 1000)
                    if deadline_ms is not None and now_ms >= deadline_ms:
                        await self._async_store.delete(store_key)
                    else:
                        decoded = self._sync_backend._decode_driver_payload(payload)
                        if self._l1 is not None:
                            self._l1.set(key, decoded, expiration_time=expiration_time)
                        return decoded
            except Exception:
                pass
            return default

        return self._sync_backend.get(key, default=default, expiration_time=expiration_time)

    async def get_or_create[T](
        self,
        key: str,
        creator: Callable[[], T | Awaitable[T]],
        expiration_time: int | None = None,
    ) -> T:
        """Retrieve cached value for key or await/call creator on miss."""
        marker = object()
        cached = await self.get(key, default=marker, expiration_time=expiration_time)
        if cached is not marker and cached is not NO_VALUE and cached is not DOGPILE_NO_VALUE:
            return cast("T", cached)
        candidate = creator()
        created: T = await candidate if inspect.isawaitable(candidate) else candidate
        await self.set(key, created, expiration_time=expiration_time)
        return created

    async def set(
        self,
        key: str,
        value: Any,
        expiration_time: int | float | timedelta | None = None,
    ) -> None:
        """Store key and value with optional expiration time or TTL."""
        if self._async_driver is not None:
            px_ms = self._sync_backend._resolve_driver_px(expiration_time)
            dkey = self._sync_backend._driver_key(key)
            payload = self._sync_backend._encode_driver_payload(value)
            if px_ms is not None:
                await self._async_driver.set(dkey, payload, px=px_ms)
            else:
                await self._async_driver.set(dkey, payload)

        if self._async_store is not None:
            store_key = f"{self._driver_prefix}~{key.encode('utf-8').hex()}"
            deadline_ms = None
            if expiration_time is not None:
                sec = (
                    expiration_time.total_seconds()
                    if isinstance(expiration_time, timedelta)
                    else float(expiration_time)
                )
                if sec > 0:
                    deadline_ms = math.floor((time.time() + sec) * 1000)
            payload = self._sync_backend._encode_driver_payload(value)
            envelope = b"VTBSTORE\x01" + bytes(encode_msgpack_bytes((deadline_ms, payload)))
            attrs = {"vertebra-expires-at-ms": str(deadline_ms)} if deadline_ms is not None else {}
            await self._async_store.put(store_key, envelope, attributes=attrs)

        if self._l1 is not None:
            self._l1.set(key, value, expiration_time=expiration_time)

        if (
            self._sync_backend is not self._l1
            and self._sync_backend._sync_driver is None
            and self._sync_backend._store is None
        ):
            self._sync_backend.set(key, value, expiration_time=expiration_time)

    async def delete(self, key: str) -> bool:
        """Remove key from async drivers and sync backend."""
        removed = False
        if self._async_driver is not None:
            del_count = await self._async_driver.delete(self._sync_backend._driver_key(key))
            removed = del_count > 0

        if self._async_store is not None:
            store_key = f"{self._driver_prefix}~{key.encode('utf-8').hex()}"
            try:
                existed = bool(await self._async_store.exists(store_key))
                await self._async_store.delete(store_key)
                removed = existed
            except Exception:
                pass

        if self._l1 is not None:
            removed = self._l1.delete(key) or removed

        if (
            self._sync_backend is not self._l1
            and self._sync_backend._sync_driver is None
            and self._sync_backend._store is None
        ):
            return self._sync_backend.delete(key) or removed
        return removed

    async def get_multi(
        self,
        keys: Sequence[str],
        expiration_time: int | None = None,
    ) -> list[Any]:
        """Retrieve multiple keys asynchronously."""
        results: list[Any] = []
        for key in keys:
            results.append(await self.get(key, default=NO_VALUE, expiration_time=expiration_time))
        return results

    async def set_multi(
        self,
        mapping: Mapping[str, Any],
        expiration_time: int | float | timedelta | None = None,
    ) -> None:
        """Store multiple key-value pairs asynchronously."""
        for key, value in mapping.items():
            await self.set(key, value, expiration_time=expiration_time)

    async def delete_multi(self, keys: Sequence[str]) -> None:
        """Delete multiple keys asynchronously."""
        for key in keys:
            await self.delete(key)

    async def add(
        self,
        key: str,
        value: Any,
        expiration_time: int | float | timedelta | None = None,
    ) -> bool:
        """Store key only if it does not already exist."""
        if await self.has_key(key):
            return False
        await self.set(key, value, expiration_time=expiration_time)
        return True

    async def incr(self, key: str, delta: int = 1) -> int:
        """Atomically increment integer value associated with key."""
        if self._async_driver is not None:
            res = await self._async_driver.execute_command("INCRBY", self._sync_backend._driver_key(key), delta)
            return int(res)
        return self._sync_backend.incr(key, delta)

    async def decr(self, key: str, delta: int = 1) -> int:
        """Atomically decrement integer value associated with key."""
        return await self.incr(key, -delta)

    async def touch(
        self,
        key: str,
        expiration_time: int | float | timedelta | None = None,
    ) -> bool:
        """Update expiration timestamp for key without modifying value."""
        if self._async_driver is not None:
            dkey = self._sync_backend._driver_key(key)
            px_ms = self._sync_backend._resolve_driver_px(expiration_time)
            if px_ms is not None:
                res = await self._async_driver.execute_command("PEXPIRE", dkey, px_ms)
                if self._l1 is not None:
                    self._l1.touch(key, expiration_time=expiration_time)
                return bool(res)
            if expiration_time is not None:
                raw_sec = (
                    expiration_time.total_seconds()
                    if isinstance(expiration_time, timedelta)
                    else float(expiration_time)
                )
                if raw_sec < 0:
                    if await self._async_driver.get(dkey) is None:
                        return False
                    await self._async_driver.execute_command("PERSIST", dkey)
                    if self._l1 is not None:
                        self._l1.touch(key, expiration_time=expiration_time)
                    return True
            return False
        if self._async_store is not None:
            current = await self.get(key, default=NO_VALUE)
            if current is NO_VALUE or current is DOGPILE_NO_VALUE:
                return False
            await self.set(key, current, expiration_time=expiration_time)
            return True
        return self._sync_backend.touch(key, expiration_time=expiration_time)

    async def has_key(self, key: str) -> bool:
        """Return True if unexpired key exists in cache."""
        val = await self.get(key, default=NO_VALUE)
        return val is not NO_VALUE and val is not DOGPILE_NO_VALUE

    async def invalidate_prefix(self, prefix: str) -> int:
        """Invalidate all entries matching prefix and return count removed."""
        count = 0
        if self._async_driver is not None:
            driver_pref = self._sync_backend._driver_key(prefix)
            cursor = 0
            keys: list[str] = []
            while True:
                cursor, batch = await self._async_driver.scan(cursor, match=f"{driver_pref}*")
                keys.extend(batch)
                if cursor == 0:
                    break
            if keys:
                for k in keys:
                    await self._async_driver.delete(k)
                count += len(keys)

        if (
            self._sync_backend is not self._l1
            and self._sync_backend._sync_driver is None
            and self._sync_backend._store is None
        ):
            count += self._sync_backend.invalidate_prefix(prefix)
        if self._l1 is not None:
            count += self._l1.invalidate_prefix(prefix)
        return count

    async def clear(self) -> None:
        """Clear all entries matching backend key_prefix."""
        if self._async_driver is not None:
            cursor = 0
            keys = []
            while True:
                cursor, batch = await self._async_driver.scan(cursor, match=f"{self._driver_prefix}*")
                keys.extend(batch)
                if cursor == 0:
                    break
            for k in keys:
                await self._async_driver.delete(k)

        if self._async_store is not None:
            try:
                for obj in await self._async_store.list():
                    obj_path = str(getattr(obj, "path", obj))
                    if obj_path.startswith(self._driver_prefix):
                        await self._async_store.delete(obj_path)
            except Exception:
                pass

        if self._l1 is not None:
            self._l1.clear()

        if (
            self._sync_backend is not self._l1
            and self._sync_backend._sync_driver is None
            and self._sync_backend._store is None
        ):
            self._sync_backend.clear()

    async def aclose(self) -> None:
        """Close driver connections."""
        if self._async_driver is not None and hasattr(self._async_driver, "close"):
            await self._async_driver.close()
        self._sync_backend.close()


class DogpileShmBackend(DogpileBackendApi):
    """Dogpile cache backend adapter over POSIX shared memory (shm://)."""

    def __init__(self, arguments: dict[str, Any]) -> None:
        args = dict(arguments)
        raw_url = args.pop("url", None)
        url = raw_url or f"shm://{args.get('shm_name') or 'vertebra-alchemy-cache'}"
        self.backend = SyncCacheBackend(url, **args)

    def get(self, key: str) -> Any:
        return self.backend.get(key)

    def set(self, key: str, value: Any) -> None:
        self.backend.set(key, value)

    def delete(self, key: str) -> None:
        self.backend.delete(key)

    def get_multi(self, keys: Sequence[str]) -> list[Any]:
        return self.backend.get_multi(keys)

    def set_multi(self, mapping: Mapping[str, Any]) -> None:
        self.backend.set_multi(mapping)

    def delete_multi(self, keys: Sequence[str]) -> None:
        self.backend.delete_multi(keys)


class DogpileMemoryBackend(DogpileBackendApi):
    """Dogpile cache backend adapter over in-process memory (memory://)."""

    def __init__(self, arguments: dict[str, Any]) -> None:
        args = dict(arguments)
        args.pop("url", None)
        self.backend = SyncCacheBackend("memory://", **args)

    def get(self, key: str) -> Any:
        return self.backend.get(key)

    def set(self, key: str, value: Any) -> None:
        self.backend.set(key, value)

    def delete(self, key: str) -> None:
        self.backend.delete(key)

    def get_multi(self, keys: Sequence[str]) -> list[Any]:
        return self.backend.get_multi(keys)

    def set_multi(self, mapping: Mapping[str, Any]) -> None:
        self.backend.set_multi(mapping)

    def delete_multi(self, keys: Sequence[str]) -> None:
        self.backend.delete_multi(keys)


class DogpileValkeyBackend(DogpileBackendApi):
    """Dogpile cache backend adapter over native Valkey."""

    def __init__(self, arguments: dict[str, Any]) -> None:
        args = dict(arguments)
        url = args.pop("url", None) or "valkey://localhost:6379/0"
        self.backend = SyncCacheBackend(url, **args)

    def get(self, key: str) -> Any:
        return self.backend.get(key)

    def set(self, key: str, value: Any) -> None:
        self.backend.set(key, value)

    def delete(self, key: str) -> None:
        self.backend.delete(key)

    def get_multi(self, keys: Sequence[str]) -> list[Any]:
        return self.backend.get_multi(keys)

    def set_multi(self, mapping: Mapping[str, Any]) -> None:
        self.backend.set_multi(mapping)

    def delete_multi(self, keys: Sequence[str]) -> None:
        self.backend.delete_multi(keys)


class DogpileRedisBackend(DogpileBackendApi):
    """Dogpile cache backend adapter over native Redis."""

    def __init__(self, arguments: dict[str, Any]) -> None:
        args = dict(arguments)
        url = args.pop("url", None) or "redis://localhost:6379/0"
        self.backend = SyncCacheBackend(url, **args)

    def get(self, key: str) -> Any:
        return self.backend.get(key)

    def set(self, key: str, value: Any) -> None:
        self.backend.set(key, value)

    def delete(self, key: str) -> None:
        self.backend.delete(key)

    def get_multi(self, keys: Sequence[str]) -> list[Any]:
        return self.backend.get_multi(keys)

    def set_multi(self, mapping: Mapping[str, Any]) -> None:
        self.backend.set_multi(mapping)

    def delete_multi(self, keys: Sequence[str]) -> None:
        self.backend.delete_multi(keys)


class DogpileStorageBackend(DogpileBackendApi):
    """Dogpile cache backend adapter over native ObjectStore."""

    def __init__(self, arguments: dict[str, Any]) -> None:
        args = dict(arguments)
        url = args.pop("url", None) or "file:///tmp/vertebra-cache"
        self.backend = SyncCacheBackend(url, **args)

    def get(self, key: str) -> Any:
        return self.backend.get(key)

    def set(self, key: str, value: Any) -> None:
        self.backend.set(key, value)

    def delete(self, key: str) -> None:
        self.backend.delete(key)

    def get_multi(self, keys: Sequence[str]) -> list[Any]:
        return self.backend.get_multi(keys)

    def set_multi(self, mapping: Mapping[str, Any]) -> None:
        self.backend.set_multi(mapping)

    def delete_multi(self, keys: Sequence[str]) -> None:
        self.backend.delete_multi(keys)


def register_dogpile_backends() -> None:
    """Register Vertebra dogpile.cache backends if dogpile.cache is installed."""
    if not _HAS_DOGPILE or register_backend is None:
        return
    for name, target in (
        ("vertebra.shm", DogpileShmBackend),
        ("vertebra.memory", DogpileMemoryBackend),
        ("vertebra.valkey", DogpileValkeyBackend),
        ("vertebra.redis", DogpileRedisBackend),
        ("vertebra.storage", DogpileStorageBackend),
    ):
        with contextlib.suppress(Exception):
            register_backend(name, "vertebra.extensions.alchemy.cache", target.__name__)


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

    @property
    def size(self) -> int:
        """Return count of currently unexpired in-process cache entries."""
        return len(self)

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

    def get_or_create[T](
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

    async def get_or_create_async[T](
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
        """Serialize arbitrary Python cache values into framed bytes for L2 storage."""
        return SyncCacheBackend._encode_driver_payload(value)

    @staticmethod
    def _decode_driver_payload(raw: bytes) -> Any:
        """Deserialize framed bytes retrieved from L2 storage back into the original value."""
        return SyncCacheBackend._decode_driver_payload(raw)


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

    async def get_or_create_async[T](
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

    async def get_entity_async[T](
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

    async def get_many_async[T](self, key: str, model_class: type[T]) -> list[T] | None:
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

    async def get_many_and_count_async[T](
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

    def get_schema_sync[T](
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

    async def get_schema_async[T](
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

    def get_many_schema_sync[T](
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

    async def get_many_schema_async[T](
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
    url: str | None = "shm://vertebra-alchemy-cache",
    *,
    l1_url: str | None = None,
    backend: SyncCacheBackend | AsyncCacheBackend | SQLAlchemyCacheBackend | None = None,
    expiration_time: int = 3600,
    use_dogpile: bool = False,
    key_prefix: str = "vtb:aa:",
    capacity: int = 2048,
    shm_name: str | None = None,
    format: Literal["msgpack", "json"] = "msgpack",
) -> CacheConfig:
    """Create a pre-wired Advanced Alchemy CacheConfig backed by SyncCacheBackend/SQLAlchemyCacheBackend and msgspec."""
    resolved_url = url or "vertebra.memory"
    if use_dogpile:
        register_dogpile_backends()
        dogpile_name = "vertebra.memory"
        if resolved_url.startswith("shm://"):
            dogpile_name = "vertebra.shm"
        elif resolved_url.startswith(("valkey://", "valkeys://")):
            dogpile_name = "vertebra.valkey"
        elif resolved_url.startswith(("redis://", "rediss://")):
            dogpile_name = "vertebra.redis"
        elif resolved_url.startswith(("s3://", "gs://", "az://", "r2://", "oci://", "file://")):
            dogpile_name = "vertebra.storage"

        return CacheConfig(
            backend=dogpile_name,
            expiration_time=expiration_time,
            arguments={
                "url": resolved_url,
                "l1_url": l1_url,
                "capacity": capacity,
                "shm_name": shm_name,
                "key_prefix": key_prefix,
            },
            key_prefix=key_prefix,
            enabled=True,
            serializer=partial(msgspec_model_serializer, format=format),
            deserializer=partial(msgspec_model_deserializer, format=format),
        )

    resolved_backend: SyncCacheBackend | SQLAlchemyCacheBackend
    if isinstance(backend, (SyncCacheBackend, SQLAlchemyCacheBackend)):
        resolved_backend = backend
    elif isinstance(backend, AsyncCacheBackend):
        resolved_backend = backend._sync_backend
    else:
        resolved_backend = SyncCacheBackend(
            resolved_url,
            capacity=capacity,
            default_ttl=float(expiration_time) if expiration_time > 0 else None,
            shm_name=shm_name,
            key_prefix=key_prefix,
            l1_url=l1_url,
            region_mode=True,
        )
    resolved_backend._region_mode = True

    def _region_factory(_cfg: CacheConfig) -> Any:
        return resolved_backend

    return CacheConfig(
        backend=resolved_url,
        expiration_time=expiration_time,
        arguments={
            "url": resolved_url,
            "l1_url": l1_url,
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
