"""Vertebra Store adapter for Litestar response caching and AsyncCacheStore-backed KV stores."""

from __future__ import annotations

import math
import os
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Self, cast

from vertebra.cache import AsyncCacheStore
from vertebra.db.adapters.redis import AsyncRedisDriver, RedisConfig
from vertebra.db.adapters.valkey import AsyncValkeyDriver, ValkeyConfig
from vertebra.storage import AsyncObjectStore, AsyncReadableFile, StorageConfig
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module, import_optional
from vertebra.utils.serializers import decode_msgpack, encode_msgpack_bytes
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from litestar.stores.base import Store as _LitestarStore

__all__ = (
    "MemoryStore",
    "ObjectStoreFileSystemAdapter",
    "ObjectStoreLitestarStore",
    "RedisStore",
    "ResponseCacheStore",
    "ValkeyStore",
    "swap_response_cache_store",
)

_CACHED_STORE_CLASSES: dict[str, type[Any]] = {}


class _AsyncCacheStoreAdapter:
    """Litestar Store adapter delegating key-value operations to ``AsyncCacheStore``."""

    __slots__ = ("_cache", "_client_tracking", "_driver", "_key_prefix", "_l1_cache")

    def __init__(
        self,
        *,
        url: str = "memory://",
        l1_url: str | None = None,
        driver: Any = None,
        key_prefix: str = "vertebra",
        client_tracking: bool = True,
        capacity: int = 2048,
    ) -> None:
        self._key_prefix = validate_identifier(key_prefix, "key_prefix", allow_hyphens=True)
        self._client_tracking = client_tracking
        self._l1_cache: dict[str, tuple[bytes, float | None]] = {}
        self._cache = AsyncCacheStore(
            url,
            l1_url=l1_url,
            capacity=capacity,
            key_prefix=f"{self._key_prefix}:",
        )
        if driver is not None:
            self._cache._state = None
            self._cache._driver = driver
            self._cache._scheme = "redis"
            if self._cache._l1 is None:
                self._cache._l1 = AsyncCacheStore(
                    "memory://",
                    capacity=capacity,
                    key_prefix=f"{self._key_prefix}:",
                )
        active_driver = driver if driver is not None else self._cache._driver
        self._driver = active_driver
        if self._client_tracking and active_driver is not None and hasattr(active_driver, "add_invalidation_listener"):
            active_driver.add_invalidation_listener(self._on_driver_invalidate)

    @property
    def driver(self) -> Any:
        """Return the underlying Valkey or Redis driver when configured."""
        return self._driver

    @property
    def key_prefix(self) -> str:
        """Return the namespace prepended to every stored key."""
        return self._key_prefix

    @property
    def _entries(self) -> dict[str, tuple[bytes, float | None]]:
        """Return the underlying L1 cache entries mapping."""
        return self._l1_cache

    async def __aenter__(self) -> Any:
        """Enter the async context manager and return the store instance."""
        return self

    async def __aexit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> None:
        """Exit the async context manager and close underlying driver."""
        _ = (exc_type, exc_val, exc_tb)
        await self.close()

    async def set(
        self,
        key: str,
        value: str | bytes,
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Store value under key with an optional expiration duration."""
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        ttl_ms = self._ttl_milliseconds(expires_in)
        ttl_s = None if ttl_ms is None else ttl_ms / 1000.0
        expires_at = None if ttl_s is None else time.monotonic() + ttl_s
        full_key = self._make_key(key)
        self._l1_cache[full_key] = (raw, expires_at)
        await self._cache.set(key, raw, ttl=ttl_s)

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Retrieve bytes for key if present and unexpired, optionally renewing TTL."""
        full_key = self._make_key(key)
        entry = self._l1_cache.get(full_key)
        now = time.monotonic()
        if entry is not None and entry[1] is not None and now >= entry[1]:
            self._l1_cache.pop(full_key, None)
            if self._cache._l1 is not None:
                await self._cache._l1.delete(key)

        value = await self._cache.get(key)
        if value is None:
            self._l1_cache.pop(full_key, None)
            return None

        if renew_for is not None:
            ttl_ms = self._ttl_milliseconds(renew_for)
            ttl_s = None if ttl_ms is None else ttl_ms / 1000.0
            remaining = await self._remaining_ttl_seconds(key)
            if remaining is not None and ttl_s is not None:
                await self._cache.touch(key, ttl=ttl_s)
                self._l1_cache[full_key] = (value, time.monotonic() + ttl_s)
        elif full_key not in self._l1_cache:
            remaining = await self._remaining_ttl_seconds(key)
            expires_at = None if remaining is None else time.monotonic() + remaining
            self._l1_cache[full_key] = (value, expires_at)
        return value

    async def delete(self, key: str) -> None:
        """Delete key from the store if present."""
        self._l1_cache.pop(self._make_key(key), None)
        await self._cache.delete(key)

    async def delete_all(self) -> None:
        """Delete every key under this store's key_prefix with prefix-scoped SCAN and UNLINK."""
        self._l1_cache.clear()
        await self._cache.clear()

    async def exists(self, key: str) -> bool:
        """Return True if key exists and has not expired."""
        return await self._cache.has(key)

    async def expires_in(self, key: str) -> int | None:
        """Return remaining TTL in seconds, or None when missing or expired."""
        remaining = await self._remaining_ttl_seconds(key)
        if remaining is None:
            return None
        return max(0, int(remaining))

    async def set_many(
        self,
        mapping: Mapping[str, str | bytes],
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Store multiple key-value pairs with an optional shared expiration duration."""
        for key, value in mapping.items():
            await self.set(key, value, expires_in=expires_in)

    async def get_many(self, keys: Sequence[str]) -> dict[str, bytes | None]:
        """Retrieve multiple keys in a single batch dictionary."""
        return {key: await self.get(key) for key in keys}

    async def close(self) -> None:
        """Close the underlying cache store and driver connections."""
        await self._cache.aclose()

    def _make_key(self, key: str) -> str:
        return f"{self._key_prefix}:{key}"

    @staticmethod
    def _ttl_milliseconds(expires_in: int | timedelta | None) -> int | None:
        if expires_in is None:
            return None
        seconds = expires_in.total_seconds() if isinstance(expires_in, timedelta) else float(expires_in)
        if seconds <= 0:
            raise ValueError("expires_in must be a positive duration")
        return math.ceil(seconds * 1000)

    async def _remaining_ttl_seconds(self, key: str) -> float | None:
        if self._cache._state is not None:
            hit = self._cache._state.get_with_expiry(key, record_stats=False)
            if hit is None or hit[1] is None:
                return None
            remaining = hit[1] - self._cache._clock()
            return None if remaining <= 0 else remaining
        if self._cache._l1 is not None and self._cache._l1._state is not None:
            hit = self._cache._l1._state.get_with_expiry(key, record_stats=False)
            if hit is not None:
                if hit[1] is None:
                    return None
                remaining = hit[1] - self._cache._l1._clock()
                if remaining > 0:
                    return remaining
                return None
        if self._cache._driver is not None and hasattr(self._cache._driver, "pttl"):
            pttl_ms = await self._cache._driver.pttl(self._make_key(key))
            if pttl_ms in (-2, -1):
                return None
            return max(0.0, pttl_ms / 1000.0)
        return None

    def _on_driver_invalidate(self, keys: Any) -> None:
        prefix = f"{self._key_prefix}:"
        for key in keys:
            str_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            self._l1_cache.pop(str_key, None)
            if self._cache._l1 is not None and self._cache._l1._state is not None:
                short_key = str_key[len(prefix) :] if str_key.startswith(prefix) else str_key
                self._cache._l1._state.delete(short_key)


class _MemoryStoreImpl(_AsyncCacheStoreAdapter):
    """Litestar store backed by AsyncCacheStore with optional Valkey/Redis backing."""

    __slots__ = ()

    def __init__(
        self,
        *,
        driver: Any = None,
        key_prefix: str = "vertebra",
        client_tracking: bool = False,
        capacity: int = 2048,
    ) -> None:
        super().__init__(
            url="memory://",
            driver=driver,
            key_prefix=key_prefix,
            client_tracking=client_tracking,
            capacity=capacity,
        )

    @classmethod
    def from_valkey(
        cls,
        config_or_url: ValkeyConfig | str,
        *,
        key_prefix: str = "vertebra",
        client_tracking: bool = True,
    ) -> Self:
        """Create a store backed by Valkey with client-side caching."""
        cfg = ValkeyConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(
            driver=AsyncValkeyDriver(cfg),
            key_prefix=key_prefix,
            client_tracking=client_tracking,
        )

    @classmethod
    def from_redis(
        cls,
        config_or_url: RedisConfig | str,
        *,
        key_prefix: str = "vertebra",
        client_tracking: bool = True,
    ) -> Self:
        """Create a store backed by Redis."""
        cfg = RedisConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(
            driver=AsyncRedisDriver(cfg),
            key_prefix=key_prefix,
            client_tracking=client_tracking,
        )


class _ValkeyStoreImpl(_AsyncCacheStoreAdapter):
    """Litestar-compatible Store backed by AsyncCacheStore and native Valkey driver."""

    __slots__ = ()

    def __init__(
        self,
        driver: AsyncValkeyDriver | None = None,
        config_or_url: ValkeyConfig | str | None = None,
        *,
        key_prefix: str = "vertebra",
        client_tracking: bool = True,
        capacity: int = 2048,
    ) -> None:
        if driver is not None:
            resolved_driver = driver
        elif config_or_url is not None:
            cfg = ValkeyConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
            resolved_driver = AsyncValkeyDriver(cfg)
        else:
            resolved_driver = AsyncValkeyDriver(ValkeyConfig())
        super().__init__(
            driver=resolved_driver,
            key_prefix=key_prefix,
            client_tracking=client_tracking,
            capacity=capacity,
        )

    @classmethod
    def from_url(cls, url: str, *, key_prefix: str = "vertebra", client_tracking: bool = True) -> Self:
        """Create a ValkeyStore from a valkey:// connection URL."""
        return cls(config_or_url=url, key_prefix=key_prefix, client_tracking=client_tracking)


class _RedisStoreImpl(_AsyncCacheStoreAdapter):
    """Litestar-compatible Store backed by AsyncCacheStore and native Redis driver."""

    __slots__ = ()

    def __init__(
        self,
        driver: AsyncRedisDriver | None = None,
        config_or_url: RedisConfig | str | None = None,
        *,
        key_prefix: str = "vertebra",
        client_tracking: bool = True,
        capacity: int = 2048,
    ) -> None:
        if driver is not None:
            resolved_driver = driver
        elif config_or_url is not None:
            cfg = RedisConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
            resolved_driver = AsyncRedisDriver(cfg)
        else:
            resolved_driver = AsyncRedisDriver(RedisConfig())
        super().__init__(
            driver=resolved_driver,
            key_prefix=key_prefix,
            client_tracking=client_tracking,
            capacity=capacity,
        )

    @classmethod
    def from_url(cls, url: str, *, key_prefix: str = "vertebra", client_tracking: bool = True) -> Self:
        """Create a RedisStore from a redis:// connection URL."""
        return cls(config_or_url=url, key_prefix=key_prefix, client_tracking=client_tracking)


class _ResponseCacheStoreImpl(_MemoryStoreImpl):
    """Litestar response cache store backed by AsyncCacheStore fast memory/SHM cache."""

    __slots__ = ()


class _ObjectStoreLitestarStoreImpl:
    """Litestar key/value store with persistent, versioned wall-clock expiration."""

    __slots__ = ("_prefix", "_store")

    def __init__(self, store: AsyncObjectStore | StorageConfig | str, *, namespace: str | None = None) -> None:
        self._store = _resolve_object_store(store)
        self._prefix = "vertebra-store/"
        if namespace is not None:
            self._prefix += validate_identifier(namespace, "namespace", allow_dots=False, allow_hyphens=True) + "/"

    def with_namespace(self, namespace: str) -> ObjectStoreLitestarStore:
        """Return an isolated child sharing this store's backend."""
        child = type(self)(self._store, namespace=namespace)
        child._prefix = self._prefix + child._prefix.removeprefix("vertebra-store/")
        return cast("ObjectStoreLitestarStore", child)

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Store bytes and their optional deadline in one authoritative envelope."""
        deadline = None
        if expires_in is not None:
            seconds = expires_in.total_seconds() if isinstance(expires_in, timedelta) else expires_in
            deadline = self._now_ms() + math.ceil(seconds * 1000)
        payload = value.encode("utf-8") if isinstance(value, str) else value
        attributes = {} if deadline is None else {"vertebra-expires-at-ms": str(deadline)}
        await self._store.put(
            self._make_key(key), b"VTBSTORE\x01" + encode_msgpack_bytes((deadline, payload)), attributes=attributes
        )

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Read live bytes, renewing only entries with an existing expiration."""
        entry = await self._load(key)
        if entry is None:
            return None
        deadline, payload = entry
        if renew_for is not None and deadline is not None:
            await self.set(key, payload, expires_in=renew_for)
        return payload

    async def delete(self, key: str) -> None:
        """Delete a logical key, allowing absent keys."""
        await self._store.delete(self._make_key(key))

    async def delete_all(self) -> None:
        """Delete this namespace and its descendants through bounded pages."""
        cursor = None
        while True:
            items, cursor = await self._store.list_page(self._prefix, cursor=cursor, limit=1000)
            for item in items:
                if item.path.startswith(self._prefix):
                    await self._store.delete(item.path)
            if cursor is None:
                break

    async def exists(self, key: str) -> bool:
        """Return whether a live entry exists, including empty payloads."""
        return await self._load(key) is not None

    async def expires_in(self, key: str) -> int | None:
        """Return whole remaining seconds for a live expiring entry."""
        entry = await self._load(key)
        if entry is None or entry[0] is None:
            return None
        return max(0, int((entry[0] - self._now_ms()) / 1000))

    def _make_key(self, key: str) -> str:
        return self._prefix + "~" + key.encode("utf-8").hex()

    @staticmethod
    def _now_ms() -> int:
        return math.floor(time.time() * 1000)

    async def _load(self, key: str) -> tuple[int | None, bytes] | None:
        try:
            raw = await self._store.get(self._make_key(key))
        except FileNotFoundError:
            return None
        if not raw.startswith(b"VTBSTORE\x01"):
            raise ValueError("Invalid object store envelope magic or version")
        entry: tuple[int | None, bytes] = decode_msgpack(raw[9:], type=tuple[int | None, bytes])
        if entry[0] is not None and self._now_ms() >= entry[0]:
            await self.delete(key)
            return None
        return entry


class ObjectStoreFileSystemAdapter:
    """Read-only async filesystem exposing a confined virtual object-store root."""

    __slots__ = ("_base_path", "_store")

    def __init__(self, store: AsyncObjectStore | StorageConfig | str, *, base_path: str = "") -> None:
        self._store = _resolve_object_store(store)
        if base_path.startswith("/") or "\\" in base_path or ".." in base_path.split("/"):
            raise ValueError("base_path must be a relative prefix without parent segments")
        self._base_path = "/".join(part for part in base_path.split("/") if part and part != ".")

    async def info(self, path: str | os.PathLike[str], **kwargs: Any) -> dict[str, Any]:
        """Return stat information for a file or a populated directory prefix."""
        _ = kwargs
        key = self._resolve_key(path)
        size = 0
        mtime = 0.0
        kind = "directory"
        if key != self._base_path:
            try:
                meta = await self._store.head(key)
            except FileNotFoundError:
                prefix = key + "/"
                items, _ = await self._store.list_page(prefix, limit=1)
                if not any(item.path.startswith(prefix) for item in items):
                    raise FileNotFoundError(os.fspath(path)) from None
            else:
                size = meta.size
                mtime = datetime.fromisoformat(meta.last_modified).timestamp() if meta.last_modified else 0.0
                kind = "file"
        return {
            "name": os.fspath(path),
            "size": size,
            "type": kind,
            "mtime": mtime,
            "created": 0.0,
            "gid": 0,
            "ino": 0,
            "islink": False,
            "mode": 0,
            "nlink": 0,
            "uid": 0,
        }

    async def open(
        self, file: str | os.PathLike[str], mode: str = "rb", buffering: int = -1, **kwargs: Any
    ) -> AsyncReadableFile:
        """Open a native asynchronous reader with nonblocking synchronous seek/tell."""
        _ = (buffering, kwargs)
        if mode != "rb":
            raise ValueError("Object store files support only rb mode")
        key = self._resolve_key(file)
        meta = await self._store.head(key)
        return self._store.open_reader(key, size=meta.size)

    def _resolve_key(self, path: str | os.PathLike[str]) -> str:
        raw = os.fspath(path)
        if ".." in raw.split("/") or "\\" in raw:
            raise FileNotFoundError(raw)
        relative = "/".join(part for part in raw.split("/") if part and part != ".")
        return "/".join(part for part in (self._base_path, relative) if part)


if TYPE_CHECKING:

    class MemoryStore(_MemoryStoreImpl, _LitestarStore):
        """Litestar store backed by AsyncCacheStore with optional Valkey/Redis backing."""

    class ValkeyStore(_ValkeyStoreImpl, _LitestarStore):
        """Litestar-compatible Store backed by AsyncCacheStore and native Valkey driver."""

    class RedisStore(_RedisStoreImpl, _LitestarStore):
        """Litestar-compatible Store backed by AsyncCacheStore and native Redis driver."""

    class ResponseCacheStore(_ResponseCacheStoreImpl, MemoryStore):
        """Litestar response cache store backed by AsyncCacheStore fast memory/SHM cache."""

    class ObjectStoreLitestarStore(_ObjectStoreLitestarStoreImpl, _LitestarStore):
        """Litestar key/value store with persistent, versioned wall-clock expiration."""


def _get_store_class(name: str) -> type[Any]:
    cached = _CACHED_STORE_CLASSES.get(name)
    if cached is not None:
        return cached

    base_mod = ensure_module("litestar.stores.base", extra="litestar")
    litestar_store = base_mod.Store

    impl_map: dict[str, type[Any]] = {
        "MemoryStore": _MemoryStoreImpl,
        "ValkeyStore": _ValkeyStoreImpl,
        "RedisStore": _RedisStoreImpl,
        "ObjectStoreLitestarStore": _ObjectStoreLitestarStoreImpl,
    }
    if name == "ResponseCacheStore":
        memory_cls = _get_store_class("MemoryStore")
        created = type(
            "ResponseCacheStore",
            (_ResponseCacheStoreImpl, memory_cls),
            {
                "__module__": __name__,
                "__doc__": _ResponseCacheStoreImpl.__doc__,
                "__slots__": (),
            },
        )
    else:
        impl_cls = impl_map[name]
        created = type(
            name,
            (impl_cls, litestar_store),
            {
                "__module__": __name__,
                "__doc__": impl_cls.__doc__,
                "__slots__": (),
            },
        )
    _CACHED_STORE_CLASSES[name] = created
    return created


def __getattr__(name: str) -> Any:
    """Lazily bind Litestar Store subclasses to ``litestar.stores.base.Store``."""
    if name in {"MemoryStore", "ValkeyStore", "RedisStore", "ResponseCacheStore", "ObjectStoreLitestarStore"}:
        return _get_store_class(name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    """Return module attributes including lazily bound Litestar Store subclasses."""
    return sorted(set(globals()) | set(__all__))


def swap_response_cache_store(
    app: Any,
    store: MemoryStore | None = None,
) -> MemoryStore | None:
    """Transparently replace Litestar's default response_cache MemoryStore with Vertebra's MemoryStore."""
    stores = getattr(app, "stores", None)
    if stores is None:
        return None
    cache_cfg = getattr(app, "response_cache_config", None)
    store_name = getattr(cache_cfg, "store", "response_cache") if cache_cfg is not None else "response_cache"
    internal_dict = getattr(stores, "_stores", None)
    if not isinstance(internal_dict, dict):
        if isinstance(stores, dict):
            internal_dict = stores
        else:
            return None

    memory_store_cls = _get_store_class("MemoryStore")
    mem_mod = import_optional("litestar.stores.memory")
    litestar_mem_cls = getattr(mem_mod, "MemoryStore", None) if mem_mod is not None else None
    current = internal_dict.get(store_name)
    is_memory_store = (litestar_mem_cls is not None and isinstance(current, litestar_mem_cls)) or (
        type(current).__name__ == "MemoryStore" and not isinstance(current, memory_store_cls)
    )
    if is_memory_store or store is not None:
        replacement = store if store is not None else memory_store_cls()
        internal_dict[store_name] = replacement
        return cast("MemoryStore", replacement)
    if isinstance(current, memory_store_cls):
        return cast("MemoryStore", current)
    return None


def _resolve_object_store(store: AsyncObjectStore | StorageConfig | str) -> AsyncObjectStore:
    """Resolve an explicit backend without silently selecting a default."""
    if store is None or store == "":
        raise ImproperConfigurationError("An explicit object store configuration is required")
    return store if isinstance(store, AsyncObjectStore) else AsyncObjectStore(store)
