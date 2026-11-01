"""Vertebra Store adapter for Litestar response caching."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any, Self

from litestar.stores.base import Store

from vertebra.db.redis import AsyncRedisDriver, RedisConfig
from vertebra.db.valkey import AsyncValkeyDriver, ValkeyConfig
from vertebra.utils.module_loader import import_optional

__all__ = (
    "MemoryStore",
    "ResponseCacheStore",
    "swap_response_cache_store",
)


class MemoryStore(Store):
    """Litestar store backed by Vertebra monotonic TTL cache with optional Valkey/Redis backing."""

    __slots__ = ("_driver", "_entries")

    def __init__(self, *, driver: AsyncValkeyDriver | AsyncRedisDriver | None = None) -> None:
        """Initialize store with optional Valkey or Redis backing driver."""
        self._entries: dict[str, tuple[bytes, float | None]] = {}
        self._driver = driver
        if isinstance(driver, AsyncValkeyDriver):
            driver.add_invalidation_listener(self._on_driver_invalidate)

    @classmethod
    def from_valkey(cls, config_or_url: ValkeyConfig | str) -> Self:
        """Create a Store backed by Valkey with RESP3 client-side cache tracking."""
        cfg = ValkeyConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(driver=AsyncValkeyDriver(cfg))

    @classmethod
    def from_redis(cls, config_or_url: RedisConfig | str) -> Self:
        """Create a Store backed by Redis."""
        cfg = RedisConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(driver=AsyncRedisDriver(cfg))

    @property
    def driver(self) -> AsyncValkeyDriver | AsyncRedisDriver | None:
        """Return the underlying Valkey or Redis driver when configured."""
        return self._driver

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
        if self._driver is not None:
            await self._driver.close()
        return None

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Retrieve bytes for key if present and unexpired, optionally renewing TTL."""
        entry = self._entries.get(key)
        if entry is None:
            if self._driver is not None:
                return await self._driver.get(key)
            return None
        value, expires_at = entry
        now = time.monotonic()
        if expires_at is not None and now >= expires_at:
            self._entries.pop(key, None)
            return None
        if renew_for is not None:
            ttl_s = renew_for.total_seconds() if isinstance(renew_for, timedelta) else float(renew_for)
            self._entries[key] = (value, now + ttl_s)
        return value

    async def set(
        self,
        key: str,
        value: str | bytes,
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Store value under key with an optional expiration duration."""
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        if expires_in is None:
            expires_at = None
            ttl_int = None
        else:
            ttl_s = expires_in.total_seconds() if isinstance(expires_in, timedelta) else float(expires_in)
            expires_at = time.monotonic() + ttl_s
            ttl_int = int(ttl_s)
        self._entries[key] = (raw, expires_at)
        if self._driver is not None:
            await self._driver.set(key, raw, ex=ttl_int)

    async def delete(self, key: str) -> None:
        """Delete key from the store if present."""
        self._entries.pop(key, None)
        if self._driver is not None:
            await self._driver.delete(key)

    async def delete_all(self) -> None:
        """Clear all entries from the store."""
        self._entries.clear()
        if self._driver is not None:
            await self._driver.flushdb()

    async def exists(self, key: str) -> bool:
        """Return True if key exists and has not expired."""
        return (await self.get(key)) is not None

    async def expires_in(self, key: str) -> int | None:
        """Return remaining TTL in seconds for key, or None if unset or expired."""
        entry = self._entries.get(key)
        if entry is not None:
            _value, expires_at = entry
            if expires_at is None:
                return -1
            remaining = expires_at - time.monotonic()
            if remaining <= 0:
                self._entries.pop(key, None)
                return None
            return int(remaining)
        if self._driver is not None:
            pttl_ms = await self._driver.pttl(key)
            if pttl_ms == -2:
                return None
            if pttl_ms == -1:
                return -1
            return max(0, pttl_ms // 1000)
        return None

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
        result: dict[str, bytes | None] = {}
        for key in keys:
            result[key] = await self.get(key)
        return result

    async def close(self) -> None:
        """Close the underlying Valkey or Redis driver when configured."""
        if self._driver is not None:
            await self._driver.close()

    def _on_driver_invalidate(self, keys: Any) -> None:
        """Evict local cache entries matching invalidated keys."""
        for key in keys:
            self._entries.pop(key, None)


class ResponseCacheStore(MemoryStore):
    """Litestar response cache store backed by Vertebra fast memory/SHM cache."""

    __slots__ = ()


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
    current = internal_dict.get(store_name)
    is_memory_store = (_MemoryStoreCls is not None and isinstance(current, _MemoryStoreCls)) or (
        type(current).__name__ == "MemoryStore" and not isinstance(current, MemoryStore)
    )
    if is_memory_store or store is not None:
        replacement = store if store is not None else MemoryStore()
        internal_dict[store_name] = replacement
        return replacement
    if isinstance(current, MemoryStore):
        return current
    return None


def _load_store_types() -> type[Any] | None:
    """Load Litestar MemoryStore and register MemoryStore and ResponseCacheStore as Store virtual subclasses."""
    base_mod = import_optional("litestar.stores.base")
    mem_mod = import_optional("litestar.stores.memory")
    if base_mod is None or mem_mod is None:
        return None
    register_fn = getattr(base_mod.Store, "register", None)
    if callable(register_fn):
        register_fn(MemoryStore)
        register_fn(ResponseCacheStore)
    memory_cls: type[Any] = mem_mod.MemoryStore
    return memory_cls


_MemoryStoreCls = _load_store_types()
