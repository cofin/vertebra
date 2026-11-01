"""Sync and async native CacheStore for Starlette and FastAPI applications."""

from __future__ import annotations

import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Self

from vertebra.db.redis import AsyncRedisDriver, RedisConfig, SyncRedisDriver
from vertebra.db.valkey import AsyncValkeyDriver, SyncValkeyDriver, ValkeyConfig

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = (
    "CacheStore",
    "attach_cache_store",
)


class CacheStore:
    """Monotonic TTL cache store with both async and sync methods and optional Valkey/Redis backing."""

    __slots__ = ("_async_driver", "_entries", "_sync_driver")

    def __init__(
        self,
        *,
        async_driver: AsyncValkeyDriver | AsyncRedisDriver | None = None,
        sync_driver: SyncValkeyDriver | SyncRedisDriver | None = None,
    ) -> None:
        self._entries: dict[str, tuple[bytes, float | None]] = {}
        self._async_driver = async_driver
        self._sync_driver = sync_driver
        if isinstance(async_driver, AsyncValkeyDriver):
            async_driver.add_invalidation_listener(self._on_driver_invalidate)

    @classmethod
    def from_valkey(cls, config_or_url: ValkeyConfig | str, *, include_sync: bool = True) -> Self:
        """Create a CacheStore backed by Valkey with RESP3 client-side invalidation tracking."""
        cfg = ValkeyConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(
            async_driver=AsyncValkeyDriver(cfg),
            sync_driver=SyncValkeyDriver(cfg) if include_sync else None,
        )

    @classmethod
    def from_redis(cls, config_or_url: RedisConfig | str, *, include_sync: bool = True) -> Self:
        """Create a CacheStore backed by Redis."""
        cfg = RedisConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(
            async_driver=AsyncRedisDriver(cfg),
            sync_driver=SyncRedisDriver(cfg) if include_sync else None,
        )

    def get_sync(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Synchronously retrieve bytes for key if present and unexpired."""
        entry = self._entries.get(key)
        if entry is None:
            if self._sync_driver is not None:
                return self._sync_driver.get(key)
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

    def set_sync(
        self,
        key: str,
        value: str | bytes,
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Synchronously store value under key with an optional expiration duration."""
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        expires_at, ttl_int = _compute_expiry(expires_in)
        self._entries[key] = (raw, expires_at)
        if self._sync_driver is not None:
            self._sync_driver.set(key, raw, ex=ttl_int)

    def delete_sync(self, key: str) -> None:
        """Synchronously delete key from the store."""
        self._entries.pop(key, None)
        if self._sync_driver is not None:
            self._sync_driver.delete(key)

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Asynchronously retrieve bytes for key if present and unexpired."""
        entry = self._entries.get(key)
        if entry is None:
            if self._async_driver is not None:
                return await self._async_driver.get(key)
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
        """Asynchronously store value under key with an optional expiration duration."""
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        expires_at, ttl_int = _compute_expiry(expires_in)
        self._entries[key] = (raw, expires_at)
        if self._async_driver is not None:
            await self._async_driver.set(key, raw, ex=ttl_int)

    async def delete(self, key: str) -> None:
        """Asynchronously delete key from the store."""
        self._entries.pop(key, None)
        if self._async_driver is not None:
            await self._async_driver.delete(key)

    async def delete_all(self) -> None:
        """Clear all entries from the store."""
        self._entries.clear()
        if self._async_driver is not None:
            await self._async_driver.flushdb()

    async def get_many(self, keys: Sequence[str]) -> dict[str, bytes | None]:
        """Retrieve multiple keys in a single batch dictionary."""
        return {k: await self.get(k) for k in keys}

    async def set_many(
        self,
        mapping: Mapping[str, str | bytes],
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Store multiple key-value pairs with an optional shared expiration."""
        for k, v in mapping.items():
            await self.set(k, v, expires_in=expires_in)

    async def close(self) -> None:
        """Close any underlying Valkey or Redis drivers."""
        if self._async_driver is not None:
            await self._async_driver.close()
        if self._sync_driver is not None:
            self._sync_driver.close()

    def _on_driver_invalidate(self, keys: Any) -> None:
        for key in keys:
            self._entries.pop(key, None)


def attach_cache_store(app: Any, store: CacheStore | None = None) -> CacheStore:
    """Attach a CacheStore instance to ``app.state.cache``."""
    resolved = store if store is not None else CacheStore()
    state = getattr(app, "state", None)
    if state is not None:
        if isinstance(state, dict):
            state["cache"] = resolved
        else:
            state.cache = resolved
    return resolved


def _compute_expiry(expires_in: int | timedelta | None) -> tuple[float | None, int | None]:
    if expires_in is None:
        return None, None
    ttl_s = expires_in.total_seconds() if isinstance(expires_in, timedelta) else float(expires_in)
    return time.monotonic() + ttl_s, int(ttl_s)
