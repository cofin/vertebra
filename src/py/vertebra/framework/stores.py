"""Framework key-value stores with monotonic TTL, Valkey RESP3 caching, and Redis backing."""

from __future__ import annotations

import time
from datetime import timedelta
from typing import Self

from vertebra.db.redis import AsyncRedisDriver, RedisConfig
from vertebra.db.valkey import AsyncValkeyDriver, ValkeyConfig

__all__ = (
    "MemoryStore",
    "NamespacedStore",
    "ResponseCacheStore",
)


class ResponseCacheStore:
    """Lock-free monotonic TTL store with optional Valkey/Redis backing and RESP3 invalidation."""

    __slots__ = ("_driver", "_entries")

    def __init__(self, *, driver: AsyncValkeyDriver | AsyncRedisDriver | None = None) -> None:
        """Initialize store with optional Valkey or Redis backing driver."""
        self._entries: dict[str, tuple[bytes, float | None]] = {}
        self._driver = driver

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

    async def __aenter__(self) -> Self:
        """Enter the async context manager and return the store instance."""
        return self

    async def __aexit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> None:
        """Exit the async context manager."""
        _ = (exc_type, exc_val, exc_tb)
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

    async def exists(self, key: str) -> bool:
        """Return True if key exists and has not expired."""
        return (await self.get(key)) is not None

    async def expires_in(self, key: str) -> int | None:
        """Return remaining TTL in seconds for key, or None if unset or expired."""
        entry = self._entries.get(key)
        if entry is None:
            return None
        _value, expires_at = entry
        if expires_at is None:
            return -1
        remaining = expires_at - time.monotonic()
        if remaining <= 0:
            self._entries.pop(key, None)
            return None
        return int(remaining)


class NamespacedStore:
    """Prefix-isolated view over a ResponseCacheStore."""

    __slots__ = ("_namespace", "_store")

    def __init__(self, store: ResponseCacheStore, namespace: str) -> None:
        """Initialize with a backing store and namespace prefix."""
        self._store = store
        self._namespace = namespace.strip(":")

    @property
    def namespace(self) -> str:
        """Return the configured key namespace."""
        return self._namespace

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Retrieve bytes for the namespaced key."""
        return await self._store.get(f"{self._namespace}:{key}", renew_for=renew_for)

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Store a value under the namespaced key."""
        await self._store.set(f"{self._namespace}:{key}", value, expires_in=expires_in)

    async def delete(self, key: str) -> None:
        """Delete the namespaced key."""
        await self._store.delete(f"{self._namespace}:{key}")


MemoryStore = ResponseCacheStore
