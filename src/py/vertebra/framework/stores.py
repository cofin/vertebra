"""Framework key-value stores with monotonic TTL, Valkey RESP3 caching, and Redis backing."""

from __future__ import annotations

import math
import time
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any, Self

from vertebra.db.adapters.redis import AsyncRedisDriver, RedisConfig
from vertebra.db.adapters.valkey import AsyncValkeyDriver, ValkeyConfig
from vertebra.utils.text import validate_identifier

__all__ = (
    "KeyValueStore",
    "NamespacedStore",
)


class KeyValueStore:
    """Lock-free monotonic TTL store with optional Valkey/Redis backing and RESP3 invalidation."""

    __slots__ = ("_driver", "_entries", "_key_prefix")

    def __init__(
        self,
        *,
        driver: AsyncValkeyDriver | AsyncRedisDriver | None = None,
        key_prefix: str = "vertebra",
    ) -> None:
        """Initialize store with optional Valkey or Redis backing driver.

        Args:
            driver: Optional Valkey or Redis driver.
            key_prefix: Namespace prepended to every key as ``<key_prefix>:<key>``; ``delete_all`` removes only keys under it.

        Raises:
            ValueError: When ``key_prefix`` is not a valid identifier.
        """
        self._key_prefix = validate_identifier(key_prefix, "key_prefix", allow_hyphens=True)
        self._entries: dict[str, tuple[bytes, float | None]] = {}
        self._driver = driver
        if isinstance(driver, (AsyncValkeyDriver, AsyncRedisDriver)):
            driver.add_invalidation_listener(self._on_driver_invalidate)

    @property
    def driver(self) -> AsyncValkeyDriver | AsyncRedisDriver | None:
        """Return the underlying Valkey or Redis driver when configured."""
        return self._driver

    @property
    def key_prefix(self) -> str:
        """Return the namespace prepended to every stored key."""
        return self._key_prefix

    @classmethod
    def from_valkey(cls, config_or_url: ValkeyConfig | str, *, key_prefix: str = "vertebra") -> Self:
        """Create a Store backed by Valkey with RESP3 client-side cache tracking."""
        cfg = ValkeyConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(driver=AsyncValkeyDriver(cfg), key_prefix=key_prefix)

    @classmethod
    def from_redis(cls, config_or_url: RedisConfig | str, *, key_prefix: str = "vertebra") -> Self:
        """Create a Store backed by Redis."""
        cfg = RedisConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(driver=AsyncRedisDriver(cfg), key_prefix=key_prefix)

    async def __aenter__(self) -> Self:
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
        full_key = self._make_key(key)
        entry = self._entries.get(full_key)
        now = time.monotonic()
        if entry is not None:
            value, expires_at = entry
            if expires_at is not None and now >= expires_at:
                self._entries.pop(full_key, None)
                entry = None
        if entry is None:
            if self._driver is None:
                return None
            raw = await self._driver.get(full_key)
            if raw is None:
                return None
            val_bytes = raw.encode("utf-8") if isinstance(raw, str) else bytes(raw)
            if renew_for is not None:
                ttl_ms = self._ttl_milliseconds(renew_for)
                new_expires = None if ttl_ms is None else time.monotonic() + ttl_ms / 1000.0
                self._entries[full_key] = (val_bytes, new_expires)
                await self._driver.set(full_key, val_bytes, px=ttl_ms)
                return val_bytes
            new_expires = None
            if hasattr(self._driver, "pttl"):
                pttl_ms = await self._driver.pttl(full_key)
                if pttl_ms == -2:
                    return None
                if pttl_ms >= 0:
                    new_expires = time.monotonic() + pttl_ms / 1000.0
            self._entries[full_key] = (val_bytes, new_expires)
            return val_bytes
        value, _expires_at = entry
        if renew_for is not None:
            ttl_ms = self._ttl_milliseconds(renew_for)
            new_expires = None if ttl_ms is None else now + ttl_ms / 1000.0
            self._entries[full_key] = (value, new_expires)
            if self._driver is not None:
                await self._driver.set(full_key, value, px=ttl_ms)
        return value

    async def set(
        self,
        key: str,
        value: str | bytes,
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Store value under key with an optional expiration duration."""
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        full_key = self._make_key(key)
        ttl_ms = self._ttl_milliseconds(expires_in)
        expires_at = None if ttl_ms is None else time.monotonic() + ttl_ms / 1000.0
        self._entries[full_key] = (raw, expires_at)
        if self._driver is not None:
            await self._driver.set(full_key, raw, px=ttl_ms)

    async def delete(self, key: str) -> None:
        """Delete key from the store if present."""
        full_key = self._make_key(key)
        self._entries.pop(full_key, None)
        if self._driver is not None:
            await self._driver.delete(full_key)

    async def delete_all(self) -> None:
        """Delete every key under this store's ``key_prefix`` with ``SCAN`` + ``UNLINK``."""
        self._entries.clear()
        if self._driver is None:
            return
        pattern = f"{self._key_prefix}:*"
        cursor = 0
        while True:
            cursor, keys = await self._driver.scan(cursor, match=pattern, count=500)
            if keys:
                await self._driver.unlink(*keys)
            if cursor == 0:
                break

    async def exists(self, key: str) -> bool:
        """Return True if key exists and has not expired."""
        return (await self.get(key)) is not None

    async def expires_in(self, key: str) -> int | None:
        """Return remaining TTL in seconds, or None when the key is missing, expired, or has no expiry."""
        full_key = self._make_key(key)
        entry = self._entries.get(full_key)
        if entry is not None:
            _value, expires_at = entry
            if expires_at is None:
                return None
            remaining = expires_at - time.monotonic()
            if remaining <= 0:
                self._entries.pop(full_key, None)
                return None
            return int(remaining)
        if self._driver is not None:
            pttl_ms = await self._driver.pttl(full_key)
            if pttl_ms in (-2, -1):
                return None
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

    def namespace(self, prefix: str) -> NamespacedStore:
        """Return a prefix-isolated ``NamespacedStore`` view over this store."""
        return NamespacedStore(self, prefix)

    async def close(self) -> None:
        """Close the underlying Valkey or Redis driver when configured."""
        if self._driver is not None:
            await self._driver.close()

    def _make_key(self, key: str) -> str:
        """Return the driver key ``<key_prefix>:<key>``."""
        return f"{self._key_prefix}:{key}"

    @staticmethod
    def _ttl_milliseconds(expires_in: int | timedelta | None) -> int | None:
        """Convert ``expires_in`` to whole milliseconds, rounding up.

        Raises:
            ValueError: When ``expires_in`` is zero or negative.
        """
        if expires_in is None:
            return None
        seconds = expires_in.total_seconds() if isinstance(expires_in, timedelta) else float(expires_in)
        if seconds <= 0:
            raise ValueError("expires_in must be a positive duration")
        return math.ceil(seconds * 1000)

    def _on_driver_invalidate(self, keys: Any) -> None:
        """Evict local cache entries matching invalidated keys."""
        for key in keys:
            self._entries.pop(key, None)


class NamespacedStore:
    """Prefix-isolated view over a KeyValueStore."""

    __slots__ = ("_namespace", "_store")

    def __init__(self, store: KeyValueStore, namespace: str) -> None:
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
