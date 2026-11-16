"""SQLAlchemy caching entry points including SyncCacheBackend, AsyncCacheBackend, CompiledCache, and ResultCache."""

from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from typing import Any

from vertebra.extensions.alchemy.cache import (
    NO_VALUE,
    AsyncCacheBackend,
    CacheEntry,
    CacheStats,
    SQLAlchemyCacheBackend,
    SyncCacheBackend,
)

__all__ = (
    "AsyncCacheBackend",
    "CacheEntry",
    "CacheStats",
    "CompiledCache",
    "ResultCache",
    "SQLAlchemyCacheBackend",
    "SyncCacheBackend",
)


class CompiledCache(MutableMapping[Any, Any]):
    """Thread-safe compiled statement cache for SQLAlchemy execution_options."""

    __slots__ = ("_backend", "_capacity", "_key_map")

    def __init__(self, capacity: int = 1024, *, backend: SyncCacheBackend | None = None) -> None:
        """Initialize compiled cache with maximum capacity and backing cache."""
        self._backend = backend or SyncCacheBackend("memory://", capacity=capacity)
        self._capacity = capacity
        self._key_map: dict[str, Any] = {}

    @staticmethod
    def _normalize_key(key: Any) -> str:
        """Normalize an arbitrary SQLAlchemy cache key into a unique string key."""
        return key if isinstance(key, str) else f"__repr__:{key!r}"

    def __getitem__(self, key: Any) -> Any:
        """Retrieve compiled statement plan from cache."""
        norm = self._normalize_key(key)
        val = self._backend.get(norm, default=NO_VALUE)
        if val is NO_VALUE:
            self._key_map.pop(norm, None)
            raise KeyError(key)
        return val

    def __setitem__(self, key: Any, value: Any) -> None:
        """Store compiled statement plan into cache."""
        norm = self._normalize_key(key)
        self._backend.set(norm, value)
        self._key_map[norm] = key
        if len(self._key_map) > max(64, self._backend.size * 2):
            live = set(self._backend.keys())
            stale = [k for k in self._key_map if k not in live]
            for k in stale:
                self._key_map.pop(k, None)

    def __delitem__(self, key: Any) -> None:
        """Remove compiled statement plan from cache."""
        norm = self._normalize_key(key)
        if not self._backend.has_key(norm):
            self._key_map.pop(norm, None)
            raise KeyError(key)
        self._key_map.pop(norm, None)
        self._backend.delete(norm)

    def __iter__(self) -> Iterator[Any]:
        """Iterate over currently live compiled cache keys."""
        live_keys = self._backend.keys()
        return iter([self._key_map.get(k, k) for k in live_keys])

    def __len__(self) -> int:
        """Return number of cached compiled statements."""
        return self._backend.size

    def __contains__(self, key: Any) -> bool:
        """Check if compiled statement key exists in cache."""
        return self._backend.has_key(self._normalize_key(key))

    def clear(self) -> None:
        """Remove all compiled statement plans from the backing cache."""
        self._key_map.clear()
        self._backend.clear()


class ResultCache:
    """SQLAlchemy query result cache wrapping synchronous and asynchronous cache backends."""

    __slots__ = ("_async_backend", "_default_ttl", "_sync_backend")

    def __init__(
        self,
        backend: SyncCacheBackend | AsyncCacheBackend | str = "shm://vertebra-query-cache",
        *,
        default_ttl: int = 300,
    ) -> None:
        """Initialize query result cache with backend and default TTL."""
        if isinstance(backend, str):
            self._sync_backend = SyncCacheBackend(backend, default_ttl=default_ttl)
            self._async_backend = AsyncCacheBackend(backend, default_ttl=default_ttl)
        elif isinstance(backend, SyncCacheBackend):
            self._sync_backend = backend
            self._async_backend = AsyncCacheBackend(default_ttl=default_ttl)
        else:
            self._async_backend = backend
            self._sync_backend = SyncCacheBackend(default_ttl=default_ttl)
        self._default_ttl = default_ttl

    def get(self, key: str) -> Any:
        """Synchronously retrieve cached query results."""
        return self._sync_backend.get(key)

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Synchronously store query results in cache."""
        self._sync_backend.set(key, value, expiration_time=ttl or self._default_ttl)

    def delete(self, key: str) -> None:
        """Synchronously delete cached query results."""
        self._sync_backend.delete(key)

    async def get_async(self, key: str) -> Any:
        """Asynchronously retrieve cached query results."""
        return await self._async_backend.get(key)

    async def set_async(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Asynchronously store query results in cache."""
        await self._async_backend.set(key, value, expiration_time=ttl or self._default_ttl)

    async def delete_async(self, key: str) -> None:
        """Asynchronously delete cached query results."""
        await self._async_backend.delete(key)
