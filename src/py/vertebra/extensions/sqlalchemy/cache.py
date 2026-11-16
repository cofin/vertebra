"""SQLAlchemy caching entry points including SyncCacheBackend, AsyncCacheBackend, CompiledCache, and ResultCache."""

from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from typing import Any

from vertebra.extensions.alchemy.cache import (
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

    __slots__ = ("_backend", "_capacity")

    def __init__(self, capacity: int = 1024, *, backend: SyncCacheBackend | None = None) -> None:
        """Initialize compiled cache with maximum capacity and backing cache."""
        self._backend = backend or SyncCacheBackend("memory://", capacity=capacity)
        self._capacity = capacity

    def __getitem__(self, key: Any) -> Any:
        """Retrieve compiled statement plan from cache."""
        val = self._backend.get(str(key), default=None)
        if val is None:
            raise KeyError(key)
        return val

    def __setitem__(self, key: Any, value: Any) -> None:
        """Store compiled statement plan into cache."""
        self._backend.set(str(key), value)

    def __delitem__(self, key: Any) -> None:
        """Remove compiled statement plan from cache."""
        if not self._backend.has_key(str(key)):
            raise KeyError(key)
        self._backend.delete(str(key))

    def __iter__(self) -> Iterator[Any]:
        """Iterate over compiled cache entries."""
        return iter([])

    def __len__(self) -> int:
        """Return number of cached compiled statements."""
        return len(self._backend) if hasattr(self._backend, "__len__") else 0

    def __contains__(self, key: Any) -> bool:
        """Check if compiled statement key exists in cache."""
        return self._backend.has_key(str(key))


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
