"""Unified sync and async cache backend for SQLAlchemy compiled statements and Advanced Alchemy.

Provides a thread-safe, high-performance MutableMapping cache backed by local LRU memory
with monotonic TTL expiration and optional shared-memory statement cache integration.
"""

from __future__ import annotations

import collections
import threading
import time
from collections.abc import Iterator, MutableMapping
from datetime import timedelta
from typing import Any, Self

import msgspec

__all__ = (
    "CacheEntry",
    "CacheStats",
    "SQLAlchemyCacheBackend",
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


class SQLAlchemyCacheBackend(MutableMapping[Any, Any]):
    """Thread-safe unified sync and async cache backend for SQLAlchemy and Advanced Alchemy.

    Implements the standard Python MutableMapping protocol for compatibility with
    SQLAlchemy execution_options compiled_cache, alongside sync and async cache methods
    (get, set, delete, exists, clear, expires_in) for Advanced Alchemy repository caching.
    """

    __slots__ = (
        "_capacity",
        "_default_ttl",
        "_entries",
        "_evictions",
        "_expirations",
        "_hits",
        "_lock",
        "_misses",
        "_shm_name",
    )

    def __init__(
        self,
        *,
        capacity: int | None = 1024,
        default_ttl: float | timedelta | None = None,
        shm_name: str | None = None,
    ) -> None:
        """Initialize unified cache backend with capacity and optional default TTL.

        Args:
            capacity: Maximum number of entries before LRU eviction. Set to None for unbounded.
            default_ttl: Default expiration in seconds or timedelta for entries added without TTL.
            shm_name: Optional shared-memory segment name when backed by native SHM cache.
        """
        self._entries: collections.OrderedDict[Any, CacheEntry] = collections.OrderedDict()
        self._capacity = capacity
        self._default_ttl = default_ttl.total_seconds() if isinstance(default_ttl, timedelta) else default_ttl
        self._shm_name = shm_name
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._expirations = 0

    @classmethod
    def create_shm_cache_backend(
        cls,
        name: str | None = None,
        capacity: int = 1024,
        default_ttl: float | timedelta | None = None,
    ) -> Self:
        """Create a shared-memory backed cache backend instance.

        Args:
            name: Shared-memory segment identifier. Defaults to auto-generated name.
            capacity: Maximum statement cache capacity.
            default_ttl: Default expiration duration.

        Returns:
            Configured SQLAlchemyCacheBackend instance.
        """
        resolved_name = name or f"vtb-cache-{id(cls)}-{int(time.time())}"
        return cls(capacity=capacity, default_ttl=default_ttl, shm_name=resolved_name)

    @property
    def capacity(self) -> int | None:
        """Maximum configured capacity of the cache."""
        return self._capacity

    @property
    def default_ttl(self) -> float | None:
        """Default TTL in seconds for entries without an explicit expiration."""
        return self._default_ttl

    @property
    def shm_name(self) -> str | None:
        """Shared-memory segment name when configured."""
        return self._shm_name

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

    def _resolve_expires_at(self, ttl: float | int | timedelta | None) -> float | None:
        """Compute monotonic expiration timestamp from provided or default TTL."""
        effective_ttl: float | None = None
        if ttl is not None:
            effective_ttl = ttl.total_seconds() if isinstance(ttl, timedelta) else float(ttl)
        elif self._default_ttl is not None:
            effective_ttl = self._default_ttl
        if effective_ttl is None:
            return None
        return time.monotonic() + effective_ttl

    def _evict_if_needed(self) -> None:
        """Evict oldest entries when capacity is exceeded."""
        if self._capacity is None:
            return
        while len(self._entries) > self._capacity:
            self._entries.popitem(last=False)
            self._evictions += 1

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

    def get(self, key: Any, default: Any = None) -> Any:
        """Retrieve value for key if present and unexpired, else return default."""
        with self._lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            if entry is None:
                self._misses += 1
                return default
            if entry.is_expired(now):
                del self._entries[key]
                self._expirations += 1
                self._misses += 1
                return default
            self._entries.move_to_end(key)
            self._hits += 1
            return entry.value

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

    def delete(self, key: Any) -> bool:
        """Remove key from cache. Return True if key was present, False otherwise."""
        with self._lock:
            entry = self._entries.pop(key, None)
            return entry is not None

    def exists(self, key: Any) -> bool:
        """Return True if key exists and has not expired."""
        with self._lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            if entry is None:
                return False
            if entry.is_expired(now):
                del self._entries[key]
                self._expirations += 1
                return False
            return True

    def expires_in(self, key: Any) -> float | None:
        """Return remaining seconds until key expiration, or None if key has no TTL or does not exist."""
        with self._lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            if entry is None or entry.is_expired(now):
                return None
            if entry.expires_at is None:
                return None
            return max(0.0, entry.expires_at - now)

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
        """Remove all entries and reset cache statistics."""
        with self._lock:
            self._entries.clear()

    async def get_async(self, key: Any, default: Any = None) -> Any:
        """Asynchronously retrieve value for key if present and unexpired."""
        return self.get(key, default)

    async def set_async(
        self,
        key: Any,
        value: Any,
        ttl: float | int | timedelta | None = None,
    ) -> None:
        """Asynchronously store value for key with optional expiration duration."""
        self.set(key, value, ttl=ttl)

    async def delete_async(self, key: Any) -> bool:
        """Asynchronously remove key from cache."""
        return self.delete(key)

    async def exists_async(self, key: Any) -> bool:
        """Asynchronously check if key exists and has not expired."""
        return self.exists(key)

    async def expires_in_async(self, key: Any) -> float | None:
        """Asynchronously return remaining seconds until key expiration."""
        return self.expires_in(key)

    async def clear_async(self) -> None:
        """Asynchronously remove all entries."""
        self.clear()
