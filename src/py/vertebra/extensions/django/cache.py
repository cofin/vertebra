"""Django CacheBackend supporting full synchronous and *a-prefixed asynchronous cache operations."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = ("CacheBackend",)


class CacheBackend:
    """Drop-in Django cache backend (``vertebra.extensions.django.cache.CacheBackend``)."""

    __slots__ = ("_default_timeout", "_key_prefix", "_location", "_options", "_store")

    def __init__(self, server: str = "", params: dict[str, Any] | None = None) -> None:
        cfg = dict(params or {})
        self._location = server
        self._default_timeout = int(cfg.get("TIMEOUT", 300) or 300)
        self._key_prefix = str(cfg.get("KEY_PREFIX", "vtb:dj:"))
        self._options = dict(cfg.get("OPTIONS") or {})
        self._store: dict[str, tuple[bytes, float | None]] = {}

    def make_key(self, key: str, version: int | None = None) -> str:
        """Build the prefixed and versioned cache key."""
        ver = 1 if version is None else version
        return f"{self._key_prefix}:{ver}:{key}"

    def _expiry(self, timeout: int | None) -> float | None:
        effective = self._default_timeout if timeout is None else timeout
        if effective <= 0:
            return None
        return time.monotonic() + effective

    def get(self, key: str, default: Any = None, version: int | None = None) -> Any:
        """Synchronously fetch ``key`` from the cache."""
        full = self.make_key(key, version=version)
        entry = self._store.get(full)
        if entry is None:
            return default
        raw, expires_at = entry
        if expires_at is not None and time.monotonic() >= expires_at:
            self._store.pop(full, None)
            return default
        return decode_json(raw)

    async def aget(self, key: str, default: Any = None, version: int | None = None) -> Any:
        """Asynchronously fetch ``key`` from the cache."""
        return self.get(key, default=default, version=version)

    def set(self, key: str, value: Any, timeout: int | None = None, version: int | None = None) -> None:
        """Synchronously store ``value`` under ``key``."""
        full = self.make_key(key, version=version)
        self._store[full] = (encode_json_bytes(value), self._expiry(timeout))

    async def aset(self, key: str, value: Any, timeout: int | None = None, version: int | None = None) -> None:
        """Asynchronously store ``value`` under ``key``."""
        self.set(key, value, timeout=timeout, version=version)

    def add(self, key: str, value: Any, timeout: int | None = None, version: int | None = None) -> bool:
        """Store ``value`` only if ``key`` is not already present."""
        sentinel = object()
        if self.get(key, default=sentinel, version=version) is not sentinel:
            return False
        self.set(key, value, timeout=timeout, version=version)
        return True

    async def aadd(self, key: str, value: Any, timeout: int | None = None, version: int | None = None) -> bool:
        """Asynchronously store ``value`` only if ``key`` is not already present."""
        return self.add(key, value, timeout=timeout, version=version)

    def delete(self, key: str, version: int | None = None) -> bool:
        """Delete ``key`` and return True if it existed."""
        full = self.make_key(key, version=version)
        return self._store.pop(full, None) is not None

    async def adelete(self, key: str, version: int | None = None) -> bool:
        """Asynchronously delete ``key``."""
        return self.delete(key, version=version)

    def get_many(self, keys: Iterable[str], version: int | None = None) -> dict[str, Any]:
        """Synchronously fetch multiple keys and return a mapping of found keys."""
        out: dict[str, Any] = {}
        sentinel = object()
        for k in keys:
            val = self.get(k, default=sentinel, version=version)
            if val is not sentinel:
                out[k] = val
        return out

    async def aget_many(self, keys: Iterable[str], version: int | None = None) -> dict[str, Any]:
        """Asynchronously fetch multiple keys."""
        return self.get_many(keys, version=version)

    def set_many(self, data: Mapping[str, Any], timeout: int | None = None, version: int | None = None) -> list[str]:
        """Synchronously store multiple key-value pairs."""
        for k, v in data.items():
            self.set(k, v, timeout=timeout, version=version)
        return []

    async def aset_many(
        self,
        data: Mapping[str, Any],
        timeout: int | None = None,
        version: int | None = None,
    ) -> list[str]:
        """Asynchronously store multiple key-value pairs."""
        return self.set_many(data, timeout=timeout, version=version)

    def delete_many(self, keys: Iterable[str], version: int | None = None) -> None:
        """Synchronously delete multiple keys."""
        for k in keys:
            self.delete(k, version=version)

    async def adelete_many(self, keys: Iterable[str], version: int | None = None) -> None:
        """Asynchronously delete multiple keys."""
        self.delete_many(keys, version=version)

    def incr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Increment an integer value at ``key`` by ``delta``."""
        sentinel = object()
        cur = self.get(key, default=sentinel, version=version)
        if cur is sentinel:
            updated = delta
        elif isinstance(cur, int):
            updated = cur + delta
        else:
            msg = f"Key '{key}' is not an integer"
            raise ValueError(msg)
        self.set(key, updated, version=version)
        return updated

    async def aincr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Asynchronously increment an integer value at ``key`` by ``delta``."""
        return self.incr(key, delta=delta, version=version)

    def decr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Decrement an integer value at ``key`` by ``delta``."""
        return self.incr(key, delta=-delta, version=version)

    async def adecr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Asynchronously decrement an integer value at ``key`` by ``delta``."""
        return self.decr(key, delta=delta, version=version)

    def clear(self) -> None:
        """Clear all entries from the cache."""
        self._store.clear()

    async def aclear(self) -> None:
        """Asynchronously clear all entries from the cache."""
        self._store.clear()
