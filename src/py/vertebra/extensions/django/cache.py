"""Django CacheBackend supporting full synchronous and *a-prefixed asynchronous cache operations."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache

from vertebra.extensions.alchemy.cache import NO_VALUE, AsyncCacheBackend, SyncCacheBackend

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = ("CacheBackend",)

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


class CacheBackend(BaseCache):
    """Drop-in Django cache backend backed by Vertebra SyncCacheBackend and AsyncCacheBackend."""

    def __init__(self, server: str = "", params: Mapping[str, Any] | None = None) -> None:
        """Initialize CacheBackend with server location URL and Django params."""
        cfg = dict(params or {})
        super().__init__(cfg)
        self._location = server
        options = dict(cfg.get("OPTIONS") or {})
        l1_location = options.get("L1_LOCATION")

        raw_location = server.strip()
        resolved_url = "memory://" if not raw_location or raw_location.startswith("vertebra://") else raw_location

        default_timeout = self.default_timeout
        prefix = str(options.get("KEY_PREFIX") or cfg.get("KEY_PREFIX") or "django")
        normalized_prefix = prefix.strip(":") or "django"
        ttl_sec = float(default_timeout) if default_timeout is not None and default_timeout > 0 else None

        self._sync_backend = SyncCacheBackend(
            resolved_url,
            l1_url=l1_location,
            key_prefix=normalized_prefix,
            default_ttl=ttl_sec,
        )
        shared_sync = None if resolved_url.startswith(_REMOTE_OR_STORAGE_PREFIXES) else self._sync_backend
        self._async_backend = AsyncCacheBackend(
            resolved_url,
            sync_backend=shared_sync,
            l1_url=l1_location,
            key_prefix=normalized_prefix,
            default_ttl=ttl_sec,
        )

    def _resolve_relative_ttl(self, timeout: Any) -> tuple[bool, float]:
        """Resolve a Django timeout argument into `(should_store, relative_seconds)`.

        Returns `(True, -1.0)` when `timeout` is `None` (cache forever), `(False, 0.0)`
        when `timeout <= 0` (expire immediately / do not store), and `(True, seconds)`
        for positive relative durations.
        """
        effective = self.default_timeout if timeout == DEFAULT_TIMEOUT else timeout
        if effective is None:
            return True, -1.0
        if isinstance(effective, bool):
            msg = "timeout value must be a number or None, not bool."
            raise TypeError(msg)
        raw_seconds = effective.total_seconds() if hasattr(effective, "total_seconds") else float(effective)
        if math.isnan(raw_seconds) or math.isinf(raw_seconds):
            msg = "timeout value must be a finite number or None."
            raise ValueError(msg)
        if raw_seconds <= 0:
            return False, 0.0
        return True, raw_seconds

    def add(
        self,
        key: str,
        value: Any,
        timeout: Any = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        """Store value only if key is not already present."""
        full_key = self.make_and_validate_key(key, version=version)
        should_store, ttl_sec = self._resolve_relative_ttl(timeout)
        if self._sync_backend.has_key(full_key):
            return False
        if not should_store:
            return True
        return bool(self._sync_backend.add(full_key, value, expiration_time=ttl_sec))

    async def aadd(
        self,
        key: str,
        value: Any,
        timeout: Any = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        """Asynchronously store value only if key is not already present."""
        full_key = self.make_and_validate_key(key, version=version)
        should_store, ttl_sec = self._resolve_relative_ttl(timeout)
        if await self._async_backend.has_key(full_key):
            return False
        if not should_store:
            return True
        return bool(await self._async_backend.add(full_key, value, expiration_time=ttl_sec))

    def get(self, key: str, default: Any = None, version: int | None = None) -> Any:
        """Synchronously fetch key from the cache."""
        full_key = self.make_and_validate_key(key, version=version)
        val = self._sync_backend.get(full_key, default=NO_VALUE)
        return default if val is NO_VALUE else val

    async def aget(self, key: str, default: Any = None, version: int | None = None) -> Any:
        """Asynchronously fetch key from the cache."""
        full_key = self.make_and_validate_key(key, version=version)
        val = await self._async_backend.get(full_key, default=NO_VALUE)
        return default if val is NO_VALUE else val

    def set(
        self,
        key: str,
        value: Any,
        timeout: Any = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> None:
        """Synchronously store value under key."""
        full_key = self.make_and_validate_key(key, version=version)
        should_store, ttl_sec = self._resolve_relative_ttl(timeout)
        if not should_store:
            self._sync_backend.delete(full_key)
            return
        self._sync_backend.set(full_key, value, expiration_time=ttl_sec)

    async def aset(
        self,
        key: str,
        value: Any,
        timeout: Any = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> None:
        """Asynchronously store value under key."""
        full_key = self.make_and_validate_key(key, version=version)
        should_store, ttl_sec = self._resolve_relative_ttl(timeout)
        if not should_store:
            await self._async_backend.delete(full_key)
            return
        await self._async_backend.set(full_key, value, expiration_time=ttl_sec)

    def touch(
        self,
        key: str,
        timeout: Any = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        """Update expiration timestamp for key without modifying value."""
        full_key = self.make_and_validate_key(key, version=version)
        should_store, ttl_sec = self._resolve_relative_ttl(timeout)
        if not should_store:
            return bool(self._sync_backend.delete(full_key))
        return bool(self._sync_backend.touch(full_key, expiration_time=ttl_sec))

    async def atouch(
        self,
        key: str,
        timeout: Any = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        """Asynchronously update expiration timestamp for key without modifying value."""
        full_key = self.make_and_validate_key(key, version=version)
        should_store, ttl_sec = self._resolve_relative_ttl(timeout)
        if not should_store:
            return bool(await self._async_backend.delete(full_key))
        return bool(await self._async_backend.touch(full_key, expiration_time=ttl_sec))

    def delete(self, key: str, version: int | None = None) -> bool:
        """Delete key and return True if it existed."""
        full_key = self.make_and_validate_key(key, version=version)
        return bool(self._sync_backend.delete(full_key))

    async def adelete(self, key: str, version: int | None = None) -> bool:
        """Asynchronously delete key and return True if it existed."""
        full_key = self.make_and_validate_key(key, version=version)
        return bool(await self._async_backend.delete(full_key))

    def get_many(self, keys: Iterable[str], version: int | None = None) -> dict[str, Any]:
        """Synchronously fetch multiple keys."""
        out: dict[str, Any] = {}
        for k in keys:
            full_key = self.make_and_validate_key(k, version=version)
            val = self._sync_backend.get(full_key, default=NO_VALUE)
            if val is not NO_VALUE:
                out[k] = val
        return out

    async def aget_many(self, keys: Iterable[str], version: int | None = None) -> dict[str, Any]:
        """Asynchronously fetch multiple keys."""
        out: dict[str, Any] = {}
        for k in keys:
            full_key = self.make_and_validate_key(k, version=version)
            val = await self._async_backend.get(full_key, default=NO_VALUE)
            if val is not NO_VALUE:
                out[k] = val
        return out

    def set_many(
        self,
        data: Mapping[str, Any],
        timeout: Any = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list[str]:
        """Synchronously store multiple key/value pairs."""
        should_store, ttl_sec = self._resolve_relative_ttl(timeout)
        for k, v in data.items():
            full_key = self.make_and_validate_key(k, version=version)
            if not should_store:
                self._sync_backend.delete(full_key)
            else:
                self._sync_backend.set(full_key, v, expiration_time=ttl_sec)
        return []

    async def aset_many(
        self,
        data: Mapping[str, Any],
        timeout: Any = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list[str]:
        """Asynchronously store multiple key/value pairs."""
        should_store, ttl_sec = self._resolve_relative_ttl(timeout)
        for k, v in data.items():
            full_key = self.make_and_validate_key(k, version=version)
            if not should_store:
                await self._async_backend.delete(full_key)
            else:
                await self._async_backend.set(full_key, v, expiration_time=ttl_sec)
        return []

    def delete_many(self, keys: Iterable[str], version: int | None = None) -> None:
        """Synchronously delete multiple keys."""
        for k in keys:
            full_key = self.make_and_validate_key(k, version=version)
            self._sync_backend.delete(full_key)

    async def adelete_many(self, keys: Iterable[str], version: int | None = None) -> None:
        """Asynchronously delete multiple keys."""
        for k in keys:
            full_key = self.make_and_validate_key(k, version=version)
            await self._async_backend.delete(full_key)

    def has_key(self, key: str, version: int | None = None) -> bool:
        """Return True if unexpired key exists in cache."""
        full_key = self.make_and_validate_key(key, version=version)
        return bool(self._sync_backend.has_key(full_key))

    async def ahas_key(self, key: str, version: int | None = None) -> bool:
        """Asynchronously return True if unexpired key exists in cache."""
        full_key = self.make_and_validate_key(key, version=version)
        return bool(await self._async_backend.has_key(full_key))

    def incr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Increment integer value for `key`, raising `ValueError` when absent."""
        full_key = self.make_and_validate_key(key, version=version)
        current = self._sync_backend.get(full_key, default=NO_VALUE)
        if current is NO_VALUE:
            msg = f"Key '{key}' not found"
            raise ValueError(msg)
        new_value = int(current + delta)
        self._sync_backend.set(full_key, new_value)
        return new_value

    async def aincr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Asynchronously increment integer value for `key`, raising `ValueError` when absent."""
        full_key = self.make_and_validate_key(key, version=version)
        current = await self._async_backend.get(full_key, default=NO_VALUE)
        if current is NO_VALUE:
            msg = f"Key '{key}' not found"
            raise ValueError(msg)
        new_value = int(current + delta)
        await self._async_backend.set(full_key, new_value)
        return new_value

    def decr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Decrement integer value for `key`, raising `ValueError` when absent."""
        return self.incr(key, delta=-delta, version=version)

    async def adecr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Asynchronously decrement integer value for `key`, raising `ValueError` when absent."""
        return await self.aincr(key, delta=-delta, version=version)

    def clear(self) -> None:
        """Clear all entries matching backend key_prefix."""
        self._sync_backend.clear()

    async def aclear(self) -> None:
        """Asynchronously clear all entries matching backend key_prefix."""
        await self._async_backend.clear()

    def close(self, **kwargs: Any) -> None:
        """Close driver connections."""
        _ = kwargs
        self._sync_backend.close()

    async def aclose(self, **kwargs: Any) -> None:
        """Asynchronously close driver connections."""
        _ = kwargs
        await self._async_backend.aclose()
