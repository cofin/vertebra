"""Django CacheBackend supporting full synchronous and *a-prefixed asynchronous cache operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache

from vertebra.extensions.alchemy.cache import NO_VALUE, AsyncCacheBackend, SyncCacheBackend

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = ("CacheBackend",)


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
        self._async_backend = AsyncCacheBackend(
            resolved_url,
            sync_backend=self._sync_backend,
            l1_url=l1_location,
            key_prefix=normalized_prefix,
            default_ttl=ttl_sec,
        )

    def add(
        self,
        key: str,
        value: Any,
        timeout: Any = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        """Store value only if key is not already present."""
        full_key = self.make_and_validate_key(key, version=version)
        timeout_sec = self.get_backend_timeout(timeout)
        return bool(self._sync_backend.add(full_key, value, expiration_time=timeout_sec))

    async def aadd(
        self,
        key: str,
        value: Any,
        timeout: Any = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        """Asynchronously store value only if key is not already present."""
        full_key = self.make_and_validate_key(key, version=version)
        timeout_sec = self.get_backend_timeout(timeout)
        return bool(await self._async_backend.add(full_key, value, expiration_time=timeout_sec))

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
        timeout_sec = self.get_backend_timeout(timeout)
        self._sync_backend.set(full_key, value, expiration_time=timeout_sec)

    async def aset(
        self,
        key: str,
        value: Any,
        timeout: Any = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> None:
        """Asynchronously store value under key."""
        full_key = self.make_and_validate_key(key, version=version)
        timeout_sec = self.get_backend_timeout(timeout)
        await self._async_backend.set(full_key, value, expiration_time=timeout_sec)

    def touch(
        self,
        key: str,
        timeout: Any = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        """Update expiration timestamp for key without modifying value."""
        full_key = self.make_and_validate_key(key, version=version)
        timeout_sec = self.get_backend_timeout(timeout)
        return bool(self._sync_backend.touch(full_key, expiration_time=timeout_sec))

    async def atouch(
        self,
        key: str,
        timeout: Any = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        """Asynchronously update expiration timestamp for key without modifying value."""
        full_key = self.make_and_validate_key(key, version=version)
        timeout_sec = self.get_backend_timeout(timeout)
        return bool(await self._async_backend.touch(full_key, expiration_time=timeout_sec))

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
        timeout_sec = self.get_backend_timeout(timeout)
        for k, v in data.items():
            full_key = self.make_and_validate_key(k, version=version)
            self._sync_backend.set(full_key, v, expiration_time=timeout_sec)
        return []

    async def aset_many(
        self,
        data: Mapping[str, Any],
        timeout: Any = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list[str]:
        """Asynchronously store multiple key/value pairs."""
        timeout_sec = self.get_backend_timeout(timeout)
        for k, v in data.items():
            full_key = self.make_and_validate_key(k, version=version)
            await self._async_backend.set(full_key, v, expiration_time=timeout_sec)
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
        """Atomically increment value associated with key."""
        full_key = self.make_and_validate_key(key, version=version)
        return int(self._sync_backend.incr(full_key, delta=delta))

    async def aincr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Asynchronously increment value associated with key."""
        full_key = self.make_and_validate_key(key, version=version)
        return int(await self._async_backend.incr(full_key, delta=delta))

    def decr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Atomically decrement value associated with key."""
        full_key = self.make_and_validate_key(key, version=version)
        return int(self._sync_backend.decr(full_key, delta=delta))

    async def adecr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Asynchronously decrement value associated with key."""
        full_key = self.make_and_validate_key(key, version=version)
        return int(await self._async_backend.decr(full_key, delta=delta))

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
