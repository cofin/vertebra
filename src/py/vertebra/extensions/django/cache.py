"""Django CacheBackend supporting full synchronous and *a-prefixed asynchronous cache operations."""

from __future__ import annotations

import math
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from vertebra.cache import AsyncCacheStore, SyncCacheStore
from vertebra.utils.module_loader import ensure_module, import_optional
from vertebra.utils.serializers import decode_msgpack, encode_msgpack_bytes

_cache_base_module = import_optional("django.core.cache.backends.base")

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from django.core.cache.backends.base import BaseCache as _BaseCache
else:
    _BaseCache = _cache_base_module.BaseCache if _cache_base_module is not None else object

DEFAULT_TIMEOUT: Any = (
    getattr(_cache_base_module, "DEFAULT_TIMEOUT", object()) if _cache_base_module is not None else object()
)

__all__ = ("CacheBackend",)


def __getattr__(name: str) -> Any:
    """Lazily resolve optional Django BaseCache classes on demand."""
    if name == "BaseCache":
        return ensure_module("django.core.cache.backends.base", package="django", extra="django").BaseCache
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


_MSGPACK_PREFIX = b"vtb:mp:\x01"


def _encode_value(value: Any) -> bytes:
    """Encode plain integers as ASCII bytes for atomic counter compatibility and frame other values."""
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value).encode("ascii")
    return _MSGPACK_PREFIX + encode_msgpack_bytes(value)


def _decode_value(raw: bytes) -> Any:
    """Decode framed msgpack payloads or ASCII integer counters returned by ``vertebra.cache``."""
    if raw.startswith(_MSGPACK_PREFIX):
        return decode_msgpack(raw[len(_MSGPACK_PREFIX) :])
    return int(raw.decode("ascii"))


class CacheBackend(_BaseCache):
    """Drop-in Django cache backend backed by ``SyncCacheStore`` and ``AsyncCacheStore``."""

    def __init__(self, server: str = "", params: Mapping[str, Any] | None = None) -> None:
        """Initialize CacheBackend with server location URL and Django params."""
        if _cache_base_module is None:
            ensure_module("django.core.cache.backends.base", package="django", extra="django")
        cfg = dict(params or {})
        super().__init__(cfg)
        raw_timeout = cfg.get("timeout", cfg.get("TIMEOUT", 300))
        if isinstance(raw_timeout, float) and not isinstance(raw_timeout, bool):
            self.default_timeout = raw_timeout
        elif isinstance(raw_timeout, timedelta):
            self.default_timeout = raw_timeout.total_seconds()
        self._location = server
        options = dict(cfg.get("OPTIONS") or {})
        l1_location = options.get("L1_LOCATION")

        raw_location = server.strip()
        resolved_url = "memory://" if not raw_location or raw_location.startswith("vertebra://") else raw_location

        prefix = str(options.get("KEY_PREFIX") or cfg.get("KEY_PREFIX") or "django")
        normalized_prefix = prefix.strip(":") or "django"

        self._sync_store = SyncCacheStore(
            resolved_url,
            l1_url=l1_location,
            key_prefix=normalized_prefix,
            default_ttl=None,
        )
        self._async_store = AsyncCacheStore.from_sync(self._sync_store)

    def _resolve_relative_ttl(self, timeout: Any) -> tuple[bool, float | None]:
        """Resolve a Django timeout argument into ``(should_store, relative_seconds)``.

        Returns ``(True, None)`` when ``timeout`` is ``None`` (cache forever), ``(False, 0.0)``
        when ``timeout <= 0`` (expire immediately / do not store), and ``(True, seconds)``
        for positive relative durations.
        """
        effective = self.default_timeout if timeout == DEFAULT_TIMEOUT else timeout
        if effective is None:
            return True, None
        if isinstance(effective, bool):
            msg = "timeout value must be a number or None, not bool."
            raise TypeError(msg)
        raw_seconds = effective.total_seconds() if isinstance(effective, timedelta) else float(effective)
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
        if self._sync_store.has(full_key):
            return False
        if not should_store:
            return True
        return bool(self._sync_store.add(full_key, _encode_value(value), ttl=ttl_sec))

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
        if await self._async_store.has(full_key):
            return False
        if not should_store:
            return True
        return bool(await self._async_store.add(full_key, _encode_value(value), ttl=ttl_sec))

    def get(self, key: str, default: Any = None, version: int | None = None) -> Any:
        """Synchronously fetch key from the cache."""
        full_key = self.make_and_validate_key(key, version=version)
        raw = self._sync_store.get(full_key)
        return default if raw is None else _decode_value(raw)

    async def aget(self, key: str, default: Any = None, version: int | None = None) -> Any:
        """Asynchronously fetch key from the cache."""
        full_key = self.make_and_validate_key(key, version=version)
        raw = await self._async_store.get(full_key)
        return default if raw is None else _decode_value(raw)

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
            self._sync_store.delete(full_key)
            return
        self._sync_store.set(full_key, _encode_value(value), ttl=ttl_sec)

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
            await self._async_store.delete(full_key)
            return
        await self._async_store.set(full_key, _encode_value(value), ttl=ttl_sec)

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
            return bool(self._sync_store.delete(full_key))
        return bool(self._sync_store.touch(full_key, ttl=ttl_sec))

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
            return bool(await self._async_store.delete(full_key))
        return bool(await self._async_store.touch(full_key, ttl=ttl_sec))

    def delete(self, key: str, version: int | None = None) -> bool:
        """Delete key and return True if it existed."""
        full_key = self.make_and_validate_key(key, version=version)
        return bool(self._sync_store.delete(full_key))

    async def adelete(self, key: str, version: int | None = None) -> bool:
        """Asynchronously delete key and return True if it existed."""
        full_key = self.make_and_validate_key(key, version=version)
        return bool(await self._async_store.delete(full_key))

    def get_many(self, keys: Iterable[str], version: int | None = None) -> dict[str, Any]:
        """Synchronously fetch multiple keys."""
        out: dict[str, Any] = {}
        for k in keys:
            full_key = self.make_and_validate_key(k, version=version)
            raw = self._sync_store.get(full_key)
            if raw is not None:
                out[k] = _decode_value(raw)
        return out

    async def aget_many(self, keys: Iterable[str], version: int | None = None) -> dict[str, Any]:
        """Asynchronously fetch multiple keys."""
        out: dict[str, Any] = {}
        for k in keys:
            full_key = self.make_and_validate_key(k, version=version)
            raw = await self._async_store.get(full_key)
            if raw is not None:
                out[k] = _decode_value(raw)
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
                self._sync_store.delete(full_key)
            else:
                self._sync_store.set(full_key, _encode_value(v), ttl=ttl_sec)
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
                await self._async_store.delete(full_key)
            else:
                await self._async_store.set(full_key, _encode_value(v), ttl=ttl_sec)
        return []

    def delete_many(self, keys: Iterable[str], version: int | None = None) -> None:
        """Synchronously delete multiple keys."""
        for k in keys:
            full_key = self.make_and_validate_key(k, version=version)
            self._sync_store.delete(full_key)

    async def adelete_many(self, keys: Iterable[str], version: int | None = None) -> None:
        """Asynchronously delete multiple keys."""
        for k in keys:
            full_key = self.make_and_validate_key(k, version=version)
            await self._async_store.delete(full_key)

    def has_key(self, key: str, version: int | None = None) -> bool:
        """Return True if unexpired key exists in cache."""
        full_key = self.make_and_validate_key(key, version=version)
        return bool(self._sync_store.has(full_key))

    async def ahas_key(self, key: str, version: int | None = None) -> bool:
        """Asynchronously return True if unexpired key exists in cache."""
        full_key = self.make_and_validate_key(key, version=version)
        return bool(await self._async_store.has(full_key))

    def incr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Increment integer value for ``key``, raising ``ValueError`` when absent."""
        full_key = self.make_and_validate_key(key, version=version)
        if not self._sync_store.has(full_key):
            msg = f"Key '{key}' not found"
            raise ValueError(msg)
        return self._sync_store.incr(full_key, delta)

    async def aincr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Asynchronously increment integer value for ``key``, raising ``ValueError`` when absent."""
        full_key = self.make_and_validate_key(key, version=version)
        if not await self._async_store.has(full_key):
            msg = f"Key '{key}' not found"
            raise ValueError(msg)
        return await self._async_store.incr(full_key, delta)

    def decr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Decrement integer value for ``key``, raising ``ValueError`` when absent."""
        return self.incr(key, delta=-delta, version=version)

    async def adecr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Asynchronously decrement integer value for ``key``, raising ``ValueError`` when absent."""
        return await self.aincr(key, delta=-delta, version=version)

    def clear(self) -> None:
        """Clear all entries matching backend key_prefix."""
        self._sync_store.clear()

    async def aclear(self) -> None:
        """Asynchronously clear all entries matching backend key_prefix."""
        await self._async_store.clear()

    def close(self, **kwargs: Any) -> None:
        """Close driver connections."""
        _ = kwargs
        self._sync_store.close()

    async def aclose(self, **kwargs: Any) -> None:
        """Asynchronously close driver connections."""
        _ = kwargs
        await self._async_store.aclose()
