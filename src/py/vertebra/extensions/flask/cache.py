"""Dual sync and async CacheBackend for Flask applications and Flask-Caching."""

from __future__ import annotations

import functools
import inspect
import time
from typing import TYPE_CHECKING, Any

from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

__all__ = (
    "CacheBackend",
    "FlaskCachingBackend",
)


class CacheBackend:
    """In-memory and Valkey/Redis cache backend with synchronous and ``*a``-prefixed async methods."""

    __slots__ = ("_default_timeout", "_key_prefix", "_remote_client", "_store")

    def __init__(
        self,
        *,
        default_timeout: int = 300,
        key_prefix: str = "vtb:flask:",
        remote_client: Any = None,
    ) -> None:
        self._default_timeout = default_timeout
        self._key_prefix = key_prefix
        self._remote_client = remote_client
        self._store: dict[str, tuple[bytes, float | None]] = {}

    def _prefixed(self, key: str) -> str:
        return f"{self._key_prefix}{key}"

    def _expiry(self, timeout: int | None) -> float | None:
        effective = self._default_timeout if timeout is None else timeout
        if effective <= 0:
            return None
        return time.monotonic() + effective

    def get(self, key: str, default: Any = None) -> Any:
        """Synchronously retrieve ``key`` from the cache."""
        full_key = self._prefixed(key)
        entry = self._store.get(full_key)
        if entry is None:
            return default
        raw, expires_at = entry
        if expires_at is not None and time.monotonic() >= expires_at:
            self._store.pop(full_key, None)
            return default
        return decode_json(raw)

    async def aget(self, key: str, default: Any = None) -> Any:
        """Asynchronously retrieve ``key`` from the cache."""
        if self._remote_client is not None and hasattr(self._remote_client, "get"):
            raw = await self._remote_client.get(self._prefixed(key))
            if raw is None:
                return default
            return decode_json(raw if isinstance(raw, bytes) else str(raw).encode("utf-8"))
        return self.get(key, default)

    def set(self, key: str, value: Any, timeout: int | None = None) -> bool:
        """Synchronously store ``value`` under ``key``."""
        raw = encode_json_bytes(value)
        self._store[self._prefixed(key)] = (raw, self._expiry(timeout))
        return True

    async def aset(self, key: str, value: Any, timeout: int | None = None) -> bool:
        """Asynchronously store ``value`` under ``key``."""
        raw = encode_json_bytes(value)
        effective = self._default_timeout if timeout is None else timeout
        if self._remote_client is not None and hasattr(self._remote_client, "set"):
            await self._remote_client.set(self._prefixed(key), raw, ex=effective if effective > 0 else None)
        self._store[self._prefixed(key)] = (raw, self._expiry(timeout))
        return True

    def delete(self, key: str) -> bool:
        """Synchronously remove ``key`` from the cache."""
        return self._store.pop(self._prefixed(key), None) is not None

    async def adelete(self, key: str) -> bool:
        """Asynchronously remove ``key`` from the cache."""
        if self._remote_client is not None and hasattr(self._remote_client, "delete"):
            await self._remote_client.delete(self._prefixed(key))
        return self.delete(key)

    def get_many(self, *keys: str) -> list[Any]:
        """Synchronously retrieve multiple keys in order."""
        return [self.get(k) for k in keys]

    async def aget_many(self, *keys: str) -> list[Any]:
        """Asynchronously retrieve multiple keys in order."""
        return [await self.aget(k) for k in keys]

    def set_many(self, mapping: Mapping[str, Any], timeout: int | None = None) -> bool:
        """Synchronously store multiple key-value pairs."""
        for k, v in mapping.items():
            self.set(k, v, timeout=timeout)
        return True

    async def aset_many(self, mapping: Mapping[str, Any], timeout: int | None = None) -> bool:
        """Asynchronously store multiple key-value pairs."""
        for k, v in mapping.items():
            await self.aset(k, v, timeout=timeout)
        return True

    def delete_many(self, *keys: Iterable[str] | str) -> bool:
        """Synchronously delete multiple keys."""
        for k in keys:
            if isinstance(k, str):
                self.delete(k)
            else:
                for sub in k:
                    self.delete(sub)
        return True

    def clear(self) -> bool:
        """Synchronously clear all cached entries."""
        self._store.clear()
        return True

    async def aclear(self) -> bool:
        """Asynchronously clear all cached entries."""
        self._store.clear()
        return True

    def cached(
        self,
        *,
        timeout: int | None = None,
        key_prefix: str = "view:",
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator caching synchronous or asynchronous function return values."""

        def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            if inspect.iscoroutinefunction(fn):

                @functools.wraps(fn)
                async def _async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    cache_key = f"{key_prefix}{fn.__qualname__}:{args!r}:{sorted(kwargs.items())!r}"
                    hit = await self.aget(cache_key)
                    if hit is not None:
                        return hit
                    val = await fn(*args, **kwargs)
                    await self.aset(cache_key, val, timeout=timeout)
                    return val

                return _async_wrapper

            @functools.wraps(fn)
            def _sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                cache_key = f"{key_prefix}{fn.__qualname__}:{args!r}:{sorted(kwargs.items())!r}"
                hit = self.get(cache_key)
                if hit is not None:
                    return hit
                val = fn(*args, **kwargs)
                self.set(cache_key, val, timeout=timeout)
                return val

            return _sync_wrapper

        return _decorator


class FlaskCachingBackend(CacheBackend):
    """Drop-in ``CACHE_TYPE`` backend class compatible with ``Flask-Caching``."""

    @classmethod
    def factory(cls, app: Any, config: dict[str, Any], args: list[Any], kwargs: dict[str, Any]) -> FlaskCachingBackend:
        """Construct a ``FlaskCachingBackend`` from Flask-Caching configuration."""
        default_timeout = int(config.get("CACHE_DEFAULT_TIMEOUT", 300))
        key_prefix = str(config.get("CACHE_KEY_PREFIX", "vtb:flask:"))
        return cls(*args, default_timeout=default_timeout, key_prefix=key_prefix, **kwargs)
