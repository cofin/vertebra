"""Dual sync and async CacheBackend and Flask-Caching BaseCache adapter for Flask applications."""

from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import inspect
import math
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

from vertebra.cache import AsyncCacheStore, SyncCacheStore
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json, decode_msgpack, encode_json_bytes, encode_msgpack_bytes

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from flask_caching.backends.base import BaseCache

__all__ = (
    "CacheBackend",
    "FlaskCachingBackend",
)

_NONE_SENTINEL = b"vtb:fc:\x00NONE"
_MSGPACK_PREFIX = b"vtb:fc:\x01"
_CACHED_BACKEND_CLS: type[Any] | None = None


def _encode_cached_value(value: Any) -> bytes:
    """Encode ``value`` with an explicit presence sentinel for ``None`` and integer counter bytes."""
    if value is None:
        return _NONE_SENTINEL
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value).encode("ascii")
    return _MSGPACK_PREFIX + encode_msgpack_bytes(value)


def _decode_cached_value(raw: bytes) -> Any:
    """Decode a cached payload previously encoded by ``_encode_cached_value``."""
    if raw == _NONE_SENTINEL:
        return None
    if raw.startswith(_MSGPACK_PREFIX):
        return decode_msgpack(raw[len(_MSGPACK_PREFIX) :])
    return int(raw.decode("ascii"))


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
        if self._remote_client is not None and hasattr(self._remote_client, "get"):
            raw_remote = self._call_remote_sync(self._remote_client.get, full_key)
            if raw_remote is None:
                return default
            return decode_json(raw_remote if isinstance(raw_remote, bytes) else str(raw_remote).encode("utf-8"))
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
            raw = await self._call_remote_async(self._remote_client.get, self._prefixed(key))
            if raw is None:
                return default
            return decode_json(raw if isinstance(raw, bytes) else str(raw).encode("utf-8"))
        return self.get(key, default)

    def set(self, key: str, value: Any, timeout: int | None = None) -> bool:
        """Synchronously store ``value`` under ``key``."""
        raw = encode_json_bytes(value)
        full_key = self._prefixed(key)
        effective = self._default_timeout if timeout is None else timeout
        if self._remote_client is not None and hasattr(self._remote_client, "set"):
            self._call_remote_sync(
                self._remote_client.set,
                full_key,
                raw,
                ex=effective if effective > 0 else None,
            )
        self._store[full_key] = (raw, self._expiry(timeout))
        return True

    async def aset(self, key: str, value: Any, timeout: int | None = None) -> bool:
        """Asynchronously store ``value`` under ``key``."""
        raw = encode_json_bytes(value)
        full_key = self._prefixed(key)
        effective = self._default_timeout if timeout is None else timeout
        if self._remote_client is not None and hasattr(self._remote_client, "set"):
            await self._call_remote_async(
                self._remote_client.set,
                full_key,
                raw,
                ex=effective if effective > 0 else None,
            )
        self._store[full_key] = (raw, self._expiry(timeout))
        return True

    def delete(self, key: str) -> bool:
        """Synchronously remove ``key`` from the cache."""
        full_key = self._prefixed(key)
        remote_deleted = False
        if self._remote_client is not None and hasattr(self._remote_client, "delete"):
            remote_deleted = bool(self._call_remote_sync(self._remote_client.delete, full_key))
        local_deleted = self._store.pop(full_key, None) is not None
        return remote_deleted or local_deleted

    async def adelete(self, key: str) -> bool:
        """Asynchronously remove ``key`` from the cache."""
        full_key = self._prefixed(key)
        remote_deleted = False
        if self._remote_client is not None and hasattr(self._remote_client, "delete"):
            remote_deleted = bool(await self._call_remote_async(self._remote_client.delete, full_key))
        local_deleted = self._store.pop(full_key, None) is not None
        return remote_deleted or local_deleted

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
        if self._remote_client is not None:
            self._clear_remote_sync()
        return True

    async def aclear(self) -> bool:
        """Asynchronously clear all cached entries."""
        self._store.clear()
        if self._remote_client is not None:
            await self._clear_remote_async()
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

    @staticmethod
    def _call_remote_sync(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Invoke a sync or async remote client method synchronously."""
        res = fn(*args, **kwargs)
        if not inspect.isawaitable(res):
            return res

        async def _await_res() -> Any:
            return await res

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(_await_res())
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, _await_res()).result()

    @staticmethod
    async def _call_remote_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Invoke a sync or async remote client method asynchronously."""
        res = fn(*args, **kwargs)
        if inspect.isawaitable(res):
            return await res
        return res

    def _clear_remote_sync(self) -> None:
        """Clear remote keys matching key_prefix or flush remote store synchronously."""
        rc = self._remote_client
        if rc is None:
            return
        if self._key_prefix and hasattr(rc, "scan"):
            cursor = 0
            pattern = f"{self._key_prefix}*"
            while True:
                try:
                    next_cursor, keys = self._call_remote_sync(rc.scan, cursor, match=pattern)
                except TypeError:
                    next_cursor, keys = self._call_remote_sync(rc.scan, cursor, match_pattern=pattern)
                str_keys = [k.decode("utf-8") if isinstance(k, bytes) else str(k) for k in keys]
                if str_keys:
                    if hasattr(rc, "unlink"):
                        self._call_remote_sync(rc.unlink, *str_keys)
                    elif hasattr(rc, "delete"):
                        for item_key in str_keys:
                            self._call_remote_sync(rc.delete, item_key)
                cursor = int(next_cursor)
                if cursor == 0:
                    break
            return
        if hasattr(rc, "clear"):
            self._call_remote_sync(rc.clear)
        elif hasattr(rc, "flushdb"):
            self._call_remote_sync(rc.flushdb)

    async def _clear_remote_async(self) -> None:
        """Clear remote keys matching key_prefix or flush remote store asynchronously."""
        rc = self._remote_client
        if rc is None:
            return
        if self._key_prefix and hasattr(rc, "scan"):
            cursor = 0
            pattern = f"{self._key_prefix}*"
            while True:
                try:
                    next_cursor, keys = await self._call_remote_async(rc.scan, cursor, match=pattern)
                except TypeError:
                    next_cursor, keys = await self._call_remote_async(rc.scan, cursor, match_pattern=pattern)
                str_keys = [k.decode("utf-8") if isinstance(k, bytes) else str(k) for k in keys]
                if str_keys:
                    if hasattr(rc, "unlink"):
                        await self._call_remote_async(rc.unlink, *str_keys)
                    elif hasattr(rc, "delete"):
                        for item_key in str_keys:
                            await self._call_remote_async(rc.delete, item_key)
                cursor = int(next_cursor)
                if cursor == 0:
                    break
            return
        if hasattr(rc, "clear"):
            await self._call_remote_async(rc.clear)
        elif hasattr(rc, "flushdb"):
            await self._call_remote_async(rc.flushdb)


class _FlaskCachingBackendImpl:
    """Implementation mixin backing ``FlaskCachingBackend`` over ``SyncCacheStore``."""

    default_timeout: int
    ignore_delete_many_errors: bool

    def __init__(
        self,
        default_timeout: int | timedelta = 300,
        *,
        ignore_delete_many_errors: bool = False,
        key_prefix: str = "vtb:flask:",
        url: str = "memory://",
        store: SyncCacheStore | None = None,
        capacity: int = 2048,
    ) -> None:
        base_mod = ensure_module("flask_caching.backends.base", package="flask-caching", extra="flask")
        base_mod.BaseCache.__init__(
            self,
            default_timeout=default_timeout,
            ignore_delete_many_errors=ignore_delete_many_errors,
        )
        self._key_prefix = str(key_prefix)
        store_prefix = self._key_prefix if self._key_prefix.strip() else "vtb:flask:"
        self._store = (
            store
            if store is not None
            else SyncCacheStore(
                url,
                capacity=capacity,
                default_ttl=None,
                key_prefix=store_prefix,
            )
        )
        self._async_store = AsyncCacheStore.from_sync(self._store)

    @classmethod
    def factory(
        cls,
        app: Any,
        config: dict[str, Any],
        args: list[Any],
        kwargs: dict[str, Any],
    ) -> FlaskCachingBackend:
        """Construct a ``FlaskCachingBackend`` from Flask-Caching configuration."""
        _ = app
        opts = dict(kwargs)
        default_timeout = opts.pop("default_timeout", config.get("CACHE_DEFAULT_TIMEOUT", 300))
        ignore_delete_many_errors = bool(
            opts.pop("ignore_delete_many_errors", config.get("CACHE_IGNORE_ERRORS", False))
        )
        key_prefix = str(opts.pop("key_prefix", config.get("CACHE_KEY_PREFIX", "vtb:flask:")))
        url = str(opts.pop("url", config.get("CACHE_REDIS_URL") or config.get("CACHE_VERTEBRA_URL") or "memory://"))
        store = opts.pop("store", None)
        backend_cls = cast("Any", cls)
        return cast(
            "FlaskCachingBackend",
            backend_cls(
                *args,
                default_timeout=default_timeout,
                ignore_delete_many_errors=ignore_delete_many_errors,
                key_prefix=key_prefix,
                url=url,
                store=store,
                **opts,
            ),
        )

    def _prefixed(self, key: str) -> str:
        return f"{self._key_prefix}{key}"

    def _resolve_ttl(self, timeout: int | float | timedelta | None) -> tuple[bool, float | None]:
        """Resolve Flask-Caching timeout into ``(should_store, ttl_seconds)``.

        A timeout of ``0`` indicates no expiry (``(True, None)``), negative values
        indicate immediate expiry (``(False, None)``), and positive finite values
        map to float TTL seconds on ``SyncCacheStore``.
        """
        raw = self.default_timeout if timeout is None else timeout
        if isinstance(raw, bool):
            msg = "timeout value must be a number or timedelta, not bool."
            raise TypeError(msg)
        seconds = float(raw.total_seconds()) if isinstance(raw, timedelta) else float(raw)
        if math.isnan(seconds) or math.isinf(seconds):
            msg = "timeout value must be a finite number."
            raise ValueError(msg)
        if seconds == 0.0:
            return True, None
        if seconds < 0.0:
            return False, None
        return True, seconds

    def get(self, key: str, default: Any = None) -> Any:
        """Synchronously retrieve ``key`` from the cache, returning ``default`` on miss."""
        raw = self._store.get(self._prefixed(key))
        if raw is None:
            return default
        return _decode_cached_value(raw)

    async def aget(self, key: str, default: Any = None) -> Any:
        """Asynchronously retrieve ``key`` from the cache, returning ``default`` on miss."""
        raw = await self._async_store.get(self._prefixed(key))
        if raw is None:
            return default
        return _decode_cached_value(raw)

    def set(self, key: str, value: Any, timeout: int | timedelta | None = None) -> bool:
        """Synchronously store ``value`` under ``key``."""
        full_key = self._prefixed(key)
        should_store, ttl_sec = self._resolve_ttl(timeout)
        if not should_store:
            self._store.delete(full_key)
            return True
        self._store.set(full_key, _encode_cached_value(value), ttl=ttl_sec)
        return True

    async def aset(self, key: str, value: Any, timeout: int | timedelta | None = None) -> bool:
        """Asynchronously store ``value`` under ``key``."""
        full_key = self._prefixed(key)
        should_store, ttl_sec = self._resolve_ttl(timeout)
        if not should_store:
            await self._async_store.delete(full_key)
            return True
        await self._async_store.set(full_key, _encode_cached_value(value), ttl=ttl_sec)
        return True

    def add(self, key: str, value: Any, timeout: int | timedelta | None = None) -> bool:
        """Store ``value`` only when ``key`` does not already exist (including cached ``None``)."""
        full_key = self._prefixed(key)
        should_store, ttl_sec = self._resolve_ttl(timeout)
        if self._store.has(full_key):
            return False
        if not should_store:
            return True
        return bool(self._store.add(full_key, _encode_cached_value(value), ttl=ttl_sec))

    def has(self, key: str) -> bool:
        """Return ``True`` when ``key`` exists in the cache (including sentinel-wrapped ``None``)."""
        return bool(self._store.has(self._prefixed(key)))

    async def ahas(self, key: str) -> bool:
        """Asynchronously return ``True`` when ``key`` exists in the cache."""
        return bool(await self._async_store.has(self._prefixed(key)))

    def delete(self, key: str) -> bool:
        """Synchronously delete ``key`` from the cache."""
        return bool(self._store.delete(self._prefixed(key)))

    async def adelete(self, key: str) -> bool:
        """Asynchronously delete ``key`` from the cache."""
        return bool(await self._async_store.delete(self._prefixed(key)))

    def get_many(self, *keys: str) -> list[Any]:
        """Synchronously retrieve multiple keys in positional order."""
        full_keys = [self._prefixed(k) for k in keys]
        raw_map = self._store.get_many(full_keys)
        return [_decode_cached_value(raw_map[fk]) if fk in raw_map else None for fk in full_keys]

    async def aget_many(self, *keys: str) -> list[Any]:
        """Asynchronously retrieve multiple keys in positional order."""
        full_keys = [self._prefixed(k) for k in keys]
        raw_map = await self._async_store.get_many(full_keys)
        return [_decode_cached_value(raw_map[fk]) if fk in raw_map else None for fk in full_keys]

    def set_many(self, mapping: Mapping[str, Any], timeout: int | timedelta | None = None) -> list[str]:
        """Synchronously store multiple key-value pairs and return the keys set."""
        set_keys: list[str] = []
        for k, v in mapping.items():
            if self.set(k, v, timeout=timeout):
                set_keys.append(k)
        return set_keys

    async def aset_many(self, mapping: Mapping[str, Any], timeout: int | timedelta | None = None) -> list[str]:
        """Asynchronously store multiple key-value pairs and return the keys set."""
        set_keys: list[str] = []
        for k, v in mapping.items():
            if await self.aset(k, v, timeout=timeout):
                set_keys.append(k)
        return set_keys

    def delete_many(self, *keys: str) -> list[str]:
        """Synchronously delete multiple keys and return the list of deleted keys."""
        deleted_keys: list[str] = []
        failed_keys: list[str] = []
        for k in keys:
            if self.delete(k) or not self.has(k):
                deleted_keys.append(k)
            else:
                failed_keys.append(k)
        if not self.ignore_delete_many_errors and failed_keys:
            msg = f"Failed to delete keys: {failed_keys}"
            raise RuntimeError(msg)
        return deleted_keys

    def clear(self) -> bool:
        """Clear all entries matching ``key_prefix`` via ``SyncCacheStore.delete_prefix``."""
        self._store.delete_prefix(self._key_prefix)
        return True

    async def aclear(self) -> bool:
        """Asynchronously clear all entries matching ``key_prefix``."""
        await self._async_store.delete_prefix(self._key_prefix)
        return True

    def inc(self, key: str, delta: int = 1) -> int | None:
        """Atomically increment integer key by ``delta``."""
        return int(self._store.incr(self._prefixed(key), delta))

    def dec(self, key: str, delta: int = 1) -> int | None:
        """Atomically decrement integer key by ``delta``."""
        return int(self._store.incr(self._prefixed(key), -delta))

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
                    if await self.ahas(cache_key):
                        return await self.aget(cache_key)
                    val = await fn(*args, **kwargs)
                    await self.aset(cache_key, val, timeout=timeout)
                    return val

                return _async_wrapper

            @functools.wraps(fn)
            def _sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                cache_key = f"{key_prefix}{fn.__qualname__}:{args!r}:{sorted(kwargs.items())!r}"
                if self.has(cache_key):
                    return self.get(cache_key)
                val = fn(*args, **kwargs)
                self.set(cache_key, val, timeout=timeout)
                return val

            return _sync_wrapper

        return _decorator


if TYPE_CHECKING:

    class FlaskCachingBackend(_FlaskCachingBackendImpl, BaseCache):
        """Drop-in ``CACHE_TYPE`` backend class compatible with ``Flask-Caching``."""


def __getattr__(name: str) -> Any:
    """Lazily bind ``FlaskCachingBackend`` to ``flask_caching.backends.base.BaseCache``."""
    global _CACHED_BACKEND_CLS
    if name == "FlaskCachingBackend":
        if _CACHED_BACKEND_CLS is None:
            base_mod = ensure_module("flask_caching.backends.base", package="flask-caching", extra="flask")
            base_cls = base_mod.BaseCache
            _CACHED_BACKEND_CLS = type(
                "FlaskCachingBackend",
                (_FlaskCachingBackendImpl, base_cls),
                {
                    "__module__": __name__,
                    "__doc__": "Drop-in ``CACHE_TYPE`` backend class compatible with ``Flask-Caching``.",
                },
            )
        return _CACHED_BACKEND_CLS
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    """Return module attributes including lazily bound ``FlaskCachingBackend``."""
    return sorted(set(globals()) | set(__all__))
