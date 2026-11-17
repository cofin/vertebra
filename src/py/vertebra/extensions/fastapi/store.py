"""Sync and async native CacheStore for FastAPI applications."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Self

from vertebra.cache import AsyncCacheStore, SyncCacheStore
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from vertebra.db.adapters.redis import RedisConfig
    from vertebra.db.adapters.valkey import ValkeyConfig

__all__ = (
    "CacheStore",
    "attach_cache_store",
)


class CacheStore:
    """Monotonic TTL cache store backed by ``vertebra.cache`` sync and async stores."""

    __slots__ = ("_async_store", "_key_prefix", "_sync_store")

    def __init__(
        self,
        url: str = "memory://",
        *,
        include_sync: bool = True,
        key_prefix: str = "vertebra",
        async_store: AsyncCacheStore | None = None,
        sync_store: SyncCacheStore | None = None,
    ) -> None:
        self._key_prefix = validate_identifier(key_prefix, "key_prefix", allow_hyphens=True)
        prefixed = f"{self._key_prefix}:"
        resolved_sync: SyncCacheStore | None
        if url == "memory://" and sync_store is None and async_store is None:
            mem_sync = SyncCacheStore("memory://", key_prefix=prefixed)
            resolved_sync = mem_sync
            resolved_async = AsyncCacheStore.from_sync(mem_sync)
        else:
            resolved_sync = (
                sync_store
                if sync_store is not None
                else (SyncCacheStore(url, key_prefix=prefixed) if include_sync else None)
            )
            resolved_async = async_store if async_store is not None else AsyncCacheStore(url, key_prefix=prefixed)
        self._sync_store: SyncCacheStore | None = resolved_sync
        self._async_store: AsyncCacheStore = resolved_async

    @classmethod
    def from_valkey(
        cls,
        config_or_url: ValkeyConfig | str,
        *,
        include_sync: bool = True,
        key_prefix: str = "vertebra",
    ) -> Self:
        """Create a ``CacheStore`` backed by Valkey."""
        url = config_or_url if isinstance(config_or_url, str) else config_or_url.url
        return cls(url, include_sync=include_sync, key_prefix=key_prefix)

    @classmethod
    def from_redis(
        cls,
        config_or_url: RedisConfig | str,
        *,
        include_sync: bool = True,
        key_prefix: str = "vertebra",
    ) -> Self:
        """Create a ``CacheStore`` backed by Redis."""
        url = config_or_url if isinstance(config_or_url, str) else config_or_url.url
        return cls(url, include_sync=include_sync, key_prefix=key_prefix)

    @property
    def key_prefix(self) -> str:
        """Return the namespace prepended to every stored key."""
        return self._key_prefix

    def get_sync(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Synchronously retrieve bytes for ``key`` if present and unexpired."""
        if self._sync_store is None:
            return None
        val = self._sync_store.get(key)
        if val is not None and renew_for is not None:
            self._sync_store.touch(key, ttl=self._ttl_seconds(renew_for))
        return val

    def set_sync(
        self,
        key: str,
        value: str | bytes,
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Synchronously store ``value`` under ``key`` with an optional expiration duration."""
        if self._sync_store is None:
            return
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        self._sync_store.set(key, raw, ttl=self._ttl_seconds(expires_in))

    def delete_sync(self, key: str) -> None:
        """Synchronously delete ``key`` from the store."""
        if self._sync_store is not None:
            self._sync_store.delete(key)

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Asynchronously retrieve bytes for ``key`` if present and unexpired."""
        val = await self._async_store.get(key)
        if val is not None and renew_for is not None:
            await self._async_store.touch(key, ttl=self._ttl_seconds(renew_for))
        return val

    async def set(
        self,
        key: str,
        value: str | bytes,
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Asynchronously store ``value`` under ``key`` with an optional expiration duration."""
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        await self._async_store.set(key, raw, ttl=self._ttl_seconds(expires_in))

    async def delete(self, key: str) -> None:
        """Asynchronously delete ``key`` from the store."""
        await self._async_store.delete(key)

    async def delete_all(self) -> None:
        """Delete every key under this store's ``key_prefix``."""
        await self._async_store.clear()

    async def get_many(self, keys: Sequence[str]) -> dict[str, bytes | None]:
        """Retrieve multiple keys in a single batch dictionary."""
        found = await self._async_store.get_many(keys)
        return {k: found.get(k) for k in keys}

    async def set_many(
        self,
        mapping: Mapping[str, str | bytes],
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Store multiple key-value pairs with an optional shared expiration."""
        encoded: dict[str, bytes] = {
            k: v.encode("utf-8") if isinstance(v, str) else bytes(v) for k, v in mapping.items()
        }
        await self._async_store.set_many(encoded, ttl=self._ttl_seconds(expires_in))

    async def close(self) -> None:
        """Close the underlying sync and async cache stores."""
        await self._async_store.aclose()
        if self._sync_store is not None:
            self._sync_store.close()

    @staticmethod
    def _ttl_seconds(expires_in: int | timedelta | None) -> float | None:
        if expires_in is None:
            return None
        seconds = expires_in.total_seconds() if isinstance(expires_in, timedelta) else float(expires_in)
        if seconds <= 0:
            msg = "expires_in must be a positive duration"
            raise ValueError(msg)
        return seconds


def attach_cache_store(app: Any, store: CacheStore | None = None) -> CacheStore:
    """Attach a ``CacheStore`` instance to ``app.state.cache``."""
    resolved = store if store is not None else CacheStore()
    state = getattr(app, "state", None)
    if state is not None:
        if isinstance(state, dict):
            state["cache"] = resolved
        else:
            state.cache = resolved
    return resolved
