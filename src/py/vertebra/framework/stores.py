"""Framework key-value stores with monotonic TTL, Valkey RESP3 caching, and Redis backing."""

from __future__ import annotations

import math
import time
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Self

from vertebra.db.adapters.redis import AsyncRedisDriver, RedisConfig
from vertebra.db.adapters.valkey import AsyncValkeyDriver, ValkeyConfig
from vertebra.utils.module_loader import import_optional
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from litestar.stores.base import Store as _StoreBase
else:
    _base_store_mod = import_optional("litestar.stores.base")
    _StoreBase = _base_store_mod.Store if _base_store_mod is not None else object

__all__ = (
    "BaseKeyValueStore",
    "KeyValueStore",
    "MemoryStore",
    "NamespacedStore",
    "RedisStore",
    "ValkeyStore",
)


class BaseKeyValueStore(_StoreBase):
    """Lock-free monotonic TTL store foundation with driver backing and RESP3 invalidation."""

    __slots__ = ("_client_tracking", "_driver", "_key_prefix", "_l1_cache", "_max_l1_entries")

    def __init__(
        self,
        *,
        driver: Any = None,
        key_prefix: str = "vertebra",
        client_tracking: bool = True,
        max_l1_entries: int = 4096,
    ) -> None:
        if max_l1_entries < 0:
            msg = f"max_l1_entries must be >= 0, got {max_l1_entries}"
            raise ValueError(msg)
        self._key_prefix = validate_identifier(key_prefix, "key_prefix", allow_hyphens=True)
        self._client_tracking = client_tracking
        self._max_l1_entries = max_l1_entries
        self._l1_cache: OrderedDict[str, tuple[bytes, float | None]] = OrderedDict()
        self._driver = driver
        if self._client_tracking and driver is not None and hasattr(driver, "add_invalidation_listener"):
            driver.add_invalidation_listener(self._on_driver_invalidate)

    @property
    def driver(self) -> Any:
        """Return the underlying Valkey or Redis driver when configured."""
        return self._driver

    @property
    def max_l1_entries(self) -> int:
        """Return the maximum number of entries retained in the L1 cache."""
        return self._max_l1_entries

    @classmethod
    def from_valkey(
        cls,
        config_or_url: ValkeyConfig | str,
        *,
        key_prefix: str = "vertebra",
        client_tracking: bool = True,
        max_l1_entries: int = 4096,
    ) -> Self:
        """Create a store backed by Valkey with client-side caching."""
        cfg = ValkeyConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(
            driver=AsyncValkeyDriver(cfg),
            key_prefix=key_prefix,
            client_tracking=client_tracking,
            max_l1_entries=max_l1_entries,
        )

    @classmethod
    def from_redis(
        cls,
        config_or_url: RedisConfig | str,
        *,
        key_prefix: str = "vertebra",
        client_tracking: bool = True,
        max_l1_entries: int = 4096,
    ) -> Self:
        """Create a store backed by Redis."""
        cfg = RedisConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(
            driver=AsyncRedisDriver(cfg),
            key_prefix=key_prefix,
            client_tracking=client_tracking,
            max_l1_entries=max_l1_entries,
        )

    @property
    def key_prefix(self) -> str:
        """Return the namespace prepended to every stored key."""
        return self._key_prefix

    @property
    def _entries(self) -> OrderedDict[str, tuple[bytes, float | None]]:
        """Return the underlying L1 cache entries mapping."""
        return self._l1_cache

    async def __aenter__(self) -> Any:
        """Enter the async context manager and return the store instance."""
        return self

    async def __aexit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> None:
        """Exit the async context manager and close underlying driver."""
        _ = (exc_type, exc_val, exc_tb)
        await self.close()
        return None

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Retrieve bytes for key if present and unexpired, optionally renewing TTL."""
        full_key = self._make_key(key)
        entry = self._l1_cache.get(full_key)
        now = time.monotonic()
        if entry is not None:
            value, expires_at = entry
            if expires_at is not None and now >= expires_at:
                self._l1_cache.pop(full_key, None)
                entry = None
            else:
                self._l1_cache.move_to_end(full_key)
        if entry is None:
            if self._driver is None:
                return None
            raw = await self._driver.get(full_key)
            if raw is None:
                return None
            val_bytes = raw.encode("utf-8") if isinstance(raw, str) else bytes(raw)
            if renew_for is not None:
                ttl_ms = self._ttl_milliseconds(renew_for)
                new_expires = None if ttl_ms is None else time.monotonic() + ttl_ms / 1000.0
                self._l1_put(full_key, val_bytes, new_expires)
                await self._driver.set(full_key, val_bytes, px=ttl_ms)
                return val_bytes
            new_expires = None
            if hasattr(self._driver, "pttl"):
                pttl_ms = await self._driver.pttl(full_key)
                if pttl_ms == -2:
                    return None
                if pttl_ms >= 0:
                    new_expires = time.monotonic() + pttl_ms / 1000.0
            self._l1_put(full_key, val_bytes, new_expires)
            return val_bytes
        value, _expires_at = entry
        if renew_for is not None:
            ttl_ms = self._ttl_milliseconds(renew_for)
            new_expires = None if ttl_ms is None else now + ttl_ms / 1000.0
            self._l1_put(full_key, value, new_expires)
            if self._driver is not None:
                await self._driver.set(full_key, value, px=ttl_ms)
        return value

    async def set(
        self,
        key: str,
        value: str | bytes,
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Store value under key with an optional expiration duration."""
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        full_key = self._make_key(key)
        ttl_ms = self._ttl_milliseconds(expires_in)
        expires_at = None if ttl_ms is None else time.monotonic() + ttl_ms / 1000.0
        self._l1_put(full_key, raw, expires_at)
        if self._driver is not None:
            await self._driver.set(full_key, raw, px=ttl_ms)

    async def delete(self, key: str) -> None:
        """Delete key from the store if present."""
        full_key = self._make_key(key)
        self._l1_cache.pop(full_key, None)
        if self._driver is not None:
            await self._driver.delete(full_key)

    async def delete_all(self) -> None:
        """Delete every key under this store's key_prefix with prefix-scoped SCAN and UNLINK."""
        self._l1_cache.clear()
        if self._driver is None:
            return
        pattern = f"{self._key_prefix}:*"
        cursor = 0
        while True:
            cursor, keys = await self._driver.scan(cursor, match=pattern, count=500)
            if keys:
                await self._driver.unlink(*keys)
            if cursor == 0:
                break

    async def exists(self, key: str) -> bool:
        """Return True if key exists and has not expired."""
        return (await self.get(key)) is not None

    async def expires_in(self, key: str) -> int | None:
        """Return remaining TTL in seconds, or None when missing or expired."""
        full_key = self._make_key(key)
        entry = self._l1_cache.get(full_key)
        if entry is not None:
            _value, expires_at = entry
            if expires_at is None:
                return None
            remaining = expires_at - time.monotonic()
            if remaining <= 0:
                self._l1_cache.pop(full_key, None)
                return None
            return int(remaining)
        if self._driver is not None and hasattr(self._driver, "pttl"):
            pttl_ms = await self._driver.pttl(full_key)
            if pttl_ms in (-2, -1):
                return None
            return int(max(0, pttl_ms // 1000))
        return None

    async def set_many(
        self,
        mapping: Mapping[str, str | bytes],
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Store multiple key-value pairs with an optional shared expiration duration."""
        for key, value in mapping.items():
            await self.set(key, value, expires_in=expires_in)

    async def get_many(self, keys: Sequence[str]) -> dict[str, bytes | None]:
        """Retrieve multiple keys in a single batch dictionary."""
        result: dict[str, bytes | None] = {}
        for key in keys:
            result[key] = await self.get(key)
        return result

    def namespace(self, prefix: str) -> NamespacedStore:
        """Return a prefix-isolated NamespacedStore view over this store."""
        return NamespacedStore(self, prefix)

    async def close(self) -> None:
        """Close the underlying driver when configured."""
        if self._driver is not None and hasattr(self._driver, "close"):
            await self._driver.close()

    def _make_key(self, key: str) -> str:
        """Return the driver key <key_prefix>:<key>."""
        return f"{self._key_prefix}:{key}"

    @staticmethod
    def _ttl_milliseconds(expires_in: int | timedelta | None) -> int | None:
        """Convert expires_in to whole milliseconds, rounding up."""
        if expires_in is None:
            return None
        seconds = expires_in.total_seconds() if isinstance(expires_in, timedelta) else float(expires_in)
        if seconds <= 0:
            raise ValueError("expires_in must be a positive duration")
        return math.ceil(seconds * 1000)

    def _l1_put(self, full_key: str, value: bytes, expires_at: float | None) -> None:
        """Insert or update an L1 cache entry and evict oldest entries past max_l1_entries."""
        if self._max_l1_entries == 0:
            return
        self._l1_cache[full_key] = (value, expires_at)
        self._l1_cache.move_to_end(full_key)
        while len(self._l1_cache) > self._max_l1_entries:
            self._l1_cache.popitem(last=False)

    def _on_driver_invalidate(self, keys: Any) -> None:
        """Evict local cache entries matching invalidated keys."""
        for key in keys:
            str_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            self._l1_cache.pop(str_key, None)


class ValkeyStore(BaseKeyValueStore):
    """Litestar-compatible Store backed by native vertebra._valkey with RESP3 client-side caching."""

    __slots__ = ()

    def __init__(
        self,
        driver: AsyncValkeyDriver | None = None,
        config_or_url: ValkeyConfig | str | None = None,
        *,
        key_prefix: str = "vertebra",
        client_tracking: bool = True,
        max_l1_entries: int = 4096,
    ) -> None:
        if driver is not None:
            resolved_driver = driver
        elif config_or_url is not None:
            cfg = ValkeyConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
            resolved_driver = AsyncValkeyDriver(cfg)
        else:
            resolved_driver = AsyncValkeyDriver(ValkeyConfig())
        super().__init__(
            driver=resolved_driver,
            key_prefix=key_prefix,
            client_tracking=client_tracking,
            max_l1_entries=max_l1_entries,
        )

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        key_prefix: str = "vertebra",
        client_tracking: bool = True,
        max_l1_entries: int = 4096,
    ) -> Self:
        """Create a ValkeyStore from a valkey:// connection URL."""
        return cls(
            config_or_url=url,
            key_prefix=key_prefix,
            client_tracking=client_tracking,
            max_l1_entries=max_l1_entries,
        )


class RedisStore(BaseKeyValueStore):
    """Litestar-compatible Store backed by native vertebra._redis with RESP3 client-side caching."""

    __slots__ = ()

    def __init__(
        self,
        driver: AsyncRedisDriver | None = None,
        config_or_url: RedisConfig | str | None = None,
        *,
        key_prefix: str = "vertebra",
        client_tracking: bool = True,
        max_l1_entries: int = 4096,
    ) -> None:
        if driver is not None:
            resolved_driver = driver
        elif config_or_url is not None:
            cfg = RedisConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
            resolved_driver = AsyncRedisDriver(cfg)
        else:
            resolved_driver = AsyncRedisDriver(RedisConfig())
        super().__init__(
            driver=resolved_driver,
            key_prefix=key_prefix,
            client_tracking=client_tracking,
            max_l1_entries=max_l1_entries,
        )

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        key_prefix: str = "vertebra",
        client_tracking: bool = True,
        max_l1_entries: int = 4096,
    ) -> Self:
        """Create a RedisStore from a redis:// connection URL."""
        return cls(
            config_or_url=url,
            key_prefix=key_prefix,
            client_tracking=client_tracking,
            max_l1_entries=max_l1_entries,
        )


class MemoryStore(BaseKeyValueStore):
    """In-memory monotonic TTL store conforming to Litestar Store protocol."""

    __slots__ = ()

    def __init__(
        self,
        *,
        driver: Any = None,
        key_prefix: str = "vertebra",
        client_tracking: bool = False,
        max_l1_entries: int = 4096,
    ) -> None:
        super().__init__(
            driver=driver,
            key_prefix=key_prefix,
            client_tracking=client_tracking,
            max_l1_entries=max_l1_entries,
        )


class KeyValueStore(BaseKeyValueStore):
    """Lock-free monotonic TTL store with optional Valkey/Redis backing and RESP3 invalidation."""

    __slots__ = ()

    def __init__(
        self,
        *,
        driver: AsyncValkeyDriver | AsyncRedisDriver | None = None,
        key_prefix: str = "vertebra",
        client_tracking: bool = True,
        max_l1_entries: int = 4096,
    ) -> None:
        super().__init__(
            driver=driver,
            key_prefix=key_prefix,
            client_tracking=client_tracking,
            max_l1_entries=max_l1_entries,
        )


class NamespacedStore:
    """Prefix-isolated view over a BaseKeyValueStore."""

    __slots__ = ("_namespace", "_store")

    def __init__(self, store: BaseKeyValueStore, namespace: str) -> None:
        self._store = store
        self._namespace = namespace.strip(":")

    @property
    def namespace(self) -> str:
        """Return the configured key namespace."""
        return self._namespace

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Retrieve bytes for the namespaced key."""
        return await self._store.get(f"{self._namespace}:{key}", renew_for=renew_for)

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Store a value under the namespaced key."""
        await self._store.set(f"{self._namespace}:{key}", value, expires_in=expires_in)

    async def delete(self, key: str) -> None:
        """Delete the namespaced key."""
        await self._store.delete(f"{self._namespace}:{key}")


def _register_stores() -> None:
    """Register stores with Litestar Store ABC if litestar is available."""
    base_mod = import_optional("litestar.stores.base")
    if base_mod is not None:
        register_fn = getattr(base_mod.Store, "register", None)
        if callable(register_fn):
            register_fn(BaseKeyValueStore)
            register_fn(MemoryStore)
            register_fn(ValkeyStore)
            register_fn(RedisStore)
            register_fn(KeyValueStore)
            register_fn(NamespacedStore)


_register_stores()
