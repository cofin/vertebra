"""Lock-free Vertebra Store implementation for Litestar response caching."""

import importlib
import time
from datetime import timedelta
from typing import Any, Self

__all__ = ("VertebraMemoryStore", "swap_litestar_response_cache_store")


class VertebraMemoryStore:
    """Lock-free monotonic TTL store replacing Litestar's anyio.Lock-gated MemoryStore."""

    __slots__ = ("_entries",)

    def __init__(self) -> None:
        self._entries: dict[str, tuple[bytes, float | None]] = {}

    async def __aenter__(self) -> Self:
        """Enter the async context manager and return the store instance."""
        return self

    async def __aexit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> None:
        """Exit the async context manager."""
        _ = (exc_type, exc_val, exc_tb)
        return None

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Retrieve bytes for key if present and unexpired, optionally renewing TTL."""
        entry = self._entries.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        now = time.monotonic()
        if expires_at is not None and now >= expires_at:
            self._entries.pop(key, None)
            return None
        if renew_for is not None:
            ttl_s = renew_for.total_seconds() if isinstance(renew_for, timedelta) else float(renew_for)
            self._entries[key] = (value, now + ttl_s)
        return value

    async def set(
        self,
        key: str,
        value: str | bytes,
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Store value under key with an optional expiration duration."""
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        if expires_in is None:
            expires_at = None
        else:
            ttl_s = expires_in.total_seconds() if isinstance(expires_in, timedelta) else float(expires_in)
            expires_at = time.monotonic() + ttl_s
        self._entries[key] = (raw, expires_at)

    async def delete(self, key: str) -> None:
        """Delete key from the store if present."""
        self._entries.pop(key, None)

    async def delete_all(self) -> None:
        """Clear all entries from the store."""
        self._entries.clear()

    async def exists(self, key: str) -> bool:
        """Return True if key exists and has not expired."""
        return (await self.get(key)) is not None

    async def expires_in(self, key: str) -> int | None:
        """Return remaining TTL in seconds for key, or None if unset or expired."""
        entry = self._entries.get(key)
        if entry is None:
            return None
        _value, expires_at = entry
        if expires_at is None:
            return -1
        remaining = expires_at - time.monotonic()
        if remaining <= 0:
            self._entries.pop(key, None)
            return None
        return int(remaining)


def swap_litestar_response_cache_store(app: Any) -> VertebraMemoryStore | None:
    """Transparently replace Litestar's default response_cache MemoryStore with VertebraMemoryStore."""
    stores = getattr(app, "stores", None)
    if stores is None:
        return None
    cache_cfg = getattr(app, "response_cache_config", None)
    store_name = getattr(cache_cfg, "store", "response_cache") if cache_cfg is not None else "response_cache"
    internal_dict = getattr(stores, "_stores", None)
    if not isinstance(internal_dict, dict):
        if isinstance(stores, dict):
            internal_dict = stores
        else:
            return None
    current = internal_dict.get(store_name)
    is_memory_store = (_MemoryStoreCls is not None and isinstance(current, _MemoryStoreCls)) or type(
        current
    ).__name__ == "MemoryStore"
    if is_memory_store:
        replacement = VertebraMemoryStore()
        internal_dict[store_name] = replacement
        return replacement
    if isinstance(current, VertebraMemoryStore):
        return current
    return None


def _load_store_types() -> type[Any] | None:
    """Load Litestar MemoryStore and register VertebraMemoryStore as a Store virtual subclass."""
    try:
        base_mod = importlib.import_module("litestar.stores.base")
        mem_mod = importlib.import_module("litestar.stores.memory")
        register_fn = getattr(base_mod.Store, "register", None)
        if callable(register_fn):
            register_fn(VertebraMemoryStore)
        memory_cls: type[Any] = mem_mod.MemoryStore
        return memory_cls
    except ImportError:
        return None


_MemoryStoreCls = _load_store_types()
