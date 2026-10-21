"""Framework key-value stores with monotonic TTL, Valkey RESP3 client-side caching, and Redis backing."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.extensions.litestar.store import (
    MemoryStore,
    swap_litestar_response_cache_store,
)

if TYPE_CHECKING:
    from datetime import timedelta


class NamespacedStore:
    """Prefix-isolated view over a ``MemoryStore``."""

    __slots__ = ("_namespace", "_store")

    def __init__(self, store: MemoryStore, namespace: str) -> None:
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


__all__ = (
    "MemoryStore",
    "NamespacedStore",
    "swap_litestar_response_cache_store",
)
