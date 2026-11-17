"""Framework-neutral synchronous and asynchronous cache stores for Vertebra."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.cache._async import AsyncCacheStore
    from vertebra.cache._sync import SyncCacheStore
    from vertebra.cache._types import CacheStats

__all__ = (
    "AsyncCacheStore",
    "CacheStats",
    "SyncCacheStore",
)

__lazy_modules__ = [
    "vertebra.cache._async",
    "vertebra.cache._sync",
    "vertebra.cache._types",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "AsyncCacheStore": ("vertebra.cache._async", "AsyncCacheStore"),
    "CacheStats": ("vertebra.cache._types", "CacheStats"),
    "SyncCacheStore": ("vertebra.cache._sync", "SyncCacheStore"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
