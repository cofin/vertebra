"""Shared value types for the framework-neutral ``vertebra.cache`` package."""

from __future__ import annotations

import msgspec

__all__ = ("CacheStats",)


class CacheStats(msgspec.Struct, frozen=True, gc=False):
    """Immutable snapshot of cache hit, miss, eviction, expiration, and capacity counters."""

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    size: int = 0
    capacity: int = 0
