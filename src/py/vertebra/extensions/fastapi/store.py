"""Sync and async native CacheStore for FastAPI applications."""

from __future__ import annotations

from vertebra.extensions.starlette.store import CacheStore, attach_cache_store

__all__ = (
    "CacheStore",
    "attach_cache_store",
)
