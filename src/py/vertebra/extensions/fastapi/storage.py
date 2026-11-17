"""Sync and async object storage integration for FastAPI applications."""

from __future__ import annotations

from typing import Any

from vertebra.framework.storage import (
    AsyncObjectStore,
    AsyncStorageRegistry,
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    S3Config,
    StorageConfig,
    SyncObjectStore,
    SyncStorageRegistry,
)

__all__ = (
    "AsyncObjectStore",
    "AsyncStorageRegistry",
    "GCSConfig",
    "LocalConfig",
    "MemoryConfig",
    "S3Config",
    "StorageConfig",
    "SyncObjectStore",
    "SyncStorageRegistry",
    "attach_storage",
)


def attach_storage(
    app: Any,
    store_or_registry: SyncStorageRegistry
    | AsyncStorageRegistry
    | SyncObjectStore
    | AsyncObjectStore
    | StorageConfig
    | str
    | None = None,
    *,
    async_mode: bool = False,
) -> SyncStorageRegistry | AsyncStorageRegistry:
    """Attach a ``SyncStorageRegistry`` or ``AsyncStorageRegistry`` to ``app.state.storage``."""
    registry: SyncStorageRegistry | AsyncStorageRegistry
    if isinstance(store_or_registry, (SyncStorageRegistry, AsyncStorageRegistry)):
        registry = store_or_registry
    elif async_mode or isinstance(store_or_registry, AsyncObjectStore):
        registry = AsyncStorageRegistry(default_store=store_or_registry or MemoryConfig())
    else:
        registry = SyncStorageRegistry(default_store=store_or_registry or MemoryConfig())
    state = getattr(app, "state", None)
    if state is not None:
        if isinstance(state, dict):
            state["storage"] = registry
        else:
            state.storage = registry
    return registry
