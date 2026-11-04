"""Sync and async object storage integration for Starlette and FastAPI applications."""

from __future__ import annotations

from typing import Any

from vertebra.storage import (
    AsyncObjectStore,
    AzureConfig,
    GCSConfig,
    HTTPConfig,
    LocalConfig,
    MemoryConfig,
    OCIConfig,
    R2Config,
    S3Config,
    StorageConfig,
    SyncObjectStore,
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

_STORAGE_CONFIGS = (LocalConfig, S3Config, GCSConfig, AzureConfig, OCIConfig, R2Config, HTTPConfig, MemoryConfig)


class SyncStorageRegistry:
    """Named registry of Vertebra SyncObjectStore backends."""

    __slots__ = ("_default", "_stores")

    def __init__(
        self,
        default_store: SyncObjectStore | AsyncObjectStore | StorageConfig | str | None = None,
        *,
        default_name: str = "default",
    ) -> None:
        self._stores: dict[str, SyncObjectStore] = {}
        self._default = default_name
        if default_store is not None:
            self.register(default_name, default_store, default=True)

    def register(
        self,
        name: str,
        store: SyncObjectStore | AsyncObjectStore | StorageConfig | str,
        *,
        default: bool = False,
    ) -> None:
        """Register a named SyncObjectStore backend."""
        if isinstance(store, SyncObjectStore):
            sync_store = store
        elif isinstance(store, AsyncObjectStore):
            sync_store = store.to_sync()
        elif isinstance(store, _STORAGE_CONFIGS):
            sync_store = SyncObjectStore.from_config(store)
        elif isinstance(store, str):
            sync_store = SyncObjectStore(store)
        else:
            msg = f"Expected SyncObjectStore, AsyncObjectStore, StorageConfig or URI, got {type(store)!r}"
            raise TypeError(msg)

        self._stores[name] = sync_store
        if default or len(self._stores) == 1:
            self._default = name

    def get(self, name: str | None = None) -> SyncObjectStore:
        """Return the requested SyncObjectStore backend or the default backend."""
        target = name or self._default
        if target not in self._stores:
            self.register(target, MemoryConfig(), default=False)
        return self._stores[target]


class AsyncStorageRegistry:
    """Named registry of Vertebra AsyncObjectStore backends."""

    __slots__ = ("_default", "_stores")

    def __init__(
        self,
        default_store: AsyncObjectStore | SyncObjectStore | StorageConfig | str | None = None,
        *,
        default_name: str = "default",
    ) -> None:
        self._stores: dict[str, AsyncObjectStore] = {}
        self._default = default_name
        if default_store is not None:
            self.register(default_name, default_store, default=True)

    def register(
        self,
        name: str,
        store: AsyncObjectStore | SyncObjectStore | StorageConfig | str,
        *,
        default: bool = False,
    ) -> None:
        """Register a named AsyncObjectStore backend."""
        if isinstance(store, AsyncObjectStore):
            async_store = store
        elif isinstance(store, SyncObjectStore):
            async_store = store.to_async()
        elif isinstance(store, _STORAGE_CONFIGS):
            async_store = AsyncObjectStore.from_config(store)
        elif isinstance(store, str):
            async_store = AsyncObjectStore(store)
        else:
            msg = f"Expected AsyncObjectStore, SyncObjectStore, StorageConfig or URI, got {type(store)!r}"
            raise TypeError(msg)

        self._stores[name] = async_store
        if default or len(self._stores) == 1:
            self._default = name

    def get(self, name: str | None = None) -> AsyncObjectStore:
        """Return the requested AsyncObjectStore backend or the default backend."""
        target = name or self._default
        if target not in self._stores:
            self.register(target, MemoryConfig(), default=False)
        return self._stores[target]


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
    """Attach a SyncStorageRegistry or AsyncStorageRegistry to ``app.state.storage``."""
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
