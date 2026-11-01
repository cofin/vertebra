"""Sync and async object storage integration for Starlette and FastAPI applications."""

from __future__ import annotations

from typing import Any

from vertebra.storage import (
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    ObjectStore,
    S3Config,
)

__all__ = (
    "GCSConfig",
    "LocalConfig",
    "MemoryConfig",
    "ObjectStore",
    "S3Config",
    "StorageRegistry",
    "attach_storage",
)


class StorageRegistry:
    """Named registry of Vertebra ObjectStore backends (MemoryConfig, LocalConfig, S3Config, GCSConfig)."""

    __slots__ = ("_default", "_stores")

    def __init__(self, default_store: ObjectStore | None = None, *, default_name: str = "default") -> None:
        self._stores: dict[str, ObjectStore] = {}
        self._default = default_name
        if default_store is not None:
            self._stores[default_name] = default_store

    def register(self, name: str, store: ObjectStore, *, default: bool = False) -> None:
        """Register a named ObjectStore backend."""
        self._stores[name] = store
        if default or len(self._stores) == 1:
            self._default = name

    def get(self, name: str | None = None) -> ObjectStore:
        """Return the requested ObjectStore backend or the default backend."""
        target = name or self._default
        if target not in self._stores:
            self._stores[target] = ObjectStore.from_config(MemoryConfig())
        return self._stores[target]


def attach_storage(
    app: Any,
    store_or_registry: StorageRegistry | ObjectStore | None = None,
) -> StorageRegistry:
    """Attach a StorageRegistry to ``app.state.storage``."""
    if isinstance(store_or_registry, StorageRegistry):
        registry = store_or_registry
    else:
        registry = StorageRegistry(default_store=store_or_registry or ObjectStore.from_config(MemoryConfig()))
    state = getattr(app, "state", None)
    if state is not None:
        if isinstance(state, dict):
            state["storage"] = registry
        else:
            state.storage = registry
    return registry
