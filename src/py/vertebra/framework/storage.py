"""Framework-neutral synchronous and asynchronous object storage registries."""

from __future__ import annotations

from collections.abc import Mapping

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
    "AzureConfig",
    "GCSConfig",
    "HTTPConfig",
    "LocalConfig",
    "MemoryConfig",
    "OCIConfig",
    "R2Config",
    "S3Config",
    "StorageConfig",
    "SyncObjectStore",
    "SyncStorageRegistry",
)

_STORAGE_CONFIGS = (
    LocalConfig,
    S3Config,
    GCSConfig,
    AzureConfig,
    OCIConfig,
    R2Config,
    HTTPConfig,
    MemoryConfig,
)

type StorageInput = SyncObjectStore | AsyncObjectStore | StorageConfig | str


class SyncStorageRegistry:
    """Named registry of Vertebra ``SyncObjectStore`` backends."""

    __slots__ = ("_default", "_stores")

    def __init__(
        self,
        stores: Mapping[str, StorageInput] | StorageInput | None = None,
        *,
        default_store: StorageInput | None = None,
        default_name: str = "default",
    ) -> None:
        self._stores: dict[str, SyncObjectStore] = {}
        self._default = default_name
        if isinstance(stores, Mapping):
            for alias, store in stores.items():
                self.register(alias, store, default=(alias == default_name))
        elif stores is not None:
            self.register(default_name, stores, default=True)
        elif default_store is not None:
            self.register(default_name, default_store, default=True)

    @property
    def default_name(self) -> str:
        """Return the default registered storage alias."""
        return self._default

    def register(
        self,
        name: str,
        store: StorageInput,
        *,
        default: bool = False,
    ) -> None:
        """Register a named ``SyncObjectStore`` backend under ``name``."""
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
        """Return the ``SyncObjectStore`` registered under ``name`` or raise ``KeyError``."""
        target = self._default if name is None else name
        if target not in self._stores:
            available = ", ".join(sorted(self._stores)) or "<none>"
            msg = f"Unknown storage alias {target!r}; registered aliases: {available}"
            raise KeyError(msg)
        return self._stores[target]

    def names(self) -> tuple[str, ...]:
        """Return all registered storage alias names in insertion order."""
        return tuple(self._stores.keys())

    def __getitem__(self, name: str) -> SyncObjectStore:
        """Return the ``SyncObjectStore`` registered under ``name``."""
        return self.get(name)

    def __contains__(self, name: object) -> bool:
        """Return ``True`` when ``name`` is a registered storage alias."""
        return isinstance(name, str) and name in self._stores

    def __len__(self) -> int:
        """Return the number of registered storage backends."""
        return len(self._stores)


class AsyncStorageRegistry:
    """Named registry of Vertebra ``AsyncObjectStore`` backends."""

    __slots__ = ("_default", "_stores")

    def __init__(
        self,
        stores: Mapping[str, StorageInput] | StorageInput | None = None,
        *,
        default_store: StorageInput | None = None,
        default_name: str = "default",
    ) -> None:
        self._stores: dict[str, AsyncObjectStore] = {}
        self._default = default_name
        if isinstance(stores, Mapping):
            for alias, store in stores.items():
                self.register(alias, store, default=(alias == default_name))
        elif stores is not None:
            self.register(default_name, stores, default=True)
        elif default_store is not None:
            self.register(default_name, default_store, default=True)

    @property
    def default_name(self) -> str:
        """Return the default registered storage alias."""
        return self._default

    def register(
        self,
        name: str,
        store: StorageInput,
        *,
        default: bool = False,
    ) -> None:
        """Register a named ``AsyncObjectStore`` backend under ``name``."""
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
        """Return the ``AsyncObjectStore`` registered under ``name`` or raise ``KeyError``."""
        target = self._default if name is None else name
        if target not in self._stores:
            available = ", ".join(sorted(self._stores)) or "<none>"
            msg = f"Unknown storage alias {target!r}; registered aliases: {available}"
            raise KeyError(msg)
        return self._stores[target]

    def names(self) -> tuple[str, ...]:
        """Return all registered storage alias names in insertion order."""
        return tuple(self._stores.keys())

    def __getitem__(self, name: str) -> AsyncObjectStore:
        """Return the ``AsyncObjectStore`` registered under ``name``."""
        return self.get(name)

    def __contains__(self, name: object) -> bool:
        """Return ``True`` when ``name`` is a registered storage alias."""
        return isinstance(name, str) and name in self._stores

    def __len__(self) -> int:
        """Return the number of registered storage backends."""
        return len(self._stores)
