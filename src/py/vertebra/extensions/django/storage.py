"""Django Storage backend over Vertebra Rust ObjectStore with sync and async methods."""

from __future__ import annotations

from typing import Any

from vertebra.storage import AsyncObjectStore, GCSConfig, LocalConfig, MemoryConfig, S3Config, SyncObjectStore

__all__ = (
    "AsyncObjectStore",
    "AsyncStorageRegistry",
    "DjangoStorage",
    "GCSConfig",
    "LocalConfig",
    "MemoryConfig",
    "S3Config",
    "SyncObjectStore",
    "SyncStorageRegistry",
)


class SyncStorageRegistry:
    """Named ``SyncObjectStore`` registry for Django applications."""

    __slots__ = ("_stores",)

    def __init__(self, stores: dict[str, SyncObjectStore] | None = None) -> None:
        self._stores: dict[str, SyncObjectStore] = dict(stores or {})

    def register(self, name: str, store: SyncObjectStore) -> None:
        """Register a ``SyncObjectStore`` under ``name``."""
        self._stores[name] = store

    def get(self, name: str = "default") -> SyncObjectStore:
        """Return the ``SyncObjectStore`` registered under ``name``."""
        return self._stores[name]


class AsyncStorageRegistry:
    """Named ``AsyncObjectStore`` registry for Django applications."""

    __slots__ = ("_stores",)

    def __init__(self, stores: dict[str, AsyncObjectStore] | None = None) -> None:
        self._stores: dict[str, AsyncObjectStore] = dict(stores or {})

    def register(self, name: str, store: AsyncObjectStore) -> None:
        """Register an ``AsyncObjectStore`` under ``name``."""
        self._stores[name] = store

    def get(self, name: str = "default") -> AsyncObjectStore:
        """Return the ``AsyncObjectStore`` registered under ``name``."""
        return self._stores[name]


class DjangoStorage:
    """Drop-in ``STORAGES["default"]["BACKEND"]`` backed by Vertebra ``SyncObjectStore`` and ``AsyncObjectStore``."""

    __slots__ = ("_async_store", "_base_url", "_store")

    def __init__(
        self,
        config: Any = None,
        *,
        store: SyncObjectStore | AsyncObjectStore | None = None,
        base_url: str = "/media/",
    ) -> None:
        if isinstance(store, SyncObjectStore):
            self._store = store
            self._async_store = store.to_async()
        elif isinstance(store, AsyncObjectStore):
            self._async_store = store
            self._store = store.to_sync()
        elif isinstance(config, SyncObjectStore):
            self._store = config
            self._async_store = config.to_async()
        elif isinstance(config, AsyncObjectStore):
            self._async_store = config
            self._store = config.to_sync()
        elif config is not None:
            self._store = SyncObjectStore.from_config(config)
            self._async_store = AsyncObjectStore.from_config(config)
        else:
            mem_cfg = MemoryConfig()
            self._store = SyncObjectStore.from_config(mem_cfg)
            self._async_store = AsyncObjectStore.from_config(mem_cfg)
        if self._store.scheme == "memory":
            if self._async_store._memory and not self._store._memory:
                self._store._memory = self._async_store._memory
            else:
                self._async_store._memory = self._store._memory
        self._base_url = base_url if base_url.endswith("/") else f"{base_url}/"

    @property
    def store(self) -> SyncObjectStore:
        """Return the underlying ``SyncObjectStore``."""
        return self._store

    @property
    def async_store(self) -> AsyncObjectStore:
        """Return the underlying ``AsyncObjectStore``."""
        return self._async_store

    def open(self, name: str, mode: str = "rb") -> bytes:
        """Synchronously open ``name`` and return its bytes."""
        return self._open(name, mode=mode)

    async def aopen(self, name: str, mode: str = "rb") -> bytes:
        """Asynchronously open ``name`` and return its bytes."""
        _ = mode
        return bytes(await self._async_store.get(name))

    def save(self, name: str, content: Any, max_length: int | None = None) -> str:
        """Synchronously save ``content`` under ``name``."""
        _ = max_length
        return self._save(name, content)

    async def asave(self, name: str, content: Any, max_length: int | None = None) -> str:
        """Asynchronously save ``content`` under ``name``."""
        _ = max_length
        raw = self._coerce_bytes(content)
        await self._async_store.put(name, raw)
        return name

    def delete(self, name: str) -> None:
        """Synchronously delete ``name``."""
        self._store.delete(name)

    async def adelete(self, name: str) -> None:
        """Asynchronously delete ``name``."""
        await self._async_store.delete(name)

    def exists(self, name: str) -> bool:
        """Synchronously check whether ``name`` exists in storage."""
        return bool(self._store.exists(name))

    async def aexists(self, name: str) -> bool:
        """Asynchronously check whether ``name`` exists in storage."""
        return bool(await self._async_store.exists(name))

    def size(self, name: str) -> int:
        """Return the byte size of ``name``."""
        meta = self._store.head(name)
        return int(getattr(meta, "size", 0))

    async def asize(self, name: str) -> int:
        """Asynchronously return the byte size of ``name``."""
        meta = await self._async_store.head(name)
        return int(getattr(meta, "size", 0))

    def url(self, name: str) -> str:
        """Return the public URL for ``name``."""
        return f"{self._base_url}{name.lstrip('/')}"

    async def aurl(self, name: str) -> str:
        """Asynchronously return the public URL for ``name``."""
        return self.url(name)

    def _open(self, name: str, mode: str = "rb") -> bytes:
        """Open ``name`` and return its raw bytes."""
        _ = mode
        return bytes(self._store.get(name))

    def _save(self, name: str, content: Any) -> str:
        """Save ``content`` under ``name`` and return the stored key."""
        raw = self._coerce_bytes(content)
        self._store.put(name, raw)
        return name

    @staticmethod
    def _coerce_bytes(content: Any) -> bytes:
        """Coerce ``content`` into raw ``bytes``."""
        if isinstance(content, (bytes, bytearray, memoryview)):
            return bytes(content)
        if hasattr(content, "read"):
            raw_read = content.read()
            return raw_read.encode("utf-8") if isinstance(raw_read, str) else bytes(raw_read)
        return str(content).encode("utf-8")
