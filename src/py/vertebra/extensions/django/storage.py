"""Django Storage backend over Vertebra Rust ObjectStore with sync and async methods."""

from __future__ import annotations

from typing import Any

from vertebra.storage import GCSConfig, LocalConfig, MemoryConfig, ObjectStore, S3Config

__all__ = (
    "DjangoStorage",
    "GCSConfig",
    "LocalConfig",
    "MemoryConfig",
    "ObjectStore",
    "S3Config",
    "StorageRegistry",
)


class StorageRegistry:
    """Named ``ObjectStore`` registry for Django applications."""

    __slots__ = ("_stores",)

    def __init__(self, stores: dict[str, ObjectStore] | None = None) -> None:
        self._stores: dict[str, ObjectStore] = dict(stores or {})

    def register(self, name: str, store: ObjectStore) -> None:
        """Register an ``ObjectStore`` under ``name``."""
        self._stores[name] = store

    def get(self, name: str = "default") -> ObjectStore:
        """Return the ``ObjectStore`` registered under ``name``."""
        return self._stores[name]


class DjangoStorage:
    """Drop-in ``STORAGES["default"]["BACKEND"]`` backed by Vertebra ``ObjectStore``."""

    __slots__ = ("_base_url", "_store")

    def __init__(
        self,
        config: Any = None,
        *,
        store: ObjectStore | None = None,
        base_url: str = "/media/",
    ) -> None:
        if store is not None:
            self._store = store
        elif isinstance(config, ObjectStore):
            self._store = config
        elif config is not None:
            self._store = ObjectStore.from_config(config)
        else:
            self._store = ObjectStore.from_config(MemoryConfig())
        self._base_url = base_url if base_url.endswith("/") else f"{base_url}/"

    @property
    def store(self) -> ObjectStore:
        """Return the underlying ``ObjectStore``."""
        return self._store

    def _open(self, name: str, mode: str = "rb") -> bytes:
        """Open ``name`` and return its raw bytes."""
        _ = mode
        return bytes(self._store.get(name))

    def open(self, name: str, mode: str = "rb") -> bytes:
        """Synchronously open ``name`` and return its bytes."""
        return self._open(name, mode=mode)

    async def aopen(self, name: str, mode: str = "rb") -> bytes:
        """Asynchronously open ``name`` and return its bytes."""
        _ = mode
        return bytes(await self._store.get_async(name))

    def _save(self, name: str, content: Any) -> str:
        """Save ``content`` under ``name`` and return the stored key."""
        if isinstance(content, (bytes, bytearray, memoryview)):
            raw = bytes(content)
        elif hasattr(content, "read"):
            raw_read = content.read()
            raw = raw_read.encode("utf-8") if isinstance(raw_read, str) else bytes(raw_read)
        else:
            raw = str(content).encode("utf-8")
        self._store.put(name, raw)
        return name

    def save(self, name: str, content: Any, max_length: int | None = None) -> str:
        """Synchronously save ``content`` under ``name``."""
        _ = max_length
        return self._save(name, content)

    async def asave(self, name: str, content: Any) -> str:
        """Asynchronously save ``content`` under ``name``."""
        if isinstance(content, (bytes, bytearray, memoryview)):
            raw = bytes(content)
        elif hasattr(content, "read"):
            raw_read = content.read()
            raw = raw_read.encode("utf-8") if isinstance(raw_read, str) else bytes(raw_read)
        else:
            raw = str(content).encode("utf-8")
        await self._store.put_async(name, raw)
        return name

    def delete(self, name: str) -> None:
        """Synchronously delete ``name``."""
        self._store.delete(name)

    async def adelete(self, name: str) -> None:
        """Asynchronously delete ``name``."""
        await self._store.delete_async(name)

    def exists(self, name: str) -> bool:
        """Synchronously check whether ``name`` exists in storage."""
        return bool(self._store.exists(name))

    async def aexists(self, name: str) -> bool:
        """Asynchronously check whether ``name`` exists in storage."""
        return bool(await self._store.exists_async(name))

    def size(self, name: str) -> int:
        """Return the byte size of ``name``."""
        meta = self._store.head(name)
        return int(getattr(meta, "size", 0))

    async def asize(self, name: str) -> int:
        """Asynchronously return the byte size of ``name``."""
        meta = await self._store.head_async(name)
        return int(getattr(meta, "size", 0))

    def url(self, name: str) -> str:
        """Return the public URL for ``name``."""
        return f"{self._base_url}{name.lstrip('/')}"
