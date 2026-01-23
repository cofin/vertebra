"""Advanced Alchemy storage backend."""

from __future__ import annotations

from vertebra.resources.broker import AsyncBrokerClient
from vertebra.utils.module_loader import ensure_dependency

ensure_dependency("advanced_alchemy", install_package="advanced-alchemy")

from advanced_alchemy.types.file_object import StorageBackend, storages  # noqa: E402


class VertebraStorageBackend(StorageBackend):
    """Routes AA StoredObject operations through Vertebra broker."""

    def __init__(self, pool: str = "storage", key: str = "vertebra") -> None:
        self.pool = pool
        self.key = key
        self._client = AsyncBrokerClient()

    async def read_bytes(self, path: str) -> bytes:
        return await self._client.storage_read(self.pool, path)

    async def write_bytes(self, path: str, data: bytes) -> None:
        await self._client.storage_write(self.pool, path, data)

    async def list_objects(self, prefix: str | None = None) -> list[str]:
        return await self._client.storage_list(self.pool, prefix)

    async def exists(self, path: str) -> bool:
        return await self._client.storage_exists(self.pool, path)

    async def delete(self, path: str) -> None:
        await self._client.storage_delete(self.pool, path)

    async def copy(self, src: str, dst: str) -> None:
        await self._client.storage_copy(self.pool, src, dst)

    async def move(self, src: str, dst: str) -> None:
        await self._client.storage_move(self.pool, src, dst)

    async def glob(self, pattern: str) -> list[str]:
        return await self._client.storage_glob(self.pool, pattern)


# Register backend with Advanced Alchemy if instantiation succeeds
# Note: VertebraStorageBackend may need updates if AA's StorageBackend interface changes
try:
    storages.register_backend(VertebraStorageBackend())
except TypeError:
    # StorageBackend abstract methods may have changed; skip registration
    pass
