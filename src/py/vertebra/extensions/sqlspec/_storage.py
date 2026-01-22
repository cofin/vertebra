"""Storage integration for SQLSpec routing through Vertebra broker."""

from collections.abc import AsyncIterator
from typing import Protocol, runtime_checkable

from vertebra.resources.broker import AsyncBrokerClient


@runtime_checkable
class ObjectStoreBase(Protocol):
    """Protocol for object storage backend."""

    async def read_bytes(self, path: str) -> bytes: ...
    async def write_bytes(self, path: str, data: bytes) -> None: ...
    async def list_objects(self, prefix: str | None = None) -> AsyncIterator[str]: ...
    async def exists(self, path: str) -> bool: ...
    async def delete(self, path: str) -> None: ...
    async def copy(self, src: str, dst: str) -> None: ...
    async def move(self, src: str, dst: str) -> None: ...
    async def glob(self, pattern: str) -> AsyncIterator[str]: ...


class VertebraObjectStore(ObjectStoreBase):
    """Routes SQLSpec storage operations through Vertebra broker."""

    def __init__(self, pool: str = "storage", client: AsyncBrokerClient | None = None) -> None:
        self.pool = pool
        self._client = client or AsyncBrokerClient()

    async def read_bytes(self, path: str) -> bytes:
        return await self._client.storage_read(self.pool, path)

    async def write_bytes(self, path: str, data: bytes) -> None:
        await self._client.storage_write(self.pool, path, data)

    async def list_objects(self, prefix: str | None = None) -> AsyncIterator[str]:
        # Broker returns list[str], convert to AsyncIterator
        paths = await self._client.storage_list(self.pool, prefix)
        for path in paths:
            yield path

    async def exists(self, path: str) -> bool:
        return await self._client.storage_exists(self.pool, path)

    async def delete(self, path: str) -> None:
        await self._client.storage_delete(self.pool, path)

    async def copy(self, src: str, dst: str) -> None:
        await self._client.storage_copy(self.pool, src, dst)

    async def move(self, src: str, dst: str) -> None:
        await self._client.storage_move(self.pool, src, dst)

    async def glob(self, pattern: str) -> AsyncIterator[str]:
        paths = await self._client.storage_glob(self.pool, pattern)
        for path in paths:
            yield path
