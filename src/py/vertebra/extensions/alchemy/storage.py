"""Advanced Alchemy FileObject/StoredObject storage backend powered by VertebraStore."""

import inspect
from collections.abc import AsyncIterable, Buffer, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from vertebra.storage import ObjectMeta, VertebraStore

__all__ = (
    "AlchemyFileAttachment",
    "AlchemyStore",
    "FileAttachment",
    "ObjectStoreBackend",
    "StorageBackend",
    "VertebraAlchemyStorageBackend",
    "VertebraStorageBackend",
)

type PathLike = str | Path
type SyncDataLike = Buffer | Path | Iterable[bytes] | Any
type AsyncDataLike = SyncDataLike | AsyncIterable[bytes]


@dataclass(slots=True)
class FileAttachment:
    """Value object representing an Advanced Alchemy FileObject attachment."""

    path: str
    filename: str | None = None
    content_type: str = "application/octet-stream"
    size: int = 0
    etag: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


AlchemyFileAttachment = FileAttachment


class ObjectStoreBackend:
    """Advanced Alchemy compatible storage backend backed by ObjectStore."""

    driver: str = "vertebra"
    protocol: str = "vertebra"
    __slots__ = ("_prefix", "_store", "key")

    def __init__(
        self,
        key: str = "default",
        fs: VertebraStore | None = None,
        *,
        store: VertebraStore | None = None,
        prefix: str = "",
    ) -> None:
        actual_store = store or fs
        if not isinstance(actual_store, VertebraStore):
            msg = "VertebraAlchemyStorageBackend requires a VertebraStore instance"
            raise TypeError(msg)
        self.key = key
        self._store = actual_store
        self._prefix = prefix.strip("/")

    @property
    def store(self) -> VertebraStore:
        """Return the underlying VertebraStore instance."""
        return self._store

    def save_object(
        self,
        file_object: AlchemyFileAttachment,
        data: SyncDataLike,
        **kwargs: Any,
    ) -> AlchemyFileAttachment:
        """Persist data synchronously and populate size and etag on file_object."""
        _ = kwargs
        qualified = self._qualify(file_object.path)
        if isinstance(data, Path):
            put_res = self._store.put_file(qualified, data)
        else:
            raw = self._resolve_sync_data(data)
            put_res = self._store.put(qualified, raw)
        file_object.size = put_res.size
        file_object.etag = put_res.etag
        if put_res.etag:
            file_object.metadata["etag"] = put_res.etag
        return file_object

    async def save_object_async(
        self,
        file_object: AlchemyFileAttachment,
        data: AsyncDataLike,
        **kwargs: Any,
    ) -> AlchemyFileAttachment:
        """Persist data asynchronously and populate size and etag on file_object."""
        _ = kwargs
        qualified = self._qualify(file_object.path)
        if isinstance(data, Path):
            put_res = await self._store.put_file_async(qualified, data)
        else:
            raw = await self._resolve_async_data(data)
            put_res = await self._store.put_async(qualified, raw)
        file_object.size = put_res.size
        file_object.etag = put_res.etag
        if put_res.etag:
            file_object.metadata["etag"] = put_res.etag
        return file_object

    def get_content(self, path: PathLike, *, options: dict[str, Any] | None = None) -> bytes:
        """Read attachment bytes synchronously."""
        _ = options
        return self._store.get(self._qualify(path))

    async def get_content_async(self, path: PathLike, *, options: dict[str, Any] | None = None) -> bytes:
        """Read attachment bytes asynchronously."""
        _ = options
        return await self._store.get_async(self._qualify(path))

    def get_metadata(self, path: PathLike) -> ObjectMeta:
        """Read object metadata synchronously."""
        return self._store.head(self._qualify(path))

    def delete_object(self, paths: PathLike | Sequence[PathLike]) -> None:
        """Delete one or more stored attachments synchronously."""
        items = [paths] if isinstance(paths, (str, Path)) else list(paths)
        for item in items:
            qualified = self._qualify(item)
            if self._store.exists(qualified):
                self._store.delete(qualified)

    async def delete_object_async(self, paths: PathLike | Sequence[PathLike]) -> None:
        """Delete one or more stored attachments asynchronously."""
        items = [paths] if isinstance(paths, (str, Path)) else list(paths)
        for item in items:
            qualified = self._qualify(item)
            if await self._store.exists_async(qualified):
                await self._store.delete_async(qualified)

    def sign_object_url(
        self,
        path: PathLike,
        *,
        expires_in_secs: int = 3600,
        method: str = "GET",
        timestamp: int | None = None,
    ) -> str:
        """Generate a presigned GET or PUT URL for an attachment."""
        qualified = self._qualify(path)
        if method.upper() == "PUT":
            return self._store.presign_put(qualified, expires_in_secs=expires_in_secs, timestamp=timestamp)
        return self._store.presign_get(qualified, expires_in_secs=expires_in_secs, timestamp=timestamp)

    def _qualify(self, path: PathLike) -> str:
        raw_path = path.as_posix() if isinstance(path, Path) else str(path)
        cleaned = raw_path.strip().lstrip("/")
        if not self._prefix:
            return cleaned
        return f"{self._prefix}/{cleaned}" if cleaned else self._prefix

    @staticmethod
    def _resolve_sync_data(data: SyncDataLike) -> Buffer:
        if isinstance(data, Buffer):
            return data
        if isinstance(data, Path):
            return data.read_bytes()
        if hasattr(data, "read"):
            return bytes(data.read())
        return b"".join(bytes(chunk) for chunk in data)

    @staticmethod
    async def _resolve_async_data(data: AsyncDataLike) -> Buffer:
        if isinstance(data, Buffer):
            return data
        if isinstance(data, Path):
            return data.read_bytes()
        if hasattr(data, "read"):
            result = data.read()
            if inspect.isawaitable(result):
                result = await result
            return bytes(result)
        if hasattr(data, "__aiter__"):
            chunks: list[bytes] = []
            async for chunk in data:
                chunks.append(bytes(chunk))
            return b"".join(chunks)
        return b"".join(bytes(chunk) for chunk in data)


StorageBackend = ObjectStoreBackend
AlchemyStore = ObjectStoreBackend
VertebraAlchemyStorageBackend = ObjectStoreBackend
VertebraStorageBackend = ObjectStoreBackend
