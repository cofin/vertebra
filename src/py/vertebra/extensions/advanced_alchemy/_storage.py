"""Advanced Alchemy storage backend."""

from __future__ import annotations

import os
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator, Sequence
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, cast

from vertebra.resources.broker import AsyncBrokerClient
from vertebra.utils.module_loader import ensure_dependency

ensure_dependency("advanced_alchemy", install_package="advanced-alchemy")

from advanced_alchemy.types.file_object import StorageBackend, storages  # noqa: E402
from advanced_alchemy.utils.sync_tools import async_  # noqa: E402

if TYPE_CHECKING:
    from advanced_alchemy.types.file_object import FileObject

PathLike = str | Path | os.PathLike[Any]
DataLike = IO[bytes] | Path | bytes | Iterator[bytes] | Iterable[bytes]
AsyncDataLike = IO[bytes] | Path | bytes | AsyncIterator[bytes] | AsyncIterable[bytes] | Iterator[bytes] | Iterable[bytes]


class VertebraStorageBackend(StorageBackend):
    """Routes AA file operations through Vertebra broker."""

    driver: str = "vertebra"
    protocol: str = "vertebra"

    def __init__(self, key: str = "vertebra", pool: str = "storage", **_kwargs: Any) -> None:
        self.key = key
        self.pool = pool
        self._client = AsyncBrokerClient()

    async def _to_bytes_async(self, data: AsyncDataLike) -> bytes:
        """Convert DataLike/AsyncDataLike to bytes."""
        if isinstance(data, bytes):
            return data
        if isinstance(data, Path):
            return data.read_bytes()
        if hasattr(data, "read"):
            # IO[bytes] has read() method
            return cast("IO[bytes]", data).read()
        # Handle async iterators/iterables
        if isinstance(data, (AsyncIterator, AsyncIterable)):
            chunks: list[bytes] = [chunk async for chunk in data]
            return b"".join(chunks)
        # Sync Iterator/Iterable of bytes - join all chunks
        return b"".join(chunk for chunk in data)

    # --- Required Abstract Methods ---

    def get_content(self, path: PathLike, *, options: dict[str, Any] | None = None) -> bytes:
        """Get file content synchronously."""
        return async_(self.get_content_async)(path=path, options=options)  # type: ignore[return-value]

    async def get_content_async(self, path: PathLike, *, options: dict[str, Any] | None = None) -> bytes:
        """Get file content asynchronously."""
        del options  # unused
        return await self._client.storage_read(self.pool, str(path))

    def save_object(
        self,
        file_object: FileObject,
        data: DataLike,
        *,
        use_multipart: bool | None = None,
        chunk_size: int = 5 * 1024 * 1024,
        max_concurrency: int = 12,
    ) -> FileObject:
        """Save file synchronously and return updated FileObject."""
        return async_(self.save_object_async)(  # type: ignore[return-value]
            file_object=file_object,
            data=data,
            use_multipart=use_multipart,
            chunk_size=chunk_size,
            max_concurrency=max_concurrency,
        )

    async def save_object_async(
        self,
        file_object: FileObject,
        data: AsyncDataLike,
        *,
        use_multipart: bool | None = None,
        chunk_size: int = 5 * 1024 * 1024,
        max_concurrency: int = 12,
    ) -> FileObject:
        """Save file asynchronously and return updated FileObject."""
        del use_multipart, chunk_size, max_concurrency  # unused - broker handles chunking
        content = await self._to_bytes_async(data)
        await self._client.storage_write(self.pool, file_object.path, content)

        # Update FileObject metadata
        file_object.size = len(content)
        return file_object

    def delete_object(self, paths: PathLike | Sequence[PathLike]) -> None:
        """Delete file(s) synchronously."""
        async_(self.delete_object_async)(paths=paths)

    async def delete_object_async(self, paths: PathLike | Sequence[PathLike]) -> None:
        """Delete file(s) asynchronously."""
        if isinstance(paths, (str, Path, os.PathLike)):
            await self._client.storage_delete(self.pool, str(paths))
        else:
            for path in paths:
                await self._client.storage_delete(self.pool, str(path))

    def sign(
        self,
        paths: PathLike | Sequence[PathLike],
        *,
        expires_in: int | None = None,
        for_upload: bool = False,
    ) -> str | list[str]:
        """Generate signed URL(s) - not supported by broker."""
        del paths, expires_in, for_upload  # unused
        msg = "Vertebra broker does not support signed URLs"
        raise NotImplementedError(msg)

    async def sign_async(
        self,
        paths: PathLike | Sequence[PathLike],
        *,
        expires_in: int | None = None,
        for_upload: bool = False,
    ) -> str | list[str]:
        """Generate signed URL(s) - not supported by broker."""
        del paths, expires_in, for_upload  # unused
        msg = "Vertebra broker does not support signed URLs"
        raise NotImplementedError(msg)


# Register backend
storages.register_backend(VertebraStorageBackend())
