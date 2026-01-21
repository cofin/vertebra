"""fsspec integration for Vertebra filesystem clients."""

import io
from typing import Any, cast

from vertebra.exceptions import MissingDependencyError
from vertebra.storage.client import FileSystemError
from vertebra.storage.store import (
    AsyncGCSStore,
    AsyncLocalStore,
    AsyncObjectStore,
    AsyncS3Store,
    GCSStore,
    LocalStore,
    ObjectMeta,
    S3Store,
    SyncObjectStore,
    async_store_from_url,
    store_from_url,
)
from vertebra.utils.dependencies import FSSPEC_INSTALLED

_FSSPEC_AVAILABLE = FSSPEC_INSTALLED
_FSSPEC_IMPORT_ERROR: Exception | None = None

if _FSSPEC_AVAILABLE:
    try:  # pragma: no cover - optional dependency
        from fsspec.spec import AbstractFileSystem
    except ImportError as exc:  # pragma: no cover - optional dependency
        _FSSPEC_AVAILABLE = False
        _FSSPEC_IMPORT_ERROR = exc

if not _FSSPEC_AVAILABLE:
    AbstractFileSystem = object


class _BufferedFile(io.BytesIO):
    def __init__(self, store: SyncObjectStore, path: str, mode: str) -> None:
        self._store = store
        self._path = path
        self._mode = mode
        data = b""
        if "r" in mode or "a" in mode:
            data = store.get(path)
        super().__init__(data)
        if "a" in mode:
            self.seek(0, io.SEEK_END)

    def close(self) -> None:
        if not self.closed and any(flag in self._mode for flag in ("w", "a", "+")):
            self._store.put(self._path, self.getvalue())
        super().close()


class SyncVertebraFileSystem(AbstractFileSystem):  # type: ignore[misc]
    protocol = "vertebra"

    def __init__(
        self,
        backend: str = "local",
        *,
        bucket: str | None = None,
        options: dict[str, Any] | None = None,
        store: SyncObjectStore | None = None,
        url: str | None = None,
        **kwargs: Any,
    ) -> None:
        if not _FSSPEC_AVAILABLE:
            raise MissingDependencyError(package="fsspec") from _FSSPEC_IMPORT_ERROR
        super().__init__(**kwargs)
        self._backend = backend
        if store is not None:
            self._store = store
        elif url:
            self._store = store_from_url(url, options=options)
        elif backend == "s3":
            if not bucket:
                msg = "bucket is required for s3 filesystem"
                raise FileSystemError(msg)
            self._store = S3Store(bucket=bucket, options=options)
        elif backend == "gcs":
            if not bucket:
                msg = "bucket is required for gcs filesystem"
                raise FileSystemError(msg)
            self._store = GCSStore(bucket=bucket, options=options)
        else:
            self._store = LocalStore()

    def ls(self, path: str, **kwargs: Any) -> list[dict[str, Any]]:
        entries = list(self._store.list(path))
        return [entry.__dict__ for entry in entries]

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        entry = self._store.head(path)
        return entry.__dict__

    def exists(self, path: str, **kwargs: Any) -> bool:
        return self._store.exists(path)

    def rm(self, path: str, recursive: bool = False, **kwargs: Any) -> None:
        self._store.delete(path)

    def mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        if hasattr(self._store, "mkdir"):
            self._store.mkdir(path)
        else:
            msg = "mkdir is only supported for local stores"
            raise FileSystemError(msg)

    def rmdir(self, path: str) -> None:
        if hasattr(self._store, "rmdir"):
            self._store.rmdir(path, recursive=True)
        else:
            msg = "rmdir is only supported for local stores"
            raise FileSystemError(msg)

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> io.BytesIO:
        if "b" not in mode:
            mode = f"{mode}b"
        return _BufferedFile(self._store, path, mode)


class AsyncVertebraFileSystem:
    def __init__(
        self,
        backend: str = "local",
        *,
        bucket: str | None = None,
        options: dict[str, Any] | None = None,
        store: AsyncObjectStore | None = None,
        url: str | None = None,
    ) -> None:
        if store is not None:
            self._store = store
        elif url:
            self._store = async_store_from_url(url, options=options)
        elif backend == "s3":
            if not bucket:
                msg = "bucket is required for s3 filesystem"
                raise FileSystemError(msg)
            self._store = cast("AsyncObjectStore", AsyncS3Store(bucket=bucket, options=options))
        elif backend == "gcs":
            if not bucket:
                msg = "bucket is required for gcs filesystem"
                raise FileSystemError(msg)
            self._store = cast("AsyncObjectStore", AsyncGCSStore(bucket=bucket, options=options))
        else:
            self._store = cast("AsyncObjectStore", AsyncLocalStore())

    async def ls(self, path: str) -> list[ObjectMeta]:
        return [entry async for entry in self._store.list(path)]

    async def info(self, path: str) -> ObjectMeta:
        return await self._store.head(path)

    async def exists(self, path: str) -> bool:
        return await self._store.exists(path)

    async def rm(self, path: str, recursive: bool = False) -> None:
        await self._store.delete(path)

    async def mkdir(self, path: str) -> None:
        if hasattr(self._store, "mkdir"):
            await self._store.mkdir(path)
        else:
            msg = "mkdir is only supported for local stores"
            raise FileSystemError(msg)

    async def rmdir(self, path: str) -> None:
        if hasattr(self._store, "rmdir"):
            await self._store.rmdir(path, recursive=True)
        else:
            msg = "rmdir is only supported for local stores"
            raise FileSystemError(msg)

    async def read_bytes(self, path: str) -> bytes:
        return await self._store.get(path)

    async def write_bytes(self, path: str, data: bytes) -> int:
        result = await self._store.put(path, data)
        return result.bytes
