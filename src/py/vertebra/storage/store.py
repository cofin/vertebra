"""Object store protocol and Bun-backed implementations."""

from __future__ import annotations

import io
import posixpath
import tempfile
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

from vertebra.filesystem.client import AsyncFileSystemClient, FileEntry, FileSystemClient


@dataclass(frozen=True, slots=True)
class ByteRange:
    offset: int
    length: int | None = None

    def as_slice(self) -> tuple[int, int | None]:
        start = max(0, int(self.offset))
        if self.length is None:
            return start, None
        return start, max(0, int(self.length)) + start


@dataclass(frozen=True, slots=True)
class PutOptions:
    content_type: str | None = None
    metadata: dict[str, str] | None = None


@dataclass(frozen=True, slots=True)
class PutResult:
    bytes: int
    etag: str | None = None


@dataclass(frozen=True, slots=True)
class ObjectMeta:
    path: str
    size: int | None = None
    modified: str | None = None
    etag: str | None = None


@dataclass(frozen=True, slots=True)
class ListResult:
    objects: list[ObjectMeta]
    prefixes: list[str]


class SyncObjectStore(Protocol):
    def get(self, path: str) -> bytes:
        """Return the full object contents."""

    def get_range(self, path: str, byte_range: ByteRange) -> bytes:
        """Return a byte range from an object."""

    def get_ranges(self, path: str, ranges: Sequence[ByteRange]) -> list[bytes]:
        """Return multiple byte ranges from an object."""

    def put(self, path: str, data: bytes, *, options: PutOptions | None = None) -> PutResult:
        """Write object contents."""

    def delete(self, path: str) -> None:
        """Delete an object."""

    def head(self, path: str) -> ObjectMeta:
        """Return metadata for an object."""

    def exists(self, path: str) -> bool:
        """Return whether the object exists."""

    def list(self, prefix: str = "") -> Iterator[ObjectMeta]:
        """Iterate over objects with the given prefix."""

    def list_with_delimiter(self, prefix: str = "", delimiter: str = "/") -> ListResult:
        """List objects and common prefixes with a delimiter."""

    def open_reader(self, path: str) -> io.BytesIO:
        """Return a readable file-like object."""

    def open_writer(self, path: str, *, options: PutOptions | None = None) -> "StoreWriter":
        """Return a writable file-like object."""

    def iter_chunks(self, path: str, *, chunk_size: int = 8 * 1024 * 1024) -> Iterator[bytes]:
        """Iterate over object contents in chunks."""

    def put_stream(
        self,
        path: str,
        chunks: Iterator[bytes],
        *,
        options: PutOptions | None = None,
    ) -> PutResult:
        """Write object contents from a chunk iterator."""

    def download_to_path(self, path: str, local_path: str) -> int:
        """Download object to a local file path."""

    def upload_from_path(
        self,
        local_path: str,
        path: str,
        *,
        options: PutOptions | None = None,
    ) -> PutResult:
        """Upload object from a local file path."""

    def signed_url(
        self,
        path: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> str | None:
        """Return a signed URL if supported."""


class AsyncObjectStore(Protocol):
    async def get(self, path: str) -> bytes:
        """Return the full object contents asynchronously."""

    async def get_range(self, path: str, byte_range: ByteRange) -> bytes:
        """Return a byte range from an object asynchronously."""

    async def get_ranges(self, path: str, ranges: Sequence[ByteRange]) -> list[bytes]:
        """Return multiple byte ranges from an object asynchronously."""

    async def put(self, path: str, data: bytes, *, options: PutOptions | None = None) -> PutResult:
        """Write object contents asynchronously."""

    async def delete(self, path: str) -> None:
        """Delete an object asynchronously."""

    async def head(self, path: str) -> ObjectMeta:
        """Return metadata for an object asynchronously."""

    async def exists(self, path: str) -> bool:
        """Return whether the object exists asynchronously."""

    def list(self, prefix: str = "") -> AsyncIterator[ObjectMeta]:
        """Iterate over objects with the given prefix asynchronously."""

    async def list_with_delimiter(self, prefix: str = "", delimiter: str = "/") -> ListResult:
        """List objects and common prefixes asynchronously."""

    async def open_reader(self, path: str) -> "AsyncStoreReader":
        """Return a readable async file-like object."""

    async def open_writer(self, path: str, *, options: PutOptions | None = None) -> "AsyncStoreWriter":
        """Return a writable async file-like object."""

    async def iter_chunks(self, path: str, *, chunk_size: int = 8 * 1024 * 1024) -> AsyncIterator[bytes]:
        """Iterate over object contents in chunks asynchronously."""

    async def put_stream(
        self,
        path: str,
        chunks: AsyncIterator[bytes],
        *,
        options: PutOptions | None = None,
    ) -> PutResult:
        """Write object contents from an async chunk iterator."""

    async def download_to_path(self, path: str, local_path: str) -> int:
        """Download object to a local file path asynchronously."""

    async def upload_from_path(
        self,
        local_path: str,
        path: str,
        *,
        options: PutOptions | None = None,
    ) -> PutResult:
        """Upload object from a local file path asynchronously."""

    async def signed_url(
        self,
        path: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> str | None:
        """Return a signed URL asynchronously if supported."""


class StoreWriter(io.BytesIO):
    def __init__(self, store: "SyncObjectStoreImpl", path: str, options: PutOptions | None) -> None:
        super().__init__()
        self._store = store
        self._path = path
        self._options = options

    def close(self) -> None:
        if not self.closed:
            self._store.put(self._path, self.getvalue(), options=self._options)
        super().close()


class AsyncStoreReader:
    def __init__(self, data: bytes) -> None:
        self._buffer = io.BytesIO(data)

    async def read(self, size: int | None = None) -> bytes:
        return self._buffer.read(-1 if size is None else size)

    async def close(self) -> None:
        self._buffer.close()


class AsyncStoreWriter:
    def __init__(self, store: "AsyncObjectStoreImpl", path: str, options: PutOptions | None) -> None:
        self._store = store
        self._path = path
        self._options = options
        self._buffer = io.BytesIO()

    async def write(self, data: bytes) -> int:
        return self._buffer.write(data)

    async def close(self) -> None:
        await self._store.put(self._path, self._buffer.getvalue(), options=self._options)
        self._buffer.close()


class SyncObjectStoreImpl:
    def __init__(self, client: FileSystemClient, prefix: str = "", is_local: bool = False) -> None:
        self._client = client
        self._prefix = prefix.strip("/")
        self._is_local = is_local

    def _resolve(self, path: str) -> str:
        if self._is_local:
            candidate = Path(path)
            if candidate.is_absolute():
                return str(candidate)
            root = Path(self._prefix or ".")
            return str(root / path)
        if not self._prefix:
            return path.lstrip("/")
        return posixpath.join(self._prefix, path.lstrip("/"))

    def _meta(self, entry: FileEntry) -> ObjectMeta:
        path = entry.path
        if not self._is_local and "://" in path:
            _, rest = path.split("://", 1)
            _, _, key = rest.partition("/")
            path = key
        return ObjectMeta(path=path, size=entry.size, modified=entry.modified, etag=entry.etag)

    def get(self, path: str) -> bytes:
        return self._client.read_bytes(self._resolve(path))

    def get_range(self, path: str, byte_range: ByteRange) -> bytes:
        start, end = byte_range.as_slice()
        return self._client.read_range(self._resolve(path), start, None if end is None else end - start)

    def get_ranges(self, path: str, ranges: Sequence[ByteRange]) -> list[bytes]:
        return [self.get_range(path, byte_range) for byte_range in ranges]

    def put(self, path: str, data: bytes, *, options: PutOptions | None = None) -> PutResult:
        opts = options or PutOptions()
        bytes_written = self._client.write_bytes(
            self._resolve(path), data, content_type=opts.content_type, metadata=opts.metadata
        )
        return PutResult(bytes=bytes_written)

    def delete(self, path: str) -> None:
        self._client.delete(self._resolve(path))

    def head(self, path: str) -> ObjectMeta:
        entry = self._client.stat(self._resolve(path))
        return self._meta(entry)

    def exists(self, path: str) -> bool:
        return self._client.exists(self._resolve(path))

    def list(self, prefix: str = "") -> Iterator[ObjectMeta]:
        resolved = self._resolve(prefix) if prefix else self._resolve("")
        entries = self._client.list(resolved)
        for entry in entries:
            yield self._meta(entry)

    def list_with_delimiter(self, prefix: str = "", delimiter: str = "/") -> ListResult:
        objects = list(self.list(prefix))
        prefixes: set[str] = set()
        for obj in objects:
            suffix = obj.path
            if prefix:
                suffix = suffix[len(self._resolve(prefix)) :].lstrip(delimiter)
            if delimiter in suffix:
                prefixes.add(suffix.split(delimiter, 1)[0])
        return ListResult(objects=objects, prefixes=sorted(prefixes))

    def open_reader(self, path: str) -> io.BytesIO:
        return io.BytesIO(self.get(path))

    def open_writer(self, path: str, *, options: PutOptions | None = None) -> StoreWriter:
        return StoreWriter(self, path, options)

    def iter_chunks(self, path: str, *, chunk_size: int = 8 * 1024 * 1024) -> Iterator[bytes]:
        meta = self.head(path)
        if meta.size is None:
            yield self.get(path)
            return
        offset = 0
        size = max(0, int(meta.size))
        while offset < size:
            length = min(chunk_size, size - offset)
            yield self.get_range(path, ByteRange(offset, length))
            offset += length

    def put_stream(
        self,
        path: str,
        chunks: Iterator[bytes],
        *,
        options: PutOptions | None = None,
    ) -> PutResult:
        if self._is_local:
            resolved = Path(self._resolve(path))
            resolved.parent.mkdir(parents=True, exist_ok=True)
            total = 0
            with resolved.open("wb") as handle:
                for chunk in chunks:
                    if not chunk:
                        continue
                    total += handle.write(chunk)
            return PutResult(bytes=total)
        with tempfile.NamedTemporaryFile(delete=False) as handle:
            total = 0
            for chunk in chunks:
                if not chunk:
                    continue
                total += handle.write(chunk)
            temp_path = handle.name
        try:
            result = self.upload_from_path(temp_path, path, options=options)
        finally:
            Path(temp_path).unlink(missing_ok=True)
        return result

    def download_to_path(self, path: str, local_path: str) -> int:
        if self._is_local:
            source = Path(self._resolve(path))
            destination = Path(local_path)
            destination.parent.mkdir(parents=True, exist_ok=True)
            with source.open("rb") as src, destination.open("wb") as dst:
                return dst.write(src.read())
        return self._client.read_file(self._resolve(path), local_path)

    def upload_from_path(
        self,
        local_path: str,
        path: str,
        *,
        options: PutOptions | None = None,
    ) -> PutResult:
        opts = options or PutOptions()
        bytes_written = self._client.write_file(
            self._resolve(path),
            local_path,
            content_type=opts.content_type,
            metadata=opts.metadata,
        )
        return PutResult(bytes=bytes_written)

    def signed_url(
        self,
        path: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> str | None:
        if self._is_local:
            return None
        return self._client.signed_url(
            self._resolve(path),
            method=method,
            expires_in=expires_in,
            content_type=content_type,
            acl=acl,
        )


class AsyncObjectStoreImpl:
    def __init__(self, client: AsyncFileSystemClient, prefix: str = "", is_local: bool = False) -> None:
        self._client = client
        self._prefix = prefix.strip("/")
        self._is_local = is_local

    def _resolve(self, path: str) -> str:
        if self._is_local:
            candidate = Path(path)
            if candidate.is_absolute():
                return str(candidate)
            root = Path(self._prefix or ".")
            return str(root / path)
        if not self._prefix:
            return path.lstrip("/")
        return posixpath.join(self._prefix, path.lstrip("/"))

    def _meta(self, entry: FileEntry) -> ObjectMeta:
        path = entry.path
        if not self._is_local and "://" in path:
            _, rest = path.split("://", 1)
            _, _, key = rest.partition("/")
            path = key
        return ObjectMeta(path=path, size=entry.size, modified=entry.modified, etag=entry.etag)

    async def get(self, path: str) -> bytes:
        return await self._client.read_bytes(self._resolve(path))

    async def get_range(self, path: str, byte_range: ByteRange) -> bytes:
        start, end = byte_range.as_slice()
        length = None if end is None else end - start
        return await self._client.read_range(self._resolve(path), start, length)

    async def get_ranges(self, path: str, ranges: Sequence[ByteRange]) -> list[bytes]:
        return [await self.get_range(path, byte_range) for byte_range in ranges]

    async def put(self, path: str, data: bytes, *, options: PutOptions | None = None) -> PutResult:
        opts = options or PutOptions()
        bytes_written = await self._client.write_bytes(
            self._resolve(path), data, content_type=opts.content_type, metadata=opts.metadata
        )
        return PutResult(bytes=bytes_written)

    async def delete(self, path: str) -> None:
        await self._client.delete(self._resolve(path))

    async def head(self, path: str) -> ObjectMeta:
        entry = await self._client.stat(self._resolve(path))
        return self._meta(entry)

    async def exists(self, path: str) -> bool:
        return await self._client.exists(self._resolve(path))

    async def list(self, prefix: str = "") -> AsyncIterator[ObjectMeta]:
        resolved = self._resolve(prefix) if prefix else self._resolve("")
        entries = await self._client.list(resolved)
        for entry in entries:
            yield self._meta(entry)

    async def list_with_delimiter(self, prefix: str = "", delimiter: str = "/") -> ListResult:
        objects = [obj async for obj in self.list(prefix)]
        prefixes: set[str] = set()
        for obj in objects:
            suffix = obj.path
            if prefix:
                suffix = suffix[len(self._resolve(prefix)) :].lstrip(delimiter)
            if delimiter in suffix:
                prefixes.add(suffix.split(delimiter, 1)[0])
        return ListResult(objects=objects, prefixes=sorted(prefixes))

    async def open_reader(self, path: str) -> AsyncStoreReader:
        return AsyncStoreReader(await self.get(path))

    async def open_writer(self, path: str, *, options: PutOptions | None = None) -> AsyncStoreWriter:
        return AsyncStoreWriter(self, path, options)

    async def iter_chunks(self, path: str, *, chunk_size: int = 8 * 1024 * 1024) -> AsyncIterator[bytes]:
        meta = await self.head(path)
        if meta.size is None:
            yield await self.get(path)
            return
        offset = 0
        size = max(0, int(meta.size))
        while offset < size:
            length = min(chunk_size, size - offset)
            yield await self.get_range(path, ByteRange(offset, length))
            offset += length

    async def put_stream(
        self,
        path: str,
        chunks: AsyncIterator[bytes],
        *,
        options: PutOptions | None = None,
    ) -> PutResult:
        temp_path = await _write_chunks_to_temp(chunks)
        try:
            result = await self.upload_from_path(temp_path, path, options=options)
        finally:
            Path(temp_path).unlink(missing_ok=True)
        return result

    async def download_to_path(self, path: str, local_path: str) -> int:
        if self._is_local:
            source = Path(self._resolve(path))
            destination = Path(local_path)

            def _copy() -> int:
                destination.parent.mkdir(parents=True, exist_ok=True)
                with source.open("rb") as src, destination.open("wb") as dst:
                    return dst.write(src.read())

            return await _run_in_thread(_copy)
        return await self._client.read_file(self._resolve(path), local_path)

    async def upload_from_path(
        self,
        local_path: str,
        path: str,
        *,
        options: PutOptions | None = None,
    ) -> PutResult:
        opts = options or PutOptions()
        bytes_written = await self._client.write_file(
            self._resolve(path),
            local_path,
            content_type=opts.content_type,
            metadata=opts.metadata,
        )
        return PutResult(bytes=bytes_written)

    async def signed_url(
        self,
        path: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> str | None:
        if self._is_local:
            return None
        return await self._client.signed_url(
            self._resolve(path),
            method=method,
            expires_in=expires_in,
            content_type=content_type,
            acl=acl,
        )


async def _run_in_thread(func: Callable[[], Any]) -> Any:
    import asyncio as _asyncio

    return await _asyncio.to_thread(func)


async def _write_chunks_to_temp(chunks: AsyncIterator[bytes]) -> str:
    with tempfile.NamedTemporaryFile(delete=False) as handle:
        async for chunk in chunks:
            if not chunk:
                continue
            handle.write(chunk)
        return handle.name


class LocalStore(SyncObjectStoreImpl):
    def __init__(self, root: str = ".", *, client: FileSystemClient | None = None) -> None:
        super().__init__(client or FileSystemClient(backend="local"), prefix=root, is_local=True)

    def mkdir(self, path: str) -> None:
        self._client.mkdir(self._resolve(path))

    def rmdir(self, path: str, *, recursive: bool = False) -> None:
        self._client.rmdir(self._resolve(path), recursive=recursive)


class AsyncLocalStore(AsyncObjectStoreImpl):
    def __init__(self, root: str = ".", *, client: AsyncFileSystemClient | None = None) -> None:
        super().__init__(client or AsyncFileSystemClient(backend="local"), prefix=root, is_local=True)

    async def mkdir(self, path: str) -> None:
        await self._client.mkdir(self._resolve(path))

    async def rmdir(self, path: str, *, recursive: bool = False) -> None:
        await self._client.rmdir(self._resolve(path), recursive=recursive)


class S3Store(SyncObjectStoreImpl):
    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "",
        options: dict[str, Any] | None = None,
        client: FileSystemClient | None = None,
    ) -> None:
        super().__init__(
            client or FileSystemClient(backend="s3", bucket=bucket, options=options),
            prefix=prefix,
        )


class AsyncS3Store(AsyncObjectStoreImpl):
    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "",
        options: dict[str, Any] | None = None,
        client: AsyncFileSystemClient | None = None,
    ) -> None:
        super().__init__(
            client or AsyncFileSystemClient(backend="s3", bucket=bucket, options=options),
            prefix=prefix,
        )


class GCSStore(SyncObjectStoreImpl):
    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "",
        options: dict[str, Any] | None = None,
        client: FileSystemClient | None = None,
    ) -> None:
        super().__init__(
            client or FileSystemClient(backend="gcs", bucket=bucket, options=options),
            prefix=prefix,
        )


class AsyncGCSStore(AsyncObjectStoreImpl):
    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "",
        options: dict[str, Any] | None = None,
        client: AsyncFileSystemClient | None = None,
    ) -> None:
        super().__init__(
            client or AsyncFileSystemClient(backend="gcs", bucket=bucket, options=options),
            prefix=prefix,
        )


StoreFactory = Callable[..., SyncObjectStore]
AsyncStoreFactory = Callable[..., AsyncObjectStore]


class StoreRegistry:
    def __init__(self) -> None:
        self._sync: dict[str, StoreFactory] = {}
        self._async: dict[str, AsyncStoreFactory] = {}

    def register(
        self,
        scheme: str,
        *,
        sync_factory: StoreFactory | None = None,
        async_factory: AsyncStoreFactory | None = None,
    ) -> None:
        if sync_factory:
            self._sync[scheme] = sync_factory
        if async_factory:
            self._async[scheme] = async_factory

    def get(self, scheme: str, *, async_: bool = False) -> StoreFactory | AsyncStoreFactory:
        factories = self._async if async_ else self._sync
        if scheme not in factories:
            msg = f"Store backend '{scheme}' is not registered"
            raise KeyError(msg)
        return factories[scheme]

    def from_url(self, url: str, *, async_: bool = False, options: dict[str, Any] | None = None) -> Any:
        parsed = urlparse(url)
        scheme = parsed.scheme or "file"
        if scheme in ("file", "local"):
            root = parsed.path or parsed.netloc or "."
            factory = self.get("file", async_=async_)
            return factory(root=root)
        if scheme in ("s3", "gs", "gcs"):
            bucket = parsed.netloc
            prefix = parsed.path.lstrip("/")
            factory = self.get("gcs" if scheme in ("gs", "gcs") else "s3", async_=async_)
            return factory(bucket=bucket, prefix=prefix, options=options)
        factory = self.get(scheme, async_=async_)
        return factory(url=url, options=options)


store_registry = StoreRegistry()
store_registry.register("file", sync_factory=LocalStore, async_factory=AsyncLocalStore)
store_registry.register("s3", sync_factory=S3Store, async_factory=AsyncS3Store)
store_registry.register("gcs", sync_factory=GCSStore, async_factory=AsyncGCSStore)


def store_from_url(url: str, *, options: dict[str, Any] | None = None) -> SyncObjectStore:
    return store_registry.from_url(url, options=options)


def async_store_from_url(url: str, *, options: dict[str, Any] | None = None) -> AsyncObjectStore:
    return store_registry.from_url(url, async_=True, options=options)


__all__ = [
    "AsyncGCSStore",
    "AsyncLocalStore",
    "AsyncObjectStore",
    "AsyncS3Store",
    "AsyncStoreReader",
    "AsyncStoreWriter",
    "ByteRange",
    "GCSStore",
    "ListResult",
    "LocalStore",
    "ObjectMeta",
    "PutOptions",
    "PutResult",
    "S3Store",
    "StoreRegistry",
    "StoreWriter",
    "SyncObjectStore",
    "async_store_from_url",
    "store_from_url",
    "store_registry",
]
