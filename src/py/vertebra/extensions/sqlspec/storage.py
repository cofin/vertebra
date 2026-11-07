"""SQLSpec ObjectStoreProtocol backend, Arrow Flight codec, and DuckDB GCS ADC helpers."""

from __future__ import annotations

import asyncio
import fnmatch
import re
from collections.abc import AsyncIterable, AsyncIterator, Buffer, Iterable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast, overload
from urllib.parse import urlparse

from vertebra.storage import (
    AsyncGcpTokenCache,
    AsyncObjectStore,
    AsyncReadableFile,
    AzureConfig,
    GCSConfig,
    HTTPConfig,
    LocalConfig,
    MemoryConfig,
    ObjectMeta,
    OCIConfig,
    S3Config,
    SigningConfig,
    StorageConfig,
    StreamingStorageResponse,
    SyncGcpTokenCache,
    SyncObjectStore,
    SyncReadableFile,
)
from vertebra.typing import ImproperConfigurationError, MissingDependencyError
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_msgpack, encode_msgpack_bytes
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from sqlspec.protocols import ObjectStoreProtocol
    from sqlspec.storage.registry import StorageRegistry
    from sqlspec.typing import ArrowRecordBatch, ArrowTable

    from vertebra._storage import StorageBuffer
    from vertebra.db.arrow import ArrowCursor

__all__ = (
    "ArrowFlightSerializer",
    "ObjectStoreBackend",
    "StreamingStorageResponse",
    "build_duckdb_gcs_secret_sql",
    "register_storage_alias",
)

_GLOB_CHARS_RE = re.compile(r"[*?[]")
_STORAGE_CONFIGS = (LocalConfig, S3Config, GCSConfig, AzureConfig, OCIConfig, HTTPConfig, MemoryConfig)


class ArrowFlightSerializer:
    """Serializer converting Arrow Flight descriptors to and from MessagePack."""

    __slots__ = ()

    @staticmethod
    def serialize_descriptor(descriptor: Any) -> bytes:
        """Serialize a flight descriptor to MessagePack bytes."""
        return encode_msgpack_bytes(descriptor)

    @staticmethod
    def deserialize_descriptor(payload: bytes) -> Any:
        """Deserialize a flight descriptor from MessagePack bytes."""
        return decode_msgpack(payload)


class ObjectStoreBackend:
    """SQLSpec ObjectStoreProtocol implementation backed by Vertebra's native ObjectStore."""

    backend_type: ClassVar[str] = "vertebra"
    __slots__ = ("_async_store", "_base_path", "_sync_store", "protocol", "store_uri")

    def __init__(
        self,
        uri: str | SyncObjectStore | AsyncObjectStore | StorageConfig | None = None,
        *,
        store: SyncObjectStore | AsyncObjectStore | StorageConfig | None = None,
        base_path: str = "",
        prefix: str = "",
        options: dict[str, str] | None = None,
        signing: SigningConfig | None = None,
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the SQLSpec storage backend from a URI or pre-configured ObjectStore.

        Args:
            uri: Either a storage URI (`memory://`, `file://`, `s3://`, `gs://`, `az://`, `oci://`) or an `ObjectStore`.
            store: Optional explicit `ObjectStore` instance taking precedence over `uri`.
            base_path: Base path prefix prepended to relative object paths.
            prefix: Alias for `base_path` when configuring key namespaces directly.
            options: Optional string options passed to `ObjectStore` when constructed from a URI.
            signing: Optional `SigningConfig` for presigned GET/PUT URL generation.
            token_cache: Optional shared `GcpTokenCache` for GCS OAuth2 bearer token refresh.
            **kwargs: Additional scalar backend options merged into `options`.
        """
        merged_options: dict[str, str] = dict(options or {})
        for key, value in kwargs.items():
            if isinstance(value, (str, int, float, bool)):
                merged_options.setdefault(str(key), str(value))

        target = store if store is not None else uri
        if target is None or (isinstance(target, str) and not target.strip()):
            msg = "An explicit storage URI, configuration, or store is required"
            raise ImproperConfigurationError(msg)
        effective_prefix = SyncObjectStore._normalize_key(base_path or prefix, prefix=True)
        sync_store: SyncObjectStore | None = None
        async_store: AsyncObjectStore | None = None

        if isinstance(target, SyncObjectStore):
            sync_store = target
            if token_cache is not None and getattr(sync_store, "_gcp_token_cache", None) is None:
                sync_store._gcp_token_cache = (
                    token_cache if isinstance(token_cache, SyncGcpTokenCache) else token_cache.to_sync()
                )
        elif isinstance(target, AsyncObjectStore):
            async_store = target
            if token_cache is not None and getattr(async_store, "_gcp_token_cache", None) is None:
                async_store._gcp_token_cache = (
                    token_cache if isinstance(token_cache, AsyncGcpTokenCache) else token_cache.to_async()
                )
        elif isinstance(target, _STORAGE_CONFIGS):
            sync_store = SyncObjectStore.from_config(target, signing=signing, token_cache=token_cache)
        elif isinstance(target, str):
            sync_store = SyncObjectStore(
                target,
                options=merged_options or None,
                signing=signing,
                token_cache=token_cache,
            )
        else:
            msg = f"Unsupported storage target: {type(target).__name__}"
            raise ImproperConfigurationError(msg)

        self._sync_store = sync_store
        self._async_store = async_store
        self._base_path = effective_prefix
        primary_store = sync_store if sync_store is not None else async_store
        assert primary_store is not None
        if store is not None and isinstance(uri, str) and uri.strip():
            self.store_uri = uri
            parsed_scheme = urlparse(uri).scheme
            self.protocol = self._normalize_protocol(parsed_scheme or primary_store.scheme)
        else:
            self.store_uri = primary_store.url
            self.protocol = self._normalize_protocol(primary_store.scheme)

    @property
    def store(self) -> SyncObjectStore:
        """Return the underlying SyncObjectStore instance."""
        return self.sync_store

    @property
    def sync_store(self) -> SyncObjectStore:
        """Return the underlying SyncObjectStore instance."""
        if self._sync_store is None:
            assert self._async_store is not None
            self._sync_store = self._async_store.to_sync()
        return self._sync_store

    @property
    def async_store(self) -> AsyncObjectStore:
        """Return the underlying AsyncObjectStore instance."""
        if self._async_store is None:
            assert self._sync_store is not None
            self._async_store = self._sync_store.to_async()
        return self._async_store

    @property
    def base_path(self) -> str:
        """Return the configured base path prefix."""
        return self._base_path

    @property
    def supports_signing(self) -> bool:
        """Return True when presigned URL generation is configured on the underlying store."""
        return self.sync_store.supports_signing

    @classmethod
    def register_alias(
        cls,
        alias: str,
        uri_or_store: str | SyncObjectStore | AsyncObjectStore,
        *,
        registry: StorageRegistry | None = None,
        base_path: str = "",
        **kwargs: Any,
    ) -> None:
        """Register this backend under a named alias in SQLSpec's `storage_registry`."""
        reg_mod = ensure_module("sqlspec.storage.registry", package="sqlspec", extra="sqlspec")
        target_registry = reg_mod.storage_registry if registry is None else registry
        backend_config: dict[str, Any] = dict(kwargs)
        if base_path:
            backend_config["base_path"] = base_path
        if isinstance(uri_or_store, (SyncObjectStore, AsyncObjectStore)):
            stored_uri = uri_or_store.url
            backend_config["store"] = uri_or_store
        else:
            stored_uri = uri_or_store
        target_registry._alias_configs[alias] = (
            cast("type[ObjectStoreProtocol]", cls),
            stored_uri,
            backend_config,
        )
        target_registry.clear_cache(alias)

    def resolve_uri(self, path: str | Path) -> str:
        """Resolve a backend-relative path to its unsigned canonical address."""
        resolved = self._qualify(path, prefix=True)
        parsed = urlparse(self.store_uri)
        if self.protocol == "file":
            root = Path(parsed.netloc + parsed.path if parsed.netloc else parsed.path)
            return str((root / resolved).resolve()) if resolved else str(root.resolve())
        joined = "/".join(part.strip("/") for part in (parsed.path, resolved) if part.strip("/"))
        authority = f"{parsed.scheme}://{parsed.netloc}"
        if parsed.netloc:
            return f"{authority}/{joined}" if joined else authority
        return f"{authority}{joined}"

    def read_bytes_sync(self, path: str | Path, **kwargs: Any) -> bytes:
        """Read full object bytes synchronously."""
        _ = kwargs
        return self.sync_store.get(self._qualify(path))

    def write_bytes_sync(self, path: str | Path, data: bytes, **kwargs: Any) -> None:
        """Write full object bytes synchronously."""
        self.sync_store.put(self._qualify(path), data, **kwargs)

    def read_text_sync(self, path: str | Path, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read and decode UTF-8 text from an object synchronously."""
        return self.read_bytes_sync(path, **kwargs).decode(encoding)

    def write_text_sync(self, path: str | Path, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Encode and write text to an object synchronously."""
        self.write_bytes_sync(path, data.encode(encoding), **kwargs)

    def exists_sync(self, path: str | Path, **kwargs: Any) -> bool:
        """Return True if the object exists synchronously."""
        _ = kwargs
        return self.sync_store.exists(self._qualify(path))

    def delete_sync(self, path: str | Path, **kwargs: Any) -> None:
        """Delete an object synchronously."""
        _ = kwargs
        self.sync_store.delete(self._qualify(path))

    def copy_sync(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Copy an object within the store synchronously."""
        _ = kwargs
        self.sync_store.copy(self._qualify(source), self._qualify(destination))

    def move_sync(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Move an object within the store synchronously."""
        _ = kwargs
        src_key = self._qualify(source)
        dst_key = self._qualify(destination)
        self.sync_store.rename(src_key, dst_key)

    def list_objects_sync(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List relative object keys matching the optional prefix synchronously."""
        _ = kwargs
        qualified_prefix = self._qualify(prefix, prefix=True)
        items = self.sync_store.list(qualified_prefix)
        rel_paths = [self._unqualify(item.path) for item in items]
        return self._filter_recursive(rel_paths, prefix=self._unqualify(qualified_prefix), recursive=recursive)

    def list_page_sync(
        self, prefix: str = "", *, cursor: str | None = None, limit: int = 1000
    ) -> tuple[list[str], str | None]:
        """List relative keys with an opaque cursor from the native store."""
        qualified = self._qualify(prefix, prefix=True)
        items, next_cursor = self.sync_store.list_page(qualified, cursor=cursor, limit=limit)
        return [self._unqualify(item.path) for item in items], next_cursor

    def get_stream_sync(self, path: str | Path, *, chunk_size: int | None = None) -> Iterator[StorageBuffer]:
        """Yield native storage buffers without materializing the complete object."""
        return self.sync_store.get_stream(self._qualify(path), chunk_size=chunk_size)

    def put_stream_sync(self, path: str | Path, chunks: Iterable[Buffer], **kwargs: Any) -> None:
        """Upload buffer chunks incrementally through a native multipart session."""
        self.sync_store.put_stream(self._qualify(path), chunks, **kwargs)

    def open_sync(self, path: str | Path, *, size: int | None = None, chunk_size: int = 65_536) -> SyncReadableFile:
        """Return a seekable context-managed reader over a backend-relative key."""
        return self.sync_store.open_readable_file(self._qualify(path), size=size, chunk_size=chunk_size)

    def open_async(self, path: str | Path, *, size: int | None = None, chunk_size: int = 65_536) -> AsyncReadableFile:
        """Return a reader whose reads and context lifecycle are asynchronous."""
        return self.async_store.open_readable_file(self._qualify(path), size=size, chunk_size=chunk_size)

    def glob_sync(self, pattern: str, **kwargs: Any) -> list[str]:
        """Match relative object keys against a shell-style or URI-qualified glob pattern synchronously."""
        _ = kwargs
        cleaned_pattern, static_prefix = self._normalize_glob_pattern(pattern)
        candidates = self.list_objects_sync(static_prefix, recursive=True)
        return [
            candidate
            for candidate in candidates
            if fnmatch.fnmatch(candidate, cleaned_pattern)
            or fnmatch.fnmatch(candidate.rsplit("/", 1)[-1], cleaned_pattern)
        ]

    async def glob_async(self, pattern: str, **kwargs: Any) -> list[str]:
        """Match relative object keys against a shell-style or URI-qualified glob pattern asynchronously."""
        _ = kwargs
        cleaned_pattern, static_prefix = self._normalize_glob_pattern(pattern)
        candidates = await self.list_objects_async(static_prefix, recursive=True)
        return [
            candidate
            for candidate in candidates
            if fnmatch.fnmatch(candidate, cleaned_pattern)
            or fnmatch.fnmatch(candidate.rsplit("/", 1)[-1], cleaned_pattern)
        ]

    def is_object_sync(self, path: str | Path) -> bool:
        """Return True when `path` points to a stored object."""
        return self.exists_sync(path)

    def is_path_sync(self, path: str | Path) -> bool:
        """Return True when `path` acts as a non-empty key prefix."""
        qualified = self._qualify(path, prefix=True)
        items = self.sync_store.list(qualified)
        return any(item.path.startswith(f"{qualified}/") for item in items) if qualified else bool(items)

    def get_metadata_sync(self, path: str | Path, **kwargs: Any) -> dict[str, object]:
        """Return metadata dictionary for a stored object synchronously."""
        _ = kwargs
        meta: ObjectMeta = self.sync_store.head(self._qualify(path))
        return {
            "path": self._unqualify(meta.path),
            "size": meta.size,
            "last_modified": meta.last_modified,
            "etag": meta.etag,
            "version": meta.version,
        }

    def read_arrow_sync(self, path: str | Path, **kwargs: Any) -> ArrowTable:
        """Read a Parquet or Arrow IPC object into a `pyarrow.Table` synchronously."""
        pa = ensure_module("pyarrow", package="pyarrow", extra="arrow")
        file_format = str(kwargs.pop("file_format", "") or self._infer_arrow_format(path))
        key = self._qualify(path)
        if file_format == "parquet":
            pq = ensure_module("pyarrow.parquet", package="pyarrow", extra="arrow")
            with self.sync_store.open_readable_file(key) as reader:
                return pq.read_table(reader, **kwargs)
        ipc = ensure_module("pyarrow.ipc", package="pyarrow", extra="arrow")
        view = self.sync_store.get_view(key)
        buf = pa.py_buffer(view)
        try:
            with ipc.open_stream(buf) as stream_reader:
                return stream_reader.read_all()
        except pa.ArrowInvalid:
            with ipc.open_file(buf) as file_reader:
                return file_reader.read_all()

    def write_arrow_sync(self, path: str | Path, table: ArrowTable, **kwargs: Any) -> None:
        """Serialize and write a `pyarrow.Table` as Parquet or Arrow IPC synchronously."""
        payload = self._serialize_arrow_table(path, table, **kwargs)
        self.sync_store.put(self._qualify(path), payload)

    def write_arrow_cursor_sync(self, path: str | Path, cursor: ArrowCursor | Any, **kwargs: Any) -> None:
        """Write an `ArrowCursor` to object storage as zero-copy Arrow IPC or batch-streamed Parquet."""
        file_format = str(kwargs.pop("file_format", "") or self._infer_arrow_format(path))
        key = self._qualify(path)
        view = cursor.buffer_view() if hasattr(cursor, "buffer_view") else memoryview(cursor)
        if file_format == "arrow-ipc" and len(view) > 0:
            self.sync_store.put(key, view)
            return
        payload = self._serialize_arrow_cursor_parquet(cursor, **kwargs)
        self.sync_store.put(key, payload)

    def stream_arrow_sync(
        self,
        pattern: str,
        *,
        file_format: Literal["parquet"] = "parquet",
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> Iterator[ArrowRecordBatch]:
        """Stream `pyarrow.RecordBatch` frames from matching Parquet or IPC objects synchronously."""
        pa = ensure_module("pyarrow", package="pyarrow", extra="arrow")
        matched = self._resolve_pattern_matches_sync(pattern)
        for rel_path in matched:
            key = self._qualify(rel_path)
            if file_format == "parquet":
                pq = ensure_module("pyarrow.parquet", package="pyarrow", extra="arrow")
                with self.sync_store.open_readable_file(key) as reader:
                    parquet_file = pq.ParquetFile(reader)
                    yield from parquet_file.iter_batches(batch_size=batch_size, **kwargs)
            else:
                ipc = ensure_module("pyarrow.ipc", package="pyarrow", extra="arrow")
                buf = pa.py_buffer(self.sync_store.get_view(key))
                with ipc.open_stream(buf) as stream_reader:
                    yield from stream_reader

    def stream_read_sync(
        self,
        path: str | Path,
        chunk_size: int | None = None,
        **kwargs: Any,
    ) -> Iterator[bytes]:
        """Stream fixed-size byte chunks from a stored object synchronously."""
        _ = kwargs
        step = chunk_size if chunk_size is not None and chunk_size > 0 else 65_536
        key = self._qualify(path)
        with self.sync_store.open_readable_file(key) as reader:
            while True:
                chunk = reader.read(step)
                if not chunk:
                    break
                yield chunk

    def stream_response(
        self,
        path: str | Path,
        *,
        chunk_size: int = 65_536,
        media_type: str = "application/octet-stream",
        filename: str | None = None,
    ) -> StreamingStorageResponse:
        """Construct a zero-copy `StreamingStorageResponse` descriptor for HTTP streaming."""
        key = self._qualify(path)
        meta = self.sync_store.head(key)
        headers: dict[str, str] = {"Content-Length": str(meta.size)}
        if meta.etag:
            headers["ETag"] = meta.etag
        if filename:
            headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        return StreamingStorageResponse(
            store=self.sync_store,
            key=key,
            total_size=meta.size,
            chunk_size=chunk_size,
            media_type=media_type,
            headers=headers,
        )

    async def read_bytes_async(self, path: str | Path, **kwargs: Any) -> bytes:
        """Read full object bytes asynchronously."""
        _ = kwargs
        return await self.async_store.get(self._qualify(path))

    async def write_bytes_async(self, path: str | Path, data: bytes, **kwargs: Any) -> None:
        """Write full object bytes asynchronously."""
        await self.async_store.put(self._qualify(path), data, **kwargs)

    async def read_text_async(self, path: str | Path, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read and decode UTF-8 text from an object asynchronously."""
        raw = await self.read_bytes_async(path, **kwargs)
        return raw.decode(encoding)

    async def write_text_async(self, path: str | Path, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Encode and write text to an object asynchronously."""
        await self.write_bytes_async(path, data.encode(encoding), **kwargs)

    async def stream_read_async(
        self,
        path: str | Path,
        chunk_size: int | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[bytes]:
        """Return an asynchronous byte chunk iterator over a stored object."""
        _ = kwargs
        step = chunk_size if chunk_size is not None and chunk_size > 0 else 65_536
        return self._iter_bytes_async(self._qualify(path), step)

    async def exists_async(self, path: str | Path, **kwargs: Any) -> bool:
        """Return True if the object exists asynchronously."""
        _ = kwargs
        return await self.async_store.exists(self._qualify(path))

    async def delete_async(self, path: str | Path, **kwargs: Any) -> None:
        """Delete an object asynchronously."""
        _ = kwargs
        await self.async_store.delete(self._qualify(path))

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List relative object keys matching the optional prefix asynchronously."""
        _ = kwargs
        qualified_prefix = self._qualify(prefix, prefix=True)
        items = await self.async_store.list(qualified_prefix)
        rel_paths = [self._unqualify(item.path) for item in items]
        return self._filter_recursive(rel_paths, prefix=self._unqualify(qualified_prefix), recursive=recursive)

    async def list_page_async(
        self, prefix: str = "", *, cursor: str | None = None, limit: int = 1000
    ) -> tuple[list[str], str | None]:
        """List relative keys asynchronously with an opaque native cursor."""
        qualified = self._qualify(prefix, prefix=True)
        items, next_cursor = await self.async_store.list_page(qualified, cursor=cursor, limit=limit)
        return [self._unqualify(item.path) for item in items], next_cursor

    async def get_stream_async(
        self, path: str | Path, *, chunk_size: int | None = None
    ) -> AsyncIterator[StorageBuffer]:
        """Yield asynchronously produced native storage buffers."""
        async for chunk in self.async_store.get_stream(self._qualify(path), chunk_size=chunk_size):
            yield chunk

    async def put_stream_async(
        self, path: str | Path, chunks: AsyncIterable[Buffer] | Iterable[Buffer], **kwargs: Any
    ) -> None:
        """Upload sync or async buffer chunks through a native multipart session."""
        await self.async_store.put_stream(self._qualify(path), chunks, **kwargs)

    async def copy_async(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Copy an object within the store asynchronously."""
        _ = kwargs
        await self.async_store.copy(self._qualify(source), self._qualify(destination))

    async def move_async(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Move an object within the store asynchronously."""
        _ = kwargs
        src_key = self._qualify(source)
        dst_key = self._qualify(destination)
        await self.async_store.rename(src_key, dst_key)

    async def get_metadata_async(self, path: str | Path, **kwargs: Any) -> dict[str, object]:
        """Return metadata dictionary for a stored object asynchronously."""
        _ = kwargs
        meta: ObjectMeta = await self.async_store.head(self._qualify(path))
        return {
            "path": self._unqualify(meta.path),
            "size": meta.size,
            "last_modified": meta.last_modified,
            "etag": meta.etag,
            "version": meta.version,
        }

    async def read_arrow_async(self, path: str | Path, **kwargs: Any) -> ArrowTable:
        """Read a Parquet or Arrow IPC object into a `pyarrow.Table` asynchronously."""
        return await asyncio.to_thread(self.read_arrow_sync, path, **kwargs)

    async def write_arrow_async(self, path: str | Path, table: ArrowTable, **kwargs: Any) -> None:
        """Serialize and write a `pyarrow.Table` as Parquet or Arrow IPC asynchronously."""
        payload = await asyncio.to_thread(self._serialize_arrow_table, path, table, **kwargs)
        await self.async_store.put(self._qualify(path), payload)

    async def write_arrow_cursor_async(self, path: str | Path, cursor: ArrowCursor | Any, **kwargs: Any) -> None:
        """Write an `ArrowCursor` asynchronously as zero-copy Arrow IPC or batch-streamed Parquet."""
        file_format = str(kwargs.pop("file_format", "") or self._infer_arrow_format(path))
        key = self._qualify(path)
        view = cursor.buffer_view() if hasattr(cursor, "buffer_view") else memoryview(cursor)
        if file_format == "arrow-ipc" and len(view) > 0:
            await self.async_store.put(key, view)
            return
        payload = await asyncio.to_thread(self._serialize_arrow_cursor_parquet, cursor, **kwargs)
        await self.async_store.put(key, payload)

    def stream_arrow_async(
        self,
        pattern: str,
        *,
        file_format: Literal["parquet"] = "parquet",
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> AsyncIterator[ArrowRecordBatch]:
        """Return an `AsyncIterator[ArrowRecordBatch]` streaming matching Parquet or IPC objects."""
        return self._iter_arrow_async(
            pattern,
            file_format=file_format,
            batch_size=batch_size,
            **kwargs,
        )

    @overload
    def sign_sync(self, paths: str, expires_in: int = 3600, for_upload: bool = False) -> str: ...
    @overload
    def sign_sync(self, paths: list[str], expires_in: int = 3600, for_upload: bool = False) -> list[str]: ...
    def sign_sync(
        self,
        paths: str | list[str],
        expires_in: int = 3600,
        for_upload: bool = False,
    ) -> str | list[str]:
        """Generate presigned GET or PUT URLs synchronously."""
        if isinstance(paths, str):
            return self._sign_single(paths, expires_in=expires_in, for_upload=for_upload)
        return [self._sign_single(item, expires_in=expires_in, for_upload=for_upload) for item in paths]

    @overload
    async def sign_async(self, paths: str, expires_in: int = 3600, for_upload: bool = False) -> str: ...
    @overload
    async def sign_async(self, paths: list[str], expires_in: int = 3600, for_upload: bool = False) -> list[str]: ...
    async def sign_async(
        self,
        paths: str | list[str],
        expires_in: int = 3600,
        for_upload: bool = False,
    ) -> str | list[str]:
        """Generate presigned GET or PUT URLs asynchronously."""
        if isinstance(paths, str):
            return await self._sign_single_async(paths, expires_in=expires_in, for_upload=for_upload)
        return [await self._sign_single_async(item, expires_in=expires_in, for_upload=for_upload) for item in paths]

    async def _iter_bytes_async(self, key: str, chunk_size: int) -> AsyncIterator[bytes]:
        """Yield byte slices asynchronously using range reads against the underlying store."""
        meta = await self.async_store.head(key)
        offset = 0
        while offset < meta.size:
            length = min(chunk_size, meta.size - offset)
            chunk = await self.async_store.get_range(key, offset, length)
            if not chunk:
                break
            offset += len(chunk)
            yield chunk

    async def _iter_arrow_async(
        self,
        pattern: str,
        *,
        file_format: Literal["parquet"] = "parquet",
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> AsyncIterator[ArrowRecordBatch]:
        """Yield Arrow record batches asynchronously."""
        batches = await asyncio.to_thread(
            lambda: list(self.stream_arrow_sync(pattern, file_format=file_format, batch_size=batch_size, **kwargs))
        )
        for batch in batches:
            yield batch

    def _sign_single(self, path: str, *, expires_in: int, for_upload: bool) -> str:
        """Sign a single object key using the underlying ObjectStore."""
        qualified = self._qualify(path)
        if not self.sync_store.supports_signing:
            msg = f"URL signing not supported by backend ({self.protocol})"
            raise NotImplementedError(msg)
        if for_upload:
            return self.sync_store.presign_put(qualified, expires_in_secs=expires_in)
        return self.sync_store.presign_get(qualified, expires_in_secs=expires_in)

    async def _sign_single_async(self, path: str, *, expires_in: int, for_upload: bool) -> str:
        """Sign a single object key asynchronously using the underlying AsyncObjectStore."""
        qualified = self._qualify(path)
        if not self.async_store.supports_signing:
            msg = f"URL signing not supported by backend ({self.protocol})"
            raise NotImplementedError(msg)
        if for_upload:
            return await self.async_store.presign_put(qualified, expires_in_secs=expires_in)
        return await self.async_store.presign_get(qualified, expires_in_secs=expires_in)

    def _serialize_arrow_table(self, path: str | Path, table: ArrowTable, **kwargs: Any) -> memoryview:
        """Serialize a `pyarrow.Table` into a zero-copy `BufferOutputStream` buffer."""
        pa = ensure_module("pyarrow", package="pyarrow", extra="arrow")
        file_format = str(kwargs.pop("file_format", "") or self._infer_arrow_format(path))
        sink = pa.BufferOutputStream()
        if file_format == "parquet":
            pq = ensure_module("pyarrow.parquet", package="pyarrow", extra="arrow")
            pq.write_table(table, sink, **kwargs)
        else:
            ipc = ensure_module("pyarrow.ipc", package="pyarrow", extra="arrow")
            with ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
        return memoryview(sink.getvalue())

    def _serialize_arrow_cursor_parquet(self, cursor: ArrowCursor | Any, **kwargs: Any) -> memoryview:
        """Stream `RecordBatch` frames from an `ArrowCursor` into an in-memory Parquet buffer."""
        pa = ensure_module("pyarrow", package="pyarrow", extra="arrow")
        pq = ensure_module("pyarrow.parquet", package="pyarrow", extra="arrow")
        ipc = ensure_module("pyarrow.ipc", package="pyarrow", extra="arrow")
        sink = pa.BufferOutputStream()
        view = cursor.buffer_view() if hasattr(cursor, "buffer_view") else memoryview(cursor)
        if len(view) > 0:
            try:
                with (
                    ipc.open_stream(pa.py_buffer(view)) as reader,
                    pq.ParquetWriter(sink, reader.schema, **kwargs) as writer,
                ):
                    for batch in reader:
                        writer.write_batch(batch)
                return memoryview(sink.getvalue())
            except (TypeError, ValueError, OSError):
                pass
        table = cursor.to_arrow_table() if hasattr(cursor, "to_arrow_table") else cursor
        pq.write_table(table, sink, **kwargs)
        return memoryview(sink.getvalue())

    def _strip_matching_uri_prefix(self, raw_path_or_pattern: str) -> str:
        """Remove matching URI qualification while preserving raw key and glob text."""
        if "://" not in raw_path_or_pattern:
            return raw_path_or_pattern
        scheme, _, address = raw_path_or_pattern.partition("://")
        authority, separator, raw_path = address.partition("/")
        parsed_store = urlparse(self.store_uri)
        in_proto = self._normalize_protocol(scheme)
        store_proto = self._normalize_protocol(parsed_store.scheme or self.protocol)
        if raw_path_or_pattern != raw_path_or_pattern.strip() or in_proto != store_proto:
            msg = f"URI scheme {scheme!r} does not match store URI {self.store_uri!r}"
            raise ValueError(msg)
        if authority != parsed_store.netloc:
            msg = f"URI authority {authority!r} does not match store URI {self.store_uri!r}"
            raise ValueError(msg)
        if in_proto == "file":
            return f"/{raw_path}" if separator else ""
        store_prefix = parsed_store.path.removeprefix("/").removesuffix("/")
        if store_prefix and raw_path.startswith(f"{store_prefix}/"):
            raw_path = raw_path[len(store_prefix) + 1 :]
        elif store_prefix and raw_path == store_prefix:
            raw_path = ""
        return raw_path

    def _normalize_glob_pattern(self, pattern: str) -> tuple[str, str]:
        """Normalize a relative or URI-qualified glob pattern and extract its static prefix."""
        rel_pattern = self._unqualify(self._qualify(pattern, prefix=True))
        match = _GLOB_CHARS_RE.search(rel_pattern)
        if match is None:
            static_prefix = rel_pattern.rsplit("/", 1)[0] if "/" in rel_pattern else ""
        else:
            before_wildcard = rel_pattern[: match.start()]
            static_prefix = before_wildcard.rsplit("/", 1)[0] if "/" in before_wildcard else ""
        return rel_pattern, static_prefix

    def _resolve_pattern_matches_sync(self, pattern: str) -> list[str]:
        """Resolve either a glob wildcard pattern or an exact object key."""
        normalized = self._strip_matching_uri_prefix(pattern)
        if any(ch in normalized for ch in ("*", "?", "[")):
            return self.glob_sync(normalized)
        return [normalized] if self.exists_sync(normalized) else []

    def _qualify(self, path: str | Path, *, prefix: bool = False) -> str:
        """Normalize a relative, URI-qualified, or local-root-qualified path into an ObjectStore key."""
        raw_str = path.as_posix() if isinstance(path, Path) else str(path)
        if prefix and raw_str in {"", "/"}:
            raw_str = ""
        raw_str = self._strip_matching_uri_prefix(raw_str)
        if self.protocol == "file":
            parsed = urlparse(self.store_uri)
            local_root = Path(parsed.netloc + parsed.path if parsed.netloc else parsed.path).resolve()
            candidate = Path(raw_str)
            if candidate.is_absolute():
                SyncObjectStore._normalize_key(raw_str.removeprefix("/"), prefix=prefix)
                resolved_candidate = candidate.resolve()
                try:
                    rel_to_root = resolved_candidate.relative_to(local_root)
                except ValueError as exc:
                    msg = f"Path traversal outside storage root rejected: {raw_str}"
                    raise ValueError(msg) from exc
                relative = "" if rel_to_root == Path(".") else rel_to_root.as_posix()
                return SyncObjectStore._normalize_key(relative, prefix=prefix)

        cleaned = SyncObjectStore._normalize_key(raw_str, prefix=prefix)
        if not self._base_path:
            return cleaned
        if not cleaned:
            return self._base_path
        if cleaned == self._base_path or cleaned.startswith(f"{self._base_path}/"):
            return cleaned
        return f"{self._base_path}/{cleaned}"

    def _unqualify(self, qualified_path: str) -> str:
        """Strip any configured `base_path` prefix from a returned store key."""
        if self._base_path and qualified_path.startswith(f"{self._base_path}/"):
            return qualified_path[len(self._base_path) + 1 :]
        if self._base_path and qualified_path == self._base_path:
            return ""
        return qualified_path

    @staticmethod
    def _filter_recursive(paths: list[str], *, prefix: str, recursive: bool) -> list[str]:
        """Filter listed relative paths when `recursive=False`."""
        if recursive:
            return paths
        norm_prefix = SyncObjectStore._normalize_key(prefix, prefix=True)
        base_prefix = f"{norm_prefix}/" if norm_prefix else ""
        filtered: list[str] = []
        for item in paths:
            if base_prefix and not item.startswith(base_prefix):
                continue
            remainder = item[len(base_prefix) :] if base_prefix else item
            if "/" not in remainder:
                filtered.append(item)
        return filtered

    @staticmethod
    def _infer_arrow_format(path: str | Path) -> str:
        """Infer Parquet vs Arrow IPC format from file suffix."""
        suffix = Path(str(path)).suffix.lower()
        if suffix in {".arrow", ".ipc", ".feather"}:
            return "arrow-ipc"
        return "parquet"

    @staticmethod
    def _normalize_protocol(scheme: str) -> str:
        """Normalize scheme names to SQLSpec protocol strings."""
        lowered = scheme.lower()
        if lowered == "r2":
            raise ImproperConfigurationError(f"Unsupported storage URI scheme {lowered!r}")
        if lowered == "gcs":
            return "gs"
        if lowered == "azure":
            return "az"
        return lowered


def register_storage_alias(
    alias: str,
    uri_or_store: str | SyncObjectStore | AsyncObjectStore,
    *,
    registry: StorageRegistry | None = None,
    base_path: str = "",
    **kwargs: Any,
) -> None:
    """Register `ObjectStoreBackend` under `alias` in SQLSpec's `storage_registry`."""
    ObjectStoreBackend.register_alias(
        alias,
        uri_or_store,
        registry=registry,
        base_path=base_path,
        **kwargs,
    )


def build_duckdb_gcs_secret_sql(
    secret_name: str = "vertebra_gcs",
    config: GCSConfig | None = None,
    *,
    bearer_token: str | None = None,
) -> tuple[str, tuple[str]]:
    """Build parameterized DuckDB `httpfs` GCS bearer-token `CREATE OR REPLACE SECRET` SQL and parameters.

    Resolves an OAuth2 bearer token via `GCSConfig.resolve_adc_token()` (including optional
    `target_service_account` impersonation) when `bearer_token` is omitted.
    """
    validate_identifier(secret_name, "DuckDB secret name", allow_dots=False)
    token = bearer_token
    if token is None and config is not None:
        token = config.resolve_adc_token()
    if not token:
        raise MissingDependencyError(
            "google-auth",
            install_package="google-auth",
            extra="sqlspec",
        )
    sql = f"CREATE OR REPLACE SECRET {secret_name} (TYPE gcs, BEARER_TOKEN ?)"
    return sql, (token,)
