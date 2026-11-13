"""SQLSpec ObjectStoreProtocol backend and Arrow Flight codec."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Buffer, Iterable, Iterator, Mapping
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
    PutResult,
    R2Config,
    S3Config,
    SigningConfig,
    StorageConfig,
    StreamingStorageResponse,
    SyncGcpTokenCache,
    SyncObjectStore,
    SyncReadableFile,
)
from vertebra.storage._arrow import (
    iter_batches_async,
    iter_batches_sync,
    read_table_async,
    read_table_sync,
    write_batches_async,
    write_batches_sync,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_msgpack, encode_msgpack_bytes

if TYPE_CHECKING:
    from sqlspec.protocols import ObjectStoreProtocol
    from sqlspec.storage.registry import StorageRegistry
    from sqlspec.typing import ArrowRecordBatch, ArrowTable

    from vertebra._storage import StorageBuffer
    from vertebra.db.arrow import ArrowCursor

__all__ = (
    "ArrowFlightSerializer",
    "ObjectStoreBackend",
    "register_storage_alias",
)

_STORAGE_CONFIGS = (LocalConfig, S3Config, GCSConfig, AzureConfig, OCIConfig, R2Config, HTTPConfig, MemoryConfig)


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
    supports_direct_arrow_write: ClassVar[bool] = True
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
            uri: Either a storage URI (`memory://`, `file://`, `s3://`, `gs://`, `az://`, `oci://`, `r2://`) or an `ObjectStore`.
            store: Optional explicit `ObjectStore` instance taking precedence over `uri`.
            base_path: Base path prefix prepended to relative object paths.
            prefix: Alias for `base_path` when configuring key namespaces directly.
            options: Optional string options passed to `ObjectStore` when constructed from a URI.
            signing: Optional `SigningConfig` for presigned GET/PUT URL generation.
            token_cache: Optional shared `GcpTokenCache` for GCS OAuth2 bearer token refresh when building a store
                from a config or URI.
            **kwargs: Additional scalar backend options merged into `options`.

        Raises:
            ImproperConfigurationError: When no target is supplied, or `signing` / `token_cache` accompany a
                prebuilt object store whose authentication is already fixed.
        """
        self._reject_traversal(base_path)
        self._reject_traversal(prefix)
        merged_options: dict[str, str] = dict(options or {})
        for key, value in kwargs.items():
            if isinstance(value, (str, int, float, bool)):
                merged_options.setdefault(str(key), str(value))

        target = store if store is not None else uri
        if target is None or (isinstance(target, str) and not target.strip()):
            msg = "SQLSpec storage requires an explicit store, configuration, or URI"
            raise ImproperConfigurationError(msg)
        if isinstance(target, (SyncObjectStore, AsyncObjectStore)) and (signing is not None or token_cache is not None):
            msg = "signing and token_cache apply only when building a store from a StorageConfig or URI"
            raise ImproperConfigurationError(msg)
        sync_store: SyncObjectStore
        async_store: AsyncObjectStore

        if isinstance(target, SyncObjectStore):
            sync_store = target
            async_store = target.to_async()
        elif isinstance(target, AsyncObjectStore):
            async_store = target
            sync_store = target.to_sync()
        elif isinstance(target, _STORAGE_CONFIGS):
            sync_store = SyncObjectStore.from_config(target, signing=signing, token_cache=token_cache)
            async_store = sync_store.to_async()
        else:
            uri_str = str(target)
            sync_store = SyncObjectStore(
                uri_str,
                options=merged_options or None,
                signing=signing,
                token_cache=token_cache,
            )
            async_store = sync_store.to_async()

        effective_prefix = (base_path or prefix).strip("/")
        self._sync_store = sync_store
        self._async_store = async_store
        self._base_path = effective_prefix
        if store is not None and isinstance(uri, str) and uri.strip():
            self.store_uri = uri
            parsed_scheme = urlparse(uri).scheme
            self.protocol = self._normalize_protocol(parsed_scheme or sync_store.scheme)
        else:
            self.store_uri = sync_store.url
            self.protocol = self._normalize_protocol(sync_store.scheme)

    @property
    def sync_store(self) -> SyncObjectStore:
        """Return the underlying SyncObjectStore instance."""
        return self._sync_store

    @property
    def async_store(self) -> AsyncObjectStore:
        """Return the underlying AsyncObjectStore instance."""
        return self._async_store

    @property
    def base_path(self) -> str:
        """Return the configured base path prefix."""
        return self._base_path

    @property
    def supports_signing(self) -> bool:
        """Return True when presigned URL generation is configured on the underlying store."""
        return self._sync_store.supports_signing

    def resolve_uri(self, path: str | Path) -> str:
        """Resolve a backend-relative path to its unsigned canonical address."""
        resolved = self._qualify(path)
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
        return self._sync_store.get(self._qualify(path), expected_sha256=kwargs.get("expected_sha256"))

    def write_bytes_sync(self, path: str | Path, data: bytes, **kwargs: Any) -> None:
        """Write full object bytes synchronously."""
        self._sync_store.put(self._qualify(path), data, **self._write_options(kwargs))

    def read_text_sync(self, path: str | Path, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read and decode UTF-8 text from an object synchronously."""
        return self.read_bytes_sync(path, **kwargs).decode(encoding)

    def write_text_sync(self, path: str | Path, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Encode and write text to an object synchronously."""
        self.write_bytes_sync(path, data.encode(encoding), **kwargs)

    def exists_sync(self, path: str | Path, **kwargs: Any) -> bool:
        """Return True if the object exists synchronously."""
        _ = kwargs
        return self._sync_store.exists(self._qualify(path))

    def delete_sync(self, path: str | Path, **kwargs: Any) -> None:
        """Delete an object synchronously."""
        _ = kwargs
        self._sync_store.delete(self._qualify(path))

    def copy_sync(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Copy an object within the store synchronously."""
        _ = kwargs
        self._sync_store.copy(self._qualify(source), self._qualify(destination))

    def move_sync(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Move an object within the store synchronously."""
        _ = kwargs
        self._sync_store.rename(self._qualify(source), self._qualify(destination))

    def list_objects_sync(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List relative object keys matching the optional prefix synchronously."""
        _ = kwargs
        qualified_prefix = self._qualify(prefix) if prefix else self._base_path
        items = self._sync_store.list(qualified_prefix)
        rel_paths = [self._unqualify(item.path) for item in items]
        return self._filter_recursive(rel_paths, prefix=prefix, recursive=recursive)

    def glob_sync(self, pattern: str, **kwargs: Any) -> list[str]:
        """Match relative object keys against a shell-style or URI-qualified glob pattern synchronously.

        The pattern is qualified with `base_path` and matched against full keys by `SyncObjectStore.glob`;
        returned keys are relative to `base_path`. An empty pattern matches nothing.
        """
        _ = kwargs
        if not pattern.strip():
            return []
        return [self._unqualify(meta.path) for meta in self._sync_store.glob(self._qualify(pattern))]

    def is_object_sync(self, path: str | Path) -> bool:
        """Return True when `path` points to a stored object."""
        return self.exists_sync(path)

    def is_path_sync(self, path: str | Path) -> bool:
        """Return True when `path` acts as a non-empty key prefix."""
        prefix_str = (path.as_posix() if isinstance(path, Path) else str(path)).strip("/")
        if not prefix_str:
            return bool(self.list_objects_sync("", recursive=True))
        probe = f"{prefix_str}/"
        return any(item.startswith(probe) for item in self.list_objects_sync(prefix_str, recursive=True))

    def get_metadata_sync(self, path: str | Path, **kwargs: Any) -> dict[str, object]:
        """Return metadata dictionary for a stored object synchronously."""
        _ = kwargs
        meta: ObjectMeta = self._sync_store.head(self._qualify(path))
        return {
            "path": self._unqualify(meta.path),
            "size": meta.size,
            "last_modified": meta.last_modified,
            "etag": meta.etag,
            "version": meta.version,
        }

    def read_arrow_sync(self, path: str | Path, **kwargs: Any) -> ArrowTable:
        """Read a Parquet or Arrow IPC object into a `pyarrow.Table` synchronously."""
        file_format = str(kwargs.pop("file_format", "") or self._infer_arrow_format(path))
        return read_table_sync(self._sync_store, self._qualify(path), file_format=file_format, options=kwargs)

    def write_arrow_sync(self, path: str | Path, table: ArrowTable, **kwargs: Any) -> None:
        """Write a Table through bounded native multipart codec output."""
        qualified, file_format, integrity, codec_options = self._prepare_arrow_write(path, kwargs)
        write_batches_sync(
            self._sync_store, qualified, table, file_format=file_format, options=codec_options, **integrity
        )

    def write_arrow_cursor_sync(self, path: str | Path, cursor: ArrowCursor | Any, **kwargs: Any) -> None:
        """Write IPC cursor buffers through a persistent batch codec reader."""
        qualified, file_format, integrity, codec_options = self._prepare_arrow_write(path, kwargs)
        write_batches_sync(
            self._sync_store, qualified, cursor, file_format=file_format, options=codec_options, **integrity
        )

    def stream_arrow_sync(
        self,
        pattern: str,
        *,
        file_format: Literal["parquet"] = "parquet",
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> Iterator[ArrowRecordBatch]:
        """Yield projected batches on demand from each matching native object."""
        for rel_path in self._resolve_pattern_matches_sync(pattern):
            yield from iter_batches_sync(
                self._sync_store,
                self._qualify(rel_path),
                file_format=file_format,
                batch_size=batch_size,
                options=kwargs,
            )

    def stream_read_sync(
        self,
        path: str | Path,
        chunk_size: int | None = None,
        **kwargs: Any,
    ) -> Iterator[bytes]:
        """Stream fixed-size byte chunks from a stored object synchronously."""
        _ = kwargs
        step = chunk_size if chunk_size is not None and chunk_size > 0 else 65_536
        for chunk in self.get_stream_sync(path, chunk_size=step):
            yield bytes(chunk)

    def get_stream_sync(self, path: str | Path, *, chunk_size: int | None = None) -> Iterator[StorageBuffer]:
        """Return native buffer chunks without buffering the object."""
        return self._sync_store.get_stream(self._qualify(path), chunk_size=chunk_size)

    def put_stream_sync(self, path: str | Path, chunks: Iterable[Buffer], **kwargs: Any) -> PutResult:
        """Upload buffer chunks incrementally with native write options."""
        return self._sync_store.put_stream(self._qualify(path), chunks, **self._write_options(kwargs))

    def list_page_sync(
        self, prefix: str = "", *, cursor: str | None = None, limit: int = 1000
    ) -> tuple[list[str], str | None]:
        """List a page of relative keys while preserving the native cursor."""
        items, next_cursor = self._sync_store.list_page(self._qualify(prefix), cursor=cursor, limit=limit)
        return [self._unqualify(item.path) for item in items], next_cursor

    def open_sync(self, path: str | Path, *, size: int | None = None, chunk_size: int = 65536) -> SyncReadableFile:
        """Create a seekable reader, resolving metadata unless its size is supplied."""
        return self._sync_store.open_reader(self._qualify(path), size=size, chunk_size=chunk_size)

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
        meta = self._sync_store.head(key)
        headers: dict[str, str] = {"Content-Length": str(meta.size)}
        if meta.etag:
            headers["ETag"] = meta.etag
        if filename:
            headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        return StreamingStorageResponse(
            store=self._sync_store,
            key=key,
            total_size=meta.size,
            chunk_size=chunk_size,
            media_type=media_type,
            headers=headers,
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
        """Generate presigned GET or PUT URLs synchronously.

        Raises:
            NotImplementedError: When the underlying store has no `SigningConfig`.
            ValueError: When the native signer rejects the expiry or credentials.
        """
        if isinstance(paths, str):
            return self._sign_single(paths, expires_in=expires_in, for_upload=for_upload)
        return [self._sign_single(item, expires_in=expires_in, for_upload=for_upload) for item in paths]

    async def read_bytes_async(self, path: str | Path, **kwargs: Any) -> bytes:
        """Read full object bytes asynchronously."""
        return await self._async_store.get(self._qualify(path), expected_sha256=kwargs.get("expected_sha256"))

    async def write_bytes_async(self, path: str | Path, data: bytes, **kwargs: Any) -> None:
        """Write full object bytes asynchronously."""
        await self._async_store.put(self._qualify(path), data, **self._write_options(kwargs))

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
        return await self._async_store.exists(self._qualify(path))

    async def delete_async(self, path: str | Path, **kwargs: Any) -> None:
        """Delete an object asynchronously."""
        _ = kwargs
        await self._async_store.delete(self._qualify(path))

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List relative object keys matching the optional prefix asynchronously."""
        _ = kwargs
        qualified_prefix = self._qualify(prefix) if prefix else self._base_path
        items = await self._async_store.list(qualified_prefix)
        rel_paths = [self._unqualify(item.path) for item in items]
        return self._filter_recursive(rel_paths, prefix=prefix, recursive=recursive)

    async def copy_async(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Copy an object within the store asynchronously."""
        _ = kwargs
        await self._async_store.copy(self._qualify(source), self._qualify(destination))

    async def move_async(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Move an object within the store asynchronously."""
        _ = kwargs
        await self._async_store.rename(self._qualify(source), self._qualify(destination))

    async def get_metadata_async(self, path: str | Path, **kwargs: Any) -> dict[str, object]:
        """Return metadata dictionary for a stored object asynchronously."""
        _ = kwargs
        meta: ObjectMeta = await self._async_store.head(self._qualify(path))
        return {
            "path": self._unqualify(meta.path),
            "size": meta.size,
            "last_modified": meta.last_modified,
            "etag": meta.etag,
            "version": meta.version,
        }

    async def read_arrow_async(self, path: str | Path, **kwargs: Any) -> ArrowTable:
        """Read a Parquet or Arrow IPC object into a `pyarrow.Table` asynchronously."""
        file_format = str(kwargs.pop("file_format", "") or self._infer_arrow_format(path))
        return await read_table_async(self._async_store, self._qualify(path), file_format=file_format, options=kwargs)

    async def write_arrow_async(self, path: str | Path, table: ArrowTable, **kwargs: Any) -> None:
        """Encode off-loop and upload incrementally using native asynchronous I/O."""
        qualified, file_format, integrity, codec_options = self._prepare_arrow_write(path, kwargs)
        await write_batches_async(
            self._async_store, qualified, table, file_format=file_format, options=codec_options, **integrity
        )

    async def write_arrow_cursor_async(self, path: str | Path, cursor: ArrowCursor | Any, **kwargs: Any) -> None:
        """Write cursor or buffer IPC through the shared off-loop batch encoder."""
        qualified, file_format, integrity, codec_options = self._prepare_arrow_write(path, kwargs)
        await write_batches_async(
            self._async_store, qualified, cursor, file_format=file_format, options=codec_options, **integrity
        )

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

    async def glob_async(self, pattern: str, **kwargs: Any) -> list[str]:
        """Match relative object keys against a shell-style or URI-qualified glob pattern asynchronously.

        The pattern is qualified with `base_path` and matched against full keys by `AsyncObjectStore.glob`;
        returned keys are relative to `base_path`. An empty pattern matches nothing.
        """
        _ = kwargs
        if not pattern.strip():
            return []
        return [self._unqualify(meta.path) for meta in await self._async_store.glob(self._qualify(pattern))]

    def get_stream_async(self, path: str | Path, *, chunk_size: int | None = None) -> AsyncIterator[StorageBuffer]:
        """Return an asynchronous native buffer iterator."""
        return self._async_store.get_stream(self._qualify(path), chunk_size=chunk_size)

    async def put_stream_async(
        self, path: str | Path, chunks: AsyncIterable[Buffer] | Iterable[Buffer], **kwargs: Any
    ) -> PutResult:
        """Upload synchronous or asynchronous chunks incrementally."""
        return await self._async_store.put_stream(self._qualify(path), chunks, **self._write_options(kwargs))

    async def list_page_async(
        self, prefix: str = "", *, cursor: str | None = None, limit: int = 1000
    ) -> tuple[list[str], str | None]:
        """List an asynchronous page of relative keys with a native cursor."""
        items, next_cursor = await self._async_store.list_page(self._qualify(prefix), cursor=cursor, limit=limit)
        return [self._unqualify(item.path) for item in items], next_cursor

    def open_async(self, path: str | Path, *, size: int | None = None, chunk_size: int = 65536) -> AsyncReadableFile:
        """Create a seekable asynchronous reader without performing storage I/O."""
        return self._async_store.open_reader(self._qualify(path), size=size, chunk_size=chunk_size)

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
        """Generate presigned GET or PUT URLs asynchronously.

        Raises:
            NotImplementedError: When the underlying store has no `SigningConfig`.
            ValueError: When the native signer rejects the expiry or credentials.
        """
        if isinstance(paths, str):
            return await self._sign_single_async(paths, expires_in=expires_in, for_upload=for_upload)
        return [await self._sign_single_async(item, expires_in=expires_in, for_upload=for_upload) for item in paths]

    async def _iter_bytes_async(self, key: str, chunk_size: int) -> AsyncIterator[bytes]:
        """Yield byte chunks asynchronously from the native storage stream."""
        async for chunk in self._async_store.get_stream(key, chunk_size=chunk_size):
            yield bytes(chunk)

    async def _iter_arrow_async(
        self,
        pattern: str,
        *,
        file_format: Literal["parquet"] = "parquet",
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> AsyncIterator[ArrowRecordBatch]:
        """Yield Arrow record batches asynchronously."""
        for rel_path in await self._resolve_pattern_matches_async(pattern):
            batches = iter_batches_async(
                self._async_store,
                self._qualify(rel_path),
                file_format=file_format,
                batch_size=batch_size,
                options=kwargs,
            )
            async with contextlib.aclosing(cast("AsyncGenerator[ArrowRecordBatch, None]", batches)):
                async for batch in batches:
                    yield batch

    def _require_signing(self) -> None:
        """Reject signing requests when the underlying store has no `SigningConfig`."""
        if not self.supports_signing:
            msg = f"URL signing not supported by backend ({self.protocol}): no SigningConfig is configured"
            raise NotImplementedError(msg)

    def _sign_single(self, path: str, *, expires_in: int, for_upload: bool) -> str:
        """Sign a single object key using the underlying ObjectStore."""
        qualified = self._qualify(path)
        self._require_signing()
        if for_upload:
            return self._sync_store.presign_put(qualified, expires_in_secs=expires_in)
        return self._sync_store.presign_get(qualified, expires_in_secs=expires_in)

    async def _sign_single_async(self, path: str, *, expires_in: int, for_upload: bool) -> str:
        """Sign a single object key asynchronously using the underlying AsyncObjectStore."""
        qualified = self._qualify(path)
        self._require_signing()
        if for_upload:
            return await self._async_store.presign_put(qualified, expires_in_secs=expires_in)
        return await self._async_store.presign_get(qualified, expires_in_secs=expires_in)

    def _strip_matching_uri_prefix(self, raw_path_or_pattern: str) -> str:
        """Strip a scheme/bucket prefix when `raw_path_or_pattern` is a full URI matching `self.store_uri`."""
        self._reject_traversal(raw_path_or_pattern)
        stripped = raw_path_or_pattern.strip()
        if "://" not in stripped:
            return stripped
        parsed_in = urlparse(stripped)
        parsed_store = urlparse(self.store_uri)
        in_proto = self._normalize_protocol(parsed_in.scheme)
        store_proto = self._normalize_protocol(parsed_store.scheme or self.protocol)
        if in_proto != store_proto:
            msg = f"URI scheme {parsed_in.scheme!r} does not match store URI {self.store_uri!r}"
            raise ValueError(msg)
        if in_proto == "file":
            return parsed_in.netloc + parsed_in.path if parsed_in.netloc else parsed_in.path
        if in_proto == "memory":
            if parsed_store.netloc and parsed_in.netloc and parsed_in.netloc != parsed_store.netloc:
                msg = f"Memory store authority {parsed_in.netloc!r} does not match {self.store_uri!r}"
                raise ValueError(msg)
            return parsed_in.path.lstrip("/")
        if parsed_in.netloc != parsed_store.netloc:
            msg = f"URI bucket {parsed_in.netloc!r} does not match store URI {self.store_uri!r}"
            raise ValueError(msg)
        rel_path = parsed_in.path.lstrip("/")
        store_prefix = parsed_store.path.strip("/")
        if store_prefix and rel_path.startswith(f"{store_prefix}/"):
            rel_path = rel_path[len(store_prefix) + 1 :]
        elif store_prefix and rel_path == store_prefix:
            rel_path = ""
        return rel_path

    def _resolve_pattern_matches_sync(self, pattern: str) -> list[str]:
        """Resolve either a glob wildcard pattern or an exact object key."""
        normalized = self._strip_matching_uri_prefix(pattern)
        if any(ch in normalized for ch in ("*", "?", "[")):
            return self.glob_sync(normalized)
        return [normalized] if self.exists_sync(normalized) else []

    async def _resolve_pattern_matches_async(self, pattern: str) -> list[str]:
        """Resolve URI-aware patterns through native asynchronous listing and metadata."""
        normalized = self._strip_matching_uri_prefix(pattern)
        if any(ch in normalized for ch in ("*", "?", "[")):
            return await self.glob_async(normalized)
        return [normalized] if await self.exists_async(normalized) else []

    def _qualify(self, path: str | Path) -> str:
        """Normalize a relative, URI-qualified, or local-root-qualified path into an ObjectStore key."""
        raw_str = path.as_posix() if isinstance(path, Path) else str(path)
        raw_str = self._strip_matching_uri_prefix(raw_str)
        if self.protocol == "file":
            parsed = urlparse(self.store_uri)
            local_root = Path(parsed.netloc + parsed.path if parsed.netloc else parsed.path).resolve()
            candidate = Path(raw_str)
            if candidate.is_absolute():
                resolved_candidate = candidate.resolve()
                try:
                    rel_to_root = resolved_candidate.relative_to(local_root)
                except ValueError as exc:
                    msg = f"Path traversal outside storage root rejected: {raw_str}"
                    raise ValueError(msg) from exc
                return rel_to_root.as_posix()

        cleaned = raw_str.strip().lstrip("/")
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
    def _reject_traversal(path: str) -> None:
        """Reject parent segments before resolving a key or configured namespace."""
        if ".." in path.split("/"):
            msg = f"Path traversal rejected: {path}"
            raise ValueError(msg)

    @staticmethod
    def _write_options(kwargs: dict[str, Any]) -> dict[str, Any]:
        """Select supported native conditional, metadata, and integrity write options."""
        return {
            key: kwargs[key]
            for key in (
                "content_type",
                "attributes",
                "if_not_exists",
                "if_match",
                "if_none_match",
                "checksum",
                "expected_sha256",
                "expected_crc32c",
                "expected_md5",
            )
            if key in kwargs
        }

    @staticmethod
    def _filter_recursive(paths: list[str], *, prefix: str, recursive: bool) -> list[str]:
        """Filter listed relative paths when `recursive=False`."""
        if recursive:
            return paths
        norm_prefix = prefix.strip("/")
        base_prefix = f"{norm_prefix}/" if norm_prefix else ""
        filtered: list[str] = []
        for item in paths:
            if base_prefix and not item.startswith(base_prefix):
                continue
            remainder = item[len(base_prefix) :] if base_prefix else item
            if "/" not in remainder:
                filtered.append(item)
        return filtered

    def _prepare_arrow_write(
        self, path: str | Path, kwargs: dict[str, Any]
    ) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
        """Resolve key, format, integrity arguments, and normalized Arrow codec options."""
        raw_format = kwargs.pop("file_format", None) or kwargs.pop("format_hint", None)
        file_format = self._normalize_arrow_format(raw_format, path)
        self._normalize_arrow_write_options(file_format, kwargs)
        integrity: dict[str, Any] = {
            key: kwargs.pop(key)
            for key in ("checksum", "expected_sha256", "expected_crc32c", "expected_md5")
            if key in kwargs
        }
        return self._qualify(path), file_format, integrity, kwargs

    @staticmethod
    def _normalize_arrow_write_options(file_format: str, options: dict[str, Any]) -> None:
        """Normalize SQLSpec Arrow options while preserving PyArrow default settings."""
        options.pop("format_hint", None)
        extra_options = options.pop("write_options", None)
        if isinstance(extra_options, Mapping):
            for key, value in extra_options.items():
                options.setdefault(key, value)
        if "compression" in options and options["compression"] is None:
            options.pop("compression", None)
        if file_format == "parquet" and options.get("row_group_size") in (None, -1):
            options["row_group_size"] = 1024 * 1024

    @classmethod
    def _normalize_arrow_format(cls, raw_format: Any, path: str | Path) -> str:
        """Normalize Arrow format hints to Vertebra's canonical Arrow writer format names."""
        if not raw_format:
            return cls._infer_arrow_format(path)
        lowered = str(raw_format).strip().lower()
        if lowered in {"arrow", "ipc", "arrow-ipc", "feather"}:
            return "arrow-ipc"
        return str(raw_format)

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
        cast("type[ObjectStoreProtocol]", ObjectStoreBackend),
        stored_uri,
        backend_config,
    )
    target_registry.clear_cache(alias)
