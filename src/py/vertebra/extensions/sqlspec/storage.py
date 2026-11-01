"""SQLSpec ObjectStoreProtocol backend, Arrow Flight codec, and DuckDB GCS ADC helpers."""

from __future__ import annotations

import asyncio
import fnmatch
import re
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast, overload
from urllib.parse import urlparse

import msgspec

from vertebra.storage import GCSConfig, ObjectMeta, ObjectStore, SigningConfig
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_msgpack, encode_msgpack_bytes

if TYPE_CHECKING:
    from sqlspec.protocols import ObjectStoreProtocol
    from sqlspec.storage.registry import StorageRegistry
    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = (
    "ArrowFlightSerializer",
    "ObjectStoreBackend",
    "StreamingStorageResponse",
    "build_duckdb_gcs_secret_sql",
    "register_storage_alias",
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


class StreamingStorageResponse(msgspec.Struct, frozen=True, gc=False):
    """Zero-copy chunked HTTP response descriptor backed by ObjectStore."""

    store: ObjectStore
    key: str
    total_size: int
    chunk_size: int = 65536
    media_type: str = "application/octet-stream"
    status_code: int = 200
    headers: dict[str, str] = msgspec.field(default_factory=dict)

    def iter_chunks(self) -> Iterator[memoryview]:
        """Yield fixed-size zero-copy memoryview slices over the stored object."""
        view = self.store.get_view(self.key)
        for offset in range(0, len(view), self.chunk_size):
            yield view[offset : offset + self.chunk_size]

    async def aiter_chunks(self) -> AsyncIterator[memoryview]:
        """Asynchronously yield fixed-size zero-copy memoryview slices over the stored object."""
        view = await self.store.get_view_async(self.key)
        for offset in range(0, len(view), self.chunk_size):
            yield view[offset : offset + self.chunk_size]


class ObjectStoreBackend:
    """SQLSpec ObjectStoreProtocol implementation backed by Vertebra's native ObjectStore."""

    backend_type: ClassVar[str] = "vertebra"
    __slots__ = ("_base_path", "_store", "protocol", "store_uri")

    def __init__(
        self,
        uri: str | ObjectStore = "memory://",
        *,
        store: ObjectStore | None = None,
        base_path: str = "",
        prefix: str = "",
        options: dict[str, str] | None = None,
        signing: SigningConfig | None = None,
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
            **kwargs: Additional scalar backend options merged into `options`.
        """
        merged_options: dict[str, str] = dict(options or {})
        for key, value in kwargs.items():
            if isinstance(value, (str, int, float, bool)):
                merged_options.setdefault(str(key), str(value))

        if store is not None:
            resolved_store = store
        elif isinstance(uri, ObjectStore):
            resolved_store = uri
        else:
            resolved_store = ObjectStore(uri, options=merged_options or None, signing=signing)

        effective_prefix = (base_path or prefix).strip("/")
        self._store = resolved_store
        self._base_path = effective_prefix
        self.store_uri = resolved_store.url
        self.protocol = self._normalize_protocol(resolved_store.scheme)

    @property
    def store(self) -> ObjectStore:
        """Return the underlying ObjectStore instance."""
        return self._store

    @property
    def base_path(self) -> str:
        """Return the configured base path prefix."""
        return self._base_path

    @property
    def supports_signing(self) -> bool:
        """Return True when presigned URL generation is configured on the underlying store."""
        return self._store.supports_signing

    @classmethod
    def register_alias(
        cls,
        alias: str,
        uri_or_store: str | ObjectStore,
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
        if isinstance(uri_or_store, ObjectStore):
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
        _ = kwargs
        return self._store.get(self._qualify(path))

    def write_bytes_sync(self, path: str | Path, data: bytes, **kwargs: Any) -> None:
        """Write full object bytes synchronously."""
        _ = kwargs
        self._store.put(self._qualify(path), data)

    def read_text_sync(self, path: str | Path, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read and decode UTF-8 text from an object synchronously."""
        return self.read_bytes_sync(path, **kwargs).decode(encoding)

    def write_text_sync(self, path: str | Path, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Encode and write text to an object synchronously."""
        self.write_bytes_sync(path, data.encode(encoding), **kwargs)

    def exists_sync(self, path: str | Path, **kwargs: Any) -> bool:
        """Return True if the object exists synchronously."""
        _ = kwargs
        return self._store.exists(self._qualify(path))

    def delete_sync(self, path: str | Path, **kwargs: Any) -> None:
        """Delete an object synchronously."""
        _ = kwargs
        self._store.delete(self._qualify(path))

    def copy_sync(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Copy an object within the store synchronously."""
        _ = kwargs
        self._store.copy(self._qualify(source), self._qualify(destination))

    def move_sync(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Move an object within the store synchronously."""
        _ = kwargs
        src_key = self._qualify(source)
        dst_key = self._qualify(destination)
        self._store.copy(src_key, dst_key)
        self._store.delete(src_key)

    def list_objects_sync(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List relative object keys matching the optional prefix synchronously."""
        _ = kwargs
        qualified_prefix = self._qualify(prefix) if prefix else self._base_path
        items = self._store.list(qualified_prefix)
        rel_paths = [self._unqualify(item.path) for item in items]
        return self._filter_recursive(rel_paths, prefix=prefix, recursive=recursive)

    def glob_sync(self, pattern: str, **kwargs: Any) -> list[str]:
        """Match relative object keys against a shell-style glob pattern synchronously."""
        _ = kwargs
        cleaned_pattern = pattern.strip().lstrip("/")
        candidates = self.list_objects_sync("", recursive=True)
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
        prefix_str = (path.as_posix() if isinstance(path, Path) else str(path)).strip("/")
        if not prefix_str:
            return bool(self.list_objects_sync("", recursive=True))
        probe = f"{prefix_str}/"
        return any(item.startswith(probe) for item in self.list_objects_sync(prefix_str, recursive=True))

    def get_metadata_sync(self, path: str | Path, **kwargs: Any) -> dict[str, object]:
        """Return metadata dictionary for a stored object synchronously."""
        _ = kwargs
        meta: ObjectMeta = self._store.head(self._qualify(path))
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
            with self._store.open_readable_file(key) as reader:
                return pq.read_table(reader, **kwargs)
        ipc = ensure_module("pyarrow.ipc", package="pyarrow", extra="arrow")
        view = self._store.get_view(key)
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
        self._store.put(self._qualify(path), payload)

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
                with self._store.open_readable_file(key) as reader:
                    parquet_file = pq.ParquetFile(reader)
                    yield from parquet_file.iter_batches(batch_size=batch_size, **kwargs)
            else:
                ipc = ensure_module("pyarrow.ipc", package="pyarrow", extra="arrow")
                buf = pa.py_buffer(self._store.get_view(key))
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
        with self._store.open_readable_file(key) as reader:
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
        meta = self._store.head(key)
        headers: dict[str, str] = {"Content-Length": str(meta.size)}
        if meta.etag:
            headers["ETag"] = meta.etag
        if filename:
            headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        return StreamingStorageResponse(
            store=self._store,
            key=key,
            total_size=meta.size,
            chunk_size=chunk_size,
            media_type=media_type,
            headers=headers,
        )

    async def read_bytes_async(self, path: str | Path, **kwargs: Any) -> bytes:
        """Read full object bytes asynchronously."""
        _ = kwargs
        return await self._store.get_async(self._qualify(path))

    async def write_bytes_async(self, path: str | Path, data: bytes, **kwargs: Any) -> None:
        """Write full object bytes asynchronously."""
        _ = kwargs
        await self._store.put_async(self._qualify(path), data)

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
        return await self._store.exists_async(self._qualify(path))

    async def delete_async(self, path: str | Path, **kwargs: Any) -> None:
        """Delete an object asynchronously."""
        _ = kwargs
        await self._store.delete_async(self._qualify(path))

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List relative object keys matching the optional prefix asynchronously."""
        _ = kwargs
        qualified_prefix = self._qualify(prefix) if prefix else self._base_path
        items = await self._store.list_async(qualified_prefix)
        rel_paths = [self._unqualify(item.path) for item in items]
        return self._filter_recursive(rel_paths, prefix=prefix, recursive=recursive)

    async def copy_async(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Copy an object within the store asynchronously."""
        _ = kwargs
        await self._store.copy_async(self._qualify(source), self._qualify(destination))

    async def move_async(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Move an object within the store asynchronously."""
        _ = kwargs
        src_key = self._qualify(source)
        dst_key = self._qualify(destination)
        await self._store.copy_async(src_key, dst_key)
        await self._store.delete_async(src_key)

    async def get_metadata_async(self, path: str | Path, **kwargs: Any) -> dict[str, object]:
        """Return metadata dictionary for a stored object asynchronously."""
        _ = kwargs
        meta: ObjectMeta = await self._store.head_async(self._qualify(path))
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
        await self._store.put_async(self._qualify(path), payload)

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
        return await asyncio.to_thread(self.sign_sync, paths, expires_in, for_upload)

    async def _iter_bytes_async(self, key: str, chunk_size: int) -> AsyncIterator[bytes]:
        """Yield byte slices asynchronously using range reads against the underlying store."""
        meta = await self._store.head_async(key)
        offset = 0
        while offset < meta.size:
            length = min(chunk_size, meta.size - offset)
            chunk = await self._store.get_range_async(key, offset, length)
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
        try:
            if for_upload:
                return self._store.presign_put(qualified, expires_in_secs=expires_in)
            return self._store.presign_get(qualified, expires_in_secs=expires_in)
        except (RuntimeError, ValueError) as exc:
            msg = f"URL signing not supported by backend ({self.protocol}): {exc}"
            raise NotImplementedError(msg) from exc

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

    def _resolve_pattern_matches_sync(self, pattern: str) -> list[str]:
        """Resolve either a glob wildcard pattern or an exact object key."""
        if any(ch in pattern for ch in ("*", "?", "[")):
            return self.glob_sync(pattern)
        return [pattern] if self.exists_sync(pattern) else []

    def _qualify(self, path: str | Path) -> str:
        """Normalize a relative or local-root-qualified path into an ObjectStore key."""
        raw_str = path.as_posix() if isinstance(path, Path) else str(path)
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
    uri_or_store: str | ObjectStore,
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
    if not _IDENTIFIER_RE.match(secret_name):
        msg = f"Invalid SQL identifier for DuckDB secret name: {secret_name!r}"
        raise ValueError(msg)
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
