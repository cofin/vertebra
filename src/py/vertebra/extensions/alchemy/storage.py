"""Advanced Alchemy StorageBackend implementation backed by SyncObjectStore and AsyncObjectStore."""

from __future__ import annotations

import builtins
import contextlib
import datetime as dt
import inspect
import os
from collections.abc import Buffer, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from vertebra.storage import (
    AsyncObjectStore,
    AzureConfig,
    GCSConfig,
    HTTPConfig,
    LocalConfig,
    MemoryConfig,
    ObjectMeta,
    OCIConfig,
    R2Config,
    S3Config,
    StorageConfig,
    SyncObjectStore,
)
from vertebra.typing import MissingDependencyError

try:
    from advanced_alchemy.types.file_object import StorageBackend, storages
except ImportError as exc:
    raise MissingDependencyError(
        "advanced-alchemy",
        install_package="advanced-alchemy",
        extra="alchemy",
    ) from exc

_storage_get_attr = "get"
if not hasattr(storages, _storage_get_attr):
    with contextlib.suppress(AttributeError, TypeError):
        setattr(storages, _storage_get_attr, storages.get_backend)

if TYPE_CHECKING:
    from advanced_alchemy.types.file_object.base import AsyncDataLike, DataLike, PathLike
    from advanced_alchemy.types.file_object.file import FileObject
    from advanced_alchemy.types.file_object.registry import StorageRegistry

__all__ = (
    "ObjectStoreBackend",
    "register_storage_backend",
)

_STORAGE_CONFIGS = (LocalConfig, S3Config, GCSConfig, AzureConfig, OCIConfig, R2Config, HTTPConfig, MemoryConfig)


class ObjectStoreBackend(StorageBackend):
    """Advanced Alchemy StorageBackend delegating zero-copy and async I/O to ObjectStore instances."""

    driver = "vertebra"

    def __init__(
        self,
        key: str = "default",
        fs: SyncObjectStore | AsyncObjectStore | str | StorageConfig | None = None,
        *,
        store: SyncObjectStore | AsyncObjectStore | str | StorageConfig | None = None,
        prefix: str = "",
        **kwargs: Any,
    ) -> None:
        """Initialize the storage backend for Advanced Alchemy FileObject and StoredObject columns.

        Args:
            key: Logical storage backend key registered in Advanced Alchemy's `storages` registry.
            fs: Either an `ObjectStore` instance or a storage URI string (`memory://`, `file://`, `s3://`, `gs://`, `az://`, `oci://`, `r2://`).
            store: Explicit `ObjectStore` keyword alias when constructing directly.
            prefix: Optional key namespace prefix prepended to all stored object paths.
            **kwargs: Optional string options or signing configuration when fs is a URI string.
        """
        signing = kwargs.pop("signing", None)
        string_options: dict[str, str] = {
            str(k): str(v) for k, v in kwargs.items() if isinstance(v, (str, int, float, bool))
        }
        target = store if store is not None else fs

        sync_store: SyncObjectStore
        async_store: AsyncObjectStore

        if isinstance(target, SyncObjectStore):
            sync_store = target
            async_store = target.to_async()
        elif isinstance(target, AsyncObjectStore):
            async_store = target
            sync_store = target.to_sync()
        elif isinstance(target, _STORAGE_CONFIGS):
            sync_store = SyncObjectStore.from_config(target)
            async_store = sync_store.to_async()
        elif isinstance(target, str):
            sync_store = SyncObjectStore(target, options=string_options or None, signing=signing)
            async_store = sync_store.to_async()
        elif target is None:
            sync_store = SyncObjectStore("memory://", options=string_options or None, signing=signing)
            async_store = sync_store.to_async()
        else:
            msg = f"Expected SyncObjectStore, AsyncObjectStore, or URI string, got {type(target)!r}"
            raise TypeError(msg)

        super().__init__(key=key, fs=sync_store, **kwargs)
        self._sync_store = sync_store
        self._async_store = async_store
        self._prefix = prefix.strip("/")
        self.protocol = self._normalize_protocol(sync_store.scheme)

    @property
    def store(self) -> SyncObjectStore:
        """Return the underlying SyncObjectStore instance."""
        return self._sync_store

    @property
    def sync_store(self) -> SyncObjectStore:
        """Return the underlying SyncObjectStore instance."""
        return self._sync_store

    @property
    def async_store(self) -> AsyncObjectStore:
        """Return the underlying AsyncObjectStore instance."""
        return self._async_store

    @property
    def prefix(self) -> str:
        """Return the configured object key prefix."""
        return self._prefix

    def get_content(self, path: PathLike, *, options: dict[str, Any] | None = None) -> bytes:
        """Read full object bytes synchronously."""
        _ = options
        return self._sync_store.get(self._qualify(path))

    async def get_content_async(self, path: PathLike, *, options: dict[str, Any] | None = None) -> bytes:
        """Read full object bytes asynchronously."""
        _ = options
        return await self._async_store.get(self._qualify(path))

    def get_range(self, path: PathLike, start: int, end: int) -> bytes:
        """Read a byte range [start, end) synchronously."""
        length = max(0, end - start)
        return self._sync_store.get_range(self._qualify(path), start, length)

    async def get_range_async(self, path: PathLike, start: int, end: int) -> bytes:
        """Read a byte range [start, end) asynchronously."""
        length = max(0, end - start)
        return await self._async_store.get_range(self._qualify(path), start, length)

    def get_metadata(self, path: PathLike) -> ObjectMeta:
        """Read stored object metadata synchronously."""
        return self._sync_store.head(self._qualify(path))

    async def get_metadata_async(self, path: PathLike) -> ObjectMeta:
        """Read stored object metadata asynchronously."""
        return await self._async_store.head(self._qualify(path))

    def exists(self, path: PathLike) -> bool:
        """Return True if the object path exists synchronously."""
        return self._sync_store.exists(self._qualify(path))

    async def exists_async(self, path: PathLike) -> bool:
        """Return True if the object path exists asynchronously."""
        return await self._async_store.exists(self._qualify(path))

    def save_object(
        self,
        file_object: FileObject,
        data: DataLike,
        *,
        use_multipart: bool | None = None,
        chunk_size: int = 5 * 1024 * 1024,
        max_concurrency: int = 12,
    ) -> FileObject:
        """Persist a FileObject payload synchronously and populate size, etag, and last_modified."""
        _ = max_concurrency
        qualified = self._qualify(file_object.path)
        if isinstance(data, Path):
            put_res = self._sync_store.put_file(qualified, data)
        else:
            raw = self._resolve_sync_data(data)
            view = memoryview(raw).cast("B")
            if use_multipart and chunk_size > 0:
                parts = [view[idx : idx + chunk_size] for idx in range(0, len(view), chunk_size)] or [memoryview(b"")]
                put_res = self._sync_store.put_multipart(qualified, parts)
            else:
                put_res = self._sync_store.put(qualified, view)
        self._apply_put_metadata(file_object, put_res.size, put_res.etag)
        return file_object

    async def save_object_async(
        self,
        file_object: FileObject,
        data: AsyncDataLike,
        *,
        use_multipart: bool | None = None,
        chunk_size: int = 5 * 1024 * 1024,
        max_concurrency: int = 12,
    ) -> FileObject:
        """Persist a FileObject payload asynchronously and populate size, etag, and last_modified."""
        _ = max_concurrency
        qualified = self._qualify(file_object.path)
        if isinstance(data, Path):
            put_res = await self._async_store.put_file(qualified, data)
        else:
            raw = await self._resolve_async_data(data)
            view = memoryview(raw).cast("B")
            if use_multipart and chunk_size > 0:
                parts = [view[idx : idx + chunk_size] for idx in range(0, len(view), chunk_size)] or [memoryview(b"")]
                put_res = await self._async_store.put_multipart(qualified, parts)
            else:
                put_res = await self._async_store.put(qualified, view)
        self._apply_put_metadata(file_object, put_res.size, put_res.etag)
        return file_object

    def copy_object(self, src: PathLike, dst: PathLike) -> None:
        """Copy an object synchronously within the storage backend."""
        self._sync_store.copy(self._qualify(src), self._qualify(dst))

    async def copy_object_async(self, src: PathLike, dst: PathLike) -> None:
        """Copy an object asynchronously within the storage backend."""
        await self._async_store.copy(self._qualify(src), self._qualify(dst))

    def list_objects(self, prefix: PathLike = "") -> builtins.list[ObjectMeta]:
        """List stored object metadata under the given prefix synchronously."""
        qualified_prefix = self._qualify(prefix) if prefix else self._prefix
        return self._sync_store.list(qualified_prefix)

    async def list_objects_async(self, prefix: PathLike = "") -> builtins.list[ObjectMeta]:
        """List stored object metadata under the given prefix asynchronously."""
        qualified_prefix = self._qualify(prefix) if prefix else self._prefix
        return await self._async_store.list(qualified_prefix)

    def delete_object(self, paths: PathLike | Sequence[PathLike]) -> None:
        """Delete one or more stored objects synchronously."""
        items: list[PathLike] = [paths] if isinstance(paths, (str, Path, os.PathLike)) else list(paths)
        for item in items:
            self._sync_store.delete(self._qualify(item))

    async def delete_object_async(self, paths: PathLike | Sequence[PathLike]) -> None:
        """Delete one or more stored objects asynchronously."""
        items: list[PathLike] = [paths] if isinstance(paths, (str, Path, os.PathLike)) else list(paths)
        for item in items:
            await self._async_store.delete(self._qualify(item))

    def sign(
        self,
        paths: PathLike | Sequence[PathLike],
        *,
        expires_in: int | None = None,
        for_upload: bool = False,
    ) -> str | list[str]:
        """Generate presigned GET or PUT URLs for one or more object paths."""
        ttl = 3600 if expires_in is None else expires_in
        if isinstance(paths, (str, Path, os.PathLike)):
            return self._sign_one(paths, expires_in=ttl, for_upload=for_upload)
        return [self._sign_one(item, expires_in=ttl, for_upload=for_upload) for item in paths]

    async def sign_async(
        self,
        paths: PathLike | Sequence[PathLike],
        *,
        expires_in: int | None = None,
        for_upload: bool = False,
    ) -> str | list[str]:
        """Generate presigned GET or PUT URLs asynchronously."""
        ttl = 3600 if expires_in is None else expires_in
        if isinstance(paths, (str, Path, os.PathLike)):
            return await self._sign_one_async(paths, expires_in=ttl, for_upload=for_upload)
        return [await self._sign_one_async(item, expires_in=ttl, for_upload=for_upload) for item in paths]

    def _sign_one(self, path: PathLike, *, expires_in: int, for_upload: bool) -> str:
        """Sign a single path using the underlying SyncObjectStore presigner."""
        qualified = self._qualify(path)
        try:
            if for_upload:
                return self._sync_store.presign_put(qualified, expires_in_secs=expires_in)
            return self._sync_store.presign_get(qualified, expires_in_secs=expires_in)
        except (RuntimeError, ValueError) as exc:
            msg = f"URL signing is not configured for backend {self.key!r}: {exc}"
            raise NotImplementedError(msg) from exc

    async def _sign_one_async(self, path: PathLike, *, expires_in: int, for_upload: bool) -> str:
        """Sign a single path using the underlying AsyncObjectStore presigner."""
        qualified = self._qualify(path)
        try:
            if for_upload:
                res = self._async_store.presign_put(qualified, expires_in_secs=expires_in)
            else:
                res = self._async_store.presign_get(qualified, expires_in_secs=expires_in)
            return await res if inspect.isawaitable(res) else res
        except (RuntimeError, ValueError) as exc:
            msg = f"URL signing is not configured for backend {self.key!r}: {exc}"
            raise NotImplementedError(msg) from exc

    def _qualify(self, path: PathLike) -> str:
        """Normalize a PathLike key and prepend any configured namespace prefix."""
        raw_path = Path(path).as_posix() if isinstance(path, (Path, os.PathLike)) else str(path)
        cleaned = raw_path.strip().lstrip("/")
        if not self._prefix:
            return cleaned
        return f"{self._prefix}/{cleaned}" if cleaned else self._prefix

    @staticmethod
    def _normalize_protocol(scheme: str) -> str:
        """Normalize scheme aliases to canonical Advanced Alchemy protocol identifiers."""
        lowered = scheme.lower()
        if lowered == "gs":
            return "gcs"
        if lowered == "az":
            return "azure"
        return lowered

    @staticmethod
    def _apply_put_metadata(file_object: FileObject, size: int, etag: str | None) -> None:
        """Populate size, etag, and UTC last_modified timestamp on a saved FileObject."""
        file_object.size = size
        file_object.last_modified = dt.datetime.now(tz=dt.UTC).timestamp()
        if etag:
            file_object.etag = etag

    @staticmethod
    def _resolve_sync_data(data: DataLike) -> Buffer:
        """Materialize synchronous DataLike inputs into a contiguous Buffer."""
        if isinstance(data, Buffer):
            return data
        if isinstance(data, Path):
            return data.read_bytes()
        if hasattr(data, "read"):
            return bytes(data.read())
        file_attr = getattr(data, "file", None)
        if file_attr is not None and hasattr(file_attr, "read"):
            return bytes(file_attr.read())
        stream_attr = getattr(data, "stream", None)
        if stream_attr is not None and hasattr(stream_attr, "read"):
            return bytes(stream_attr.read())
        return b"".join(bytes(chunk) for chunk in data)

    @staticmethod
    async def _resolve_async_data(data: AsyncDataLike) -> Buffer:
        """Materialize synchronous or asynchronous DataLike inputs into a contiguous Buffer."""
        if isinstance(data, Buffer):
            return data
        if isinstance(data, Path):
            return data.read_bytes()
        if hasattr(data, "read"):
            result = data.read()
            if inspect.isawaitable(result):
                result = await result
            return result if isinstance(result, Buffer) else bytes(result)
        file_attr = getattr(data, "file", None)
        if file_attr is not None and hasattr(file_attr, "read"):
            result = file_attr.read()
            if inspect.isawaitable(result):
                result = await result
            return result if isinstance(result, Buffer) else bytes(result)
        stream_attr = getattr(data, "stream", None)
        if stream_attr is not None and hasattr(stream_attr, "read"):
            result = stream_attr.read()
            if inspect.isawaitable(result):
                result = await result
            return result if isinstance(result, Buffer) else bytes(result)
        if hasattr(data, "__aiter__"):
            chunks: list[bytes] = []
            async for chunk in data:
                chunks.append(bytes(chunk))
            return b"".join(chunks)
        return b"".join(bytes(chunk) for chunk in data)


def register_storage_backend(
    store_or_key: SyncObjectStore | AsyncObjectStore | str,
    *,
    store: SyncObjectStore | AsyncObjectStore | str | None = None,
    key: str = "default",
    prefix: str = "",
    set_default: bool = False,
    registry: StorageRegistry | None = None,
) -> ObjectStoreBackend:
    """Register an ObjectStoreBackend into Advanced Alchemy's `storages` registry.

    Args:
        store_or_key: Either a configured ObjectStore, storage URI string, or backend key name.
        store: Optional explicit ObjectStore or storage URI string when first argument is key name.
        key: Backend key referenced by `FileObject(backend=key)` and `StoredObject(backend=key)`.
        prefix: Optional object key prefix applied to all operations on this backend.
        set_default: When True, configure `ObjectStoreBackend` as the registry's default URI backend class.
        registry: Optional custom StorageRegistry instance (defaults to advanced_alchemy.types.file_object.storages).

    Returns:
        The registered `ObjectStoreBackend` instance.
    """
    target_registry = storages if registry is None else registry
    if set_default:
        target_registry.set_default_backend("vertebra.extensions.alchemy.storage.ObjectStoreBackend")
    if store is not None:
        resolved_key = str(store_or_key) if key == "default" else key
        resolved_store = store
    elif isinstance(store_or_key, str) and ("://" not in store_or_key and not store_or_key.startswith("/")):
        resolved_key = store_or_key
        resolved_store = "memory://"
    else:
        resolved_key = key
        resolved_store = store_or_key
    backend = ObjectStoreBackend(key=resolved_key, fs=resolved_store, prefix=prefix)
    target_registry.register_backend(backend)
    if not hasattr(target_registry, _storage_get_attr):
        with contextlib.suppress(AttributeError, TypeError):
            setattr(target_registry, _storage_get_attr, target_registry.get_backend)
    return backend
