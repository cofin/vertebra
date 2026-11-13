"""Advanced Alchemy StorageBackend implementation backed by SyncObjectStore and AsyncObjectStore."""

from __future__ import annotations

import builtins
import datetime as dt
import inspect
import os
from collections.abc import AsyncIterable, AsyncIterator, Buffer, Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from vertebra.storage import (
    AsyncGcpTokenCache,
    AsyncObjectStore,
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
    SyncGcpTokenCache,
    SyncObjectStore,
)
from vertebra.storage._arrow import normalize_arrow_format, write_batches_async, write_batches_sync
from vertebra.typing import ImproperConfigurationError, MissingDependencyError

try:
    from advanced_alchemy.types.file_object import StorageBackend as AAStorageBackend
    from advanced_alchemy.types.file_object import storages
except ImportError as exc:
    raise MissingDependencyError(
        "advanced-alchemy",
        install_package="advanced-alchemy",
        extra="alchemy",
    ) from exc

if TYPE_CHECKING:
    from advanced_alchemy.types.file_object.base import AsyncDataLike, DataLike, PathLike
    from advanced_alchemy.types.file_object.file import FileObject
    from advanced_alchemy.types.file_object.registry import StorageRegistry

__all__ = (
    "ObjectStoreBackend",
    "register_storage_backend",
)

_STORAGE_CONFIGS = (LocalConfig, S3Config, GCSConfig, AzureConfig, OCIConfig, R2Config, HTTPConfig, MemoryConfig)


class ObjectStoreBackend(AAStorageBackend):
    """Advanced Alchemy StorageBackend delegating zero-copy and async I/O to ObjectStore instances."""

    driver = "vertebra"

    def __init__(
        self,
        key: str = "default",
        fs: SyncObjectStore | AsyncObjectStore | str | StorageConfig | None = None,
        *,
        store: SyncObjectStore | AsyncObjectStore | str | StorageConfig | None = None,
        prefix: str = "",
        signing: SigningConfig | None = None,
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the storage backend for Advanced Alchemy FileObject and StoredObject columns.

        Args:
            key: Logical storage backend key registered in Advanced Alchemy's `storages` registry.
            fs: Either an `ObjectStore` instance, a `StorageConfig`, or a storage URI string (`memory://`, `file://`, `s3://`, `gs://`, `az://`, `oci://`, `r2://`).
            store: Explicit `ObjectStore`, `StorageConfig`, or URI keyword taking precedence over `fs`.
            prefix: Optional key namespace prefix prepended to all stored object paths.
            signing: Optional `SigningConfig` for presigned GET/PUT URLs when building a store from a config or URI.
            token_cache: Optional shared GCS OAuth2 token cache when building a store from a config or URI.
            **kwargs: Optional scalar string options applied when the target is a URI string.

        Raises:
            ImproperConfigurationError: When no target is supplied, or `signing` / `token_cache` accompany a
                prebuilt object store whose authentication is already fixed.
            TypeError: When the target is not an object store, storage config, or URI string.
        """
        string_options: dict[str, str] = {
            str(k): str(v) for k, v in kwargs.items() if isinstance(v, (str, int, float, bool))
        }
        target = store if store is not None else fs
        self._prefix = self._clean_key(prefix).strip("/")
        if target is None or isinstance(target, str) and not target.strip():
            msg = "An explicit object store, storage config, or URI is required"
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
            sync_store = SyncObjectStore(target, signing=signing, token_cache=token_cache)
            async_store = sync_store.to_async()
        elif isinstance(target, str):
            sync_store = SyncObjectStore(
                target, options=string_options or None, signing=signing, token_cache=token_cache
            )
            async_store = sync_store.to_async()
        else:
            msg = f"Expected SyncObjectStore, AsyncObjectStore, StorageConfig, or URI string, got {type(target)!r}"
            raise TypeError(msg)

        super().__init__(key=key, fs=sync_store, **kwargs)
        self._sync_store = sync_store
        self._async_store = async_store
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

    @property
    def supports_signing(self) -> bool:
        """Return True when presigned URL generation is configured on the underlying store."""
        return self._sync_store.supports_signing

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
        checksum: bool = False,
        expected_sha256: str | None = None,
        expected_crc32c: str | None = None,
        expected_md5: str | None = None,
    ) -> FileObject:
        """Persist a FileObject payload synchronously and populate size, etag, and last_modified."""
        _ = max_concurrency
        if chunk_size < 1:
            msg = "chunk_size must be >= 1"
            raise ValueError(msg)
        qualified = self._qualify(file_object.path)
        if isinstance(data, Path):
            put_res = self._sync_store.put_file(
                qualified,
                data,
                checksum=checksum,
                expected_sha256=expected_sha256,
                expected_crc32c=expected_crc32c,
                expected_md5=expected_md5,
            )
        elif isinstance(data, Buffer) and not use_multipart:
            put_res = self._sync_store.put(
                qualified,
                data,
                checksum=checksum,
                expected_sha256=expected_sha256,
                expected_crc32c=expected_crc32c,
                expected_md5=expected_md5,
            )
        else:
            put_res = self._sync_store.put_stream(
                qualified,
                self._iter_sync_data(data, chunk_size),
                checksum=checksum,
                expected_sha256=expected_sha256,
                expected_crc32c=expected_crc32c,
                expected_md5=expected_md5,
            )
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
        checksum: bool = False,
        expected_sha256: str | None = None,
        expected_crc32c: str | None = None,
        expected_md5: str | None = None,
    ) -> FileObject:
        """Persist a FileObject payload asynchronously and populate size, etag, and last_modified."""
        _ = max_concurrency
        if chunk_size < 1:
            msg = "chunk_size must be >= 1"
            raise ValueError(msg)
        qualified = self._qualify(file_object.path)
        if isinstance(data, Path):
            put_res = await self._async_store.put_file(
                qualified,
                data,
                checksum=checksum,
                expected_sha256=expected_sha256,
                expected_crc32c=expected_crc32c,
                expected_md5=expected_md5,
            )
        elif isinstance(data, Buffer) and not use_multipart:
            put_res = await self._async_store.put(
                qualified,
                data,
                checksum=checksum,
                expected_sha256=expected_sha256,
                expected_crc32c=expected_crc32c,
                expected_md5=expected_md5,
            )
        else:
            put_res = await self._async_store.put_stream(
                qualified,
                self._iter_async_data(data, chunk_size),
                checksum=checksum,
                expected_sha256=expected_sha256,
                expected_crc32c=expected_crc32c,
                expected_md5=expected_md5,
            )
        self._apply_put_metadata(file_object, put_res.size, put_res.etag)
        return file_object

    def save_arrow(
        self,
        path: PathLike,
        data: Any,
        *,
        format: str | None = None,
        file_format: str | None = None,
        batch_size: int = 65536,
        schema: Any = None,
        options: Mapping[str, Any] | None = None,
        checksum: bool = False,
        expected_sha256: str | None = None,
        expected_crc32c: str | None = None,
        expected_md5: str | None = None,
        **codec_options: Any,
    ) -> PutResult:
        """Stream an Arrow Table, RecordBatch, or batch iterable directly to storage synchronously."""
        qualified, resolved_format, merged_options = self._prepare_arrow_write(
            path,
            file_format=file_format,
            format=format,
            options=options,
            codec_options=codec_options,
        )
        return write_batches_sync(
            self._sync_store,
            qualified,
            data,
            file_format=resolved_format,
            batch_size=batch_size,
            schema=schema,
            options=merged_options,
            checksum=checksum,
            expected_sha256=expected_sha256,
            expected_crc32c=expected_crc32c,
            expected_md5=expected_md5,
        )

    async def save_arrow_async(
        self,
        path: PathLike,
        data: Any,
        *,
        format: str | None = None,
        file_format: str | None = None,
        batch_size: int = 65536,
        schema: Any = None,
        options: Mapping[str, Any] | None = None,
        checksum: bool = False,
        expected_sha256: str | None = None,
        expected_crc32c: str | None = None,
        expected_md5: str | None = None,
        **codec_options: Any,
    ) -> PutResult:
        """Stream an Arrow Table, RecordBatch, or batch iterable directly to storage asynchronously."""
        qualified, resolved_format, merged_options = self._prepare_arrow_write(
            path,
            file_format=file_format,
            format=format,
            options=options,
            codec_options=codec_options,
        )
        return await write_batches_async(
            self._async_store,
            qualified,
            data,
            file_format=resolved_format,
            batch_size=batch_size,
            schema=schema,
            options=merged_options,
            checksum=checksum,
            expected_sha256=expected_sha256,
            expected_crc32c=expected_crc32c,
            expected_md5=expected_md5,
        )

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
        """Generate presigned GET or PUT URLs for one or more object paths.

        Raises:
            NotImplementedError: When the underlying store has no `SigningConfig`.
            ValueError: When the native signer rejects the expiry or credentials.
        """
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
        """Generate presigned GET or PUT URLs asynchronously.

        Raises:
            NotImplementedError: When the underlying store has no `SigningConfig`.
            ValueError: When the native signer rejects the expiry or credentials.
        """
        ttl = 3600 if expires_in is None else expires_in
        if isinstance(paths, (str, Path, os.PathLike)):
            return await self._sign_one_async(paths, expires_in=ttl, for_upload=for_upload)
        return [await self._sign_one_async(item, expires_in=ttl, for_upload=for_upload) for item in paths]

    def _prepare_arrow_write(
        self,
        path: PathLike,
        *,
        file_format: str | None,
        format: str | None,
        options: Mapping[str, Any] | None,
        codec_options: dict[str, Any],
    ) -> tuple[str, str, dict[str, Any]]:
        """Qualify an Arrow object key, resolve its target format, and merge codec options."""
        qualified = self._qualify(path)
        raw_format = file_format if file_format is not None else format
        resolved_format = normalize_arrow_format(raw_format, qualified)
        merged = {**(options or {}), **codec_options}
        return qualified, resolved_format, merged

    def _require_signing(self) -> None:
        """Reject signing requests when the underlying store has no `SigningConfig`."""
        if not self.supports_signing:
            msg = f"URL signing is not configured for backend {self.key!r}: no SigningConfig is attached"
            raise NotImplementedError(msg)

    def _sign_one(self, path: PathLike, *, expires_in: int, for_upload: bool) -> str:
        """Sign a single path using the underlying SyncObjectStore presigner."""
        qualified = self._qualify(path)
        self._require_signing()
        if for_upload:
            return self._sync_store.presign_put(qualified, expires_in_secs=expires_in)
        return self._sync_store.presign_get(qualified, expires_in_secs=expires_in)

    async def _sign_one_async(self, path: PathLike, *, expires_in: int, for_upload: bool) -> str:
        """Sign a single path using the underlying AsyncObjectStore presigner."""
        qualified = self._qualify(path)
        self._require_signing()
        if for_upload:
            res = self._async_store.presign_put(qualified, expires_in_secs=expires_in)
        else:
            res = self._async_store.presign_get(qualified, expires_in_secs=expires_in)
        return await res if inspect.isawaitable(res) else res

    def _qualify(self, path: PathLike) -> str:
        """Normalize a PathLike key and prepend any configured namespace prefix."""
        cleaned = self._clean_key(path)
        if not self._prefix:
            return cleaned
        if cleaned == self._prefix or cleaned.startswith(f"{self._prefix}/"):
            return cleaned
        return f"{self._prefix}/{cleaned}" if cleaned else self._prefix

    @staticmethod
    def _clean_key(path: PathLike) -> str:
        """Reject parent traversal before a key reaches native storage."""
        cleaned = os.fspath(path).replace("\\", "/").strip().lstrip("/")
        if ".." in cleaned.split("/"):
            msg = f"Parent traversal is not permitted in storage keys: {path}"
            raise ValueError(msg)
        return cleaned

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

    @classmethod
    def _iter_sync_data(cls, data: DataLike, chunk_size: int) -> Iterator[Buffer]:
        """Yield buffers or bounded file reads without collecting the source."""
        if isinstance(data, Buffer):
            view = memoryview(data).cast("B")
            for start in range(0, len(view), chunk_size):
                yield view[start : start + chunk_size]
        elif (reader := cls._get_reader(data)) is not None:
            while chunk := reader(chunk_size):
                yield chunk
        else:
            yield from cast("Iterable[Buffer]", data)

    @classmethod
    async def _iter_async_data(cls, data: AsyncDataLike, chunk_size: int) -> AsyncIterator[Buffer]:
        """Yield async chunks and bounded reads, retaining legacy one-read objects."""
        if isinstance(data, Buffer):
            for chunk in cls._iter_sync_data(data, chunk_size):
                yield chunk
        elif (reader := cls._get_reader(data)) is not None:
            bounded = True
            try:
                inspect.signature(reader).bind(chunk_size)
            except TypeError:
                bounded = False
            except ValueError:
                pass
            while True:
                result = reader(chunk_size) if bounded else reader()
                chunk = await result if inspect.isawaitable(result) else result
                if not chunk:
                    break
                yield chunk
                if not bounded:
                    break
        elif isinstance(data, AsyncIterable):
            async for chunk in data:
                yield chunk
        else:
            for chunk in cast("Iterable[Buffer]", data):
                yield chunk

    @staticmethod
    def _get_reader(data: Any) -> Any:
        """Resolve direct and upload-wrapper file readers."""
        if callable(reader := getattr(data, "read", None)):
            return reader
        for attr in ("file", "stream"):
            if callable(reader := getattr(getattr(data, attr, None), "read", None)):
                return reader
        return None


def register_storage_backend(
    store_or_key: SyncObjectStore | AsyncObjectStore | str | StorageConfig,
    *,
    store: SyncObjectStore | AsyncObjectStore | str | StorageConfig | None = None,
    key: str = "default",
    prefix: str = "",
    set_default: bool = False,
    registry: StorageRegistry | None = None,
    signing: SigningConfig | None = None,
    token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
) -> ObjectStoreBackend:
    """Register an ObjectStoreBackend into Advanced Alchemy's `storages` registry.

    Args:
        store_or_key: Either a configured ObjectStore, storage URI string, or backend key name.
        store: Optional explicit ObjectStore or storage URI string when first argument is key name.
        key: Backend key referenced by `FileObject(backend=key)` and `StoredObject(backend=key)`.
        prefix: Optional object key prefix applied to all operations on this backend.
        set_default: When True, configure `ObjectStoreBackend` as the registry's default URI backend class.
        registry: Optional custom StorageRegistry instance (defaults to advanced_alchemy.types.file_object.storages).
        signing: Optional `SigningConfig` forwarded when the store is built from a config or URI.
        token_cache: Optional shared GCS OAuth2 token cache forwarded when the store is built from a config or URI.

    Returns:
        The registered `ObjectStoreBackend` instance.
    """
    target_registry = storages if registry is None else registry
    if store is not None:
        resolved_key = str(store_or_key) if key == "default" else key
        resolved_store = store
    elif isinstance(store_or_key, str) and ("://" not in store_or_key and not store_or_key.startswith("/")):
        resolved_key = store_or_key
        msg = "A backend key requires an explicit object store, storage config, or URI"
        raise ImproperConfigurationError(msg)
    else:
        resolved_key = key
        resolved_store = store_or_key
    backend = ObjectStoreBackend(
        key=resolved_key, fs=resolved_store, prefix=prefix, signing=signing, token_cache=token_cache
    )
    target_registry.register_backend(backend)
    if set_default:
        target_registry.set_default_backend("vertebra.extensions.alchemy.storage.ObjectStoreBackend")
    return backend
