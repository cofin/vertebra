"""Django Storage backend over Vertebra Rust ObjectStore with sync and async methods."""

from __future__ import annotations

import posixpath
from collections.abc import AsyncIterable, Buffer, Iterator
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, BinaryIO, Self, TypedDict, cast, get_args
from urllib.parse import quote, unquote, urlparse

from msgspec.structs import fields

from vertebra.storage import AsyncObjectStore, StorageConfig, SyncObjectStore
from vertebra.typing import ImproperConfigurationError, MissingDependencyError
from vertebra.utils.module_loader import import_optional

_storage_module = import_optional("django.core.files.storage")
_file_module = import_optional("django.core.files.base")
_deconstruct_module = import_optional("django.utils.deconstruct")
_utils_module = import_optional("django.core.files.utils")
_exceptions_module = import_optional("django.core.exceptions")
_serializer_module = import_optional("django.db.migrations.serializer")
_writer_module = import_optional("django.db.migrations.writer")

if TYPE_CHECKING:
    from django.core.files.base import File
    from django.core.files.storage import Storage as _StorageBase
    from django.db.migrations.serializer import BaseSerializer as _SerializerBase

    _ReadableFile = File[bytes]
else:
    _StorageBase = _storage_module.Storage if _storage_module is not None else object
    File = _file_module.File if _file_module is not None else None
    _ReadableFile = File if File is not None else object
    _SerializerBase = _serializer_module.BaseSerializer if _serializer_module is not None else object

_deconstructible = _deconstruct_module.deconstructible if _deconstruct_module is not None else lambda cls: cls
_validate_file_name = _utils_module.validate_file_name if _utils_module is not None else None
_SuspiciousFileOperation = _exceptions_module.SuspiciousFileOperation if _exceptions_module is not None else ValueError

__all__ = (
    "AsyncStorageRegistry",
    "DjangoStorage",
    "SyncStorageRegistry",
)


class SyncStorageRegistry:
    """Named ``SyncObjectStore`` registry for Django applications."""

    __slots__ = ("_stores",)

    def __init__(self, stores: dict[str, SyncObjectStore] | None = None) -> None:
        self._stores: dict[str, SyncObjectStore] = dict(stores or {})

    def register(self, name: str, store: SyncObjectStore) -> None:
        """Register a ``SyncObjectStore`` under ``name``."""
        self._stores[name] = store

    def get(self, name: str = "default") -> SyncObjectStore:
        """Return the ``SyncObjectStore`` registered under ``name``."""
        return self._stores[name]


class AsyncStorageRegistry:
    """Named ``AsyncObjectStore`` registry for Django applications."""

    __slots__ = ("_stores",)

    def __init__(self, stores: dict[str, AsyncObjectStore] | None = None) -> None:
        self._stores: dict[str, AsyncObjectStore] = dict(stores or {})

    def register(self, name: str, store: AsyncObjectStore) -> None:
        """Register an ``AsyncObjectStore`` under ``name``."""
        self._stores[name] = store

    def get(self, name: str = "default") -> AsyncObjectStore:
        """Return the ``AsyncObjectStore`` registered under ``name``."""
        return self._stores[name]


class _DigestOptions(TypedDict, total=False):
    """Typed integrity kwargs forwarded to object store upload methods."""

    expected_sha256: str
    expected_crc32c: str
    expected_md5: str


@_deconstructible
class DjangoStorage(_StorageBase):
    """Deconstructible Django storage backed by paired native object stores."""

    def __init__(
        self,
        config: StorageConfig | str | SyncObjectStore | AsyncObjectStore | None = None,
        *,
        store: SyncObjectStore | AsyncObjectStore | None = None,
        async_store: AsyncObjectStore | None = None,
        base_url: str | None = None,
        location: str = "",
        file_overwrite: bool = False,
        expires_in: int = 3600,
    ) -> None:
        if _storage_module is None:
            raise MissingDependencyError("django", install_package="django", extra="django")
        target = store if store is not None else async_store if async_store is not None else config
        if target is None or isinstance(target, str) and not target.strip():
            msg = "Django storage requires an explicit store, configuration, or URI"
            raise ImproperConfigurationError(msg)
        if isinstance(target, AsyncObjectStore):
            self._async_store = target
            self._store = target.to_sync()
        else:
            self._store = (
                target
                if isinstance(target, SyncObjectStore)
                else SyncObjectStore(target)
                if isinstance(target, str)
                else SyncObjectStore.from_config(target)
            )
            self._async_store = self._store.to_async()
        self._base_url = base_url.rstrip("/") + "/" if base_url else None
        self._location = self._validate_name(location.rstrip("/"), allow_empty=True)
        self._file_overwrite = file_overwrite
        self._expires_in = expires_in

    @property
    def store(self) -> SyncObjectStore:
        """Return the synchronous handle sharing the native storage engine."""
        return self._store

    @property
    def async_store(self) -> AsyncObjectStore:
        """Return the asynchronous handle sharing the native storage engine."""
        return self._async_store

    async def aopen(self, name: str, mode: str = "rb") -> _ReadableFile:
        """Await metadata before constructing a synchronous Django file handle."""
        self._validate_mode(mode)
        key = self._qualified_name(name)
        metadata = await self._async_store.head(key)
        return _StoredFile(self._store, key, name=name, size=metadata.size, mode=mode)

    def save(self, name: str | None, content: Any, max_length: int | None = None) -> str:
        """Save content through Django's validation and available-name allocation.

        Buffer and text payloads skip Django's ``File`` wrapping and write directly through ``put``
        without an intermediate ``bytes`` copy; file objects stream through ``_save``.

        Raises:
            ValueError: If ``name`` is ``None`` for a buffer or text payload.
        """
        if not isinstance(content, (Buffer, str)):
            return super().save(name, content, max_length=max_length)
        if name is None:
            msg = "A file name is required"
            raise ValueError(msg)
        name = self.get_available_name(self._validate_name(name), max_length=max_length)
        self._store.put(self._qualified_name(name), self._encode_payload(content), **self._digest_options(content))
        return name

    async def asave(self, name: str | None, content: Any, max_length: int | None = None) -> str:
        """Allocate a Django filename and stream content through the async store.

        Raises:
            ValueError: If ``name`` is ``None`` for a buffer or text payload.
        """
        if name is None and isinstance(content, (Buffer, str)):
            msg = "A file name is required"
            raise ValueError(msg)
        name = self._validate_name(name if name is not None else content.name)
        name = await self._aget_available_name(name, max_length)
        key = self._qualified_name(name)
        digests = self._digest_options(content)
        if isinstance(content, (Buffer, str)):
            await self._async_store.put(key, self._encode_payload(content), **digests)
        elif isinstance(content, AsyncIterable):
            await self._async_store.put_stream(key, content, **digests)
        else:
            file = content if hasattr(content, "chunks") else File(content, name=name)
            await self._async_store.put_stream(key, self._file_chunks(file), **digests)
        return name

    def is_name_available(self, name: str, max_length: int | None = None) -> bool:
        """Honor overwrite while retaining Django's length and path constraints."""
        self._validate_name(name)
        return not (max_length and len(name) > max_length) and (self._file_overwrite or not self.exists(name))

    def delete(self, name: str) -> None:
        """Delete a file under the configured location."""
        self._store.delete(self._qualified_name(name))

    async def adelete(self, name: str) -> None:
        """Delete a file through the native asynchronous handle."""
        await self._async_store.delete(self._qualified_name(name))

    def exists(self, name: str) -> bool:
        """Return whether the qualified file exists."""
        return self._store.exists(self._qualified_name(name))

    async def aexists(self, name: str) -> bool:
        """Return whether the qualified file exists asynchronously."""
        return await self._async_store.exists(self._qualified_name(name))

    def size(self, name: str) -> int:
        """Return the file size from native object metadata."""
        return self._store.head(self._qualified_name(name)).size

    async def asize(self, name: str) -> int:
        """Return the file size from asynchronous object metadata."""
        return (await self._async_store.head(self._qualified_name(name))).size

    def listdir(self, path: str) -> tuple[list[str], list[str]]:
        """Return sorted direct directory and file names under a relative path."""
        prefix = self._qualified_name(path.rstrip("/"), allow_empty=True)
        prefix = prefix + "/" if prefix else ""
        objects, prefixes = self._store.list_with_delimiter(prefix)
        directories = sorted(item.removeprefix(prefix).rstrip("/") for item in prefixes if item.startswith(prefix))
        files = sorted(item.path.removeprefix(prefix) for item in objects if item.path.startswith(prefix))
        return directories, files

    def url(self, name: str | None, *, expire: int | None = None) -> str:
        """Sign a qualified key or join it to the configured public base URL."""
        key = self._qualified_name(name)
        if self._store.supports_signing:
            return self._store.sign(key, expires_in=self._expires_in if expire is None else expire)
        return self._public_url(key)

    async def aurl(self, name: str | None, *, expire: int | None = None) -> str:
        """Sign through the asynchronous store or return a public URL."""
        key = self._qualified_name(name)
        if self._async_store.supports_signing:
            return await self._async_store.sign(key, expires_in=self._expires_in if expire is None else expire)
        return self._public_url(key)

    def path(self, name: str) -> str:
        """Return a resolved local path, refusing stores without a local root."""
        key = self._qualified_name(name)
        if self._store.scheme != "file":
            msg = "This backend does not support local filesystem paths"
            raise NotImplementedError(msg)
        parsed = urlparse(self._store.url)
        root = Path(unquote(parsed.netloc + parsed.path)).resolve()
        resolved = (root / key).resolve()
        if not resolved.is_relative_to(root):
            msg = "Detected path traversal outside the storage root"
            raise _SuspiciousFileOperation(msg)
        return str(resolved)

    def get_modified_time(self, name: str) -> datetime:
        """Return the native modification timestamp as an aware UTC datetime."""
        raw = self._store.head(self._qualified_name(name)).last_modified
        try:
            modified = datetime.fromisoformat(raw)
        except (ValueError, TypeError) as exc:
            msg = "The storage backend does not expose a modification timestamp"
            raise NotImplementedError(msg) from exc
        if modified.tzinfo is None:
            msg = "The storage backend does not expose a timezone-aware timestamp"
            raise NotImplementedError(msg)
        return modified.astimezone(UTC)

    def get_created_time(self, name: str) -> datetime:
        """Raise because native metadata does not expose file creation time."""
        self._qualified_name(name)
        msg = "The storage backend does not expose creation timestamps"
        raise NotImplementedError(msg)

    def get_accessed_time(self, name: str) -> datetime:
        """Raise because native metadata does not expose file access time."""
        self._qualified_name(name)
        msg = "The storage backend does not expose access timestamps"
        raise NotImplementedError(msg)

    def _open(self, name: str, mode: str = "rb") -> _ReadableFile:
        """Return a seekable Django file over the native synchronous reader."""
        self._validate_mode(mode)
        return _StoredFile(self._store, self._qualified_name(name), name=name, mode=mode)

    def _save(self, name: str, content: Any) -> str:
        """Stream Django's already allocated file chunk by chunk into a native multipart upload."""
        self._store.put_stream(self._qualified_name(name), self._file_chunks(content), **self._digest_options(content))
        return name

    @staticmethod
    def _digest_options(content: Any) -> _DigestOptions:
        """Extract optional integrity digest attributes from a Django content object."""
        options: _DigestOptions = {}
        sha256 = getattr(content, "sha256", None)
        if sha256 is not None:
            options["expected_sha256"] = str(sha256)
        crc32c = getattr(content, "crc32c", None)
        if crc32c is not None:
            options["expected_crc32c"] = str(crc32c)
        md5 = getattr(content, "md5", None)
        if md5 is not None:
            options["expected_md5"] = str(md5)
        return options

    async def _aget_available_name(self, name: str, max_length: int | None) -> str:
        """Apply Django's alternative-name and truncation rules with async probes."""
        directory, filename = posixpath.split(name)
        extension = "".join(PurePosixPath(filename).suffixes)
        root = filename.removesuffix(extension)
        while (max_length and len(name) > max_length) or (not self._file_overwrite and await self.aexists(name)):
            name = posixpath.join(directory, self.get_alternative_name(root, extension))
            if max_length is None:
                continue
            truncation = len(name) - max_length
            if truncation > 0:
                root = root[:-truncation]
                if not root:
                    msg = f'Storage can not find an available filename for "{name}" within max_length={max_length}'
                    raise _SuspiciousFileOperation(msg)
                name = posixpath.join(directory, self.get_alternative_name(root, extension))
        return self._validate_name(name)

    def _qualified_name(self, name: str | None, *, allow_empty: bool = False) -> str:
        if name is None:
            msg = "A file name is required"
            raise ValueError(msg)
        relative = self._validate_name(name, allow_empty=allow_empty)
        return "/".join(part for part in (self._location, relative) if part)

    def _public_url(self, key: str) -> str:
        if self._base_url is None:
            msg = "A base URL or signing configuration is required"
            raise ValueError(msg)
        return self._base_url + quote(key, safe="/")

    @staticmethod
    def _encode_payload(content: Buffer | str) -> Buffer:
        """Encode text as UTF-8 and pass buffers through without copying."""
        return content.encode("utf-8") if isinstance(content, str) else content

    @staticmethod
    def _file_chunks(file: Any) -> Iterator[Buffer]:
        """Encode text file chunks without buffering the entire upload."""
        for chunk in file.chunks():
            yield chunk.encode("utf-8") if isinstance(chunk, str) else chunk

    @staticmethod
    def _validate_name(name: str, *, allow_empty: bool = False) -> str:
        normalized = str(name).replace("\\", "/")
        if not normalized and allow_empty:
            return ""
        assert _validate_file_name is not None
        _validate_file_name(normalized, allow_relative_path=True)
        return str(PurePosixPath(normalized))

    @staticmethod
    def _validate_mode(mode: str) -> None:
        if mode not in {"r", "rb"}:
            msg = "Django storage supports read-only file handles"
            raise ValueError(msg)


class _StoredFile(_ReadableFile):
    """Django file whose reopen operation remains bound to its native store."""

    def __init__(
        self,
        store: SyncObjectStore,
        key: str,
        *,
        name: str,
        size: int | None = None,
        mode: str = "rb",
    ) -> None:
        self._store = store
        self._key = key
        self.mode = mode
        super().__init__(cast("BinaryIO", store.open_reader(key, size=size)), name=name)

    def open(self, mode: str | None = None, *args: Any, **kwargs: Any) -> Self:
        """Reset the reader or reopen the native key after the handle closes."""
        resolved_mode = mode or self.mode
        DjangoStorage._validate_mode(resolved_mode)
        if args or kwargs:
            msg = "Native storage files do not accept local filesystem open options"
            raise TypeError(msg)
        if self.closed:
            self.file = cast("BinaryIO", self._store.open_reader(self._key))
            self.__dict__.pop("size", None)
        else:
            self.seek(0)
        self.mode = resolved_mode
        return self


class _StorageConfigSerializer(_SerializerBase):
    """Serialize Vertebra storage configs through public Django migration hooks."""

    def serialize(self) -> tuple[str, set[str]]:
        imports = {"import vertebra.storage"}
        arguments = []
        assert _writer_module is not None
        for field in fields(self.value):
            value, value_imports = _writer_module.MigrationWriter.serialize(getattr(self.value, field.name))
            imports.update(value_imports)
            arguments.append(f"{field.name}={value}")
        return f"vertebra.storage.{type(self.value).__name__}({', '.join(arguments)})", imports


if _writer_module is not None:
    for _config_type in get_args(StorageConfig.__value__):
        _writer_module.MigrationWriter.register_serializer(_config_type, _StorageConfigSerializer)
