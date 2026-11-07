"""Django Storage backend over Vertebra Rust ObjectStore with sync and async methods."""

from __future__ import annotations

import io
import posixpath
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, unquote, urlsplit

from vertebra.storage import (
    AsyncObjectStore,
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    S3Config,
    StorageConfig,
    SyncObjectStore,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module, import_optional_attr

if TYPE_CHECKING:
    from django.core.files.base import File

    class _Storage:
        """Type-only inherited Django API when optional django-stubs is absent."""

        def open(self, name: str, mode: str = "rb") -> File:
            raise NotImplementedError

        def save(self, name: str | None, content: Any, max_length: int | None = None) -> str:
            raise NotImplementedError

        def get_available_name(self, name: str, max_length: int | None = None) -> str:
            raise NotImplementedError

        def get_alternative_name(self, file_root: str, file_ext: str) -> str:
            raise NotImplementedError

        def get_valid_name(self, name: str) -> str:
            raise NotImplementedError

        def generate_filename(self, filename: str) -> str:
            raise NotImplementedError

        def deconstruct(self) -> tuple[str, tuple[Any, ...], dict[str, Any]]:
            raise NotImplementedError

else:
    File = import_optional_attr("django.core.files.base", "File")
    _Storage = import_optional_attr("django.core.files.storage", "Storage", object)

_deconstructible = import_optional_attr("django.utils.deconstruct", "deconstructible", lambda cls: cls)

__all__ = (
    "AsyncObjectStore",
    "AsyncStorageRegistry",
    "DjangoStorage",
    "GCSConfig",
    "LocalConfig",
    "MemoryConfig",
    "S3Config",
    "SyncObjectStore",
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


@_deconstructible
class DjangoStorage(_Storage):
    """Django file storage using native object-store I/O and migration reconstruction."""

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
        """Bind an explicit native store and configure Django naming and URLs."""
        ensure_module("django.core.files.storage", package="django", install_package="django", extra="django")
        target = store if store is not None else async_store if async_store is not None else config
        if target is None or isinstance(target, str) and not target.strip():
            raise ImproperConfigurationError("DjangoStorage requires an explicit storage configuration or store")
        if isinstance(target, AsyncObjectStore):
            self._async_store = target
            self._store = target.to_sync()
        else:
            self._store = target if isinstance(target, SyncObjectStore) else SyncObjectStore(target)
            self._async_store = self._store.to_async()
        if async_store is not None:
            self._async_store = async_store
        self._base_url = base_url.rstrip("/") + "/" if base_url is not None else None
        self._location = self._validate_name(location) if location else ""
        self._file_overwrite = file_overwrite
        self._expires_in = expires_in

    @property
    def store(self) -> SyncObjectStore:
        """Return the synchronous native object store."""
        return self._store

    @property
    def async_store(self) -> AsyncObjectStore:
        """Return the asynchronous native object store."""
        return self._async_store

    def save(self, name: str | None, content: Any, max_length: int | None = None) -> str:
        """Save Django files or byte buffers using Django's collision handling."""
        return str(super().save(name, self._file(content, name), max_length=max_length))

    async def aopen(self, name: str, mode: str = "rb") -> File:
        """Load bytes asynchronously before exposing Django's synchronous file API."""
        self._validate_mode(mode)
        payload = await self._async_store.get(self._key(name))
        readable: Any = io.BytesIO(payload)
        if "b" not in mode:
            readable = io.TextIOWrapper(readable)
        return File(readable, name=name)

    async def asave(self, name: str | None, content: Any, max_length: int | None = None) -> str:
        """Stream content asynchronously using collision checks on the async store."""
        content = self._file(content, name)
        name = self._validate_name(name if name is not None else content.name)
        while True:
            while (max_length is not None and len(name) > max_length) or (
                not self._file_overwrite and await self.aexists(name)
            ):
                name = self._alternative_name(name, max_length)
            try:
                await self._async_store.put_stream(
                    self._key(name), content.chunks(), if_not_exists=not self._file_overwrite
                )
                return name
            except FileExistsError:
                name = self._alternative_name(name, max_length)

    def delete(self, name: str) -> None:
        """Delete a stored object."""
        self._store.delete(self._key(name))

    async def adelete(self, name: str) -> None:
        """Delete a stored object asynchronously."""
        await self._async_store.delete(self._key(name))

    def exists(self, name: str) -> bool:
        """Return whether an object exists."""
        return self._store.exists(self._key(name))

    async def aexists(self, name: str) -> bool:
        """Return whether an object exists asynchronously."""
        return await self._async_store.exists(self._key(name))

    def size(self, name: str) -> int:
        """Return the stored byte length."""
        return self._store.head(self._key(name)).size

    async def asize(self, name: str) -> int:
        """Return the stored byte length asynchronously."""
        return (await self._async_store.head(self._key(name))).size

    def listdir(self, path: str) -> tuple[list[str], list[str]]:
        """Return immediate child directory and file names beneath a prefix."""
        prefix = self._key(path) if path else self._location
        objects, directories = self._store.list_with_delimiter(prefix)
        return (
            sorted(PurePosixPath(directory).name for directory in directories),
            sorted(PurePosixPath(item.path).name for item in objects),
        )

    def url(self, name: str | None, *, expire: int | None = None) -> str:
        """Return a signed URL or the configured public URL for a named file."""
        key = self._key(name)
        if self._store.supports_signing:
            return self._store.sign(key, expires_in=self._expires_in if expire is None else expire)
        return self._public_url(name)

    async def aurl(self, name: str | None, *, expire: int | None = None) -> str:
        """Sign through the async native store or return the public URL."""
        key = self._key(name)
        if self._async_store.supports_signing:
            return await self._async_store.sign(key, expires_in=self._expires_in if expire is None else expire)
        return self._public_url(name)

    def path(self, name: str) -> str:
        """Resolve a local object path, rejecting remote backends and traversal."""
        if self._store.scheme != "file":
            raise NotImplementedError("Remote object stores do not expose filesystem paths")
        parsed = urlsplit(self._store.url)
        root = Path(unquote(parsed.netloc + parsed.path)).resolve()
        resolved = (root / self._key(name)).resolve()
        if not resolved.is_relative_to(root):
            raise ValueError("Storage path escapes the configured root")
        return str(resolved)

    def is_name_available(self, name: str, max_length: int | None = None) -> bool:
        """Honor overwrite policy while retaining Django filename length checks."""
        return (max_length is None or len(name) <= max_length) and (self._file_overwrite or not self.exists(name))

    def get_modified_time(self, name: str) -> datetime:
        """Return the object's modification timestamp in UTC."""
        return datetime.fromisoformat(self._store.head(self._key(name)).last_modified).astimezone(UTC)

    def get_created_time(self, name: str) -> datetime:
        """Use modification time because object stores do not expose creation time."""
        return self.get_modified_time(name)

    def get_accessed_time(self, name: str) -> datetime:
        """Use modification time because object stores do not track access time."""
        return self.get_modified_time(name)

    def _open(self, name: str, mode: str = "rb") -> File:
        """Wrap the native seekable reader in a Django file."""
        self._validate_mode(mode)
        readable: Any = self._store.open_readable_file(self._key(name))
        if "b" not in mode:
            readable = io.TextIOWrapper(readable)
        return File(readable, name=name)

    def _save(self, name: str, content: Any) -> str:
        """Stream file chunks and retry native create-only collisions safely."""
        content = self._file(content, name)
        while True:
            try:
                self._store.put_stream(self._key(name), content.chunks(), if_not_exists=not self._file_overwrite)
                return name
            except FileExistsError:
                name = self.get_available_name(name)

    def _key(self, name: str | None) -> str:
        """Validate an object name and apply the configured namespace."""
        normalized = self._validate_name(name)
        return posixpath.join(self._location, normalized) if self._location else normalized

    def _public_url(self, name: str | None) -> str:
        """Join a validated relative name onto the configured public base URL."""
        if self._base_url is None:
            raise ValueError("A base_url or signing credentials are required to generate URLs")
        return self._base_url + quote(self._validate_name(name), safe="/")

    def _alternative_name(self, name: str, max_length: int | None) -> str:
        """Generate an alternate name whose randomized suffix fits the field limit."""
        directory, filename = posixpath.split(name)
        extension = "".join(PurePosixPath(filename).suffixes)
        root = filename.removesuffix(extension)
        candidate = posixpath.join(directory, self.get_alternative_name(root, extension))
        if max_length is not None and len(candidate) > max_length:
            root = root[: max_length - len(candidate)]
            if not root:
                module = ensure_module(
                    "django.core.exceptions", package="django", install_package="django", extra="django"
                )
                raise module.SuspiciousFileOperation("The filename cannot fit within max_length")
            candidate = posixpath.join(directory, self.get_alternative_name(root, extension))
        return str(candidate)

    @staticmethod
    def _file(content: Any, name: str | None) -> File:
        """Wrap byte buffers and file-like inputs in Django's file interface."""
        if isinstance(content, (bytes, bytearray, memoryview)):
            content = io.BytesIO(content)
        return content if isinstance(content, File) else File(content, name=name)

    @staticmethod
    def _validate_name(name: str | None) -> str:
        """Apply Django's relative filename and traversal validation."""
        module = ensure_module("django.core.files.utils", package="django", install_package="django", extra="django")
        if not name:
            raise ValueError("A non-empty object name is required")
        normalized = name.replace("\\", "/")
        return str(module.validate_file_name(normalized, allow_relative_path=True))

    @staticmethod
    def _validate_mode(mode: str) -> None:
        """Reject writes through read-only object handles."""
        if mode not in {"r", "rb", "rt"}:
            raise ValueError("Object storage handles support read-only modes")
