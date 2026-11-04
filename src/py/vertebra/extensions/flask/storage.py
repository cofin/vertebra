"""Flask storage extension using Vertebra Rust ObjectStore for send_file and storage registry."""

from __future__ import annotations

import mimetypes
from typing import Any

from vertebra._vertebra import Response
from vertebra.storage import AsyncObjectStore, GCSConfig, LocalConfig, MemoryConfig, S3Config, SyncObjectStore

__all__ = (
    "AsyncObjectStore",
    "AsyncStorageRegistry",
    "GCSConfig",
    "LocalConfig",
    "MemoryConfig",
    "S3Config",
    "SyncObjectStore",
    "SyncStorageRegistry",
    "attach_storage",
    "send_storage_file",
)


class SyncStorageRegistry:
    """Named ``SyncObjectStore`` registry attached to a Flask application."""

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
    """Named ``AsyncObjectStore`` registry attached to a Flask application."""

    __slots__ = ("_stores",)

    def __init__(self, stores: dict[str, AsyncObjectStore] | None = None) -> None:
        self._stores: dict[str, AsyncObjectStore] = dict(stores or {})

    def register(self, name: str, store: AsyncObjectStore) -> None:
        """Register an ``AsyncObjectStore`` under ``name``."""
        self._stores[name] = store

    def get(self, name: str = "default") -> AsyncObjectStore:
        """Return the ``AsyncObjectStore`` registered under ``name``."""
        return self._stores[name]


def attach_storage(
    app: Any,
    stores: SyncStorageRegistry
    | AsyncStorageRegistry
    | dict[str, SyncObjectStore]
    | SyncObjectStore
    | dict[str, AsyncObjectStore]
    | AsyncObjectStore,
    *,
    attribute: str = "storage",
) -> SyncStorageRegistry | AsyncStorageRegistry:
    """Attach a ``SyncStorageRegistry`` or ``AsyncStorageRegistry`` to a Flask application and ``app.extensions``."""
    registry: SyncStorageRegistry | AsyncStorageRegistry
    if isinstance(stores, (SyncStorageRegistry, AsyncStorageRegistry)):
        registry = stores
    elif isinstance(stores, AsyncObjectStore):
        registry = AsyncStorageRegistry({"default": stores})
    elif isinstance(stores, SyncObjectStore):
        registry = SyncStorageRegistry({"default": stores})
    elif any(isinstance(v, AsyncObjectStore) for v in stores.values()):
        registry = AsyncStorageRegistry({k: v for k, v in stores.items() if isinstance(v, AsyncObjectStore)})
    else:
        registry = SyncStorageRegistry({k: v for k, v in stores.items() if isinstance(v, SyncObjectStore)})
    setattr(app, attribute, registry)
    extensions = getattr(app, "extensions", None)
    if isinstance(extensions, dict):
        extensions["vertebra_storage"] = registry
    return registry


def send_storage_file(
    store: SyncObjectStore,
    key: str,
    *,
    download_name: str | None = None,
    as_attachment: bool = False,
    content_type: str | None = None,
) -> Response:
    """Read an object from ``store`` and return a Vertebra ``Response`` with Content-Disposition headers."""
    data = bytes(store.get(key))
    guessed_type = content_type or mimetypes.guess_type(download_name or key)[0] or "application/octet-stream"
    headers: list[tuple[str, str]] = [("content-type", guessed_type)]
    if as_attachment or download_name:
        filename = download_name or key.rsplit("/", 1)[-1]
        disposition = "attachment" if as_attachment else "inline"
        headers.append(("content-disposition", f'{disposition}; filename="{filename}"'))
    return Response(status=200, body=data, headers=headers)
