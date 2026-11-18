"""Flask storage extension using Vertebra Rust ObjectStore for send_file and storage registry."""

from __future__ import annotations

import mimetypes
from typing import TYPE_CHECKING, Any

from vertebra._vertebra import Response
from vertebra.framework.storage import (
    AsyncObjectStore,
    AsyncStorageRegistry,
    AzureConfig,
    GCSConfig,
    HTTPConfig,
    LocalConfig,
    MemoryConfig,
    OCIConfig,
    R2Config,
    S3Config,
    StorageConfig,
    SyncObjectStore,
    SyncStorageRegistry,
)
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from flask import Flask

__all__ = (
    "AsyncObjectStore",
    "AsyncStorageRegistry",
    "AzureConfig",
    "GCSConfig",
    "HTTPConfig",
    "LocalConfig",
    "MemoryConfig",
    "OCIConfig",
    "R2Config",
    "S3Config",
    "StorageConfig",
    "SyncObjectStore",
    "SyncStorageRegistry",
    "attach_storage",
    "send_storage_file",
)


def attach_storage(
    app: Flask | Any,
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
    ensure_module("flask", extra="flask")
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
