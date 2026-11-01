"""ObjectStorage bridge and zero-copy file sender for Flask applications."""

from __future__ import annotations

import mimetypes
from typing import Any

from vertebra._vertebra import Response
from vertebra.storage import GCSConfig, LocalConfig, MemoryConfig, ObjectStore, S3Config

__all__ = (
    "GCSConfig",
    "LocalConfig",
    "MemoryConfig",
    "ObjectStore",
    "S3Config",
    "StorageRegistry",
    "attach_storage",
    "send_storage_file",
)


class StorageRegistry:
    """Named ``ObjectStore`` registry attached to a Flask application."""

    __slots__ = ("_stores",)

    def __init__(self, stores: dict[str, ObjectStore] | None = None) -> None:
        self._stores: dict[str, ObjectStore] = dict(stores or {})

    def register(self, name: str, store: ObjectStore) -> None:
        """Register an ``ObjectStore`` under ``name``."""
        self._stores[name] = store

    def get(self, name: str = "default") -> ObjectStore:
        """Return the ``ObjectStore`` registered under ``name``."""
        return self._stores[name]


def attach_storage(
    app: Any,
    stores: dict[str, ObjectStore] | ObjectStore,
    *,
    attribute: str = "storage",
) -> StorageRegistry:
    """Attach a ``StorageRegistry`` to a Flask application and ``app.extensions``."""
    registry = StorageRegistry({"default": stores} if isinstance(stores, ObjectStore) else stores)
    setattr(app, attribute, registry)
    extensions = getattr(app, "extensions", None)
    if isinstance(extensions, dict):
        extensions["vertebra_storage"] = registry
    return registry


def send_storage_file(
    store: ObjectStore,
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
