"""Backwards compatibility shim - use vertebra.storage instead.

.. deprecated::
    This module is deprecated. Import from :mod:`vertebra.storage` instead.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from vertebra.storage import AsyncFileSystemClient, FileEntry, SyncFileSystemClient

if TYPE_CHECKING:
    from vertebra.storage.fsspec import AsyncVertebraFileSystem, SyncVertebraFileSystem


def __getattr__(name: str) -> Any:
    if name in {"AsyncVertebraFileSystem", "SyncVertebraFileSystem"}:
        from vertebra.storage.fsspec import AsyncVertebraFileSystem, SyncVertebraFileSystem

        return {"AsyncVertebraFileSystem": AsyncVertebraFileSystem, "SyncVertebraFileSystem": SyncVertebraFileSystem}[
            name
        ]
    raise AttributeError(name)


__all__ = (
    "AsyncFileSystemClient",
    "AsyncVertebraFileSystem",
    "FileEntry",
    "SyncFileSystemClient",
    "SyncVertebraFileSystem",
)
