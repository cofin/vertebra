"""Sync and async object storage integration for FastAPI applications."""

from __future__ import annotations

from vertebra.extensions._web.asgi_starlette import attach_state_storage as attach_storage
from vertebra.framework.storage import (
    AsyncObjectStore,
    AsyncStorageRegistry,
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    S3Config,
    StorageConfig,
    SyncObjectStore,
    SyncStorageRegistry,
)

__all__ = (
    "AsyncObjectStore",
    "AsyncStorageRegistry",
    "GCSConfig",
    "LocalConfig",
    "MemoryConfig",
    "S3Config",
    "StorageConfig",
    "SyncObjectStore",
    "SyncStorageRegistry",
    "attach_storage",
)
