"""Sync and async object storage integration for FastAPI applications."""

from __future__ import annotations

from vertebra.extensions.starlette.storage import (
    AsyncObjectStore,
    AsyncStorageRegistry,
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    S3Config,
    StorageConfig,
    SyncObjectStore,
    SyncStorageRegistry,
    attach_storage,
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
