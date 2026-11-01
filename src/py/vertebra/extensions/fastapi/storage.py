"""Sync and async object storage integration for FastAPI applications."""

from __future__ import annotations

from vertebra.extensions.starlette.storage import (
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    ObjectStore,
    S3Config,
    StorageRegistry,
    attach_storage,
)

__all__ = (
    "GCSConfig",
    "LocalConfig",
    "MemoryConfig",
    "ObjectStore",
    "S3Config",
    "StorageRegistry",
    "attach_storage",
)
