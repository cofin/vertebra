"""Shared object-store normalization and resolution helpers for ORM/SQLSpec storage adapters."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from vertebra.storage import (
    AsyncObjectStore,
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
)
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from vertebra.storage import AsyncGcpTokenCache, SigningConfig, SyncGcpTokenCache

__all__ = (
    "STORAGE_CONFIG_TYPES",
    "resolve_object_store_pair",
)

STORAGE_CONFIG_TYPES: tuple[type[Any], ...] = (
    LocalConfig,
    S3Config,
    GCSConfig,
    AzureConfig,
    OCIConfig,
    R2Config,
    HTTPConfig,
    MemoryConfig,
)


def resolve_object_store_pair(
    target: str | SyncObjectStore | AsyncObjectStore | StorageConfig | None,
    *,
    options: dict[str, str] | None = None,
    signing: SigningConfig | None = None,
    token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
    missing_message: str = "An explicit object store, storage config, or URI is required",
    strict_uri_type: bool = False,
) -> tuple[SyncObjectStore, AsyncObjectStore]:
    """Resolve a target store, config, or URI string into paired sync and async ``ObjectStore`` instances."""
    if target is None or (isinstance(target, str) and not target.strip()):
        raise ImproperConfigurationError(missing_message)
    if isinstance(target, (SyncObjectStore, AsyncObjectStore)) and (signing is not None or token_cache is not None):
        msg = "signing and token_cache apply only when building a store from a StorageConfig or URI"
        raise ImproperConfigurationError(msg)

    if isinstance(target, SyncObjectStore):
        return target, target.to_async()
    if isinstance(target, AsyncObjectStore):
        return target.to_sync(), target
    if isinstance(target, STORAGE_CONFIG_TYPES):
        sync_store = SyncObjectStore.from_config(
            cast("StorageConfig", target), signing=signing, token_cache=token_cache
        )
        return sync_store, sync_store.to_async()
    if isinstance(target, str):
        sync_store = SyncObjectStore(target, options=options or None, signing=signing, token_cache=token_cache)
        return sync_store, sync_store.to_async()
    if strict_uri_type:
        msg = f"Expected SyncObjectStore, AsyncObjectStore, StorageConfig, or URI string, got {type(target)!r}"
        raise TypeError(msg)
    sync_store = SyncObjectStore(str(target), options=options or None, signing=signing, token_cache=token_cache)
    return sync_store, sync_store.to_async()
