"""Advanced Alchemy integration factories, session broker, caching, and ObjectStore backend."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from advanced_alchemy.filters import RelationshipFilter
    from advanced_alchemy.filterset import FilterSet
    from advanced_alchemy.service import SchemaDumpConfig, ServiceGroup

    from vertebra.extensions.alchemy.broker import (
        AsyncConnectionLease,
        AsyncSHMBrokerTransport,
        AsyncSHMSessionBroker,
        BrokerConfig,
        BrokerMetrics,
        BrokerPlugin,
        ChannelMessage,
        StatementPlan,
        SyncConnectionLease,
        SyncSHMBrokerTransport,
        SyncSHMSessionBroker,
    )
    from vertebra.extensions.alchemy.cache import (
        AsyncCacheRegion,
        CacheConfig,
        SyncCacheRegion,
        ZeroCopyServiceMixin,
        create_cache_config,
        msgspec_model_deserializer,
        msgspec_model_serializer,
        setup_cache_listeners,
    )
    from vertebra.extensions.alchemy.plugin import (
        create_alchemy_plugin,
        create_async_alchemy_config,
        create_fastapi_alchemy,
        create_flask_alchemy,
        create_starlette_alchemy,
        create_sync_alchemy_config,
        normalize_alchemy_url,
    )
    from vertebra.extensions.alchemy.storage import (
        ObjectStoreBackend,
        register_storage_backend,
    )

__all__ = (
    "AsyncCacheRegion",
    "AsyncConnectionLease",
    "AsyncSHMBrokerTransport",
    "AsyncSHMSessionBroker",
    "BrokerConfig",
    "BrokerMetrics",
    "BrokerPlugin",
    "CacheConfig",
    "ChannelMessage",
    "FilterSet",
    "ObjectStoreBackend",
    "RelationshipFilter",
    "SchemaDumpConfig",
    "ServiceGroup",
    "StatementPlan",
    "SyncCacheRegion",
    "SyncConnectionLease",
    "SyncSHMBrokerTransport",
    "SyncSHMSessionBroker",
    "ZeroCopyServiceMixin",
    "create_alchemy_plugin",
    "create_async_alchemy_config",
    "create_cache_config",
    "create_fastapi_alchemy",
    "create_flask_alchemy",
    "create_starlette_alchemy",
    "create_sync_alchemy_config",
    "msgspec_model_deserializer",
    "msgspec_model_serializer",
    "normalize_alchemy_url",
    "register_storage_backend",
    "setup_cache_listeners",
)

__lazy_modules__ = [
    "advanced_alchemy.filters",
    "advanced_alchemy.filterset",
    "advanced_alchemy.service",
    "vertebra.extensions.alchemy.broker",
    "vertebra.extensions.alchemy.cache",
    "vertebra.extensions.alchemy.plugin",
    "vertebra.extensions.alchemy.storage",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "AsyncCacheRegion": ("vertebra.extensions.alchemy.cache", "AsyncCacheRegion"),
    "AsyncConnectionLease": ("vertebra.extensions.alchemy.broker", "AsyncConnectionLease"),
    "AsyncSHMBrokerTransport": ("vertebra.extensions.alchemy.broker", "AsyncSHMBrokerTransport"),
    "AsyncSHMSessionBroker": ("vertebra.extensions.alchemy.broker", "AsyncSHMSessionBroker"),
    "BrokerConfig": ("vertebra.extensions.alchemy.broker", "BrokerConfig"),
    "BrokerMetrics": ("vertebra.extensions.alchemy.broker", "BrokerMetrics"),
    "BrokerPlugin": ("vertebra.extensions.alchemy.broker", "BrokerPlugin"),
    "CacheConfig": ("vertebra.extensions.alchemy.cache", "CacheConfig"),
    "ChannelMessage": ("vertebra.extensions.alchemy.broker", "ChannelMessage"),
    "FilterSet": ("advanced_alchemy.filterset", "FilterSet"),
    "ObjectStoreBackend": ("vertebra.extensions.alchemy.storage", "ObjectStoreBackend"),
    "RelationshipFilter": ("advanced_alchemy.filters", "RelationshipFilter"),
    "SchemaDumpConfig": ("advanced_alchemy.service", "SchemaDumpConfig"),
    "ServiceGroup": ("advanced_alchemy.service", "ServiceGroup"),
    "StatementPlan": ("vertebra.extensions.alchemy.broker", "StatementPlan"),
    "SyncCacheRegion": ("vertebra.extensions.alchemy.cache", "SyncCacheRegion"),
    "SyncConnectionLease": ("vertebra.extensions.alchemy.broker", "SyncConnectionLease"),
    "SyncSHMBrokerTransport": ("vertebra.extensions.alchemy.broker", "SyncSHMBrokerTransport"),
    "SyncSHMSessionBroker": ("vertebra.extensions.alchemy.broker", "SyncSHMSessionBroker"),
    "ZeroCopyServiceMixin": ("vertebra.extensions.alchemy.cache", "ZeroCopyServiceMixin"),
    "create_alchemy_plugin": ("vertebra.extensions.alchemy.plugin", "create_alchemy_plugin"),
    "create_async_alchemy_config": ("vertebra.extensions.alchemy.plugin", "create_async_alchemy_config"),
    "create_cache_config": ("vertebra.extensions.alchemy.cache", "create_cache_config"),
    "create_fastapi_alchemy": ("vertebra.extensions.alchemy.plugin", "create_fastapi_alchemy"),
    "create_flask_alchemy": ("vertebra.extensions.alchemy.plugin", "create_flask_alchemy"),
    "create_starlette_alchemy": ("vertebra.extensions.alchemy.plugin", "create_starlette_alchemy"),
    "create_sync_alchemy_config": ("vertebra.extensions.alchemy.plugin", "create_sync_alchemy_config"),
    "msgspec_model_deserializer": ("vertebra.extensions.alchemy.cache", "msgspec_model_deserializer"),
    "msgspec_model_serializer": ("vertebra.extensions.alchemy.cache", "msgspec_model_serializer"),
    "normalize_alchemy_url": ("vertebra.extensions.alchemy.plugin", "normalize_alchemy_url"),
    "register_storage_backend": ("vertebra.extensions.alchemy.storage", "register_storage_backend"),
    "setup_cache_listeners": ("vertebra.extensions.alchemy.cache", "setup_cache_listeners"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
