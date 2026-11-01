"""Advanced Alchemy integration factories, session broker, caching, and ObjectStore backend."""

from __future__ import annotations

from vertebra.extensions.alchemy.broker import (
    BrokerConfig,
    BrokerMetrics,
    BrokerPlugin,
    BrokerTransport,
    ChannelMessage,
    ConnectionLease,
    InMemoryBrokerTransport,
    SHMBrokerTransport,
    SHMSessionBroker,
    StatementCache,
    StatementPlan,
)
from vertebra.extensions.alchemy.cache import (
    AsyncCacheManager,
    CacheConfig,
    CacheEntry,
    CacheStats,
    SQLAlchemyCacheBackend,
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
)
from vertebra.extensions.alchemy.storage import (
    ObjectStoreBackend,
    register_storage_backend,
)

__all__ = (
    "AsyncCacheManager",
    "BrokerConfig",
    "BrokerMetrics",
    "BrokerPlugin",
    "BrokerTransport",
    "CacheConfig",
    "CacheEntry",
    "CacheStats",
    "ChannelMessage",
    "ConnectionLease",
    "InMemoryBrokerTransport",
    "ObjectStoreBackend",
    "SHMBrokerTransport",
    "SHMSessionBroker",
    "SQLAlchemyCacheBackend",
    "StatementCache",
    "StatementPlan",
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
    "register_storage_backend",
    "setup_cache_listeners",
)
