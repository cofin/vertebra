"""Advanced Alchemy integration factories, session broker, and ObjectStore backend."""

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
    CacheEntry,
    CacheStats,
    SQLAlchemyCacheBackend,
)
from vertebra.extensions.alchemy.plugin import (
    create_alchemy_plugin,
    create_async_alchemy_config,
    create_sync_alchemy_config,
)
from vertebra.extensions.alchemy.storage import (
    ObjectStoreBackend,
    register_storage_backend,
)

__all__ = (
    "BrokerConfig",
    "BrokerMetrics",
    "BrokerPlugin",
    "BrokerTransport",
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
    "create_alchemy_plugin",
    "create_async_alchemy_config",
    "create_sync_alchemy_config",
    "register_storage_backend",
)
