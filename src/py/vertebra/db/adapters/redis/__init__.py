"""Redis RESP3 driver, topology routing, client-side caching, and pub/sub multiplexing."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.adapters.redis.config import RedisConfig
    from vertebra.db.adapters.redis.core import (
        ClusterRedirect,
        PubSubMessage,
        RedisCore,
        crc16_xmodem,
        hash_slot,
        parse_cluster_redirect,
        redis_hash_slot,
    )
    from vertebra.db.adapters.redis.driver import AsyncRedisDriver, SyncRedisDriver
    from vertebra.db.adapters.redis.notify import (
        AsyncRedisNotificationSource,
        RedisNotifyConfig,
        SyncRedisNotificationSource,
    )

__all__ = (
    "AsyncRedisDriver",
    "AsyncRedisNotificationSource",
    "ClusterRedirect",
    "PubSubMessage",
    "RedisConfig",
    "RedisCore",
    "RedisNotifyConfig",
    "SyncRedisDriver",
    "SyncRedisNotificationSource",
    "crc16_xmodem",
    "hash_slot",
    "parse_cluster_redirect",
    "redis_hash_slot",
)

__lazy_modules__ = [
    "vertebra.db.adapters.redis.config",
    "vertebra.db.adapters.redis.core",
    "vertebra.db.adapters.redis.driver",
    "vertebra.db.adapters.redis.notify",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "AsyncRedisDriver": ("vertebra.db.adapters.redis.driver", "AsyncRedisDriver"),
    "AsyncRedisNotificationSource": ("vertebra.db.adapters.redis.notify", "AsyncRedisNotificationSource"),
    "ClusterRedirect": ("vertebra.db.adapters.redis.core", "ClusterRedirect"),
    "PubSubMessage": ("vertebra.db.adapters.redis.core", "PubSubMessage"),
    "RedisConfig": ("vertebra.db.adapters.redis.config", "RedisConfig"),
    "RedisCore": ("vertebra.db.adapters.redis.core", "RedisCore"),
    "RedisNotifyConfig": ("vertebra.db.adapters.redis.notify", "RedisNotifyConfig"),
    "SyncRedisDriver": ("vertebra.db.adapters.redis.driver", "SyncRedisDriver"),
    "SyncRedisNotificationSource": ("vertebra.db.adapters.redis.notify", "SyncRedisNotificationSource"),
    "crc16_xmodem": ("vertebra.db.adapters.redis.core", "crc16_xmodem"),
    "hash_slot": ("vertebra.db.adapters.redis.core", "hash_slot"),
    "parse_cluster_redirect": ("vertebra.db.adapters.redis.core", "parse_cluster_redirect"),
    "redis_hash_slot": ("vertebra.db.adapters.redis.core", "redis_hash_slot"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
