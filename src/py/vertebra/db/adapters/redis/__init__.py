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
        Resp3Error,
        Resp3Parser,
        crc16_xmodem,
        encode_hello3_handshake,
        encode_resp_command,
        hash_slot,
        parse_cluster_redirect,
        redis_hash_slot,
    )
    from vertebra.db.adapters.redis.driver import AsyncRedisDriver, SyncRedisDriver

__all__ = (
    "AsyncRedisDriver",
    "ClusterRedirect",
    "PubSubMessage",
    "RedisConfig",
    "RedisCore",
    "Resp3Error",
    "Resp3Parser",
    "SyncRedisDriver",
    "crc16_xmodem",
    "encode_hello3_handshake",
    "encode_resp_command",
    "hash_slot",
    "parse_cluster_redirect",
    "redis_hash_slot",
)

__lazy_modules__ = [
    "vertebra.db.adapters.redis.config",
    "vertebra.db.adapters.redis.core",
    "vertebra.db.adapters.redis.driver",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "AsyncRedisDriver": ("vertebra.db.adapters.redis.driver", "AsyncRedisDriver"),
    "ClusterRedirect": ("vertebra.db.adapters.redis.core", "ClusterRedirect"),
    "PubSubMessage": ("vertebra.db.adapters.redis.core", "PubSubMessage"),
    "RedisConfig": ("vertebra.db.adapters.redis.config", "RedisConfig"),
    "RedisCore": ("vertebra.db.adapters.redis.core", "RedisCore"),
    "Resp3Error": ("vertebra.db.adapters.redis.core", "Resp3Error"),
    "Resp3Parser": ("vertebra.db.adapters.redis.core", "Resp3Parser"),
    "SyncRedisDriver": ("vertebra.db.adapters.redis.driver", "SyncRedisDriver"),
    "crc16_xmodem": ("vertebra.db.adapters.redis.core", "crc16_xmodem"),
    "encode_hello3_handshake": ("vertebra.db.adapters.redis.core", "encode_hello3_handshake"),
    "encode_resp_command": ("vertebra.db.adapters.redis.core", "encode_resp_command"),
    "hash_slot": ("vertebra.db.adapters.redis.core", "hash_slot"),
    "parse_cluster_redirect": ("vertebra.db.adapters.redis.core", "parse_cluster_redirect"),
    "redis_hash_slot": ("vertebra.db.adapters.redis.core", "redis_hash_slot"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
