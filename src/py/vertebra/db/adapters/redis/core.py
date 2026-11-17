"""Redis RESP3 wire codecs, CRC16-XMODEM hash slot calculator, and cluster redirect parser."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.db.driver._resp import (
    HASH_SLOT_COUNT,
    NON_KEY_COMMANDS,
    ClusterRedirect,
    PubSubMessage,
)
from vertebra.db.driver._resp import (
    crc16_xmodem as _shared_crc16_xmodem,
)
from vertebra.db.driver._resp import (
    hash_slot as _shared_hash_slot,
)
from vertebra.db.driver._resp import (
    parse_cluster_redirect as _shared_parse_cluster_redirect,
)
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vertebra.db.adapters.redis.config import RedisConfig

__all__ = (
    "HASH_SLOT_COUNT",
    "NON_KEY_COMMANDS",
    "ClusterRedirect",
    "PubSubMessage",
    "RedisCore",
    "build_redis_client_kwargs",
    "crc16_xmodem",
    "hash_slot",
    "is_redis_auth_rejection",
    "parse_cluster_redirect",
    "redis_hash_slot",
)


def _reject_memory_url(url: str) -> None:
    """Validate that the given URL is a supported Redis scheme."""
    if url.startswith("memory://"):
        msg = "Redis driver does not support memory:// URLs"
        raise ImproperConfigurationError(msg)
    if url.startswith("valkey://") or url.startswith("valkeys://"):
        msg = "valkey:// URLs are not supported by the Redis driver; use vertebra.db.adapters.valkey instead."
        raise ImproperConfigurationError(msg)


def _normalize_nodes(nodes: Sequence[tuple[str, int] | str]) -> list[tuple[str, int]]:
    """Normalize (host, port) or 'host:port' node descriptors into (host, port) pairs."""
    return [
        (node[0], int(node[1])) if isinstance(node, (tuple, list)) else (node.split(":")[0], int(node.split(":")[1]))
        for node in nodes
    ]


class RedisCore:
    """Redis Cluster hash slots and redirection parsing backed by vertebra._redis."""

    @staticmethod
    def crc16_xmodem(data: bytes | bytearray | memoryview | str) -> int:
        """Compute CRC16-XMODEM (polynomial 0x1021, initial value 0x0000) for Redis Cluster."""
        return _shared_crc16_xmodem(data, native_module="vertebra._redis")

    @staticmethod
    def hash_slot(key: str | bytes) -> int:
        """Compute the Redis Cluster hash slot (0..16383) for a given key string or bytes."""
        return _shared_hash_slot(key, native_module="vertebra._redis")

    @classmethod
    def redis_hash_slot(cls, key: str | bytes) -> int:
        """Alias for hash_slot matching the C-extension export name."""
        return cls.hash_slot(key)

    @staticmethod
    def parse_cluster_redirect(error_message: str | bytes) -> ClusterRedirect | None:
        """Parse a Redis Cluster MOVED or ASK redirection error."""
        return _shared_parse_cluster_redirect(error_message, native_module="vertebra._redis")


def is_redis_auth_rejection(exc: BaseException) -> bool:
    """Return True when `exc` represents a Redis authentication rejection."""
    detail = str(exc).upper()
    return (
        "WRONGPASS" in detail
        or "NOAUTH" in detail
        or "UNAUTH" in detail
        or "ERRAUTH" in detail
        or "AUTHENTICATIONFAILED" in detail
        or "AUTHENTICATION FAILED" in detail
        or "INVALID USERNAME-PASSWORD PAIR" in detail
        or "AUTH FAILED" in detail
    )


def build_redis_client_kwargs(
    config: RedisConfig,
    *,
    resolved_password: str | None = None,
) -> dict[str, Any]:
    """Construct native `SyncRedisClient` / `AsyncRedisClient` keyword arguments."""
    uses_db_provider = config.effective_database_token_provider is not None
    effective_password = resolved_password if resolved_password is not None else config.password
    return {
        "url": config.url,
        "username": config.username,
        "password": effective_password,
        "client_name": config.client_name,
        "protocol": config.protocol,
        "topology": config.topology,
        "sentinel_master_name": config.effective_sentinel_master_name,
        "sentinel_nodes": _normalize_nodes(config.sentinel_nodes),
        "cluster_nodes": _normalize_nodes(config.cluster_nodes),
        "max_redirections": config.max_redirections,
        "enable_client_side_caching": config.enable_client_side_caching,
        "client_tracking_prefixes": list(config.client_tracking_prefixes),
        "client_cache_capacity": config.effective_cache_capacity,
        "client_cache_ttl_seconds": config.client_cache_ttl_seconds,
        "enable_gcp_iam_auth": False if uses_db_provider else config.effective_iam_enabled,
        "token_cache_capsule": None,
        "max_connections": config.max_connections,
    }


crc16_xmodem = RedisCore.crc16_xmodem
hash_slot = RedisCore.hash_slot
redis_hash_slot = RedisCore.redis_hash_slot
parse_cluster_redirect = RedisCore.parse_cluster_redirect
