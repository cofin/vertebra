"""Valkey 8.0+ / 9.0+ RESP3 driver, Cluster/Sentinel support, and GCP Memorystore for Valkey IAM."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.db.redis import (
    AsyncRedisDriver,
    ClusterRedirect,
    PubSubMessage,
    RedisConfig,
    Resp3Error,
    Resp3Parser,
    SyncRedisDriver,
    crc16_xmodem,
    encode_hello3_handshake,
    encode_resp_command,
    hash_slot,
    parse_cluster_redirect,
    redis_hash_slot,
)

if TYPE_CHECKING:
    import socket
    from collections.abc import Callable

__all__ = (
    "AsyncValkeyDriver",
    "ClusterRedirect",
    "PubSubMessage",
    "Resp3Error",
    "Resp3Parser",
    "SyncValkeyDriver",
    "ValkeyConfig",
    "crc16_xmodem",
    "encode_hello3_handshake",
    "encode_resp_command",
    "hash_slot",
    "parse_cluster_redirect",
    "redis_hash_slot",
)


class ValkeyConfig(RedisConfig, frozen=True, gc=False, kw_only=True):
    """Configuration for Valkey 8.0+ / 9.0+ standalone, Sentinel, Cluster, and GCP Memorystore."""

    url: str = "valkey://localhost:6379/0"
    """Valkey connection URI (`valkey://`, `valkeys://`, `redis://`, `rediss://`, or `memory://`)."""
    client_name: str = "vertebra-valkey"
    """Connection name registered via `HELLO 3 ... SETNAME`."""
    enable_client_side_caching: bool = True
    """When True, enable RESP3 local client-side caching with push invalidation."""
    enable_client_tracking: bool = True
    """When True, issue `CLIENT TRACKING ON` during connection handshake."""
    client_tracking_prefixes: tuple[str, ...] = ()
    """Key prefixes subscribed in `BCAST` client-tracking mode."""
    client_cache_capacity: int = 4096
    """Maximum number of keys retained in the local LRU client-side cache."""
    max_cached_keys: int = 4096
    """Alias for `client_cache_capacity` specifying maximum locally cached keys."""
    client_cache_ttl_seconds: float | None = None
    """Optional local TTL in seconds applied to cached Valkey entries."""
    enable_gcp_memorystore_iam: bool = False
    """When True, authenticate using Google Cloud Memorystore for Valkey IAM tokens."""
    iam_auth_enabled: bool = False
    """Alias for `enable_gcp_memorystore_iam` enabling IAM token authentication."""
    token_provider: Any = None
    """Optional zero-argument callable returning an ephemeral IAM access token."""
    iam_token_provider: Any = None
    """Alias for `token_provider` supplying IAM authentication tokens."""
    memorystore_instance_id: str | None = None
    """Optional Google Cloud Memorystore for Valkey instance resource identifier."""
    sentinel_nodes: tuple[tuple[str, int] | str, ...] = ()
    """Seed Valkey Sentinel `(host, port)` endpoints."""
    sentinel_master_name: str | None = None
    """Monitored primary service name queried via Valkey Sentinel."""
    cluster_enabled: bool = False
    """When True, route commands across the 16,384 CRC16-XMODEM hash slots."""
    cluster_nodes: tuple[tuple[str, int] | str, ...] = ()
    """Seed Valkey Cluster `(host, port)` endpoints used for slot map discovery."""
    max_redirections: int = 5
    """Maximum number of `-MOVED` or `-ASK` cluster redirections followed per command."""

    @property
    def uses_tls(self) -> bool:
        """Return True if the URL scheme uses TLS (`valkeys://` or `rediss://`)."""
        return self.url.startswith("valkeys://") or self.url.startswith("rediss://")

    @property
    def effective_tracking_enabled(self) -> bool:
        """Return True when RESP3 client-side tracking/caching is enabled."""
        return bool(self.enable_client_side_caching and self.enable_client_tracking)

    @property
    def effective_iam_enabled(self) -> bool:
        """Return True when GCP Memorystore IAM authentication is enabled."""
        return bool(
            self.iam_auth_enabled
            or self.enable_gcp_memorystore_iam
            or self.enable_gcp_iam_auth
            or (self.token_provider is not None)
            or (self.iam_token_provider is not None)
        )

    def to_redis_config(self) -> RedisConfig:
        """Convert to equivalent RedisConfig."""
        return self


class SyncValkeyDriver(SyncRedisDriver):
    """Synchronous Valkey 8.0+/9.0+ driver with RESP3 client-side cache tracking and topology routing."""

    __slots__ = ("_valkey_config",)

    def __init__(
        self,
        config: ValkeyConfig | None = None,
        *,
        broker: Any = None,
        socket_factory: Callable[[tuple[str, int]], socket.socket] | None = None,
    ) -> None:
        self._valkey_config = config or ValkeyConfig()
        super().__init__(self._valkey_config, broker=broker, socket_factory=socket_factory)

    @property
    def config(self) -> ValkeyConfig:
        """Return the active ValkeyConfig."""
        return self._valkey_config


class AsyncValkeyDriver(AsyncRedisDriver):
    """Asynchronous Valkey 8.0+/9.0+ RESP3 driver with client-side cache tracking and topology routing."""

    __slots__ = ("_valkey_config",)

    def __init__(self, config: ValkeyConfig | None = None, *, broker: Any = None) -> None:
        self._valkey_config = config or ValkeyConfig()
        super().__init__(self._valkey_config, broker=broker)

    @property
    def config(self) -> ValkeyConfig:
        """Return the active ValkeyConfig."""
        return self._valkey_config
