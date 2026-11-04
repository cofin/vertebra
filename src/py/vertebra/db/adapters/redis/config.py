"""Redis 7.2+/8.0+ configuration, cluster redirect directives, and Pub/Sub messages."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self
from urllib.parse import urlparse

from vertebra.db.adapters.redis.core import (
    ClusterRedirect,
    PubSubMessage,
    RedisCore,
)
from vertebra.db.driver._common import REDIS_DIALECT_TAG, BaseDatabaseConfig

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = (
    "ClusterRedirect",
    "PubSubMessage",
    "RedisConfig",
)


class RedisConfig(BaseDatabaseConfig, frozen=True, gc=False, kw_only=True):
    """Configuration for Redis 7.2+ / 8.0+ standalone, Sentinel, and Cluster connections."""

    dialect: ClassVar[str] = "redis"
    dialect_tag: ClassVar[int] = REDIS_DIALECT_TAG
    url_schemes: ClassVar[tuple[str, ...]] = ("redis", "rediss", "memory")
    driver_module: ClassVar[str] = "vertebra.db.adapters.redis.driver"
    sync_driver_name: ClassVar[str] = "SyncRedisDriver"
    async_driver_name: ClassVar[str] = "AsyncRedisDriver"

    url: str = "redis://localhost:6379/0"
    """Redis connection URI (`redis://`, `rediss://`, or `memory://`)."""
    username: str | None = None
    """Optional Redis 6+ ACL username passed to `HELLO 3 AUTH`."""
    password: str | None = None
    """Optional Redis password or ACL token passed to `HELLO 3 AUTH`."""
    client_name: str = "vertebra"
    """Connection name registered via `HELLO 3 ... SETNAME`."""
    protocol: Literal[2, 3] = 3
    """RESP wire protocol version (`3` for RESP3 with push/tracking support, `2` for legacy RESP2)."""
    topology: Literal["standalone", "sentinel", "cluster"] = "standalone"
    """Deployment topology mode (`"standalone"`, `"sentinel"`, or `"cluster"`)."""
    sentinel_service_name: str | None = None
    """Alias for `sentinel_master_name` when discovering the primary via Redis Sentinel."""
    sentinel_master_name: str | None = None
    """Monitored primary service name queried via `SENTINEL get-master-addr-by-name`."""
    sentinel_nodes: tuple[tuple[str, int] | str, ...] = ()
    """Seed Redis Sentinel `(host, port)` endpoints."""
    cluster_enabled: bool = False
    """When True, route commands across the 16,384 CRC16-XMODEM hash slots."""
    cluster_nodes: tuple[tuple[str, int] | str, ...] = ()
    """Seed Redis Cluster `(host, port)` endpoints used for slot map discovery."""
    max_redirections: int = 5
    """Maximum number of `-MOVED` or `-ASK` cluster redirections followed per command."""
    enable_client_side_caching: bool = False
    """When True, cache read responses locally and invalidate on RESP3 `>invalidate` push frames."""
    enable_client_tracking: bool = False
    """When True, issue `CLIENT TRACKING ON` during connection handshake."""
    client_tracking_prefixes: tuple[str, ...] = ()
    """Key prefixes subscribed in `BCAST` tracking mode (`PREFIX <prefix>`)."""
    client_cache_capacity: int = 4096
    """Maximum number of keys retained in the local LRU client-side cache."""
    max_cached_keys: int = 4096
    """Alias for `client_cache_capacity` specifying maximum locally cached keys."""
    client_cache_ttl_seconds: float | None = None
    """Optional local TTL in seconds applied to cached RESP3 entries."""
    enable_gcp_iam_auth: bool = False
    """When True, authenticate using Google Cloud Memorystore IAM tokens."""
    iam_auth_enabled: bool = False
    """Alias for `enable_gcp_iam_auth` enabling IAM token authentication."""
    token_provider: Any = None
    """Optional zero-argument callable returning an ephemeral IAM access token."""
    iam_token_provider: Any = None
    """Alias for `token_provider` supplying IAM authentication tokens."""
    max_connections: int = 32
    """Maximum pooled sockets maintained per target node."""
    broker: Any = None
    """Optional shared-memory `DatabaseBroker`, `SyncSHMBrokerTransport`, or `AsyncSHMBrokerTransport` for cross-worker push fan-out."""

    @property
    def uses_tls(self) -> bool:
        """Return True if the URL scheme uses TLS (`rediss://`)."""
        return self.url.startswith("rediss://")

    @property
    def effective_cache_capacity(self) -> int:
        """Return the configured LRU client cache capacity."""
        if self.client_cache_capacity != 4096:
            return self.client_cache_capacity
        if self.max_cached_keys != 4096:
            return self.max_cached_keys
        return self.client_cache_capacity

    @property
    def effective_iam_enabled(self) -> bool:
        """Return True when GCP Memorystore IAM authentication is enabled."""
        return bool(
            self.iam_auth_enabled
            or self.enable_gcp_iam_auth
            or getattr(self, "enable_gcp_memorystore_iam", False)
            or (self.token_provider is not None)
            or (self.iam_token_provider is not None)
        )

    @property
    def effective_token_provider(self) -> Callable[[], Any] | None:
        """Return the configured IAM token provider callable."""
        provider = self.token_provider if self.token_provider is not None else self.iam_token_provider
        return provider if callable(provider) else None

    @property
    def effective_tracking_enabled(self) -> bool:
        """Return True when RESP3 client-side tracking/caching is enabled."""
        return bool(self.enable_client_tracking or self.enable_client_side_caching)

    @property
    def effective_sentinel_master_name(self) -> str | None:
        """Return the configured Sentinel master service name."""
        return self.sentinel_master_name or self.sentinel_service_name

    @property
    def effective_cluster_enabled(self) -> bool:
        """Return True when Cluster topology routing is active."""
        return bool(self.cluster_enabled or self.topology == "cluster" or self.cluster_nodes)

    @property
    def effective_sentinel_enabled(self) -> bool:
        """Return True when Sentinel master discovery is active."""
        return bool(self.topology == "sentinel" or (self.sentinel_nodes and self.effective_sentinel_master_name))

    def build_handshake_commands(self, *, iam_token: str | None = None) -> list[bytes]:
        """Build the initial RESP3 HELLO 3 and optional CLIENT TRACKING wire commands."""
        parsed = urlparse(self.url)
        iam_active = self.effective_iam_enabled
        if iam_active and iam_token is not None:
            effective_password: str | None = iam_token
        else:
            effective_password = self.password if self.password is not None else parsed.password
        if iam_active:
            effective_user: str | None = self.username or parsed.username or "default"
        else:
            effective_user = self.username if self.username is not None else parsed.username
        cmds = [
            RedisCore.encode_hello3_handshake(
                username=effective_user,
                password=effective_password,
                client_name=self.client_name,
            )
        ]
        if self.effective_tracking_enabled:
            tracking_cmd: list[bytes | str] = ["CLIENT", "TRACKING", "ON"]
            for prefix in self.client_tracking_prefixes:
                tracking_cmd.extend(["PREFIX", prefix])
            tracking_cmd.append("BCAST")
            cmds.append(RedisCore.encode_resp_command(*tracking_cmd))
        return cmds

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct a `RedisConfig` from a `redis://`, `rediss://`, or `memory://` URI."""
        return cls(url=url, **overrides)

    def pool_key(self) -> tuple[Any, ...]:
        """Return the canonical shared-pool fingerprint tuple."""
        return (self.dialect, self.url)

    def build_sync_driver(self, registry: Any) -> Any:
        """Instantiate the synchronous RESP3 driver for this configuration."""
        _ = registry
        sync_cls, _ = self.resolve_driver_classes()
        return sync_cls(self)

    def build_async_driver(self, registry: Any) -> Any:
        """Instantiate the asynchronous RESP3 driver for this configuration."""
        _ = registry
        _, async_cls = self.resolve_driver_classes()
        return async_cls(self)
