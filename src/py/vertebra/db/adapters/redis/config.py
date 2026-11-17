"""Redis 7.2+/8.0+ configuration, cluster redirect directives, and Pub/Sub messages."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self
from urllib.parse import urlparse

import msgspec

from vertebra.db.adapters.redis.core import (
    ClusterRedirect,
    PubSubMessage,
)
from vertebra.db.driver._cloud_auth import (
    AZURE_REDIS_SCOPE,
    AzureEntraTokenProvider,
    DatabaseTokenProvider,
)
from vertebra.db.driver._common import REDIS_DIALECT_TAG, BaseDatabaseConfig
from vertebra.typing import ImproperConfigurationError

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
    url_schemes: ClassVar[tuple[str, ...]] = ("redis", "rediss")
    driver_module: ClassVar[str] = "vertebra.db.adapters.redis.driver"
    sync_driver_name: ClassVar[str] = "SyncRedisDriver"
    async_driver_name: ClassVar[str] = "AsyncRedisDriver"

    url: str = "redis://localhost:6379/0"
    """Redis connection URI (`redis://` or `rediss://`)."""
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
    """Optional cloud `DatabaseTokenProvider` or zero-argument callable returning an ephemeral IAM access token."""
    iam_token_provider: Any = None
    """Alias for `token_provider` supplying IAM authentication tokens."""
    max_connections: int = 32
    """Maximum pooled sockets maintained per target node."""
    broker: Any = None
    """Optional shared-memory `DatabaseBroker`, `SyncSHMBrokerTransport`, or `AsyncSHMBrokerTransport` for cross-worker push fan-out."""

    def __post_init__(self) -> None:
        if (
            self.token_provider is not None
            and self.iam_token_provider is not None
            and self.token_provider is not self.iam_token_provider
        ):
            msg = "Cannot configure both token_provider and iam_token_provider on RedisConfig"
            raise ImproperConfigurationError(msg)
        provider = self.token_provider if self.token_provider is not None else self.iam_token_provider
        gcp_iam_flag = bool(self.enable_gcp_iam_auth or self.iam_auth_enabled)
        if gcp_iam_flag and isinstance(provider, DatabaseTokenProvider):
            msg = (
                "Cannot combine GCP IAM authentication (enable_gcp_iam_auth=True) with a "
                "DatabaseTokenProvider on RedisConfig; configure one credential mechanism"
            )
            raise ImproperConfigurationError(msg)
        if isinstance(self.token_provider, AzureEntraTokenProvider):
            msgspec.structs.force_setattr(
                self,
                "token_provider",
                self.token_provider.with_default_scope(AZURE_REDIS_SCOPE),
            )
        if isinstance(self.iam_token_provider, AzureEntraTokenProvider):
            msgspec.structs.force_setattr(
                self,
                "iam_token_provider",
                self.iam_token_provider.with_default_scope(AZURE_REDIS_SCOPE),
            )

    @staticmethod
    def _redact_url(url: str) -> str:
        """Return a URL representation with embedded userinfo passwords masked."""
        if "://" in url:
            scheme, rest = url.split("://", 1)
            if "@" in rest:
                userinfo, tail = rest.split("@", 1)
                if ":" in userinfo:
                    user, _ = userinfo.split(":", 1)
                    return f"{scheme}://{user}:***@{tail}"
        return url

    def __repr__(self) -> str:
        """Return a redacted representation that omits passwords and token secrets."""
        provider = self.token_provider if self.token_provider is not None else self.iam_token_provider
        provider_name = type(provider).__name__ if provider is not None else None
        return (
            f"RedisConfig(url={self._redact_url(self.url)!r}, username={self.username!r}, "
            f"password={'***' if self.password is not None else None!r}, "
            f"protocol={self.protocol!r}, topology={self.topology!r}, "
            f"enable_gcp_iam_auth={self.enable_gcp_iam_auth!r}, "
            f"token_provider={provider_name!r})"
        )

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
    def effective_database_token_provider(self) -> DatabaseTokenProvider | None:
        """Return the configured `DatabaseTokenProvider` instance when present."""
        provider = self.token_provider if self.token_provider is not None else self.iam_token_provider
        return provider if isinstance(provider, DatabaseTokenProvider) else None

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
        parts: list[str] = ["HELLO", "3"]
        if effective_password is not None:
            parts.extend(["AUTH", effective_user or "default", effective_password])
        if self.client_name:
            parts.extend(["SETNAME", self.client_name])
        hello_cmd = f"*{len(parts)}\r\n".encode("ascii") + b"".join(
            f"${len(p.encode('utf-8'))}\r\n{p}\r\n".encode("ascii") for p in parts
        )
        cmds = [hello_cmd]
        if self.effective_tracking_enabled:
            track_parts = ["CLIENT", "TRACKING", "ON"]
            for prefix in self.client_tracking_prefixes:
                track_parts.extend(["PREFIX", prefix])
            track_parts.append("BCAST")
            track_cmd = f"*{len(track_parts)}\r\n".encode("ascii") + b"".join(
                f"${len(p.encode('utf-8'))}\r\n{p}\r\n".encode("ascii") for p in track_parts
            )
            cmds.append(track_cmd)
        return cmds

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct a `RedisConfig` from a `redis://` or `rediss://` URI."""
        return cls(url=url, **overrides)

    def pool_key(self) -> tuple[Any, ...]:
        """Return the canonical shared-pool fingerprint tuple."""
        memorystore_id = getattr(self, "memorystore_instance_id", None) or ""
        default_tracking = self.dialect == "valkey"
        db_provider = self.effective_database_token_provider
        if (
            not self.username
            and self.protocol == 3
            and self.topology == "standalone"
            and not self.effective_sentinel_master_name
            and not self.sentinel_nodes
            and not self.effective_cluster_enabled
            and not self.cluster_nodes
            and self.effective_tracking_enabled is default_tracking
            and not self.client_tracking_prefixes
            and not self.effective_iam_enabled
            and not memorystore_id
            and db_provider is None
        ):
            return (self.dialect, self.url)
        base_key = (
            self.dialect,
            self.url,
            self.username or "",
            self.protocol,
            self.topology,
            self.effective_sentinel_master_name or "",
            self.sentinel_nodes,
            self.effective_cluster_enabled,
            self.cluster_nodes,
            self.effective_tracking_enabled,
            self.client_tracking_prefixes,
            self.effective_iam_enabled,
            memorystore_id,
        )
        if db_provider is None:
            return base_key
        try:
            provider_key: Any = hash(db_provider)
        except TypeError:
            provider_key = id(db_provider)
        return (*base_key, type(db_provider).__qualname__, provider_key)
