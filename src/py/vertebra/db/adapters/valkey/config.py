"""Valkey 8.0+ / 9.0+ RESP3 configuration supporting Cluster, Sentinel, and GCP Memorystore IAM."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self
from urllib.parse import urlparse

from vertebra.db.adapters.valkey.core import (
    ClusterRedirect,
    PubSubMessage,
    ValkeyCore,
)
from vertebra.db.driver._common import VALKEY_DIALECT_TAG, BaseDatabaseConfig

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = (
    "ClusterRedirect",
    "PubSubMessage",
    "ValkeyConfig",
)


class ValkeyConfig(BaseDatabaseConfig, frozen=True, gc=False, kw_only=True):
    """Configuration for Valkey 8.0+ / 9.0+ standalone, Sentinel, Cluster, and GCP Memorystore."""

    dialect: ClassVar[str] = "valkey"
    dialect_tag: ClassVar[int] = VALKEY_DIALECT_TAG
    url_schemes: ClassVar[tuple[str, ...]] = ("valkey", "valkeys")
    driver_module: ClassVar[str] = "vertebra.db.adapters.valkey.driver"
    sync_driver_name: ClassVar[str] = "SyncValkeyDriver"
    async_driver_name: ClassVar[str] = "AsyncValkeyDriver"

    url: str = "valkey://localhost:6379/0"
    """Valkey connection URI (`valkey://`, `valkeys://`, `redis://`, or `rediss://`)."""
    username: str | None = None
    """Optional Valkey ACL username passed to `HELLO 3 AUTH`."""
    password: str | None = None
    """Optional Valkey password or ACL token passed to `HELLO 3 AUTH`."""
    client_name: str = "vertebra-valkey"
    """Connection name registered via `HELLO 3 ... SETNAME`."""
    protocol: Literal[2, 3] = 3
    """RESP wire protocol version (`3` for RESP3 with push/tracking support, `2` for legacy RESP2)."""
    topology: Literal["standalone", "sentinel", "cluster"] = "standalone"
    """Deployment topology mode (`"standalone"`, `"sentinel"`, or `"cluster"`)."""
    sentinel_service_name: str | None = None
    """Alias for `sentinel_master_name` when discovering the primary via Valkey Sentinel."""
    sentinel_master_name: str | None = None
    """Monitored primary service name queried via `SENTINEL get-master-addr-by-name`."""
    sentinel_nodes: tuple[tuple[str, int] | str, ...] = ()
    """Seed Valkey Sentinel `(host, port)` endpoints."""
    cluster_enabled: bool = False
    """When True, route commands across the 16,384 CRC16-XMODEM hash slots."""
    cluster_nodes: tuple[tuple[str, int] | str, ...] = ()
    """Seed Valkey Cluster `(host, port)` endpoints used for slot map discovery."""
    max_redirections: int = 5
    """Maximum number of `-MOVED` or `-ASK` cluster redirections followed per command."""
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
    enable_gcp_iam_auth: bool = False
    """Alias for `enable_gcp_memorystore_iam` enabling IAM token authentication."""
    token_provider: Any = None
    """Optional zero-argument callable returning an ephemeral IAM access token."""
    iam_token_provider: Any = None
    """Alias for `token_provider` supplying IAM authentication tokens."""
    memorystore_instance_id: str | None = None
    """Optional Google Cloud Memorystore for Valkey instance resource identifier."""
    max_connections: int = 32
    """Maximum pooled sockets maintained per target node."""
    broker: Any = None
    """Optional shared-memory broker for cross-worker push fan-out."""

    @property
    def uses_tls(self) -> bool:
        """Return True if the URL scheme uses TLS (`valkeys://` or `rediss://`)."""
        return self.url.startswith("valkeys://") or self.url.startswith("rediss://")

    @property
    def effective_cache_capacity(self) -> int:
        """Return the configured LRU client cache capacity."""
        if self.client_cache_capacity != 4096:
            return self.client_cache_capacity
        if self.max_cached_keys != 4096:
            return self.max_cached_keys
        return self.client_cache_capacity

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

    @property
    def effective_token_provider(self) -> Callable[[], Any] | None:
        """Return the configured IAM token provider callable."""
        provider = self.token_provider if self.token_provider is not None else self.iam_token_provider
        return provider if callable(provider) else None

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
            ValkeyCore.encode_hello3_handshake(
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
            cmds.append(ValkeyCore.encode_resp_command(*tracking_cmd))
        return cmds

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct a ValkeyConfig from a URI."""
        return cls(url=url, **overrides)

    def pool_key(self) -> tuple[Any, ...]:
        """Return the canonical shared-pool fingerprint tuple."""
        memorystore_id = self.memorystore_instance_id or ""
        if (
            not self.username
            and self.protocol == 3
            and self.topology == "standalone"
            and not self.effective_sentinel_master_name
            and not self.sentinel_nodes
            and not self.effective_cluster_enabled
            and not self.cluster_nodes
            and self.effective_tracking_enabled
            and not self.client_tracking_prefixes
            and not self.effective_iam_enabled
            and not memorystore_id
        ):
            return (self.dialect, self.url)
        return (
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

    def to_redis_config(self) -> Any:
        """Convert to equivalent configuration object."""
        return self
