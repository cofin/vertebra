"""Valkey 8.0+ / 9.0+ RESP3 driver, Cluster/Sentinel support, and GCP Memorystore for Valkey IAM."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

import msgspec

from vertebra.db.redis import AsyncRedisDriver, SyncRedisDriver

__all__ = (
    "AsyncValkeyDriver",
    "SyncValkeyDriver",
    "ValkeyConfig",
)


class ValkeyConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for Valkey 8.0+ / 9.0+ standalone, Sentinel, Cluster, and GCP Memorystore."""

    url: str = "valkey://localhost:6379/0"
    username: str | None = None
    password: str | None = None
    client_name: str = "vertebra-valkey"
    protocol: Literal[2, 3] = 3
    topology: Literal["standalone", "sentinel", "cluster"] = "standalone"
    sentinel_service_name: str | None = None
    cluster_nodes: tuple[str, ...] = ()
    enable_client_side_caching: bool = True
    client_tracking_prefixes: tuple[str, ...] = ()
    enable_gcp_memorystore_iam: bool = False
    memorystore_instance_id: str | None = None
    max_connections: int = 32

    @property
    def uses_tls(self) -> bool:
        """Return True if the URL scheme uses TLS (`valkeys://`)."""
        return self.url.startswith("valkeys://")

    def build_handshake_commands(self, *, iam_token: str | None = None) -> list[bytes]:
        """Build the initial RESP3 HELLO 3 + optional CLIENT TRACKING wire commands."""
        effective_password = iam_token if (self.enable_gcp_memorystore_iam and iam_token) else self.password
        effective_user = "default" if self.enable_gcp_memorystore_iam else self.username
        cmds = [
            SyncRedisDriver.encode_hello3_handshake(
                username=effective_user,
                password=effective_password,
                client_name=self.client_name,
            )
        ]
        if self.enable_client_side_caching:
            tracking_cmd: list[bytes | str] = ["CLIENT", "TRACKING", "ON"]
            for prefix in self.client_tracking_prefixes:
                tracking_cmd.extend(["PREFIX", prefix])
            tracking_cmd.append("BCAST")
            cmds.append(SyncRedisDriver.encode_command(*tracking_cmd))
        return cmds


class SyncValkeyDriver(SyncRedisDriver):
    """Synchronous Valkey 8.0+/9.0+ driver with RESP3 client-side cache tracking."""

    __slots__ = ("_client_cache",)

    def __init__(self, config: ValkeyConfig | None = None) -> None:
        super().__init__(config or ValkeyConfig())
        self._client_cache: dict[str, bytes] = {}

    @property
    def config(self) -> ValkeyConfig:
        """Return the active ValkeyConfig."""
        cfg: ValkeyConfig = self._config
        return cfg

    @property
    def cached_keys(self) -> tuple[str, ...]:
        """Return the keys currently tracked in the local RESP3 client-side cache."""
        return tuple(self._client_cache.keys())

    def get(self, key: str) -> bytes | None:
        """Fetch a key value, consulting the RESP3 client-side cache when enabled."""
        self._require_memory_backend()
        if self.config.enable_client_side_caching and key in self._client_cache:
            return self._client_cache[key]
        val = super().get(key)
        if val is not None and self.config.enable_client_side_caching:
            self._client_cache[key] = val
        return val

    def set(self, key: str, value: bytes | str, *, ex: int | None = None) -> None:
        """Set a key value and update/invalidate local client-side cache state."""
        super().set(key, value, ex=ex)
        if self.config.enable_client_side_caching:
            self._client_cache[key] = self._kv[key]

    def delete(self, key: str) -> bool:
        """Delete a key and evict from client-side cache."""
        self._require_memory_backend()
        self._client_cache.pop(key, None)
        return super().delete(key)

    def handle_invalidation_push(self, keys: Sequence[str]) -> int:
        """Process a RESP3 `invalidate` push frame (`>2\\r\\n$10\\r\\ninvalidate\\r\\n...`)."""
        evicted = 0
        for key in keys:
            if self._client_cache.pop(key, None) is not None:
                evicted += 1
        return evicted


class AsyncValkeyDriver(AsyncRedisDriver):
    """Asynchronous Valkey 8.0+/9.0+ RESP3 driver facade."""

    __slots__ = ()

    def __init__(self, config: ValkeyConfig | None = None) -> None:
        self._sync: SyncValkeyDriver = SyncValkeyDriver(config)

    @property
    def config(self) -> ValkeyConfig:
        """Return the active ValkeyConfig."""
        return self._sync.config

    @property
    def cached_keys(self) -> tuple[str, ...]:
        """Return the keys currently tracked in the local RESP3 client-side cache."""
        return self._sync.cached_keys

    def handle_invalidation_push(self, keys: Sequence[str]) -> int:
        """Process a RESP3 client-side caching invalidation push frame."""
        return self._sync.handle_invalidation_push(keys)
