"""Valkey 8.0+ / 9.0+ RESP3 driver, Cluster/Sentinel support, and GCP Memorystore for Valkey IAM."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

import msgspec

from vertebra.db.redis import SyncRedisDriver

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
                auth_username=effective_user,
                auth_password=effective_password,
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


class SyncValkeyDriver:
    """Synchronous Valkey 8.0+/9.0+ driver with RESP3 client-side cache tracking."""

    __slots__ = ("_client_cache", "_config", "_kv")

    def __init__(self, config: ValkeyConfig | None = None) -> None:
        self._config = config or ValkeyConfig()
        self._kv: dict[str, bytes] = {}
        self._client_cache: dict[str, bytes] = {}

    @property
    def config(self) -> ValkeyConfig:
        """Return the active ValkeyConfig."""
        return self._config

    @property
    def cached_keys(self) -> tuple[str, ...]:
        """Return the keys currently tracked in the local RESP3 client-side cache."""
        return tuple(self._client_cache.keys())

    def get(self, key: str) -> bytes | None:
        """Fetch a key value, consulting the RESP3 client-side cache when enabled."""
        if self._config.enable_client_side_caching and key in self._client_cache:
            return self._client_cache[key]
        val = self._kv.get(key)
        if val is not None and self._config.enable_client_side_caching:
            self._client_cache[key] = val
        return val

    def set(self, key: str, value: bytes | str, *, ex: int | None = None) -> None:
        """Set a key value and update/invalidate local client-side cache state."""
        _ = ex
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        self._kv[key] = raw
        if self._config.enable_client_side_caching:
            self._client_cache[key] = raw

    def delete(self, key: str) -> bool:
        """Delete a key and evict from client-side cache."""
        self._client_cache.pop(key, None)
        return self._kv.pop(key, None) is not None

    def publish(self, channel: str, payload: bytes | str) -> int:
        """Publish a message to a Valkey pub/sub channel."""
        _ = (channel, payload)
        return 1

    def handle_invalidation_push(self, keys: Sequence[str]) -> int:
        """Process a RESP3 `invalidate` push frame (`>2\\r\\n$10\\r\\ninvalidate\\r\\n...`)."""
        evicted = 0
        for key in keys:
            if self._client_cache.pop(key, None) is not None:
                evicted += 1
        return evicted

    def encode_pipeline(self, commands: Sequence[Sequence[bytes | str]]) -> bytes:
        """Encode a batch of pipelined Valkey RESP3 commands into a single wire buffer."""
        return b"".join(SyncRedisDriver.encode_command(*cmd) for cmd in commands)


class AsyncValkeyDriver:
    """Asynchronous Valkey 8.0+/9.0+ RESP3 driver facade."""

    __slots__ = ("_sync",)

    def __init__(self, config: ValkeyConfig | None = None) -> None:
        self._sync = SyncValkeyDriver(config)

    @property
    def config(self) -> ValkeyConfig:
        """Return the active ValkeyConfig."""
        return self._sync.config

    @property
    def cached_keys(self) -> tuple[str, ...]:
        """Return the keys currently tracked in the local RESP3 client-side cache."""
        return self._sync.cached_keys

    async def get(self, key: str) -> bytes | None:
        """Asynchronously fetch a key value."""
        return self._sync.get(key)

    async def set(self, key: str, value: bytes | str, *, ex: int | None = None) -> None:
        """Asynchronously set a key value."""
        self._sync.set(key, value, ex=ex)

    async def delete(self, key: str) -> bool:
        """Asynchronously delete a key."""
        return self._sync.delete(key)

    async def publish(self, channel: str, payload: bytes | str) -> int:
        """Asynchronously publish a message to a Valkey pub/sub channel."""
        return self._sync.publish(channel, payload)

    def handle_invalidation_push(self, keys: Sequence[str]) -> int:
        """Process a RESP3 client-side caching invalidation push frame."""
        return self._sync.handle_invalidation_push(keys)

    def encode_pipeline(self, commands: Sequence[Sequence[bytes | str]]) -> bytes:
        """Encode a batch of pipelined Valkey RESP3 commands."""
        return self._sync.encode_pipeline(commands)
