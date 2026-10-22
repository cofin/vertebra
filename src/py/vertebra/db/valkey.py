"""Valkey 8.0+ / 9.0+ RESP3 driver, Cluster/Sentinel support, and GCP Memorystore for Valkey IAM."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.db.redis import (
    AsyncRedisDriver,
    RedisConfig,
    SyncRedisDriver,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

__all__ = (
    "AsyncValkeyDriver",
    "SyncValkeyDriver",
    "ValkeyConfig",
)


class ValkeyConfig(RedisConfig, frozen=True, gc=False, kw_only=True):
    """Configuration for Valkey 8.0+ / 9.0+ standalone, Sentinel, Cluster, and GCP Memorystore."""

    url: str = "valkey://localhost:6379/0"
    client_name: str = "vertebra-valkey"
    enable_client_side_caching: bool = True
    client_tracking_prefixes: tuple[str, ...] = ()
    enable_gcp_memorystore_iam: bool = False
    memorystore_instance_id: str | None = None

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

    def to_redis_config(self) -> RedisConfig:
        """Convert to equivalent RedisConfig."""
        return self


class SyncValkeyDriver(SyncRedisDriver):
    """Synchronous Valkey 8.0+/9.0+ driver with RESP3 client-side cache tracking."""

    __slots__ = ("_client_cache", "_invalidation_listeners", "_valkey_config")

    def __init__(self, config: ValkeyConfig | None = None) -> None:
        self._valkey_config = config or ValkeyConfig()
        super().__init__(self._valkey_config)
        self._client_cache: dict[str, bytes] = {}
        self._invalidation_listeners: list[Callable[[Sequence[str]], None]] = []
        self.register_push_callback(self._on_push_frame)

    @property
    def config(self) -> ValkeyConfig:
        """Return the active ValkeyConfig."""
        return self._valkey_config

    @property
    def cached_keys(self) -> tuple[str, ...]:
        """Return the keys currently tracked in the local RESP3 client-side cache."""
        return tuple(self._client_cache.keys())

    def add_invalidation_listener(self, listener: Callable[[Sequence[str]], None]) -> None:
        """Register an invalidation callback for client-side caching invalidation notifications."""
        self._invalidation_listeners.append(listener)

    def handle_invalidation_push(self, keys: Sequence[str | bytes]) -> int:
        """Process a RESP3 `invalidate` push frame, evicting local entries and invoking listeners."""
        normalized: list[str] = [k.decode("utf-8") if isinstance(k, bytes) else str(k) for k in keys]
        evicted = 0
        for k in normalized:
            if self._client_cache.pop(k, None) is not None:
                evicted += 1
        for listener in self._invalidation_listeners:
            listener(normalized)
        return evicted

    def _on_push_frame(self, items: list[Any]) -> None:
        """Route incoming RESP3 push frames to invalidation handlers."""
        if items and items[0] in ("invalidate", b"invalidate") and len(items) > 1:
            raw_keys = items[1]
            if isinstance(raw_keys, list):
                self.handle_invalidation_push(raw_keys)

    def get(self, key: str) -> bytes | None:
        """Fetch a key value, consulting the RESP3 client-side cache when enabled."""
        if self._valkey_config.enable_client_side_caching and key in self._client_cache:
            return self._client_cache[key]
        val = super().get(key)
        if val is not None and self._valkey_config.enable_client_side_caching:
            self._client_cache[key] = val
        return val

    def set(self, key: str, value: bytes | str, *, ex: int | None = None) -> None:
        """Set a key value and update/invalidate local client-side cache state."""
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        if self._valkey_config.enable_client_side_caching:
            self._client_cache[key] = raw
        super().set(key, raw, ex=ex)

    def delete(self, key: str) -> bool:
        """Delete a key and evict from client-side cache."""
        self._client_cache.pop(key, None)
        return super().delete(key)

    def flushdb(self) -> None:
        """Clear the store and client-side cache."""
        self._client_cache.clear()
        super().flushdb()


class AsyncValkeyDriver(AsyncRedisDriver):
    """Asynchronous Valkey 8.0+/9.0+ RESP3 driver with client-side cache tracking."""

    __slots__ = ("_client_cache", "_invalidation_listeners", "_valkey_config")

    def __init__(self, config: ValkeyConfig | None = None) -> None:
        self._valkey_config = config or ValkeyConfig()
        super().__init__(self._valkey_config)
        self._client_cache: dict[str, bytes] = {}
        self._invalidation_listeners: list[Callable[[Sequence[str]], None]] = []
        self.register_push_callback(self._on_push_frame)

    @property
    def config(self) -> ValkeyConfig:
        """Return the active ValkeyConfig."""
        return self._valkey_config

    @property
    def cached_keys(self) -> tuple[str, ...]:
        """Return the keys currently tracked in the local RESP3 client-side cache."""
        return tuple(self._client_cache.keys())

    def add_invalidation_listener(self, listener: Callable[[Sequence[str]], None]) -> None:
        """Register an invalidation callback for client-side caching invalidation notifications."""
        self._invalidation_listeners.append(listener)

    def handle_invalidation_push(self, keys: Sequence[str | bytes]) -> int:
        """Process a RESP3 `invalidate` push frame, evicting local entries and invoking listeners."""
        normalized: list[str] = [k.decode("utf-8") if isinstance(k, bytes) else str(k) for k in keys]
        evicted = 0
        for k in normalized:
            if self._client_cache.pop(k, None) is not None:
                evicted += 1
        for listener in self._invalidation_listeners:
            listener(normalized)
        return evicted

    def _on_push_frame(self, items: list[Any]) -> None:
        """Route incoming RESP3 push frames to invalidation handlers."""
        if items and items[0] in ("invalidate", b"invalidate") and len(items) > 1:
            raw_keys = items[1]
            if isinstance(raw_keys, list):
                self.handle_invalidation_push(raw_keys)

    async def get(self, key: str) -> bytes | None:
        """Asynchronously fetch a key value, consulting client-side cache when enabled."""
        if self._valkey_config.enable_client_side_caching and key in self._client_cache:
            return self._client_cache[key]
        val = await super().get(key)
        if val is not None and self._valkey_config.enable_client_side_caching:
            self._client_cache[key] = val
        return val

    async def set(self, key: str, value: bytes | str, *, ex: int | None = None) -> None:
        """Asynchronously set a key value and update client cache."""
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        if self._valkey_config.enable_client_side_caching:
            self._client_cache[key] = raw
        await super().set(key, raw, ex=ex)

    async def delete(self, key: str) -> bool:
        """Asynchronously delete a key and evict from client cache."""
        self._client_cache.pop(key, None)
        return await super().delete(key)

    async def flushdb(self) -> None:
        """Clear the store and client cache."""
        self._client_cache.clear()
        await super().flushdb()
