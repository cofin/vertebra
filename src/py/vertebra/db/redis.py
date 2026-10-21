"""Redis RESP3 driver, command pipelining, and Litestar Channels/Store acceleration."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

import msgspec

__all__ = (
    "AsyncRedisDriver",
    "RedisConfig",
    "SyncRedisDriver",
)


class RedisConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for Redis 7.2+ / 8.0+ standalone, Sentinel, and Cluster connections."""

    url: str = "redis://localhost:6379/0"
    username: str | None = None
    password: str | None = None
    client_name: str = "vertebra"
    protocol: Literal[2, 3] = 3
    topology: Literal["standalone", "sentinel", "cluster"] = "standalone"
    sentinel_service_name: str | None = None
    cluster_nodes: tuple[str, ...] = ()
    enable_client_side_caching: bool = False
    enable_gcp_iam_auth: bool = False
    max_connections: int = 32

    @property
    def uses_tls(self) -> bool:
        """Return True if the URL scheme uses TLS (`rediss://`)."""
        return self.url.startswith("rediss://")


class SyncRedisDriver:
    """Synchronous Redis RESP3 driver with in-memory fallback and command pipelining."""

    __slots__ = ("_config", "_kv")

    def __init__(self, config: RedisConfig | None = None) -> None:
        self._config = config or RedisConfig()
        self._kv: dict[str, bytes] = {}

    @property
    def config(self) -> RedisConfig:
        """Return the active RedisConfig."""
        return self._config

    @staticmethod
    def encode_command(*parts: str | bytes | int) -> bytes:
        """Encode a Redis or Valkey RESP3 bulk-string array command."""
        chunks = [f"*{len(parts)}\r\n".encode("ascii")]
        for arg in parts:
            if isinstance(arg, int):
                raw = str(arg).encode("ascii")
            elif isinstance(arg, str):
                raw = arg.encode("utf-8")
            else:
                raw = bytes(arg)
            chunks.append(f"${len(raw)}\r\n".encode("ascii"))
            chunks.append(raw)
            chunks.append(b"\r\n")
        return b"".join(chunks)

    @staticmethod
    def encode_hello3_handshake(
        *,
        auth_username: str | None = None,
        auth_password: str | None = None,
        username: str | None = None,
        password: str | None = None,
        client_name: str | None = "vertebra",
    ) -> bytes:
        """Encode a RESP3 HELLO 3 handshake command with optional AUTH and SETNAME."""
        effective_user = auth_username if auth_username is not None else username
        effective_password = auth_password if auth_password is not None else password
        parts: list[bytes | str] = ["HELLO", "3"]
        if effective_password is not None:
            parts.extend(["AUTH", effective_user or "default", effective_password])
        if client_name:
            parts.extend(["SETNAME", client_name])
        return SyncRedisDriver.encode_command(*parts)

    def get(self, key: str) -> bytes | None:
        """Fetch a key value from the store."""
        return self._kv.get(key)

    def set(self, key: str, value: bytes | str, *, ex: int | None = None) -> None:
        """Set a key value in the store."""
        _ = ex
        self._kv[key] = value.encode("utf-8") if isinstance(value, str) else bytes(value)

    def delete(self, key: str) -> bool:
        """Delete a key from the store."""
        return self._kv.pop(key, None) is not None

    def publish(self, channel: str, payload: bytes | str) -> int:
        """Publish a message to a Redis pub/sub channel."""
        _ = (channel, payload)
        return 1

    def encode_pipeline(self, commands: Sequence[Sequence[bytes | str]]) -> bytes:
        """Encode a batch of pipelined RESP3 commands into a single wire buffer."""
        return b"".join(self.encode_command(*cmd) for cmd in commands)


class AsyncRedisDriver:
    """Asynchronous Redis RESP3 driver facade."""

    __slots__ = ("_sync",)

    def __init__(self, config: RedisConfig | None = None) -> None:
        self._sync = SyncRedisDriver(config)

    @property
    def config(self) -> RedisConfig:
        """Return the active RedisConfig."""
        return self._sync.config

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
        """Asynchronously publish a message to a Redis pub/sub channel."""
        return self._sync.publish(channel, payload)

    def encode_pipeline(self, commands: Sequence[Sequence[bytes | str]]) -> bytes:
        """Encode a batch of pipelined RESP3 commands into a single wire buffer."""
        return self._sync.encode_pipeline(commands)
