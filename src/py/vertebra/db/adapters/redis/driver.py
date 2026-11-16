"""Synchronous and asynchronous native Redis drivers powered by vertebra._redis."""

from __future__ import annotations

import collections
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, cast

from vertebra.db.adapters.redis.config import RedisConfig
from vertebra.db.adapters.redis.core import (
    PubSubMessage,
    hash_slot,
)
from vertebra.db.driver import REDIS_DIALECT_TAG, CommonDriverMixin
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra._redis import (
        AsyncRedisClient,
        AsyncRedisListener,
        AsyncRedisPipeline,
        SyncRedisClient,
        SyncRedisListener,
        SyncRedisPipeline,
    )
    from vertebra.db.adapters.redis.notify import (
        AsyncRedisNotificationSource,
        RedisNotifyConfig,
        SyncRedisNotificationSource,
    )

__all__ = (
    "AsyncRedisDriver",
    "SyncRedisDriver",
)


def _reject_memory_url(url: str) -> None:
    """Validate that the given URL is a supported Redis scheme."""
    if url.startswith("memory://"):
        msg = "Redis driver does not support memory:// URLs"
        raise ImproperConfigurationError(msg)
    if url.startswith("valkey://") or url.startswith("valkeys://"):
        msg = "valkey:// URLs are not supported by the Redis driver; use vertebra.db.adapters.valkey instead."
        raise ImproperConfigurationError(msg)


class SyncRedisDriver(CommonDriverMixin[RedisConfig]):
    """Synchronous native Redis driver powered by vertebra._redis."""

    dialect: ClassVar[str] = "redis"
    dialect_tag: ClassVar[int] = REDIS_DIALECT_TAG

    __slots__ = (
        "_active_listener",
        "_client",
        "_invalidation_listeners",
        "_master_address",
        "_pubsub_queue",
    )

    def __init__(
        self,
        config: RedisConfig | None = None,
        *,
        broker: Any = None,
        socket_factory: Any = None,
    ) -> None:
        resolved = config or RedisConfig()
        _reject_memory_url(resolved.url)
        super().__init__(resolved, broker=broker)
        mod = ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
        self._invalidation_listeners: list[Callable[[Sequence[str]], None]] = []
        self._pubsub_queue: collections.deque[PubSubMessage] = collections.deque()
        self._active_listener: SyncRedisListener | None = None

        sentinel_nodes_list = [
            (node[0], int(node[1]))
            if isinstance(node, (tuple, list))
            else (node.split(":")[0], int(node.split(":")[1]))
            for node in resolved.sentinel_nodes
        ]
        cluster_nodes_list = [
            (node[0], int(node[1]))
            if isinstance(node, (tuple, list))
            else (node.split(":")[0], int(node.split(":")[1]))
            for node in resolved.cluster_nodes
        ]

        self._client: SyncRedisClient = mod.SyncRedisClient(
            url=resolved.url,
            username=resolved.username,
            password=resolved.password,
            client_name=resolved.client_name,
            protocol=resolved.protocol,
            topology=resolved.topology,
            sentinel_master_name=resolved.effective_sentinel_master_name,
            sentinel_nodes=sentinel_nodes_list,
            cluster_nodes=cluster_nodes_list,
            max_redirections=resolved.max_redirections,
            enable_client_side_caching=resolved.enable_client_side_caching,
            client_tracking_prefixes=list(resolved.client_tracking_prefixes),
            client_cache_capacity=resolved.effective_cache_capacity,
            client_cache_ttl_seconds=resolved.client_cache_ttl_seconds,
            enable_gcp_iam_auth=resolved.effective_iam_enabled,
            token_cache_capsule=None,
            max_connections=resolved.max_connections,
        )

    @property
    def _config(self) -> RedisConfig:
        """Return the active RedisConfig."""
        return self.config

    @property
    def cached_keys(self) -> tuple[str, ...]:
        """Return the keys currently tracked in the local RESP3 client-side cache."""
        keys = self._client.cached_keys()
        if self._invalidation_listeners:
            invalidations = self._client.drain_invalidations()
            if invalidations:
                for listener in self._invalidation_listeners:
                    listener(invalidations)
        return tuple(keys)

    @property
    def slot_map(self) -> dict[int, tuple[str, int]]:
        """Return the cluster hash-slot to (host, port) mapping table."""
        return {}

    def attach_broker(self, broker: Any) -> None:
        """Attach a broker for push event forwarding."""
        self._broker = broker

    @staticmethod
    def hash_slot(key: str | bytes) -> int:
        """Compute the CRC16-XMODEM hash slot (0..16383) for a key."""
        return hash_slot(key)

    def add_invalidation_listener(self, listener: Callable[[Sequence[str]], None]) -> None:
        """Register an invalidation callback for client-side caching invalidation notifications."""
        self._invalidation_listeners.append(listener)

    def handle_invalidation_push(self, keys: Sequence[str | bytes] | None) -> int:
        """Process an invalidation push frame."""
        invalidations = self._client.drain_invalidations()
        if self._invalidation_listeners and invalidations:
            for listener in self._invalidation_listeners:
                listener(invalidations)
        return len(invalidations)

    def discover_sentinel_master(self) -> tuple[str, int]:
        """Query Redis Sentinel nodes to locate the active primary."""
        name = self.config.effective_sentinel_master_name
        if not name:
            msg = "Sentinel master discovery requires sentinel_master_name"
            raise ImproperConfigurationError(msg)
        for node in self.config.sentinel_nodes:
            h = node[0] if isinstance(node, (tuple, list)) else node.split(":")[0]
            p = int(node[1]) if isinstance(node, (tuple, list)) else int(node.split(":")[1])
            mod = ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
            sentinel_client = mod.SyncRedisClient(
                url=f"redis://{h}:{p}/0",
                client_name=self.config.client_name,
                protocol=self.config.protocol,
                topology="standalone",
            )
            try:
                raw = sentinel_client.execute_command(["SENTINEL", "get-master-addr-by-name", name])
                if isinstance(raw, (list, tuple)) and len(raw) >= 2:
                    m_host = raw[0].decode("utf-8") if isinstance(raw[0], bytes) else str(raw[0])
                    m_port = int(raw[1])
                    self._master_address = (m_host, m_port)
                    self._client.close()
                    self._client = mod.SyncRedisClient(
                        url=f"redis://{m_host}:{m_port}/0",
                        username=self.config.username,
                        password=self.config.password,
                        client_name=self.config.client_name,
                        protocol=self.config.protocol,
                        topology="standalone",
                        max_connections=self.config.max_connections,
                    )
                    return m_host, m_port
            except Exception:
                continue
            finally:
                sentinel_client.close()
        msg = f"Failed to discover Sentinel master '{name}' across seed nodes"
        raise ImproperConfigurationError(msg)

    def get(self, key: str) -> bytes | None:
        """Fetch the value associated with key from Redis."""
        return self._client.get(key)

    def set(
        self,
        key: str,
        value: bytes | str,
        *,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> None:
        """Store key and value in Redis with optional expiration or conditional flags."""
        self._client.set(key, value, ex=ex, px=px, nx=nx, xx=xx)

    def delete(self, key: str) -> bool:
        """Delete key from Redis, returning True if key was removed."""
        return bool(self._client.delete(key))

    def unlink(self, *keys: str) -> int:
        """Asynchronously unlink keys from Redis in background threads."""
        return self._client.unlink(list(keys))

    def pttl(self, key: str) -> int:
        """Return the remaining time to live of key in milliseconds."""
        return self._client.pttl(key)

    def flushdb(self) -> None:
        """Remove all keys from the current database."""
        self._client.flushdb()

    def scan(
        self,
        cursor: int = 0,
        *,
        match: str | None = None,
        count: int = 500,
    ) -> tuple[int, list[str]]:
        """Incrementally iterate over matching keys."""
        next_cursor, raw_keys = self._client.scan(cursor=cursor, match_pattern=match, count=count)
        str_keys = [k.decode("utf-8", errors="replace") if isinstance(k, bytes) else str(k) for k in raw_keys]
        return next_cursor, str_keys

    def eval_script(
        self,
        script: str,
        keys: Sequence[str] = (),
        args: Sequence[bytes | str | int | float] = (),
    ) -> Any:
        """Execute a Lua script via EVALSHA with automatic EVAL fallback."""
        return self._client.eval_script(script, list(keys), list(args))

    def execute_command(self, *parts: str | bytes | int) -> Any:
        """Execute a raw Redis command."""
        return self._client.execute_command(list(parts))

    def pipeline(self, atomic: bool = False) -> SyncRedisPipeline:
        """Create a new command pipeline."""
        return self._client.pipeline(atomic=atomic)

    def execute_pipeline(self, commands: Sequence[Sequence[bytes | str | int]]) -> list[Any]:
        """Execute a batch of commands in a single pipeline."""
        pipe = self._client.pipeline(atomic=False)
        for cmd in commands:
            pipe.execute_command(list(cmd))
        return cast("list[Any]", pipe.results())

    def publish(self, channel: str, message: bytes | str) -> int:
        """Publish a message to channel."""
        return self._client.publish(channel, message)

    def spublish(self, shard_channel: str, message: bytes | str) -> int:
        """Publish a message to a sharded pub/sub channel."""
        return self._client.spublish(shard_channel, message)

    def subscribe(self, *channels: str) -> None:
        """Subscribe to channels."""
        if self._active_listener is None:
            self._active_listener = self._client.listener()
        self._active_listener.subscribe(list(channels))

    def psubscribe(self, *patterns: str) -> None:
        """Subscribe to channel patterns."""
        if self._active_listener is None:
            self._active_listener = self._client.listener()
        self._active_listener.psubscribe(list(patterns))

    def ssubscribe(self, *shard_channels: str) -> None:
        """Subscribe to cluster shard channels."""
        if self._active_listener is None:
            self._active_listener = self._client.listener()
        self._active_listener.ssubscribe(list(shard_channels))

    def unsubscribe(self, *channels: str) -> None:
        """Unsubscribe from channels."""
        if self._active_listener is not None:
            self._active_listener.unsubscribe(list(channels))

    def punsubscribe(self, *patterns: str) -> None:
        """Unsubscribe from channel patterns."""
        if self._active_listener is not None:
            self._active_listener.punsubscribe(list(patterns))

    def sunsubscribe(self, *shard_channels: str) -> None:
        """Unsubscribe from cluster shard channels."""
        if self._active_listener is not None:
            self._active_listener.sunsubscribe(list(shard_channels))

    def drain_messages(self) -> list[PubSubMessage]:
        """Drain received pub/sub messages from the active listener."""
        messages: list[PubSubMessage] = []
        if self._active_listener is not None:
            while True:
                item = self._active_listener.next_message(timeout_ms=15 if not messages else 5)
                if item is None:
                    break
                kind, ch, payload, pat = item
                msg = PubSubMessage(kind=kind, channel=ch, payload=payload, pattern=pat)
                messages.append(msg)
                if self._broker is not None:
                    self._broker.publish(ch, payload)
        return messages

    def reconnect(self, password: str | None = None) -> None:
        """Reconnect to Redis with an optional updated password."""
        self._client.reconnect(password=password)

    def notification_source(
        self,
        config: RedisNotifyConfig | None = None,
        *,
        change_feed: Any = None,
    ) -> SyncRedisNotificationSource:
        """Create a synchronous notification source bound to this driver."""
        from vertebra.db.adapters.redis.notify import SyncRedisNotificationSource

        return SyncRedisNotificationSource(self, config=config, change_feed=change_feed)

    def close(self) -> None:
        """Close connections and release client resources."""
        if self._active_listener is not None:
            self._active_listener.close()
            self._active_listener = None
        self._client.close()


class AsyncRedisDriver(CommonDriverMixin[RedisConfig]):
    """Asynchronous native Redis driver powered by vertebra._redis."""

    dialect: ClassVar[str] = "redis"
    dialect_tag: ClassVar[int] = REDIS_DIALECT_TAG

    __slots__ = (
        "_active_listener",
        "_client",
        "_invalidation_listeners",
        "_master_address",
        "_pubsub_queue",
    )

    def __init__(
        self,
        config: RedisConfig | None = None,
        *,
        broker: Any = None,
    ) -> None:
        resolved = config or RedisConfig()
        _reject_memory_url(resolved.url)
        super().__init__(resolved, broker=broker)
        mod = ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
        self._invalidation_listeners: list[Callable[[Sequence[str]], None]] = []
        self._pubsub_queue: collections.deque[PubSubMessage] = collections.deque()
        self._active_listener: AsyncRedisListener | None = None

        sentinel_nodes_list = [
            (node[0], int(node[1]))
            if isinstance(node, (tuple, list))
            else (node.split(":")[0], int(node.split(":")[1]))
            for node in resolved.sentinel_nodes
        ]
        cluster_nodes_list = [
            (node[0], int(node[1]))
            if isinstance(node, (tuple, list))
            else (node.split(":")[0], int(node.split(":")[1]))
            for node in resolved.cluster_nodes
        ]

        self._client: AsyncRedisClient = mod.AsyncRedisClient(
            url=resolved.url,
            username=resolved.username,
            password=resolved.password,
            client_name=resolved.client_name,
            protocol=resolved.protocol,
            topology=resolved.topology,
            sentinel_master_name=resolved.effective_sentinel_master_name,
            sentinel_nodes=sentinel_nodes_list,
            cluster_nodes=cluster_nodes_list,
            max_redirections=resolved.max_redirections,
            enable_client_side_caching=resolved.enable_client_side_caching,
            client_tracking_prefixes=list(resolved.client_tracking_prefixes),
            client_cache_capacity=resolved.effective_cache_capacity,
            client_cache_ttl_seconds=resolved.client_cache_ttl_seconds,
            enable_gcp_iam_auth=resolved.effective_iam_enabled,
            token_cache_capsule=None,
            max_connections=resolved.max_connections,
        )

    @property
    def _config(self) -> RedisConfig:
        """Return the active RedisConfig."""
        return self.config

    @property
    def cached_keys(self) -> tuple[str, ...]:
        """Return the keys currently tracked in the local RESP3 client-side cache."""
        keys = self._client.cached_keys()
        if self._invalidation_listeners:
            invalidations = self._client.drain_invalidations()
            if invalidations:
                for listener in self._invalidation_listeners:
                    listener(invalidations)
        return tuple(keys)

    @property
    def slot_map(self) -> dict[int, tuple[str, int]]:
        """Return the cluster hash-slot to (host, port) mapping table."""
        return {}

    def attach_broker(self, broker: Any) -> None:
        """Attach a broker for push event forwarding."""
        self._broker = broker

    @staticmethod
    def hash_slot(key: str | bytes) -> int:
        """Compute the CRC16-XMODEM hash slot (0..16383) for a key."""
        return hash_slot(key)

    def add_invalidation_listener(self, listener: Callable[[Sequence[str]], None]) -> None:
        """Register an invalidation callback for client-side caching invalidation notifications."""
        self._invalidation_listeners.append(listener)

    def handle_invalidation_push(self, keys: Sequence[str | bytes] | None) -> int:
        """Process an invalidation push frame."""
        invalidations = self._client.drain_invalidations()
        if self._invalidation_listeners and invalidations:
            for listener in self._invalidation_listeners:
                listener(invalidations)
        return len(invalidations)

    async def discover_sentinel_master(self) -> tuple[str, int]:
        """Query Redis Sentinel nodes asynchronously to locate the active primary."""
        name = self.config.effective_sentinel_master_name
        if not name:
            msg = "Sentinel master discovery requires sentinel_master_name"
            raise ImproperConfigurationError(msg)
        for node in self.config.sentinel_nodes:
            h = node[0] if isinstance(node, (tuple, list)) else node.split(":")[0]
            p = int(node[1]) if isinstance(node, (tuple, list)) else int(node.split(":")[1])
            mod = ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
            sentinel_client = mod.AsyncRedisClient(
                url=f"redis://{h}:{p}/0",
                client_name=self.config.client_name,
                protocol=self.config.protocol,
                topology="standalone",
            )
            try:
                raw = await sentinel_client.execute_command(["SENTINEL", "get-master-addr-by-name", name])
                if isinstance(raw, (list, tuple)) and len(raw) >= 2:
                    m_host = raw[0].decode("utf-8") if isinstance(raw[0], bytes) else str(raw[0])
                    m_port = int(raw[1])
                    self._master_address = (m_host, m_port)
                    self._client.close()
                    self._client = mod.AsyncRedisClient(
                        url=f"redis://{m_host}:{m_port}/0",
                        username=self.config.username,
                        password=self.config.password,
                        client_name=self.config.client_name,
                        protocol=self.config.protocol,
                        topology="standalone",
                        max_connections=self.config.max_connections,
                    )
                    return m_host, m_port
            except Exception:
                continue
            finally:
                sentinel_client.close()
        msg = f"Failed to discover Sentinel master '{name}' across seed nodes"
        raise ImproperConfigurationError(msg)

    async def get(self, key: str) -> bytes | None:
        """Fetch the value associated with key from Redis."""
        return cast("bytes | None", await self._client.get(key))

    async def set(
        self,
        key: str,
        value: bytes | str,
        *,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> None:
        """Store key and value in Redis with optional expiration or conditional flags."""
        await self._client.set(key, value, ex=ex, px=px, nx=nx, xx=xx)

    async def delete(self, key: str) -> bool:
        """Delete key from Redis, returning True if key was removed."""
        return bool(await self._client.delete(key))

    async def unlink(self, *keys: str) -> int:
        """Asynchronously unlink keys from Redis in background threads."""
        return cast("int", await self._client.unlink(list(keys)))

    async def pttl(self, key: str) -> int:
        """Return the remaining time to live of key in milliseconds."""
        return cast("int", await self._client.pttl(key))

    async def flushdb(self) -> None:
        """Remove all keys from the current database."""
        await self._client.flushdb()

    async def scan(
        self,
        cursor: int = 0,
        *,
        match: str | None = None,
        count: int = 500,
    ) -> tuple[int, list[str]]:
        """Incrementally iterate over matching keys."""
        next_cursor, raw_keys = await self._client.scan(cursor=cursor, match_pattern=match, count=count)
        str_keys = [k.decode("utf-8", errors="replace") if isinstance(k, bytes) else str(k) for k in raw_keys]
        return next_cursor, str_keys

    async def eval_script(
        self,
        script: str,
        keys: Sequence[str] = (),
        args: Sequence[bytes | str | int | float] = (),
    ) -> Any:
        """Execute a Lua script via EVALSHA with automatic EVAL fallback."""
        return await self._client.eval_script(script, list(keys), list(args))

    async def execute_command(self, *parts: str | bytes | int) -> Any:
        """Execute a raw Redis command."""
        return await self._client.execute_command(list(parts))

    def pipeline(self, atomic: bool = False) -> AsyncRedisPipeline:
        """Create a new asynchronous command pipeline."""
        return self._client.pipeline(atomic=atomic)

    async def execute_pipeline(self, commands: Sequence[Sequence[bytes | str | int]]) -> list[Any]:
        """Execute a batch of commands in a single asynchronous pipeline."""
        pipe = self._client.pipeline(atomic=False)
        for cmd in commands:
            pipe.execute_command(list(cmd))
        return cast("list[Any]", await pipe.results())

    async def publish(self, channel: str, message: bytes | str) -> int:
        """Publish a message to channel."""
        return cast("int", await self._client.publish(channel, message))

    async def spublish(self, shard_channel: str, message: bytes | str) -> int:
        """Publish a message to a sharded pub/sub channel."""
        return cast("int", await self._client.spublish(shard_channel, message))

    async def reconnect(self, password: str | None = None) -> None:
        """Reconnect to Redis with an optional updated password."""
        self._client.reconnect(password=password)

    def notification_source(
        self,
        config: RedisNotifyConfig | None = None,
        *,
        change_feed: Any = None,
    ) -> AsyncRedisNotificationSource:
        """Create an asynchronous notification source bound to this driver."""
        from vertebra.db.adapters.redis.notify import AsyncRedisNotificationSource

        return AsyncRedisNotificationSource(self, config=config, change_feed=change_feed)

    async def close(self) -> None:
        """Close connections and release client resources."""
        if self._active_listener is not None:
            self._active_listener.close()
            self._active_listener = None
        self._client.close()
