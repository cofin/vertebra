"""Synchronous and asynchronous native Valkey drivers powered by vertebra._valkey."""

from __future__ import annotations

import collections
from collections.abc import AsyncGenerator, Callable, Generator, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, cast

import msgspec

from vertebra.db.adapters.valkey.config import ValkeyConfig
from vertebra.db.adapters.valkey.core import (
    PubSubMessage,
    ValkeyCore,
    hash_slot,
)
from vertebra.db.adapters.valkey.notify import (
    AsyncValkeyNotificationSource,
    SyncValkeyNotificationSource,
    ValkeyNotifyConfig,
)
from vertebra.db.driver import VALKEY_DIALECT_TAG, CommonDriverMixin
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra._valkey import (
        AsyncValkeyClient,
        AsyncValkeyListener,
        AsyncValkeyPipeline,
        SyncValkeyClient,
        SyncValkeyListener,
        SyncValkeyPipeline,
    )

__all__ = (
    "AsyncValkeyDriver",
    "SyncValkeyDriver",
)


def _reject_valkey_url(url: str) -> None:
    """Validate that the given URL is a supported Valkey scheme.

    Args:
        url: URL string to inspect.

    Raises:
        ImproperConfigurationError: When the URL begins with an unsupported scheme.
    """
    lower = url.lower()
    if lower.startswith("memory://"):
        msg = "Valkey driver does not support memory:// URLs"
        raise ImproperConfigurationError(msg)
    if lower.startswith("redis://") or lower.startswith("rediss://"):
        msg = "redis:// URLs are not supported by the Valkey driver; use vertebra.db.adapters.redis instead."
        raise ImproperConfigurationError(msg)


class SyncValkeyDriver(CommonDriverMixin[ValkeyConfig]):
    """Synchronous native Valkey driver powered by vertebra._valkey."""

    dialect: ClassVar[str] = "valkey"
    dialect_tag: ClassVar[int] = VALKEY_DIALECT_TAG

    __slots__ = (
        "_active_listener",
        "_client",
        "_invalidation_listeners",
        "_master_address",
        "_pubsub_queue",
    )

    def __init__(
        self,
        config: ValkeyConfig | None = None,
        *,
        broker: Any = None,
        socket_factory: Any = None,
    ) -> None:
        """Initialize synchronous Valkey driver.

        Args:
            config: Optional ValkeyConfig instance.
            broker: Optional shared-memory broker for fan-out.
            socket_factory: Optional custom socket factory.

        Raises:
            ImproperConfigurationError: When url scheme is unsupported.
            MissingDependencyError: When vertebra-core[valkey] is absent.
        """
        resolved = config or ValkeyConfig()
        _reject_valkey_url(resolved.url)
        super().__init__(resolved, broker=broker)
        self._invalidation_listeners: list[Callable[[Sequence[str]], None]] = []
        self._pubsub_queue: collections.deque[PubSubMessage] = collections.deque()
        self._active_listener: SyncValkeyListener | None = None
        self._client: SyncValkeyClient = self._create_client(resolved)

    @staticmethod
    def _create_client(config: ValkeyConfig) -> SyncValkeyClient:
        """Create a native SyncValkeyClient from ValkeyConfig."""
        mod = ensure_module("vertebra._valkey", package="vertebra-core[valkey]", extra="valkey")
        sentinel_nodes_list = [
            f"{node[0]}:{node[1]}" if isinstance(node, (tuple, list)) else str(node) for node in config.sentinel_nodes
        ]
        cluster_nodes_list = [
            f"{node[0]}:{node[1]}" if isinstance(node, (tuple, list)) else str(node) for node in config.cluster_nodes
        ]
        client_config = mod.ValkeyClientConfig(
            url=config.url,
            username=config.username,
            password=config.password,
            client_name=config.client_name,
            max_redirections=config.max_redirections,
            client_tracking_enabled=(config.enable_client_side_caching or config.enable_client_tracking),
            client_tracking_prefixes=list(config.client_tracking_prefixes),
            client_tracking_capacity=config.effective_cache_capacity,
            client_tracking_ttl_ms=int(config.client_cache_ttl_seconds * 1000)
            if config.client_cache_ttl_seconds
            else None,
            enable_gcp_memorystore_iam=config.effective_iam_enabled,
            token_provider=config.token_provider or config.iam_token_provider,
            cluster_enabled=(config.topology == "cluster" or config.cluster_enabled),
            cluster_nodes=cluster_nodes_list if cluster_nodes_list else None,
            sentinel_service_name=config.effective_sentinel_master_name,
            sentinel_nodes=sentinel_nodes_list if sentinel_nodes_list else None,
            pool_max_size=config.max_connections,
            protocol_resp3=(config.protocol == 3),
            enable_capa_redirect=(config.topology == "cluster" or config.cluster_enabled),
        )
        return cast("SyncValkeyClient", mod.SyncValkeyClient(client_config))

    @property
    def _config(self) -> ValkeyConfig:
        """Return the active ValkeyConfig."""
        return self.config

    @property
    def cached_keys(self) -> tuple[str, ...]:
        """Return the keys currently tracked in the local RESP3 client-side cache."""
        return tuple(self._client.cached_keys())

    @property
    def _client_cache(self) -> collections.OrderedDict[str, Any]:
        """Backward-compatible OrderedDict view of cached keys."""
        return collections.OrderedDict((k, True) for k in self._client.cached_keys())

    @property
    def closed(self) -> bool:
        """Return True when the underlying client is closed."""
        return self._client.closed

    @property
    def _slot_map(self) -> dict[int, tuple[str, int]]:
        return self._client.slot_map()

    @property
    def slot_map(self) -> dict[int, tuple[str, int]]:
        """Return the cluster hash-slot to (host, port) mapping table."""
        return self._client.slot_map()

    def _dispatch_invalidations(self) -> None:
        """Drain invalidation push events and forward to listeners and broker."""
        invalidations = self._client.drain_invalidations()
        if invalidations:
            if self._broker is not None and hasattr(self._broker, "publish_invalidation"):
                self._broker.publish_invalidation(invalidations)
            for listener in self._invalidation_listeners:
                listener(invalidations)

    def attach_broker(self, broker: Any) -> None:
        """Attach a broker for push event forwarding."""
        self._broker = broker

    @staticmethod
    def hash_slot(key: str | bytes) -> int:
        """Compute the CRC16-XMODEM hash slot (0..16383) for a key."""
        return hash_slot(key)

    @staticmethod
    def encode_hello3_handshake(
        *,
        username: str | None = None,
        password: str | None = None,
        client_name: str | None = None,
    ) -> bytes:
        """Encode a RESP3 HELLO 3 handshake wire frame."""
        return ValkeyCore.encode_hello3_handshake(username=username, password=password, client_name=client_name)

    @staticmethod
    def encode_resp_command(*parts: str | bytes | int | float) -> bytes:
        """Encode arbitrary command parts into a RESP3 array of bulk strings."""
        return ValkeyCore.encode_resp_command(*parts)

    def add_invalidation_listener(self, listener: Callable[[Sequence[str]], None]) -> None:
        """Register an invalidation callback for client-side caching invalidation notifications."""
        self._invalidation_listeners.append(listener)

    def handle_invalidation_push(self, keys: Sequence[str | bytes] | None) -> int:
        """Process an invalidation push frame."""
        invalidated = self._client.invalidate_keys(keys)
        self._dispatch_invalidations()
        return invalidated

    def discover_sentinel_master(self) -> tuple[str, int]:
        """Query Valkey Sentinel nodes to locate the active primary."""
        name = self.config.effective_sentinel_master_name
        if not name:
            msg = "Sentinel master discovery requires sentinel_master_name"
            raise ImproperConfigurationError(msg)
        for node in self.config.sentinel_nodes:
            h = node[0] if isinstance(node, (tuple, list)) else node.split(":")[0]
            p = int(node[1]) if isinstance(node, (tuple, list)) else int(node.split(":")[1])
            mod = ensure_module("vertebra._valkey", package="vertebra-core[valkey]", extra="valkey")
            sentinel_cfg = mod.ValkeyClientConfig(
                url=f"valkey://{h}:{p}/0",
                client_name=self.config.client_name,
                protocol_resp3=(self.config.protocol == 3),
            )
            sentinel_client = mod.SyncValkeyClient(sentinel_cfg)
            try:
                raw = sentinel_client.execute_command("SENTINEL", "get-master-addr-by-name", name)
                if isinstance(raw, (list, tuple)) and len(raw) >= 2:
                    m_host = raw[0].decode("utf-8") if isinstance(raw[0], bytes) else str(raw[0])
                    m_port = int(raw[1])
                    self._master_address = (m_host, m_port)
                    self._client.close()
                    master_cfg = mod.ValkeyClientConfig(
                        url=f"valkey://{m_host}:{m_port}/0",
                        username=self.config.username,
                        password=self.config.password,
                        client_name=self.config.client_name,
                        pool_max_size=self.config.max_connections,
                        protocol_resp3=(self.config.protocol == 3),
                    )
                    self._client = mod.SyncValkeyClient(master_cfg)
                    return m_host, m_port
            except Exception:
                continue
            finally:
                sentinel_client.close()
        msg = f"Failed to discover Sentinel master '{name}' across seed nodes"
        raise ImproperConfigurationError(msg)

    def get(self, key: str) -> bytes | None:
        """Fetch the value associated with key from Valkey."""
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
        """Store key and value in Valkey with optional expiration or conditional flags."""
        self._client.set(key, value, ex=ex, px=px, nx=nx, xx=xx)

    def delete(self, key: str) -> bool:
        """Delete key from Valkey, returning True if key was removed."""
        return bool(self._client.delete(key))

    def unlink(self, *keys: str) -> int:
        """Asynchronously unlink keys from Valkey in background threads."""
        return self._client.unlink(*keys)

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
        key_seq = [k.encode("utf-8") if isinstance(k, str) else k for k in keys]
        arg_seq = [a if isinstance(a, bytes) else str(a).encode("utf-8") for a in args]
        return self._client.eval(script, key_seq, arg_seq)

    def execute_command(self, *parts: str | bytes | int) -> Any:
        """Execute a raw Valkey command."""
        res = self._client.execute_command(*parts)
        self._dispatch_invalidations()
        return res

    def pipeline(self, atomic: bool = False) -> SyncValkeyPipeline:
        """Create a new command pipeline."""
        return self._client.pipeline(transaction=atomic)

    def execute_pipeline(self, commands: Sequence[Sequence[bytes | str | int]]) -> list[Any]:
        """Execute a batch of commands in a single pipeline."""
        pipe = self._client.pipeline(transaction=False)
        for cmd in commands:
            pipe.execute_command(*cmd)
        return pipe.execute()

    def hexpire(
        self,
        key: str | bytes,
        ttl_seconds: int,
        *fields: str | bytes | Sequence[str | bytes],
        condition: str | None = None,
    ) -> list[int]:
        """Set relative expiration in seconds for specific hash fields."""
        return self._client.hexpire(key, ttl_seconds, *fields, condition=condition)

    def hpexpire(
        self,
        key: str | bytes,
        ttl_ms: int,
        *fields: str | bytes | Sequence[str | bytes],
        condition: str | None = None,
    ) -> list[int]:
        """Set relative expiration in milliseconds for specific hash fields."""
        return self._client.hpexpire(key, ttl_ms, *fields, condition=condition)

    def hexpireat(
        self,
        key: str | bytes,
        unix_time_seconds: int,
        *fields: str | bytes | Sequence[str | bytes],
        condition: str | None = None,
    ) -> list[int]:
        """Set absolute expiration at Unix timestamp in seconds for specific hash fields."""
        return self._client.hexpireat(key, unix_time_seconds, *fields, condition=condition)

    def hpexpireat(
        self,
        key: str | bytes,
        unix_time_ms: int,
        *fields: str | bytes | Sequence[str | bytes],
        condition: str | None = None,
    ) -> list[int]:
        """Set absolute expiration at Unix timestamp in milliseconds for specific hash fields."""
        return self._client.hpexpireat(key, unix_time_ms, *fields, condition=condition)

    def httl(
        self,
        key: str | bytes,
        *fields: str | bytes | Sequence[str | bytes],
    ) -> list[int]:
        """Return remaining time to live in seconds for specific hash fields."""
        return self._client.httl(key, *fields)

    def hpttl(
        self,
        key: str | bytes,
        *fields: str | bytes | Sequence[str | bytes],
    ) -> list[int]:
        """Return remaining time to live in milliseconds for specific hash fields."""
        return self._client.hpttl(key, *fields)

    def hpersist(
        self,
        key: str | bytes,
        *fields: str | bytes | Sequence[str | bytes],
    ) -> list[int]:
        """Remove expiration on specific hash fields."""
        return self._client.hpersist(key, *fields)

    def publish(self, channel: str, message: bytes | str) -> int:
        """Publish a message to channel."""
        return self._client.publish(channel, message)

    def spublish(self, shard_channel: str, message: bytes | str) -> int:
        """Publish a message to a sharded pub/sub channel."""
        return int(self._client.execute_command("SPUBLISH", shard_channel, message))

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
                item = self._active_listener.next_message(timeout_ms=50 if not messages else 0)
                if item is None:
                    break
                kind = str(item.get("type", "message"))
                ch = str(item.get("channel", ""))
                raw_payload = item.get("data", b"")
                payload = raw_payload if isinstance(raw_payload, bytes) else str(raw_payload).encode("utf-8")
                pat = item.get("pattern")
                pat_str = str(pat) if pat is not None else None
                msg = PubSubMessage(kind=kind, channel=ch, payload=payload, pattern=pat_str)
                messages.append(msg)
                if self._broker is not None:
                    self._broker.publish(ch, payload)
        return messages

    def reconnect(self, password: str | None = None) -> None:
        """Reconnect to Valkey with an optional updated password."""
        self._client.close()
        if password is not None:
            self.config = msgspec.structs.replace(self.config, password=password)
        self._client = self._create_client(self.config)
        self._client.ping()

    def notification_source(
        self,
        config: ValkeyNotifyConfig | None = None,
        *,
        change_feed: Any = None,
    ) -> SyncValkeyNotificationSource:
        """Create a synchronous notification source bound to this driver."""
        return SyncValkeyNotificationSource(self, config=config, change_feed=change_feed)

    def close(self) -> None:
        """Close connections and release client resources."""
        if self._active_listener is not None:
            self._active_listener.close()
            self._active_listener = None
        self._client.close()


class _AsyncValkeySubscriptionStream:
    """Subscription handle supporting both awaiting and async-iteration over (channel, payload)."""

    __slots__ = ("_channels", "_command", "_driver")

    def __init__(self, driver: AsyncValkeyDriver, command: str, channels: Sequence[str]) -> None:
        self._driver = driver
        self._command = command
        self._channels = tuple(channels)

    def __await__(self) -> Generator[Any, None, _AsyncValkeySubscriptionStream]:
        return self._execute_subscribe().__await__()

    async def _execute_subscribe(self) -> _AsyncValkeySubscriptionStream:
        if self._driver._active_listener is None:
            self._driver._active_listener = self._driver._client.listener()
        if self._channels:
            if self._command == "SUBSCRIBE":
                await self._driver._active_listener.subscribe(*self._channels)
            elif self._command == "PSUBSCRIBE":
                await self._driver._active_listener.psubscribe(*self._channels)
            elif self._command == "SSUBSCRIBE":
                await self._driver._active_listener.ssubscribe(*self._channels)
        return self

    def __aiter__(self) -> AsyncGenerator[tuple[str, bytes], None]:
        return self._stream()

    async def _stream(self) -> AsyncGenerator[tuple[str, bytes], None]:
        await self._execute_subscribe()
        listener = self._driver._active_listener
        if listener is None:
            return
        while not self._driver.closed:
            msg = await listener.next_message()
            if msg is None:
                break
            if isinstance(msg, dict):
                raw_data = msg.get("data")
                if raw_data is None:
                    raw_data = msg.get("message", b"")
                payload = raw_data if isinstance(raw_data, bytes) else str(raw_data).encode("utf-8")
                yield (str(msg.get("channel", "")), payload)
            elif isinstance(msg, (tuple, list)):
                raw_data = msg[1]
                payload = raw_data if isinstance(raw_data, bytes) else str(raw_data).encode("utf-8")
                yield (str(msg[0]), payload)

    async def aclose(self) -> None:
        if self._driver._active_listener is not None:
            await self._driver._active_listener.close()
            self._driver._active_listener = None


class AsyncValkeyDriver(CommonDriverMixin[ValkeyConfig]):
    """Asynchronous native Valkey driver powered by vertebra._valkey."""

    dialect: ClassVar[str] = "valkey"
    dialect_tag: ClassVar[int] = VALKEY_DIALECT_TAG

    __slots__ = (
        "_active_listener",
        "_client",
        "_invalidation_listeners",
        "_master_address",
        "_pubsub_queue",
    )

    def __init__(
        self,
        config: ValkeyConfig | None = None,
        *,
        broker: Any = None,
    ) -> None:
        """Initialize asynchronous Valkey driver.

        Args:
            config: Optional ValkeyConfig instance.
            broker: Optional shared-memory broker for fan-out.

        Raises:
            ImproperConfigurationError: When url scheme is unsupported.
            MissingDependencyError: When vertebra-core[valkey] is absent.
        """
        resolved = config or ValkeyConfig()
        _reject_valkey_url(resolved.url)
        super().__init__(resolved, broker=broker)
        self._invalidation_listeners: list[Callable[[Sequence[str]], None]] = []
        self._pubsub_queue: collections.deque[PubSubMessage] = collections.deque()
        self._active_listener: AsyncValkeyListener | None = None
        self._client: AsyncValkeyClient = self._create_client(resolved)

    @staticmethod
    def _create_client(config: ValkeyConfig) -> AsyncValkeyClient:
        """Create a native AsyncValkeyClient from ValkeyConfig."""
        mod = ensure_module("vertebra._valkey", package="vertebra-core[valkey]", extra="valkey")
        sentinel_nodes_list = [
            f"{node[0]}:{node[1]}" if isinstance(node, (tuple, list)) else str(node) for node in config.sentinel_nodes
        ]
        cluster_nodes_list = [
            f"{node[0]}:{node[1]}" if isinstance(node, (tuple, list)) else str(node) for node in config.cluster_nodes
        ]
        client_config = mod.ValkeyClientConfig(
            url=config.url,
            username=config.username,
            password=config.password,
            client_name=config.client_name,
            max_redirections=config.max_redirections,
            client_tracking_enabled=(config.enable_client_side_caching or config.enable_client_tracking),
            client_tracking_prefixes=list(config.client_tracking_prefixes),
            client_tracking_capacity=config.effective_cache_capacity,
            client_tracking_ttl_ms=int(config.client_cache_ttl_seconds * 1000)
            if config.client_cache_ttl_seconds
            else None,
            enable_gcp_memorystore_iam=config.effective_iam_enabled,
            token_provider=config.token_provider or config.iam_token_provider,
            cluster_enabled=(config.topology == "cluster" or config.cluster_enabled),
            cluster_nodes=cluster_nodes_list if cluster_nodes_list else None,
            sentinel_service_name=config.effective_sentinel_master_name,
            sentinel_nodes=sentinel_nodes_list if sentinel_nodes_list else None,
            pool_max_size=config.max_connections,
            protocol_resp3=(config.protocol == 3),
            enable_capa_redirect=(config.topology == "cluster" or config.cluster_enabled),
        )
        return cast("AsyncValkeyClient", mod.AsyncValkeyClient(client_config))

    @property
    def _config(self) -> ValkeyConfig:
        """Return the active ValkeyConfig."""
        return self.config

    @property
    def cached_keys(self) -> tuple[str, ...]:
        """Return the keys currently tracked in the local RESP3 client-side cache."""
        return tuple(self._client.cached_keys())

    @property
    def _client_cache(self) -> collections.OrderedDict[str, Any]:
        """Backward-compatible OrderedDict view of cached keys."""
        return collections.OrderedDict((k, True) for k in self._client.cached_keys())

    @property
    def closed(self) -> bool:
        """Return True when the underlying client is closed."""
        return self._client.closed

    @property
    def _slot_map(self) -> dict[int, tuple[str, int]]:
        return self._client.slot_map()

    @property
    def slot_map(self) -> dict[int, tuple[str, int]]:
        """Return the cluster hash-slot to (host, port) mapping table."""
        return self._client.slot_map()

    def _dispatch_invalidations(self) -> None:
        """Drain invalidation push events and forward to listeners and broker."""
        invalidations = self._client.drain_invalidations()
        if invalidations:
            if self._broker is not None and hasattr(self._broker, "publish_invalidation"):
                self._broker.publish_invalidation(invalidations)
            for listener in self._invalidation_listeners:
                listener(invalidations)

    def attach_broker(self, broker: Any) -> None:
        """Attach a broker for push event forwarding."""
        self._broker = broker

    @staticmethod
    def hash_slot(key: str | bytes) -> int:
        """Compute the CRC16-XMODEM hash slot (0..16383) for a key."""
        return hash_slot(key)

    @staticmethod
    def encode_hello3_handshake(
        *,
        username: str | None = None,
        password: str | None = None,
        client_name: str | None = None,
    ) -> bytes:
        """Encode a RESP3 HELLO 3 handshake wire frame."""
        return ValkeyCore.encode_hello3_handshake(username=username, password=password, client_name=client_name)

    @staticmethod
    def encode_resp_command(*parts: str | bytes | int | float) -> bytes:
        """Encode arbitrary command parts into a RESP3 array of bulk strings."""
        return ValkeyCore.encode_resp_command(*parts)

    def add_invalidation_listener(self, listener: Callable[[Sequence[str]], None]) -> None:
        """Register an invalidation callback for client-side caching invalidation notifications."""
        self._invalidation_listeners.append(listener)

    def handle_invalidation_push(self, keys: Sequence[str | bytes] | None) -> int:
        """Process an invalidation push frame."""
        invalidated = self._client.invalidate_keys(keys)
        self._dispatch_invalidations()
        return invalidated

    async def discover_sentinel_master(self) -> tuple[str, int]:
        """Query Valkey Sentinel nodes asynchronously to locate the active primary."""
        name = self.config.effective_sentinel_master_name
        if not name:
            msg = "Sentinel master discovery requires sentinel_master_name"
            raise ImproperConfigurationError(msg)
        for node in self.config.sentinel_nodes:
            h = node[0] if isinstance(node, (tuple, list)) else node.split(":")[0]
            p = int(node[1]) if isinstance(node, (tuple, list)) else int(node.split(":")[1])
            mod = ensure_module("vertebra._valkey", package="vertebra-core[valkey]", extra="valkey")
            sentinel_cfg = mod.ValkeyClientConfig(
                url=f"valkey://{h}:{p}/0",
                client_name=self.config.client_name,
                protocol_resp3=(self.config.protocol == 3),
            )
            sentinel_client = mod.AsyncValkeyClient(sentinel_cfg)
            try:
                raw = await sentinel_client.execute_command("SENTINEL", "get-master-addr-by-name", name)
                if isinstance(raw, (list, tuple)) and len(raw) >= 2:
                    m_host = raw[0].decode("utf-8") if isinstance(raw[0], bytes) else str(raw[0])
                    m_port = int(raw[1])
                    self._master_address = (m_host, m_port)
                    await self._client.close()
                    master_cfg = mod.ValkeyClientConfig(
                        url=f"valkey://{m_host}:{m_port}/0",
                        username=self.config.username,
                        password=self.config.password,
                        client_name=self.config.client_name,
                        pool_max_size=self.config.max_connections,
                        protocol_resp3=(self.config.protocol == 3),
                    )
                    self._client = mod.AsyncValkeyClient(master_cfg)
                    return m_host, m_port
            except Exception:
                continue
            finally:
                await sentinel_client.close()
        msg = f"Failed to discover Sentinel master '{name}' across seed nodes"
        raise ImproperConfigurationError(msg)

    async def get(self, key: str) -> bytes | None:
        """Fetch the value associated with key from Valkey."""
        return await self._client.get(key)

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
        """Store key and value in Valkey with optional expiration or conditional flags."""
        await self._client.set(key, value, ex=ex, px=px, nx=nx, xx=xx)

    async def delete(self, key: str) -> bool:
        """Delete key from Valkey, returning True if key was removed."""
        return bool(await self._client.delete(key))

    async def unlink(self, *keys: str) -> int:
        """Asynchronously unlink keys from Valkey in background threads."""
        return await self._client.unlink(*keys)

    async def pttl(self, key: str) -> int:
        """Return the remaining time to live of key in milliseconds."""
        return await self._client.pttl(key)

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
        key_seq = [k.encode("utf-8") if isinstance(k, str) else k for k in keys]
        arg_seq = [a if isinstance(a, bytes) else str(a).encode("utf-8") for a in args]
        return await self._client.eval(script, key_seq, arg_seq)

    async def execute_command(self, *parts: str | bytes | int) -> Any:
        """Execute a raw Valkey command."""
        res = await self._client.execute_command(*parts)
        self._dispatch_invalidations()
        return res

    def pipeline(self, atomic: bool = False) -> AsyncValkeyPipeline:
        """Create a new asynchronous command pipeline."""
        return self._client.pipeline(transaction=atomic)

    async def execute_pipeline(self, commands: Sequence[Sequence[bytes | str | int]]) -> list[Any]:
        """Execute a batch of commands in a single asynchronous pipeline."""
        pipe = self._client.pipeline(transaction=False)
        for cmd in commands:
            pipe.execute_command(*cmd)
        return await pipe.execute()

    async def hexpire(
        self,
        key: str | bytes,
        ttl_seconds: int,
        *fields: str | bytes | Sequence[str | bytes],
        condition: str | None = None,
    ) -> list[int]:
        """Set relative expiration in seconds for specific hash fields asynchronously."""
        return await self._client.hexpire(key, ttl_seconds, *fields, condition=condition)

    async def hpexpire(
        self,
        key: str | bytes,
        ttl_ms: int,
        *fields: str | bytes | Sequence[str | bytes],
        condition: str | None = None,
    ) -> list[int]:
        """Set relative expiration in milliseconds for specific hash fields asynchronously."""
        return await self._client.hpexpire(key, ttl_ms, *fields, condition=condition)

    async def hexpireat(
        self,
        key: str | bytes,
        unix_time_seconds: int,
        *fields: str | bytes | Sequence[str | bytes],
        condition: str | None = None,
    ) -> list[int]:
        """Set absolute expiration at Unix timestamp in seconds for specific hash fields asynchronously."""
        return await self._client.hexpireat(key, unix_time_seconds, *fields, condition=condition)

    async def hpexpireat(
        self,
        key: str | bytes,
        unix_time_ms: int,
        *fields: str | bytes | Sequence[str | bytes],
        condition: str | None = None,
    ) -> list[int]:
        """Set absolute expiration at Unix timestamp in milliseconds for specific hash fields asynchronously."""
        return await self._client.hpexpireat(key, unix_time_ms, *fields, condition=condition)

    async def httl(
        self,
        key: str | bytes,
        *fields: str | bytes | Sequence[str | bytes],
    ) -> list[int]:
        """Return remaining time to live in seconds for specific hash fields asynchronously."""
        return await self._client.httl(key, *fields)

    async def hpttl(
        self,
        key: str | bytes,
        *fields: str | bytes | Sequence[str | bytes],
    ) -> list[int]:
        """Return remaining time to live in milliseconds for specific hash fields asynchronously."""
        return await self._client.hpttl(key, *fields)

    async def hpersist(
        self,
        key: str | bytes,
        *fields: str | bytes | Sequence[str | bytes],
    ) -> list[int]:
        """Remove expiration on specific hash fields asynchronously."""
        return await self._client.hpersist(key, *fields)

    async def publish(self, channel: str, message: bytes | str) -> int:
        """Publish a message to channel."""
        return await self._client.publish(channel, message)

    async def spublish(self, shard_channel: str, message: bytes | str) -> int:
        """Publish a message to a sharded pub/sub channel."""
        return int(await self._client.execute_command("SPUBLISH", shard_channel, message))

    def subscribe(self, *channels: str) -> _AsyncValkeySubscriptionStream:
        """Subscribe to channels asynchronously."""
        return _AsyncValkeySubscriptionStream(self, "SUBSCRIBE", channels)

    def psubscribe(self, *patterns: str) -> _AsyncValkeySubscriptionStream:
        """Subscribe to channel patterns asynchronously."""
        return _AsyncValkeySubscriptionStream(self, "PSUBSCRIBE", patterns)

    def ssubscribe(self, *shard_channels: str) -> _AsyncValkeySubscriptionStream:
        """Subscribe to cluster shard channels asynchronously."""
        return _AsyncValkeySubscriptionStream(self, "SSUBSCRIBE", shard_channels)

    async def unsubscribe(self, *channels: str) -> None:
        """Unsubscribe from channels asynchronously."""
        if self._active_listener is not None:
            await self._active_listener.unsubscribe(*channels)

    async def punsubscribe(self, *patterns: str) -> None:
        """Unsubscribe from channel patterns asynchronously."""
        if self._active_listener is not None:
            await self._active_listener.punsubscribe(*patterns)

    async def sunsubscribe(self, *shard_channels: str) -> None:
        """Unsubscribe from cluster shard channels asynchronously."""
        if self._active_listener is not None:
            await self._active_listener.sunsubscribe(*shard_channels)

    def drain_messages(self) -> list[PubSubMessage]:
        """Drain received pub/sub messages from the active listener."""
        messages: list[PubSubMessage] = []
        if self._active_listener is not None:
            raw_items = self._active_listener.drain_messages()
            for item in raw_items:
                kind = str(item.get("type", "message"))
                ch = str(item.get("channel", ""))
                raw_payload = item.get("data", b"")
                payload = raw_payload if isinstance(raw_payload, bytes) else str(raw_payload).encode("utf-8")
                pat = item.get("pattern")
                pat_str = str(pat) if pat is not None else None
                msg = PubSubMessage(kind=kind, channel=ch, payload=payload, pattern=pat_str)
                messages.append(msg)
                if self._broker is not None:
                    self._broker.publish(ch, payload)
        return messages

    async def reconnect(self, password: str | None = None) -> None:
        """Reconnect to Valkey with an optional updated password."""
        await self._client.close()
        if password is not None:
            self.config = msgspec.structs.replace(self.config, password=password)
        self._client = self._create_client(self.config)
        await self._client.ping()

    def notification_source(
        self,
        config: ValkeyNotifyConfig | None = None,
        *,
        change_feed: Any = None,
    ) -> AsyncValkeyNotificationSource:
        """Create an asynchronous notification source bound to this driver."""
        return AsyncValkeyNotificationSource(self, config=config, change_feed=change_feed)

    async def close(self) -> None:
        """Close connections and release client resources."""
        if self._active_listener is not None:
            await self._active_listener.close()
            self._active_listener = None
        await self._client.close()
