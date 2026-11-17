"""Asynchronous native Valkey driver powered by vertebra._valkey."""

from __future__ import annotations

import collections
import contextlib
import inspect
from typing import TYPE_CHECKING, Any, ClassVar, cast
from urllib.parse import urlparse

import msgspec

from vertebra.db.adapters.valkey.config import ValkeyConfig, is_valkey_auth_rejection
from vertebra.db.adapters.valkey.core import (
    PubSubMessage,
    ValkeyCore,
    _build_valkey_client_config,
    _decode_pubsub_item,
    _reject_valkey_url,
    hash_slot,
)
from vertebra.db.adapters.valkey.notify import (
    AsyncValkeyNotificationSource,
    ValkeyNotifyConfig,
)
from vertebra.db.driver import VALKEY_DIALECT_TAG, CommonDriverMixin, OperationalError
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Sequence

    from vertebra._valkey import (
        AsyncValkeyClient,
        AsyncValkeyListener,
        AsyncValkeyPipeline,
    )

__all__ = ("AsyncValkeyDriver",)


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
            await self._driver._ensure_token_async()
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
        "_client_token",
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
        self._client_token: str | None = None
        self._client: AsyncValkeyClient = self._create_client(resolved)

    @staticmethod
    def _create_client(
        config: ValkeyConfig,
        *,
        resolved_password: str | None = None,
    ) -> AsyncValkeyClient:
        """Create a native AsyncValkeyClient from ValkeyConfig."""
        mod = ensure_module("vertebra._valkey", package="vertebra-core[valkey]", extra="valkey")
        effective_config = config
        if config.effective_database_token_provider is not None:
            effective_config = config.with_resolved_token(resolved_password)
        elif resolved_password is not None:
            effective_config = msgspec.structs.replace(config, password=resolved_password)
        client_config = _build_valkey_client_config(mod, effective_config)
        return cast("AsyncValkeyClient", mod.AsyncValkeyClient(client_config))

    async def _ensure_token_async(self, *, force_refresh: bool = False) -> None:
        provider = self.config.effective_database_token_provider
        if provider is None:
            return
        if force_refresh:
            invalidate_fn = getattr(provider, "invalidate", None)
            if callable(invalidate_fn):
                invalidate_fn()
        token = str(await provider.resolve_async(force_refresh=force_refresh))
        if not force_refresh and token == self._client_token:
            return
        with contextlib.suppress(Exception):
            close_res = self._client.close()
            if inspect.isawaitable(close_res):
                await close_res
        self._client = self._create_client(self.config, resolved_password=token)
        self._client_token = token

    async def _run_with_client[T](self, op: Callable[[AsyncValkeyClient], Awaitable[T]]) -> T:
        provider = self.config.effective_database_token_provider
        if provider is None:
            return await op(self._client)
        for attempt in (0, 1):
            await self._ensure_token_async()
            try:
                return await op(self._client)
            except Exception as exc:
                if attempt == 0 and is_valkey_auth_rejection(exc):
                    await self._ensure_token_async(force_refresh=True)
                    continue
                raise
        msg = "Valkey authentication retry exhausted"
        raise OperationalError(msg)

    @property
    def _config(self) -> ValkeyConfig:
        """Return the active ValkeyConfig."""
        return self.config

    @property
    def cached_keys(self) -> tuple[str, ...]:
        """Return the keys currently tracked in the local RESP3 client-side cache."""
        return tuple(self._client.cached_keys())

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
        scheme = "valkeys" if self.config.uses_tls else "valkey"
        parsed = urlparse(self.config.url)
        path = parsed.path or "/0"
        query = f"?{parsed.query}" if parsed.query else ""
        resolved_master: tuple[str, int] | None = None
        last_exc: Exception | None = None
        for node in self.config.sentinel_nodes:
            sentinel_client: AsyncValkeyClient | None = None
            try:
                if isinstance(node, (tuple, list)):
                    h, p = str(node[0]), int(node[1])
                else:
                    host_part, port_part = str(node).rsplit(":", 1)
                    h, p = host_part.strip("[]"), int(port_part)
                sentinel_host = f"[{h}]" if ":" in h and not h.startswith("[") else h
                sentinel_cfg = ValkeyConfig(
                    url=f"{scheme}://{sentinel_host}:{p}/0",
                    client_name=self.config.client_name,
                    protocol=self.config.protocol,
                    topology="standalone",
                    enable_client_side_caching=False,
                    enable_client_tracking=False,
                )
                sentinel_client = self._create_client(sentinel_cfg)
                raw = await sentinel_client.execute_command("SENTINEL", "get-master-addr-by-name", name)
                if isinstance(raw, (list, tuple)) and len(raw) >= 2:
                    m_host = raw[0].decode("utf-8") if isinstance(raw[0], bytes) else str(raw[0])
                    m_port = int(raw[1])
                    resolved_master = (m_host, m_port)
                    break
            except Exception as exc:
                last_exc = exc
            finally:
                if sentinel_client is not None:
                    await sentinel_client.close()
        if resolved_master is None:
            msg = f"Failed to discover Sentinel master '{name}' across seed nodes"
            raise ImproperConfigurationError(msg) from last_exc
        m_host, m_port = resolved_master
        master_host = f"[{m_host}]" if ":" in m_host and not m_host.startswith("[") else m_host
        master_url = f"{scheme}://{master_host}:{m_port}{path}{query}"
        updated_config = msgspec.structs.replace(
            self.config,
            url=master_url,
            topology="standalone",
        )
        client_config = msgspec.structs.replace(
            updated_config,
            sentinel_master_name=None,
            sentinel_service_name=None,
            sentinel_nodes=(),
        )
        resolved_password: str | None = None
        if (provider := client_config.effective_database_token_provider) is not None:
            resolved_password = str(await provider.resolve_async())
        new_client = self._create_client(client_config, resolved_password=resolved_password)
        self._master_address = (m_host, m_port)
        self.config = updated_config
        self._client_token = resolved_password
        await self._client.close()
        self._client = new_client
        return m_host, m_port

    async def get(self, key: str) -> bytes | None:
        """Fetch the value associated with key from Valkey."""
        return await self._run_with_client(lambda client: client.get(key))

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
        await self._run_with_client(lambda client: client.set(key, value, ex=ex, px=px, nx=nx, xx=xx))

    async def delete(self, key: str) -> bool:
        """Delete key from Valkey, returning True if key was removed."""
        return bool(await self._run_with_client(lambda client: client.delete(key)))

    async def unlink(self, *keys: str) -> int:
        """Asynchronously unlink keys from Valkey in background threads."""
        return await self._run_with_client(lambda client: client.unlink(*keys))

    async def pttl(self, key: str) -> int:
        """Return the remaining time to live of key in milliseconds."""
        return await self._run_with_client(lambda client: client.pttl(key))

    async def flushdb(self) -> None:
        """Remove all keys from the current database."""
        await self._run_with_client(lambda client: client.flushdb())

    async def scan(
        self,
        cursor: int = 0,
        *,
        match: str | None = None,
        count: int = 500,
    ) -> tuple[int, list[str]]:
        """Incrementally iterate over matching keys."""
        next_cursor, raw_keys = await self._run_with_client(
            lambda client: client.scan(cursor=cursor, match_pattern=match, count=count)
        )
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
        return await self._run_with_client(lambda client: client.eval(script, key_seq, arg_seq))

    async def execute_command(self, *parts: str | bytes | int) -> Any:
        """Execute a raw Valkey command."""
        res = await self._run_with_client(lambda client: client.execute_command(*parts))
        self._dispatch_invalidations()
        return res

    def pipeline(self, atomic: bool = False) -> AsyncValkeyPipeline:
        """Create a new asynchronous command pipeline."""
        return self._client.pipeline(transaction=atomic)

    async def execute_pipeline(self, commands: Sequence[Sequence[bytes | str | int]]) -> list[Any]:
        """Execute a batch of commands in a single asynchronous pipeline."""

        async def _op(client: AsyncValkeyClient) -> list[Any]:
            pipe = client.pipeline(transaction=False)
            for cmd in commands:
                pipe.execute_command(*cmd)
            return await pipe.execute()

        return await self._run_with_client(_op)

    async def hexpire(
        self,
        key: str | bytes,
        ttl_seconds: int,
        *fields: str | bytes | Sequence[str | bytes],
        condition: str | None = None,
    ) -> list[int]:
        """Set relative expiration in seconds for specific hash fields asynchronously."""
        return await self._run_with_client(
            lambda client: client.hexpire(key, ttl_seconds, *fields, condition=condition)
        )

    async def hpexpire(
        self,
        key: str | bytes,
        ttl_ms: int,
        *fields: str | bytes | Sequence[str | bytes],
        condition: str | None = None,
    ) -> list[int]:
        """Set relative expiration in milliseconds for specific hash fields asynchronously."""
        return await self._run_with_client(lambda client: client.hpexpire(key, ttl_ms, *fields, condition=condition))

    async def hexpireat(
        self,
        key: str | bytes,
        unix_time_seconds: int,
        *fields: str | bytes | Sequence[str | bytes],
        condition: str | None = None,
    ) -> list[int]:
        """Set absolute expiration at Unix timestamp in seconds for specific hash fields asynchronously."""
        return await self._run_with_client(
            lambda client: client.hexpireat(key, unix_time_seconds, *fields, condition=condition)
        )

    async def hpexpireat(
        self,
        key: str | bytes,
        unix_time_ms: int,
        *fields: str | bytes | Sequence[str | bytes],
        condition: str | None = None,
    ) -> list[int]:
        """Set absolute expiration at Unix timestamp in milliseconds for specific hash fields asynchronously."""
        return await self._run_with_client(
            lambda client: client.hpexpireat(key, unix_time_ms, *fields, condition=condition)
        )

    async def httl(
        self,
        key: str | bytes,
        *fields: str | bytes | Sequence[str | bytes],
    ) -> list[int]:
        """Return remaining time to live in seconds for specific hash fields asynchronously."""
        return await self._run_with_client(lambda client: client.httl(key, *fields))

    async def hpttl(
        self,
        key: str | bytes,
        *fields: str | bytes | Sequence[str | bytes],
    ) -> list[int]:
        """Return remaining time to live in milliseconds for specific hash fields asynchronously."""
        return await self._run_with_client(lambda client: client.hpttl(key, *fields))

    async def hpersist(
        self,
        key: str | bytes,
        *fields: str | bytes | Sequence[str | bytes],
    ) -> list[int]:
        """Remove expiration on specific hash fields asynchronously."""
        return await self._run_with_client(lambda client: client.hpersist(key, *fields))

    async def publish(self, channel: str, message: bytes | str) -> int:
        """Publish a message to channel."""
        return await self._run_with_client(lambda client: client.publish(channel, message))

    async def spublish(self, shard_channel: str, message: bytes | str) -> int:
        """Publish a message to a sharded pub/sub channel."""
        return int(
            await self._run_with_client(lambda client: client.execute_command("SPUBLISH", shard_channel, message))
        )

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
                msg = _decode_pubsub_item(item)
                messages.append(msg)
                if self._broker is not None:
                    self._broker.publish(msg.channel, msg.payload)
        return messages

    async def reconnect(self, password: str | None = None) -> None:
        """Reconnect to Valkey with an optional updated password."""
        await self._client.close()
        provider = self.config.effective_database_token_provider
        if password is not None or provider is None:
            if password is not None:
                self.config = msgspec.structs.replace(self.config, password=password)
                self._client_token = password
            self._client = self._create_client(self.config)
            await self._client.ping()
            return
        for attempt in (0, 1):
            if attempt > 0:
                invalidate_fn = getattr(provider, "invalidate", None)
                if callable(invalidate_fn):
                    invalidate_fn()
            token = str(await provider.resolve_async(force_refresh=(attempt > 0)))
            self._client = self._create_client(self.config, resolved_password=token)
            self._client_token = token
            try:
                await self._client.ping()
                return
            except Exception as exc:
                if attempt == 0 and is_valkey_auth_rejection(exc):
                    with contextlib.suppress(Exception):
                        close_res = self._client.close()
                        if inspect.isawaitable(close_res):
                            await close_res
                    continue
                raise

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
