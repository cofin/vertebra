"""Asynchronous native Redis driver powered by vertebra._redis."""

from __future__ import annotations

import collections
import contextlib
from typing import TYPE_CHECKING, Any, ClassVar, cast
from urllib.parse import urlparse

import msgspec

from vertebra.db.adapters.redis.config import RedisConfig
from vertebra.db.adapters.redis.core import (
    PubSubMessage,
    _normalize_nodes,
    _reject_memory_url,
    build_redis_client_kwargs,
    hash_slot,
    is_redis_auth_rejection,
)
from vertebra.db.adapters.redis.notify import (
    AsyncRedisNotificationSource,
    RedisNotifyConfig,
)
from vertebra.db.driver import REDIS_DIALECT_TAG, CommonDriverMixin, OperationalError
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from vertebra._redis import (
        AsyncRedisClient,
        AsyncRedisListener,
        AsyncRedisPipeline,
    )

__all__ = ("AsyncRedisDriver",)


class AsyncRedisDriver(CommonDriverMixin[RedisConfig]):
    """Asynchronous native Redis driver powered by vertebra._redis."""

    dialect: ClassVar[str] = "redis"
    dialect_tag: ClassVar[int] = REDIS_DIALECT_TAG

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
        config: RedisConfig | None = None,
        *,
        broker: Any = None,
    ) -> None:
        resolved = config or RedisConfig()
        _reject_memory_url(resolved.url)
        super().__init__(resolved, broker=broker)
        self._invalidation_listeners: list[Callable[[Sequence[str]], None]] = []
        self._pubsub_queue: collections.deque[PubSubMessage] = collections.deque()
        self._active_listener: AsyncRedisListener | None = None
        self._client_token: str | None = None
        self._client: AsyncRedisClient = self._create_client(resolved)

    @staticmethod
    def _create_client(
        config: RedisConfig,
        *,
        resolved_password: str | None = None,
    ) -> AsyncRedisClient:
        """Create a native AsyncRedisClient from RedisConfig."""
        mod = ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
        kwargs = build_redis_client_kwargs(config, resolved_password=resolved_password)
        return cast("AsyncRedisClient", mod.AsyncRedisClient(**kwargs))

    async def _apply_token_async(self, token: str) -> None:
        reconnect_fn: Callable[..., Awaitable[None]] | None = getattr(self._client, "reconnect", None)
        if self._client_token is not None and callable(reconnect_fn):
            await reconnect_fn(password=token)
        else:
            with contextlib.suppress(Exception):
                self._client.close()
            self._client = self._create_client(self.config, resolved_password=token)
        self._client_token = token

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
        await self._apply_token_async(token)

    async def _run_with_client[T](self, op: Callable[[AsyncRedisClient], Awaitable[T]]) -> T:
        provider = self.config.effective_database_token_provider
        if provider is None:
            return await op(self._client)
        for attempt in (0, 1):
            await self._ensure_token_async()
            try:
                return await op(self._client)
            except Exception as exc:
                if attempt == 0 and is_redis_auth_rejection(exc):
                    await self._ensure_token_async(force_refresh=True)
                    continue
                raise
        msg = "Redis authentication retry exhausted"
        raise OperationalError(msg)

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
        return self._client.slot_map()

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
        invalidations = list(self._client.drain_invalidations())
        if keys is not None:
            for k in keys:
                s = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                if s not in invalidations:
                    invalidations.append(s)
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
        scheme = "rediss" if self.config.uses_tls else "redis"
        parsed = urlparse(self.config.url)
        path = parsed.path or "/0"
        query = f"?{parsed.query}" if parsed.query else ""
        resolved_master: tuple[str, int] | None = None
        last_exc: Exception | None = None
        for node in self.config.sentinel_nodes:
            sentinel_client: AsyncRedisClient | None = None
            try:
                h, p = _normalize_nodes((node,))[0]
                sentinel_host = f"[{h}]" if ":" in h and not h.startswith("[") else h
                sentinel_cfg = RedisConfig(
                    url=f"{scheme}://{sentinel_host}:{p}/0",
                    client_name=self.config.client_name,
                    protocol=self.config.protocol,
                    topology="standalone",
                )
                sentinel_client = self._create_client(sentinel_cfg)
                raw = await sentinel_client.execute_command(["SENTINEL", "get-master-addr-by-name", name])
                if isinstance(raw, (list, tuple)) and len(raw) >= 2:
                    m_host = raw[0].decode("utf-8") if isinstance(raw[0], bytes) else str(raw[0])
                    m_port = int(raw[1])
                    resolved_master = (m_host, m_port)
                    break
            except Exception as exc:
                last_exc = exc
            finally:
                if sentinel_client is not None:
                    sentinel_client.close()
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
        self._client.close()
        self._client = new_client
        return m_host, m_port

    async def get(self, key: str) -> bytes | None:
        """Fetch the value associated with key from Redis."""
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
        """Store key and value in Redis with optional expiration or conditional flags."""
        await self._run_with_client(lambda client: client.set(key, value, ex=ex, px=px, nx=nx, xx=xx))

    async def delete(self, key: str) -> bool:
        """Delete key from Redis, returning True if key was removed."""
        keys_arg = [key] if isinstance(key, str) else list(key)
        return bool(await self._run_with_client(lambda client: client.delete(keys_arg)))

    async def unlink(self, *keys: str) -> int:
        """Asynchronously unlink keys from Redis in background threads."""
        keys_list = list(keys)
        return await self._run_with_client(lambda client: client.unlink(keys_list))

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
        keys_list = list(keys)
        args_list = list(args)
        return await self._run_with_client(lambda client: client.eval_script(script, keys_list, args_list))

    async def execute_command(self, *parts: str | bytes | int) -> Any:
        """Execute a raw Redis command."""
        parts_list = list(parts)
        return await self._run_with_client(lambda client: client.execute_command(parts_list))

    def pipeline(self, atomic: bool = False) -> AsyncRedisPipeline:
        """Create a new asynchronous command pipeline."""
        return self._client.pipeline(atomic=atomic)

    async def execute_pipeline(self, commands: Sequence[Sequence[bytes | str | int]]) -> list[Any]:
        """Execute a batch of commands in a single asynchronous pipeline."""

        async def _op(client: AsyncRedisClient) -> list[Any]:
            pipe = client.pipeline(atomic=False)
            for cmd in commands:
                pipe.execute_command(list(cmd))
            return await pipe.results()

        return await self._run_with_client(_op)

    async def publish(self, channel: str, message: bytes | str) -> int:
        """Publish a message to channel."""
        return await self._run_with_client(lambda client: client.publish(channel, message))

    async def spublish(self, shard_channel: str, message: bytes | str) -> int:
        """Publish a message to a sharded pub/sub channel."""
        return await self._run_with_client(lambda client: client.spublish(shard_channel, message))

    async def reconnect(self, password: str | None = None) -> None:
        """Reconnect to Redis with an optional updated password."""
        cfg = getattr(self, "config", None)
        provider = cfg.effective_database_token_provider if cfg is not None else None
        if password is not None or provider is None:
            await self._client.reconnect(password=password)
            if password is not None and cfg is not None:
                self._client_token = password
            return
        for attempt in (0, 1):
            if attempt > 0:
                invalidate_fn = getattr(provider, "invalidate", None)
                if callable(invalidate_fn):
                    invalidate_fn()
            token = str(await provider.resolve_async(force_refresh=(attempt > 0)))
            try:
                await self._client.reconnect(password=token)
                self._client_token = token
                return
            except Exception as exc:
                if attempt == 0 and is_redis_auth_rejection(exc):
                    continue
                raise

    def notification_source(
        self,
        config: RedisNotifyConfig | None = None,
        *,
        change_feed: Any = None,
    ) -> AsyncRedisNotificationSource:
        """Create an asynchronous notification source bound to this driver."""
        return AsyncRedisNotificationSource(self, config=config, change_feed=change_feed)

    async def close(self) -> None:
        """Close connections and release client resources."""
        if self._active_listener is not None:
            self._active_listener.close()
            self._active_listener = None
        self._client.close()
