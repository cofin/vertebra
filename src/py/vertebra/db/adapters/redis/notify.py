"""Redis Pub/Sub notification transport, active subscription streaming, and message builders."""

from __future__ import annotations

import asyncio
import base64
import collections
import contextlib
import os
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from vertebra.db.driver import (
    AsyncNotificationSource,
    InterfaceError,
    NotificationEvent,
    ProgrammingError,
    SyncNotificationSource,
)
from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra._redis import AsyncRedisClient, AsyncRedisListener, SyncRedisClient, SyncRedisListener
    from vertebra.db.adapters.redis.driver import AsyncRedisDriver, SyncRedisDriver
    from vertebra.db.cdc import ChangeFeed

__all__ = (
    "AsyncRedisNotificationSource",
    "RedisNotifyConfig",
    "SyncRedisNotificationSource",
)


class RedisNotifyConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Redis Pub/Sub notification configuration."""

    channels: tuple[str, ...] = ()
    """Initial channel names to subscribe to."""

    patterns: tuple[str, ...] = ()
    """Initial pattern strings to subscribe to via PSUBSCRIBE."""

    shard_channels: tuple[str, ...] = ()
    """Initial cluster shard channel names to subscribe to via SSUBSCRIBE."""

    timeout_ms: int = 250
    """Read timeout in milliseconds for polling the underlying listener."""


class SyncRedisNotificationSource(SyncNotificationSource):
    """Publish Redis notifications and stream live SyncRedisListener frames."""

    __slots__ = (
        "_change_feed",
        "_client",
        "_closed",
        "_config",
        "_driver",
        "_listener",
        "_subscribed_channels",
        "_subscribed_patterns",
        "_subscribed_shard_channels",
    )

    def __init__(
        self,
        driver: SyncRedisDriver | None = None,
        config: RedisNotifyConfig | None = None,
        *,
        client: SyncRedisClient | None = None,
        change_feed: ChangeFeed | None = None,
    ) -> None:
        """Initialize a synchronous Redis notification source.

        Args:
            driver: Optional SyncRedisDriver instance.
            config: Optional RedisNotifyConfig instance.
            client: Optional pre-constructed SyncRedisClient.
            change_feed: Optional ChangeFeed for notification hydration.

        Raises:
            MissingDependencyError: When vertebra-core[redis] is not installed.
            ImproperConfigurationError: When neither driver nor client is provided.
        """
        ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
        self._driver = driver
        self._config = config or RedisNotifyConfig()
        self._change_feed = change_feed
        self._listener: SyncRedisListener | None = None
        self._closed = False

        if client is not None:
            self._client = client
        elif driver is not None:
            resolved_sync = getattr(driver, "_client", None)
            if resolved_sync is None:
                msg = "SyncRedisDriver has no active native client"
                raise ImproperConfigurationError(msg)
            self._client = resolved_sync
        else:
            msg = "Either driver or client must be provided to SyncRedisNotificationSource"
            raise ImproperConfigurationError(msg)

        self._subscribed_channels: set[str] = set(self._config.channels)
        self._subscribed_patterns: set[str] = set(self._config.patterns)
        self._subscribed_shard_channels: set[str] = set(self._config.shard_channels)

    @property
    def change_feed(self) -> ChangeFeed | None:
        """Return the optional ChangeFeed attached to this source."""
        return self._change_feed

    @property
    def subscribed_channels(self) -> list[str]:
        """Return the currently subscribed standard channels."""
        return sorted(self._subscribed_channels)

    @property
    def subscribed_patterns(self) -> list[str]:
        """Return the currently subscribed pattern subscriptions."""
        return sorted(self._subscribed_patterns)

    @property
    def subscribed_shard_channels(self) -> list[str]:
        """Return the currently subscribed shard channels."""
        return sorted(self._subscribed_shard_channels)

    def publish(self, channel: str, payload: bytes | str) -> None:
        """Publish a payload to the target Redis channel."""
        if self._closed:
            msg = "Cannot publish on closed SyncRedisNotificationSource"
            raise InterfaceError(msg)
        self._client.publish(channel, payload)

    def spublish(self, shard_channel: str, payload: bytes | str) -> None:
        """Publish a payload to the target cluster shard channel."""
        if self._closed:
            msg = "Cannot spublish on closed SyncRedisNotificationSource"
            raise InterfaceError(msg)
        self._client.spublish(shard_channel, payload)

    def subscribe(self, channels: Sequence[str]) -> None:
        """Subscribe to one or more notification channels."""
        if isinstance(channels, (str, bytes)):
            msg = "channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        new_channels = [c for c in channels if c not in self._subscribed_channels]
        self._subscribed_channels.update(channels)
        if self._listener is not None and new_channels:
            self._listener.subscribe(new_channels)

    def psubscribe(self, patterns: Sequence[str]) -> None:
        """Subscribe to one or more channel patterns."""
        if isinstance(patterns, (str, bytes)):
            msg = "patterns must be a sequence of strings, not a single string"
            raise TypeError(msg)
        new_patterns = [p for p in patterns if p not in self._subscribed_patterns]
        self._subscribed_patterns.update(patterns)
        if self._listener is not None and new_patterns:
            self._listener.psubscribe(new_patterns)

    def ssubscribe(self, shard_channels: Sequence[str]) -> None:
        """Subscribe to one or more cluster shard channels."""
        if isinstance(shard_channels, (str, bytes)):
            msg = "shard_channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        new_shards = [s for s in shard_channels if s not in self._subscribed_shard_channels]
        self._subscribed_shard_channels.update(shard_channels)
        if self._listener is not None and new_shards:
            self._listener.ssubscribe(new_shards)

    def unsubscribe(self, channels: Sequence[str] = ()) -> None:
        """Unsubscribe from one or more notification channels."""
        if isinstance(channels, (str, bytes)):
            msg = "channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        target_channels = list(channels) if channels else list(self._subscribed_channels)
        for c in target_channels:
            self._subscribed_channels.discard(c)
        if self._listener is not None:
            self._listener.unsubscribe(target_channels)

    def punsubscribe(self, patterns: Sequence[str] = ()) -> None:
        """Unsubscribe from one or more channel patterns."""
        if isinstance(patterns, (str, bytes)):
            msg = "patterns must be a sequence of strings, not a single string"
            raise TypeError(msg)
        target_patterns = list(patterns) if patterns else list(self._subscribed_patterns)
        for p in target_patterns:
            self._subscribed_patterns.discard(p)
        if self._listener is not None:
            self._listener.punsubscribe(target_patterns)

    def sunsubscribe(self, shard_channels: Sequence[str] = ()) -> None:
        """Unsubscribe from one or more cluster shard channels."""
        if isinstance(shard_channels, (str, bytes)):
            msg = "shard_channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        target_shards = list(shard_channels) if shard_channels else list(self._subscribed_shard_channels)
        for s in target_shards:
            self._subscribed_shard_channels.discard(s)
        if self._listener is not None:
            self._listener.sunsubscribe(target_shards)

    def _hydrate_payload(self, raw_payload: bytes) -> bytes:
        """Decode UTF-8 notification text and hydrate ChangeFeed or b64-prefixed binary payloads."""
        if self._change_feed is not None:
            try:
                text = raw_payload.decode("utf-8")
                evt = self._change_feed.hydrate_notification(text)
                if isinstance(evt.payload, dict) and "data_b64" in evt.payload:
                    return base64.b64decode(evt.payload["data_b64"])
            except Exception:
                pass
        try:
            text = raw_payload.decode("utf-8")
            if text.startswith("b64:"):
                return base64.b64decode(text[4:])
        except Exception:
            pass
        return bytes(raw_payload)

    def listen(self) -> Iterator[NotificationEvent]:
        """Yield incoming NotificationEvent frames across subscribed channels."""
        if self._closed:
            msg = "Cannot listen on closed SyncRedisNotificationSource"
            raise InterfaceError(msg)

        listener = self._client.listener()
        self._listener = listener
        try:
            if self._subscribed_channels:
                listener.subscribe(list(self._subscribed_channels))
            if self._subscribed_patterns:
                listener.psubscribe(list(self._subscribed_patterns))
            if self._subscribed_shard_channels:
                listener.ssubscribe(list(self._subscribed_shard_channels))

            while not self._closed and not listener.closed:
                item = listener.next_message(timeout_ms=self._config.timeout_ms)
                if item is None:
                    continue
                kind, ch, payload, pattern = item
                hydrated = self._hydrate_payload(payload)
                metadata: dict[str, Any] = {
                    "kind": kind,
                    "pattern": pattern,
                    "shard": (kind == "smessage"),
                }
                yield NotificationEvent(
                    channel=ch,
                    payload=hydrated,
                    sender_id=None,
                    metadata=metadata,
                )
        finally:
            listener.close()
            if self._listener is listener:
                self._listener = None

    def close(self) -> None:
        """Close this notification source and release active listener handles."""
        if self._closed:
            return
        self._closed = True
        if self._listener is not None:
            self._listener.close()
            self._listener = None

    def __enter__(self) -> Self:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Exit context manager, closing active subscriptions."""
        self.close()


class AsyncRedisNotificationSource(AsyncNotificationSource):
    """Publish Redis notifications and stream live AsyncRedisListener frames without blocking the event loop."""

    __slots__ = (
        "_change_feed",
        "_client",
        "_closed",
        "_config",
        "_driver",
        "_listener",
        "_loop",
        "_message_event",
        "_message_queue",
        "_notifier_fd",
        "_pending_ops",
        "_reader_registered",
        "_subscribed_channels",
        "_subscribed_patterns",
        "_subscribed_shard_channels",
    )

    def __init__(
        self,
        driver: AsyncRedisDriver | None = None,
        config: RedisNotifyConfig | None = None,
        *,
        client: AsyncRedisClient | None = None,
        change_feed: ChangeFeed | None = None,
    ) -> None:
        """Initialize an asynchronous Redis notification source.

        Args:
            driver: Optional AsyncRedisDriver instance.
            config: Optional RedisNotifyConfig instance.
            client: Optional pre-constructed AsyncRedisClient.
            change_feed: Optional ChangeFeed for notification hydration.

        Raises:
            MissingDependencyError: When vertebra-core[redis] is not installed.
            ImproperConfigurationError: When neither driver nor client is provided.
        """
        ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
        self._driver = driver
        self._config = config or RedisNotifyConfig()
        self._change_feed = change_feed
        self._listener: AsyncRedisListener | None = None
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._notifier_fd: int = -1
        self._reader_registered: bool = False
        self._pending_ops: dict[int, asyncio.Future[None]] = {}
        self._message_queue: collections.deque[tuple[str, str, bytes, str | None]] = collections.deque()
        self._message_event: asyncio.Event | None = None

        if client is not None:
            self._client = client
        elif driver is not None:
            resolved_async = getattr(driver, "_client", None)
            if resolved_async is None:
                msg = "AsyncRedisDriver has no active native client"
                raise ImproperConfigurationError(msg)
            self._client = resolved_async
        else:
            msg = "Either driver or client must be provided to AsyncRedisNotificationSource"
            raise ImproperConfigurationError(msg)

        self._subscribed_channels: set[str] = set(self._config.channels)
        self._subscribed_patterns: set[str] = set(self._config.patterns)
        self._subscribed_shard_channels: set[str] = set(self._config.shard_channels)

    @property
    def change_feed(self) -> ChangeFeed | None:
        """Return the optional ChangeFeed attached to this source."""
        return self._change_feed

    @property
    def subscribed_channels(self) -> list[str]:
        """Return the currently subscribed standard channels."""
        return sorted(self._subscribed_channels)

    @property
    def subscribed_patterns(self) -> list[str]:
        """Return the currently subscribed pattern subscriptions."""
        return sorted(self._subscribed_patterns)

    @property
    def subscribed_shard_channels(self) -> list[str]:
        """Return the currently subscribed shard channels."""
        return sorted(self._subscribed_shard_channels)

    async def publish(self, channel: str, payload: bytes | str) -> None:
        """Publish a payload to the target Redis channel."""
        if self._closed:
            msg = "Cannot publish on closed AsyncRedisNotificationSource"
            raise InterfaceError(msg)
        await self._client.publish(channel, payload)

    async def spublish(self, shard_channel: str, payload: bytes | str) -> None:
        """Publish a payload to the target cluster shard channel."""
        if self._closed:
            msg = "Cannot spublish on closed AsyncRedisNotificationSource"
            raise InterfaceError(msg)
        await self._client.spublish(shard_channel, payload)

    async def subscribe(self, channels: Sequence[str]) -> None:
        """Subscribe to one or more notification channels."""
        if isinstance(channels, (str, bytes)):
            msg = "channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        new_channels = [c for c in channels if c not in self._subscribed_channels]
        self._subscribed_channels.update(channels)
        if self._listener is not None and new_channels:
            await self._run_listener_op(self._listener, "submit_subscribe", "subscribe", new_channels)

    async def psubscribe(self, patterns: Sequence[str]) -> None:
        """Subscribe to one or more channel patterns."""
        if isinstance(patterns, (str, bytes)):
            msg = "patterns must be a sequence of strings, not a single string"
            raise TypeError(msg)
        new_patterns = [p for p in patterns if p not in self._subscribed_patterns]
        self._subscribed_patterns.update(patterns)
        if self._listener is not None and new_patterns:
            await self._run_listener_op(self._listener, "submit_psubscribe", "psubscribe", new_patterns)

    async def ssubscribe(self, shard_channels: Sequence[str]) -> None:
        """Subscribe to one or more cluster shard channels."""
        if isinstance(shard_channels, (str, bytes)):
            msg = "shard_channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        new_shards = [s for s in shard_channels if s not in self._subscribed_shard_channels]
        self._subscribed_shard_channels.update(shard_channels)
        if self._listener is not None and new_shards:
            await self._run_listener_op(self._listener, "submit_ssubscribe", "ssubscribe", new_shards)

    async def unsubscribe(self, channels: Sequence[str] = ()) -> None:
        """Unsubscribe from one or more notification channels."""
        if isinstance(channels, (str, bytes)):
            msg = "channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        target_channels = list(channels) if channels else list(self._subscribed_channels)
        for c in target_channels:
            self._subscribed_channels.discard(c)
        if self._listener is not None:
            await self._run_listener_op(self._listener, "submit_unsubscribe", "unsubscribe", target_channels)

    async def punsubscribe(self, patterns: Sequence[str] = ()) -> None:
        """Unsubscribe from one or more channel patterns."""
        if isinstance(patterns, (str, bytes)):
            msg = "patterns must be a sequence of strings, not a single string"
            raise TypeError(msg)
        target_patterns = list(patterns) if patterns else list(self._subscribed_patterns)
        for p in target_patterns:
            self._subscribed_patterns.discard(p)
        if self._listener is not None:
            await self._run_listener_op(self._listener, "submit_punsubscribe", "punsubscribe", target_patterns)

    async def sunsubscribe(self, shard_channels: Sequence[str] = ()) -> None:
        """Unsubscribe from one or more cluster shard channels."""
        if isinstance(shard_channels, (str, bytes)):
            msg = "shard_channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        target_shards = list(shard_channels) if shard_channels else list(self._subscribed_shard_channels)
        for s in target_shards:
            self._subscribed_shard_channels.discard(s)
        if self._listener is not None:
            await self._run_listener_op(self._listener, "submit_sunsubscribe", "sunsubscribe", target_shards)

    async def listen(self) -> AsyncIterator[NotificationEvent]:
        """Yield incoming NotificationEvent frames asynchronously across subscribed channels."""
        if self._closed:
            msg = "Cannot listen on closed AsyncRedisNotificationSource"
            raise InterfaceError(msg)

        listener = self._client.listener()
        self._listener = listener
        reader_attached = self._attach_reader(listener)
        try:
            if self._subscribed_channels:
                await self._run_listener_op(
                    listener,
                    "submit_subscribe",
                    "subscribe",
                    list(self._subscribed_channels),
                )
            if self._subscribed_patterns:
                await self._run_listener_op(
                    listener,
                    "submit_psubscribe",
                    "psubscribe",
                    list(self._subscribed_patterns),
                )
            if self._subscribed_shard_channels:
                await self._run_listener_op(
                    listener,
                    "submit_ssubscribe",
                    "ssubscribe",
                    list(self._subscribed_shard_channels),
                )

            if reader_attached:
                poll_timeout_s = max(0.005, self._config.timeout_ms / 1000.0)
                while not self._closed and not listener.closed:
                    self._drain_listener_queues()
                    while self._message_queue:
                        kind, ch, payload, pattern = self._message_queue.popleft()
                        hydrated = self._hydrate_payload(payload)
                        metadata: dict[str, Any] = {
                            "kind": kind,
                            "pattern": pattern,
                            "shard": (kind == "smessage"),
                        }
                        yield NotificationEvent(
                            channel=ch,
                            payload=hydrated,
                            sender_id=None,
                            metadata=metadata,
                        )
                    if self._closed or listener.closed:
                        break
                    assert self._message_event is not None
                    self._message_event.clear()
                    self._drain_listener_queues()
                    if self._message_queue or self._closed or listener.closed:
                        continue
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(self._message_event.wait(), timeout=poll_timeout_s)
            else:
                while not self._closed and not listener.closed:
                    item = await asyncio.to_thread(listener.next_message, self._config.timeout_ms)
                    if item is None:
                        continue
                    kind, ch, payload, pattern = item
                    hydrated = self._hydrate_payload(payload)
                    fallback_meta: dict[str, Any] = {
                        "kind": kind,
                        "pattern": pattern,
                        "shard": (kind == "smessage"),
                    }
                    yield NotificationEvent(
                        channel=ch,
                        payload=hydrated,
                        sender_id=None,
                        metadata=fallback_meta,
                    )
        finally:
            self._detach_reader()
            listener.close()
            if self._listener is listener:
                self._listener = None

    async def aclose(self) -> None:
        """Close this notification source asynchronously and release active listener handles."""
        if self._closed:
            return
        self._closed = True
        self._detach_reader()
        if self._listener is not None:
            self._listener.close()
            self._listener = None

    async def __aenter__(self) -> Self:
        """Enter asynchronous context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Exit asynchronous context manager, closing active subscriptions."""
        await self.aclose()

    def _attach_reader(self, listener: AsyncRedisListener) -> bool:
        """Register event-loop fd wakeup reader when listener exposes notifier_fd."""
        notifier_fn: Callable[[], int] | None = getattr(listener, "notifier_fd", None)
        drain_comp_fn: Callable[[], Any] | None = getattr(listener, "drain_completions", None)
        drain_msg_fn: Callable[[], Any] | None = getattr(listener, "drain_messages", None)
        if not (callable(notifier_fn) and callable(drain_comp_fn) and callable(drain_msg_fn)):
            return False
        fd = notifier_fn()
        if not isinstance(fd, int) or fd < 0:
            return False
        loop = asyncio.get_running_loop()
        try:
            loop.add_reader(fd, self._on_notifier_readable)
        except (NotImplementedError, OSError, RuntimeError, ValueError):
            return False
        self._loop = loop
        self._notifier_fd = fd
        self._reader_registered = True
        self._message_event = asyncio.Event()
        self._drain_listener_queues()
        return True

    def _detach_reader(self) -> None:
        """Remove registered notifier_fd reader and settle pending completion futures."""
        if self._reader_registered and self._loop is not None and self._notifier_fd >= 0:
            with contextlib.suppress(Exception):
                self._loop.remove_reader(self._notifier_fd)
        self._reader_registered = False
        self._notifier_fd = -1
        self._loop = None
        for fut in list(self._pending_ops.values()):
            if not fut.done():
                fut.set_exception(InterfaceError("AsyncRedisListener closed"))
        self._pending_ops.clear()
        if self._message_event is not None:
            self._message_event.set()

    def _on_notifier_readable(self) -> None:
        """Clear kernel eventfd/pipe wakeup bytes and drain completions and messages."""
        if self._notifier_fd >= 0:
            with contextlib.suppress(BlockingIOError, InterruptedError, OSError):
                os.read(self._notifier_fd, 64)
        self._drain_listener_queues()

    def _drain_listener_queues(self) -> None:
        """Drain non-blocking subscription completions and pub/sub messages from listener."""
        listener = self._listener
        if listener is None:
            return
        for op_id, ok, err_msg in listener.drain_completions():
            fut = self._pending_ops.pop(op_id, None)
            if fut is not None and not fut.done():
                if ok:
                    fut.set_result(None)
                else:
                    fut.set_exception(ProgrammingError(err_msg or "Redis pub/sub command failed"))
        messages = listener.drain_messages()
        if messages:
            self._message_queue.extend(messages)
            if self._message_event is not None:
                self._message_event.set()
        if listener.closed and self._message_event is not None:
            self._message_event.set()

    async def _run_listener_op(
        self,
        listener: AsyncRedisListener,
        submit_name: str,
        sync_name: str,
        items: list[str],
    ) -> None:
        """Submit a pub/sub control command via notifier_fd completions or thread fallback."""
        submit_fn: Callable[[list[str]], int] | None = getattr(listener, submit_name, None)
        if self._reader_registered and callable(submit_fn):
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[None] = loop.create_future()
            op_id = int(submit_fn(items))
            self._pending_ops[op_id] = fut
            self._drain_listener_queues()
            try:
                await fut
            except asyncio.CancelledError:
                self._pending_ops.pop(op_id, None)
                cancel_fn: Callable[[int], Any] | None = getattr(listener, "cancel_operation", None)
                if callable(cancel_fn):
                    cancel_fn(op_id)
                raise
            return
        sync_fn: Callable[[list[str]], Any] = getattr(listener, sync_name)
        await asyncio.to_thread(sync_fn, items)

    def _hydrate_payload(self, raw_payload: bytes) -> bytes:
        """Decode UTF-8 notification text and hydrate ChangeFeed or b64-prefixed binary payloads."""
        if self._change_feed is not None:
            try:
                text = raw_payload.decode("utf-8")
                evt = self._change_feed.hydrate_notification(text)
                if isinstance(evt.payload, dict) and "data_b64" in evt.payload:
                    return base64.b64decode(evt.payload["data_b64"])
            except Exception:
                pass
        try:
            text = raw_payload.decode("utf-8")
            if text.startswith("b64:"):
                return base64.b64decode(text[4:])
        except Exception:
            pass
        return bytes(raw_payload)
