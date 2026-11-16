"""Engine-neutral channel fan-out, bounded replay, and capability stream bridges."""

from __future__ import annotations

import asyncio
import base64
import contextlib
from collections import deque
from collections.abc import AsyncGenerator, Callable, Iterable
from typing import Self

from vertebra.db.cdc import ChangeEvent, ChangeFeed
from vertebra.db.driver import AsyncChangeSource, AsyncNotificationSource, SyncNotificationSource
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.serializers import (
    decode_json,
    decode_msgpack,
    encode_json_bytes,
    encode_msgpack_bytes,
)

__all__ = (
    "ChannelBus",
    "ChannelEnvelope",
)

_ENVELOPE_PREFIX = b"vtb:env:"


class ChannelEnvelope(FrozenBaseStruct, frozen=True, gc=False):
    """Cross-node channel envelope stamped with origin_node_id for deduplication."""

    origin_node_id: str
    channel: str
    payload: bytes


class ChannelBus:
    """Fan out local bytes and bridge notification or change-source capabilities."""

    __slots__ = (
        "_backend_kind",
        "_change_feed",
        "_change_source",
        "_history",
        "_listeners",
        "_max_history",
        "_maxsize",
        "_node_id",
        "_notification_source",
        "_queues",
        "_running",
        "_subscribed_channels",
        "_subscribers",
        "_sync_notification_source",
    )

    def __init__(
        self,
        backend: ChannelBus | str | None = None,
        *,
        history: int = 0,
        notification_source: AsyncNotificationSource | None = None,
        sync_notification_source: SyncNotificationSource | None = None,
        change_source: AsyncChangeSource | None = None,
        change_feed: ChangeFeed | None = None,
        node_id: str | None = None,
        maxsize: int = 256,
    ) -> None:
        self._backend_kind: str = backend if isinstance(backend, str) else "memory"
        bus_backend = backend if isinstance(backend, ChannelBus) else None
        self._max_history = max(0, history)
        self._maxsize = max(1, maxsize)
        self._node_id = str(node_id) if node_id is not None else None
        self._notification_source = (
            notification_source
            if notification_source is not None
            else (bus_backend.notification_source if bus_backend is not None else None)
        )
        self._sync_notification_source = (
            sync_notification_source
            if sync_notification_source is not None
            else (bus_backend.sync_notification_source if bus_backend is not None else None)
        )
        self._change_source = (
            change_source
            if change_source is not None
            else (bus_backend.change_source if bus_backend is not None else None)
        )
        self._change_feed = (
            change_feed if change_feed is not None else (bus_backend.change_feed if bus_backend is not None else None)
        )
        self._history: dict[str, deque[bytes]] = {}
        self._subscribers: dict[str, set[asyncio.Queue[tuple[str, bytes] | None]]] = {}
        self._subscribed_channels: set[str] = set()
        self._queues: set[asyncio.Queue[tuple[str, bytes] | None]] = set()
        self._listeners: list[Callable[[str, bytes], None]] = []
        self._running = False
        if self._change_feed is not None:
            self._change_feed.add_listener(self._on_changefeed_event)

    @property
    def backend(self) -> Self:
        """Return this channel bus."""
        return self

    @property
    def backend_kind(self) -> str:
        """Return the logical backend kind string."""
        return self._backend_kind

    @property
    def node_id(self) -> str | None:
        """Return the unique origin node identifier for this bus if assigned."""
        return self._node_id

    @property
    def channels(self) -> tuple[str, ...]:
        """Return subscribed channels and retained history topics."""
        return tuple(sorted(self._subscribed_channels | self._history.keys()))

    @property
    def notification_source(self) -> AsyncNotificationSource | None:
        """Return the attached notification capability."""
        return self._notification_source

    @property
    def sync_notification_source(self) -> SyncNotificationSource | None:
        """Return the attached synchronous notification capability."""
        return self._sync_notification_source

    @property
    def change_source(self) -> AsyncChangeSource | None:
        """Return the attached asynchronous change capability."""
        return self._change_source

    @property
    def change_feed(self) -> ChangeFeed | None:
        """Return the feed used for local fan-in and spill hydration."""
        return self._change_feed

    def add_listener(self, listener: Callable[[str, bytes], None]) -> None:
        """Register an immediate callback for local message arrivals."""
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(self, listener: Callable[[str, bytes], None]) -> None:
        """Unregister an immediate callback."""
        if listener in self._listeners:
            self._listeners.remove(listener)

    async def on_startup(self) -> None:
        """Enable event streams for the application lifespan."""
        self._running = True

    async def on_shutdown(self) -> None:
        """Stop streams and detach active subscriber queues."""
        self._running = False
        for queue in tuple(self._queues):
            queue.put_nowait(None)
            self._unregister_stream_queue(queue)
        self._subscribers.clear()
        self._subscribed_channels.clear()

    async def aclose(self) -> None:
        """Stop streams, close notification sources, and detach active subscriber queues."""
        await self.on_shutdown()
        if self._notification_source is not None:
            close_meth = getattr(self._notification_source, "close", None)
            if callable(close_meth):
                if asyncio.iscoroutinefunction(close_meth):
                    await close_meth()
                else:
                    close_meth()

    def close(self) -> None:
        """Synchronously stop streams and close sync notification sources."""
        self._running = False
        if self._sync_notification_source is not None:
            close_meth = getattr(self._sync_notification_source, "close", None)
            if callable(close_meth):
                close_meth()

    async def publish(self, data: bytes | str, channels: Iterable[str]) -> None:
        """Forward publications to an optional transport and fan out locally once."""
        payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        for channel in channels:
            shared_feed = (
                self._change_feed is not None
                and self._notification_source is not None
                and getattr(self._notification_source, "change_feed", None) is self._change_feed
            )
            if not shared_feed:
                self._fan_in_local(channel, payload)
            if self._notification_source is not None:
                if self._node_id is not None:
                    envelope = ChannelEnvelope(origin_node_id=self._node_id, channel=channel, payload=payload)
                    envelope_bytes = _ENVELOPE_PREFIX + encode_msgpack_bytes(envelope)
                    await self._notification_source.publish(channel, envelope_bytes)
                else:
                    await self._notification_source.publish(channel, payload)
            elif self._sync_notification_source is not None:
                if self._node_id is not None:
                    envelope = ChannelEnvelope(origin_node_id=self._node_id, channel=channel, payload=payload)
                    envelope_bytes = _ENVELOPE_PREFIX + encode_msgpack_bytes(envelope)
                    self._sync_notification_source.publish(channel, envelope_bytes)
                else:
                    self._sync_notification_source.publish(channel, payload)

    def publish_sync(self, data: bytes | str, channels: Iterable[str]) -> None:
        """Forward publications synchronously to the transport and fan out locally."""
        payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        for channel in channels:
            self._fan_in_local(channel, payload)
            if self._sync_notification_source is not None:
                if self._node_id is not None:
                    envelope = ChannelEnvelope(origin_node_id=self._node_id, channel=channel, payload=payload)
                    envelope_bytes = _ENVELOPE_PREFIX + encode_msgpack_bytes(envelope)
                    self._sync_notification_source.publish(channel, envelope_bytes)
                else:
                    self._sync_notification_source.publish(channel, payload)

    async def subscribe(self, channels: Iterable[str]) -> None:
        """Register topics with the transport and attach active stream queues."""
        topics = tuple(channels)
        if self._notification_source is not None:
            await self._notification_source.subscribe(topics)
        elif self._sync_notification_source is not None:
            self._sync_notification_source.subscribe(topics)
        for channel in topics:
            self._subscribed_channels.add(channel)
            self._subscribers.setdefault(channel, set()).update(self._queues)

    async def unsubscribe(self, channels: Iterable[str]) -> None:
        """Detach topics from the transport and active stream queues."""
        topics = tuple(channels)
        if self._notification_source is not None:
            await self._notification_source.unsubscribe(topics)
        elif self._sync_notification_source is not None:
            self._sync_notification_source.unsubscribe(topics)
        for channel in topics:
            self._subscribed_channels.discard(channel)
            self._subscribers.pop(channel, None)

    async def stream_events(self) -> AsyncGenerator[tuple[str, bytes], None]:
        """Yield publications until shutdown, releasing the queue on disconnect."""
        queue = self._register_stream_queue()
        try:
            while self._running:
                item = await queue.get()
                if item is None:
                    break
                if item[0] in self._subscribed_channels:
                    yield item
        finally:
            self._unregister_stream_queue(queue)

    async def get_history(self, channel: str, limit: int | None = None) -> list[bytes]:
        """Return retained publications, optionally limited to the latest entries."""
        items = list(self._history.get(channel, ()))
        return items[-limit:] if limit is not None and limit > 0 else items

    def _unpack_envelope(self, channel: str, payload: bytes) -> tuple[str, bytes] | None:
        """Extract stamped cross-node frame, dropping events originating from this node."""
        if payload.startswith(_ENVELOPE_PREFIX):
            try:
                env = decode_msgpack(payload[len(_ENVELOPE_PREFIX) :], type=ChannelEnvelope)
                if self._node_id is not None and env.origin_node_id == self._node_id:
                    return None
                return env.channel, env.payload
            except Exception:
                pass
        return channel, payload

    async def pump_notifications(self, source: AsyncNotificationSource | None = None) -> None:
        """Consume notification frames, hydrating feed envelopes and binary wrappers."""
        resolved = source if source is not None else self._notification_source
        if resolved is None:
            return
        async for event in resolved.listen():
            if event.sender_id is not None and event.sender_id == self._node_id:
                continue
            extracted = self._unpack_envelope(event.channel, event.payload)
            if extracted is None:
                continue
            channel, payload = extracted
            if payload.startswith(b"b64:"):
                self._fan_in_local(channel, base64.b64decode(payload[4:]))
            else:
                if self._change_feed is not None and payload.lstrip().startswith(b"{"):
                    try:
                        envelope = decode_json(payload)
                    except (ValueError, UnicodeDecodeError):
                        envelope = None
                    if isinstance(envelope, dict) and {"event_id", "channel", "operation"} <= envelope.keys():
                        change = (
                            await asyncio.to_thread(self._change_feed.hydrate_notification, payload)
                            if self._change_feed.store is not None
                            else self._change_feed.hydrate_notification(payload)
                        )
                        channel, payload = self._decode_change_event(change)
                self._fan_in_local(channel, payload)

    async def pump_change_source(self, source: AsyncChangeSource | None = None, *, channel: str | None = None) -> None:
        """Consume CDC frames while preserving independent shared-feed observation.

        A shared feed already delivers on the event topic. A different channel
        override adds one delivery there; an equal override avoids a duplicate.
        """
        resolved = source if source is not None else self._change_source
        if resolved is None:
            return
        shared_feed = self._change_feed is not None and getattr(resolved, "change_feed", None) is self._change_feed
        async for event in resolved.stream():
            topic, payload = self._decode_change_event(event)
            if not shared_feed or (channel is not None and channel != topic):
                self._fan_in_local(channel if channel is not None else topic, payload)

    def _register_stream_queue(self) -> asyncio.Queue[tuple[str, bytes] | None]:
        """Attach a stream queue to all subscribed topics with bounded backpressure."""
        queue: asyncio.Queue[tuple[str, bytes] | None] = asyncio.Queue(maxsize=self._maxsize)
        self._queues.add(queue)
        for channel in self._subscribed_channels:
            self._subscribers.setdefault(channel, set()).add(queue)
        return queue

    def _unregister_stream_queue(self, queue: asyncio.Queue[tuple[str, bytes] | None]) -> None:
        """Detach a stream queue from every topic."""
        self._queues.discard(queue)
        for queues in self._subscribers.values():
            queues.discard(queue)

    @staticmethod
    def _decode_change_event(event: ChangeEvent) -> tuple[str, bytes]:
        if event.operation == "PUBLISH" and isinstance(event.payload.get("data_b64"), str):
            return str(event.payload.get("channel", event.channel)), base64.b64decode(event.payload["data_b64"])
        return event.channel, encode_json_bytes(event)

    def _on_changefeed_event(self, event: ChangeEvent) -> None:
        channel, payload = self._decode_change_event(event)
        self._fan_in_local(channel, payload)

    def _fan_in_local(self, channel: str, payload: bytes) -> None:
        if self._max_history > 0:
            ring = self._history.get(channel)
            if ring is None:
                ring = deque(maxlen=self._max_history)
                self._history[channel] = ring
            ring.append(payload)
        for listener in tuple(self._listeners):
            with contextlib.suppress(Exception):
                listener(channel, payload)
        for queue in tuple(self._subscribers.get(channel, ())):
            try:
                queue.put_nowait((channel, payload))
            except asyncio.QueueFull:
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
                with contextlib.suppress(asyncio.QueueFull):
                    queue.put_nowait((channel, payload))
