"""Channels pub/sub backend for Flask applications."""

from __future__ import annotations

import contextlib
import queue
import uuid
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.db.driver import InterfaceError
from vertebra.framework.channels import create_channels_backend
from vertebra.server._channels import ChannelBus
from vertebra.utils.serializers import encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from vertebra.db.driver import SyncNotificationSource

__all__ = (
    "ChannelMessage",
    "ChannelsBackend",
    "attach_channels",
)


class ChannelMessage(msgspec.Struct, frozen=True, gc=False):
    """Typed pub/sub message frame."""

    channel: str
    data: bytes


class ChannelsBackend:
    """Synchronous pub/sub channel backend for Flask powered by SyncNotificationSource."""

    __slots__ = (
        "_bus",
        "_closed",
        "_node_id",
        "_queue",
        "_subscribed_channels",
        "_sync_notification_source",
    )

    def __init__(
        self,
        backend: str | ChannelBus | None = "memory",
        *,
        url: str | None = None,
        history: int = 0,
        sync_notification_source: SyncNotificationSource | None = None,
        node_id: str | None = None,
        bus: ChannelBus | None = None,
        **kwargs: Any,
    ) -> None:
        self._node_id = str(node_id) if node_id is not None else uuid.uuid4().hex
        self._closed = False
        self._subscribed_channels: set[str] = set()
        self._queue: queue.Queue[ChannelMessage] = queue.Queue(maxsize=256)
        if bus is not None:
            self._bus = bus
            self._sync_notification_source = bus.sync_notification_source
        else:
            self._bus = create_channels_backend(
                backend=backend,
                url=url,
                history=history,
                sync_notification_source=sync_notification_source,
                node_id=self._node_id,
                **kwargs,
            )
            self._sync_notification_source = self._bus.sync_notification_source
        self._bus.add_listener(self._on_bus_message)

    def _on_bus_message(self, ch: str, payload: bytes) -> None:
        """Enqueue message payload for active channel subscriptions."""
        if not self._subscribed_channels or ch in self._subscribed_channels:
            with contextlib.suppress(queue.Full):
                self._queue.put_nowait(ChannelMessage(channel=ch, data=payload))

    @property
    def bus(self) -> ChannelBus:
        """Return the underlying ChannelBus."""
        return self._bus

    @property
    def node_id(self) -> str:
        """Return the node identifier."""
        return self._node_id

    @property
    def subscribed_channels(self) -> tuple[str, ...]:
        """Return the currently subscribed channels."""
        return tuple(sorted(self._subscribed_channels))

    def publish(self, channel: str, message: bytes | str | dict[str, Any]) -> None:
        """Synchronously publish a message payload to channel."""
        if self._closed:
            msg = "Cannot publish on closed ChannelsBackend"
            raise InterfaceError(msg)
        raw = (
            encode_json_bytes(message)
            if isinstance(message, dict)
            else (message.encode("utf-8") if isinstance(message, str) else bytes(message))
        )
        self._bus.publish_sync(raw, [channel])

    def subscribe(self, channels: Iterable[str]) -> None:
        """Synchronously subscribe to notification channels."""
        topics = tuple(channels)
        self._subscribed_channels.update(topics)
        if self._sync_notification_source is not None:
            self._sync_notification_source.subscribe(topics)

    def unsubscribe(self, channels: Iterable[str]) -> None:
        """Synchronously unsubscribe from notification channels."""
        topics = tuple(channels)
        for t in topics:
            self._subscribed_channels.discard(t)
        if self._sync_notification_source is not None:
            self._sync_notification_source.unsubscribe(topics)

    def listen(self, channels: Iterable[str] | None = None) -> Iterator[ChannelMessage]:
        """Blocking synchronous generator yielding ChannelMessage items across subscribed channels."""
        if self._closed:
            return
        if channels is not None:
            self.subscribe(channels)

        if self._sync_notification_source is not None:
            for event in self._sync_notification_source.listen():
                if self._closed:
                    break
                if self._subscribed_channels and event.channel not in self._subscribed_channels:
                    continue
                extracted = self._bus._unpack_envelope(event.channel, event.payload)
                if extracted is not None:
                    ch, data = extracted
                    yield ChannelMessage(channel=ch, data=data)
        else:
            while not self._closed:
                try:
                    item = self._queue.get(timeout=0.2)
                    yield item
                except queue.Empty:
                    continue

    def close(self) -> None:
        """Close sync channels backend and release underlying resources."""
        self._closed = True
        self._bus.remove_listener(self._on_bus_message)
        self._bus.close()
        self._subscribed_channels.clear()


def attach_channels(
    app: Any,
    backend: ChannelsBackend | None = None,
    **kwargs: Any,
) -> ChannelsBackend:
    """Attach ChannelsBackend to Flask app.extensions."""
    resolved = backend if backend is not None else ChannelsBackend(**kwargs)
    extensions = getattr(app, "extensions", None)
    if extensions is not None and isinstance(extensions, dict):
        extensions["channels"] = resolved
    return resolved
