"""Channels pub/sub backend for Flask applications."""

from __future__ import annotations

import contextlib
import queue
import threading
import uuid
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.db.driver import InterfaceError
from vertebra.framework.channels import create_channels_backend
from vertebra.server._channels import ChannelBus
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from flask import Flask

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
        "_pump_lock",
        "_pump_thread",
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
        self._queue: queue.Queue[ChannelMessage | None] = queue.Queue(maxsize=256)
        self._pump_lock = threading.Lock()
        self._pump_thread: threading.Thread | None = None
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
        """Blocking synchronous generator yielding local and cross-node ChannelMessage items."""
        if self._closed:
            return
        if channels is not None:
            self.subscribe(channels)

        if self._sync_notification_source is not None:
            self._ensure_pump_thread()

        while not self._closed:
            try:
                item = self._queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if item is None:
                break
            if self._subscribed_channels and item.channel not in self._subscribed_channels:
                continue
            yield item

    def close(self) -> None:
        """Close sync channels backend and release underlying resources."""
        self._closed = True
        with contextlib.suppress(queue.Full):
            self._queue.put_nowait(None)
        self._bus.remove_listener(self._on_bus_message)
        self._bus.close()
        self._subscribed_channels.clear()

    def _ensure_pump_thread(self) -> None:
        """Start background pump forwarding remote SyncNotificationSource events into self._bus."""
        with self._pump_lock:
            if self._pump_thread is not None and self._pump_thread.is_alive():
                return
            thread = threading.Thread(
                target=self._pump_remote_notifications,
                name=f"vtb-flask-channels-{self._node_id[:8]}",
                daemon=True,
            )
            self._pump_thread = thread
            thread.start()

    def _pump_remote_notifications(self) -> None:
        """Consume cross-node notifications and fan them into self._bus without self-echo."""
        source = self._sync_notification_source
        if source is None:
            return
        with contextlib.suppress(Exception):
            for event in source.listen():
                if self._closed:
                    break
                if event.sender_id is not None and event.sender_id == self._node_id:
                    continue
                extracted = self._bus._unpack_envelope(event.channel, event.payload)
                if extracted is None:
                    continue
                ch, data = extracted
                self._bus._fan_in_local(ch, data)


def attach_channels(
    app: Flask | Any,
    backend: ChannelsBackend | None = None,
    **kwargs: Any,
) -> ChannelsBackend:
    """Attach ChannelsBackend to Flask app.extensions."""
    ensure_module("flask", extra="flask")
    resolved = backend if backend is not None else ChannelsBackend(**kwargs)
    extensions = getattr(app, "extensions", None)
    if extensions is not None and isinstance(extensions, dict):
        extensions["channels"] = resolved
    return resolved
