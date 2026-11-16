"""Django Channels ChannelLayer backed by Vertebra pub/sub rings and native drivers."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import TYPE_CHECKING, Any

from vertebra.framework.channels import create_channels_backend
from vertebra.server._channels import ChannelBus
from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from vertebra.db.driver import AsyncNotificationSource, SyncNotificationSource

__all__ = ("ChannelLayer",)


class ChannelLayer:
    """Drop-in ``CHANNEL_LAYERS`` backend implementing the Django Channels ASGI specification."""

    extensions: tuple[str, ...] = ("groups", "flush")

    __slots__ = ("_bus", "_channels", "_groups", "_prefix", "capacity", "expiry")

    def __init__(
        self,
        *,
        expiry: int = 60,
        capacity: int = 100,
        prefix: str = "vtb:asgi:",
        backend: str = "memory",
        notification_source: AsyncNotificationSource | None = None,
        sync_notification_source: SyncNotificationSource | None = None,
        bus: ChannelBus | None = None,
        node_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.expiry = expiry
        self.capacity = capacity
        self._prefix = prefix
        self._channels: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._groups: dict[str, set[str]] = {}
        if bus is not None:
            self._bus = bus
        else:
            self._bus = create_channels_backend(
                backend=backend,
                notification_source=notification_source,
                sync_notification_source=sync_notification_source,
                node_id=node_id,
                maxsize=capacity,
                **kwargs,
            )
        self._bus.add_listener(self._on_bus_message)

    @property
    def bus(self) -> ChannelBus:
        """Return the backing ChannelBus instance."""
        return self._bus

    def _get_or_create_queue(self, channel: str) -> asyncio.Queue[dict[str, Any]]:
        """Return or allocate the bounded queue for the target channel name."""
        q = self._channels.get(channel)
        if q is None:
            q = asyncio.Queue(maxsize=self.capacity)
            self._channels[channel] = q
        return q

    def _deliver_to_channel(self, channel: str, message: dict[str, Any]) -> None:
        """Deliver message to a single channel's bounded queue, dropping oldest on saturation."""
        q = self._get_or_create_queue(channel)
        try:
            q.put_nowait(dict(message))
        except asyncio.QueueFull:
            with contextlib.suppress(asyncio.QueueEmpty):
                q.get_nowait()
            with contextlib.suppress(asyncio.QueueFull):
                q.put_nowait(dict(message))

    def _on_bus_message(self, channel: str, payload: bytes) -> None:
        """Handle incoming local or cross-node publication frames."""
        try:
            data = decode_json(payload)
        except Exception:
            return
        if not isinstance(data, dict):
            return

        group_prefix = f"{self._prefix}group:"
        if channel.startswith(group_prefix):
            group_name = channel[len(group_prefix) :]
            members = self._groups.get(group_name)
            if members:
                msg = data.get("message", data)
                for member_ch in tuple(members):
                    self._deliver_to_channel(member_ch, msg)
        else:
            msg = data.get("message", data)
            self._deliver_to_channel(channel, msg)

    async def send(self, channel: str, message: dict[str, Any]) -> None:
        """Send ``message`` to ``channel`` across nodes or locally."""
        payload = encode_json_bytes({"channel": channel, "message": message})
        await self._bus.publish(payload, [channel])

    def publish_sync(self, channel: str, message: dict[str, Any]) -> None:
        """Synchronously send ``message`` to ``channel`` across nodes or locally."""
        payload = encode_json_bytes({"channel": channel, "message": message})
        self._bus.publish_sync(payload, [channel])

    async def receive(self, channel: str) -> dict[str, Any]:
        """Receive the next message queued on ``channel``."""
        q = self._get_or_create_queue(channel)
        await self._bus.subscribe([channel])
        return await q.get()

    async def new_channel(self, prefix: str = "specific") -> str:
        """Allocate a unique single-reader channel name."""
        channel_name = f"{prefix}.{uuid.uuid4().hex}"
        self._get_or_create_queue(channel_name)
        await self._bus.subscribe([channel_name])
        return channel_name

    async def group_add(self, group: str, channel: str) -> None:
        """Add ``channel`` to ``group``."""
        group_channel = f"{self._prefix}group:{group}"
        if group not in self._groups:
            self._groups[group] = set()
            await self._bus.subscribe([group_channel])
        self._groups[group].add(channel)

    async def group_discard(self, group: str, channel: str) -> None:
        """Remove ``channel`` from ``group``."""
        members = self._groups.get(group)
        if members is not None:
            members.discard(channel)
            if not members:
                self._groups.pop(group, None)
                group_channel = f"{self._prefix}group:{group}"
                await self._bus.unsubscribe([group_channel])

    async def group_send(self, group: str, message: dict[str, Any]) -> None:
        """Broadcast ``message`` to every channel in ``group``."""
        group_channel = f"{self._prefix}group:{group}"
        payload = encode_json_bytes({"group": group, "message": message})
        await self._bus.publish(payload, [group_channel])

    async def flush(self) -> None:
        """Reset all channels and group memberships."""
        self._channels.clear()
        self._groups.clear()
        await self._bus.on_shutdown()

    async def aclose(self) -> None:
        """Close backing bus and detach active subscribers asynchronously."""
        self._bus.remove_listener(self._on_bus_message)
        await self._bus.aclose()
        self._channels.clear()
        self._groups.clear()

    def close(self) -> None:
        """Close backing bus synchronously."""
        self._bus.remove_listener(self._on_bus_message)
        self._bus.close()
        self._channels.clear()
        self._groups.clear()
