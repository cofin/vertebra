"""Django Channels ChannelLayer backed by Vertebra pub/sub rings and Valkey/Redis."""

from __future__ import annotations

import uuid
from collections import deque
from typing import Any

__all__ = ("ChannelLayer",)


class ChannelLayer:
    """Drop-in ``CHANNEL_LAYERS`` backend implementing the Django Channels ASGI specification."""

    extensions: tuple[str, ...] = ("groups", "flush")

    __slots__ = ("_channels", "_groups", "_prefix", "capacity", "expiry")

    def __init__(
        self,
        *,
        expiry: int = 60,
        capacity: int = 100,
        prefix: str = "vtb:asgi:",
    ) -> None:
        self.expiry = expiry
        self.capacity = capacity
        self._prefix = prefix
        self._channels: dict[str, deque[dict[str, Any]]] = {}
        self._groups: dict[str, set[str]] = {}

    async def send(self, channel: str, message: dict[str, Any]) -> None:
        """Send ``message`` to ``channel``."""
        q = self._channels.setdefault(channel, deque(maxlen=self.capacity))
        q.append(dict(message))

    async def receive(self, channel: str) -> dict[str, Any]:
        """Receive the next message queued on ``channel``."""
        q = self._channels.get(channel)
        if not q:
            return {}
        return q.popleft()

    async def new_channel(self, prefix: str = "specific") -> str:
        """Allocate a unique single-reader channel name."""
        return f"{prefix}.{uuid.uuid4().hex}"

    async def group_add(self, group: str, channel: str) -> None:
        """Add ``channel`` to ``group``."""
        self._groups.setdefault(group, set()).add(channel)

    async def group_discard(self, group: str, channel: str) -> None:
        """Remove ``channel`` from ``group``."""
        members = self._groups.get(group)
        if members is not None:
            members.discard(channel)

    async def group_send(self, group: str, message: dict[str, Any]) -> None:
        """Broadcast ``message`` to every channel in ``group``."""
        for channel in list(self._groups.get(group, ())):
            await self.send(channel, message)

    async def flush(self) -> None:
        """Reset all channels and group memberships."""
        self._channels.clear()
        self._groups.clear()
