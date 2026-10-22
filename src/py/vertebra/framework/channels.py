"""Pub/sub channel bus backed by in-memory ring buffers, Valkey, or Redis."""

from __future__ import annotations

import asyncio
from collections import deque
from typing import TYPE_CHECKING, Self

from vertebra.db.redis import AsyncRedisDriver, RedisConfig
from vertebra.db.valkey import AsyncValkeyDriver, ValkeyConfig

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable

__all__ = ("ChannelBus", "ChannelsBackend")


class ChannelBus:
    """In-memory, Valkey, and Redis-bridged pub/sub channel bus with bounded ring history."""

    __slots__ = (
        "_driver",
        "_history",
        "_max_history",
        "_queues",
        "_running",
        "_subscribed_channels",
        "_subscribers",
    )

    def __init__(
        self,
        *,
        history: int = 0,
        driver: AsyncValkeyDriver | AsyncRedisDriver | None = None,
    ) -> None:
        """Initialize the channel bus with optional history buffer size and driver."""
        self._max_history = max(0, history)
        self._driver = driver
        self._history: dict[str, deque[bytes]] = {}
        self._subscribers: dict[str, set[asyncio.Queue[tuple[str, bytes] | None]]] = {}
        self._subscribed_channels: set[str] = set()
        self._queues: set[asyncio.Queue[tuple[str, bytes] | None]] = set()
        self._running = False

    @classmethod
    def from_valkey(cls, config_or_url: ValkeyConfig | str, *, history: int = 0) -> Self:
        """Create a channel bus backed by Valkey configuration or connection URL."""
        cfg = ValkeyConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(history=history, driver=AsyncValkeyDriver(cfg))

    @classmethod
    def from_redis(cls, config_or_url: RedisConfig | str, *, history: int = 0) -> Self:
        """Create a channel bus backed by Redis configuration or connection URL."""
        cfg = RedisConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(history=history, driver=AsyncRedisDriver(cfg))

    @property
    def channels(self) -> tuple[str, ...]:
        """Return all tracked channel names."""
        return tuple(sorted(self._subscribed_channels | set(self._history.keys())))

    @property
    def driver(self) -> AsyncValkeyDriver | AsyncRedisDriver | None:
        """Return the underlying Valkey or Redis driver when configured."""
        return self._driver

    async def on_startup(self) -> None:
        """Initialize the channels backend on application startup."""
        self._running = True

    async def on_shutdown(self) -> None:
        """Flush and clear active subscriptions on application shutdown."""
        self._running = False
        for queue in tuple(self._queues):
            queue.put_nowait(None)
        self._subscribers.clear()
        self._subscribed_channels.clear()

    async def publish(self, data: bytes, channels: Iterable[str]) -> None:
        """Publish data to all subscribers across the specified channels."""
        payload = data if isinstance(data, bytes) else bytes(data)
        for channel in channels:
            if self._driver is not None:
                await self._driver.publish(channel, payload)
            if self._max_history > 0:
                ring = self._history.get(channel)
                if ring is None:
                    ring = deque(maxlen=self._max_history)
                    self._history[channel] = ring
                ring.append(payload)
            for queue in tuple(self._subscribers.get(channel, ())):
                queue.put_nowait((channel, payload))

    async def subscribe(self, channels: Iterable[str]) -> None:
        """Ensure subscription topic slots exist for channels and attach active stream queues."""
        for channel in channels:
            self._subscribed_channels.add(channel)
            self._subscribers.setdefault(channel, set()).update(self._queues)

    async def unsubscribe(self, channels: Iterable[str]) -> None:
        """Remove subscription topic slots for channels and detach active stream queues."""
        for channel in channels:
            self._subscribed_channels.discard(channel)
            self._subscribers.pop(channel, None)

    async def stream_events(self) -> AsyncGenerator[tuple[str, bytes], None]:
        """Yield (channel, data) events as they are published to subscribed topics."""
        queue: asyncio.Queue[tuple[str, bytes] | None] = asyncio.Queue()
        self._queues.add(queue)
        for channel in tuple(self._subscribed_channels):
            self._subscribers.setdefault(channel, set()).add(queue)
        try:
            while self._running:
                item = await queue.get()
                if item is None:
                    break
                channel, payload = item
                if channel in self._subscribed_channels:
                    yield channel, payload
        finally:
            self._queues.discard(queue)
            for queues in self._subscribers.values():
                queues.discard(queue)

    async def get_history(self, channel: str, limit: int | None = None) -> list[bytes]:
        """Return buffered event history for channel up to limit entries."""
        ring = self._history.get(channel)
        if not ring:
            return []
        items = list(ring)
        if limit is not None and limit > 0:
            return items[-limit:]
        return items


ChannelsBackend = ChannelBus
