"""Vertebra pub/sub backend implementing Litestar's ChannelsBackend ABC."""

from __future__ import annotations

import asyncio
import importlib
from collections import deque
from typing import TYPE_CHECKING, Any, Self

from vertebra.db.redis import AsyncRedisDriver, RedisConfig
from vertebra.db.valkey import AsyncValkeyDriver, ValkeyConfig

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable

__all__ = (
    "ChannelsBackend",
    "VertebraChannelsBackend",
    "swap_litestar_channels_backend",
)


class ChannelsBackend:
    """In-memory, Valkey, and Redis-bridged Litestar ChannelsBackend with bounded ring history."""

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
        self._max_history = max(0, history)
        self._driver = driver
        self._history: dict[str, deque[bytes]] = {}
        self._subscribers: dict[str, set[asyncio.Queue[tuple[str, bytes] | None]]] = {}
        self._subscribed_channels: set[str] = set()
        self._queues: set[asyncio.Queue[tuple[str, bytes] | None]] = set()
        self._running = False

    @classmethod
    def from_valkey(cls, config_or_url: ValkeyConfig | str, *, history: int = 0) -> Self:
        """Create a Litestar ChannelsBackend backed by a Valkey configuration or URL."""
        cfg = ValkeyConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(history=history, driver=AsyncValkeyDriver(cfg))

    @classmethod
    def from_redis(cls, config_or_url: RedisConfig | str, *, history: int = 0) -> Self:
        """Create a Litestar ChannelsBackend backed by a Redis configuration or URL."""
        cfg = RedisConfig(url=config_or_url) if isinstance(config_or_url, str) else config_or_url
        return cls(history=history, driver=AsyncRedisDriver(cfg))

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


def swap_litestar_channels_backend(app: Any) -> ChannelsBackend | None:
    """Transparently replace MemoryChannelsBackend on a Litestar app's ChannelsPlugin."""
    plugins = getattr(app, "plugins", None)
    if plugins is None:
        return None
    plugin_list = getattr(plugins, "plugins", plugins)
    for plugin in plugin_list:
        is_channels_plugin = (_ChannelsPluginCls is not None and isinstance(plugin, _ChannelsPluginCls)) or type(
            plugin
        ).__name__ == "ChannelsPlugin"
        if is_channels_plugin:
            backend = getattr(plugin, "_backend", None)
            is_memory_backend = (
                _MemoryChannelsBackendCls is not None and isinstance(backend, _MemoryChannelsBackendCls)
            ) or type(backend).__name__ == "MemoryChannelsBackend"
            if is_memory_backend:
                history = int(getattr(backend, "_history", 0) or 0)
                replacement = ChannelsBackend(history=history)
                plugin._backend = replacement
                return replacement
            if isinstance(backend, ChannelsBackend):
                return backend
    return None


def _load_channels_types() -> tuple[type[Any] | None, type[Any] | None]:
    """Load Litestar ChannelsPlugin and MemoryChannelsBackend and register ChannelsBackend."""
    try:
        ch_mod = importlib.import_module("litestar.channels")
        mem_mod = importlib.import_module("litestar.channels.backends.memory")
        register_fn = getattr(ch_mod.ChannelsBackend, "register", None)
        if callable(register_fn):
            register_fn(ChannelsBackend)
        return ch_mod.ChannelsPlugin, mem_mod.MemoryChannelsBackend
    except ImportError:
        return None, None


VertebraChannelsBackend = ChannelsBackend
_ChannelsPluginCls, _MemoryChannelsBackendCls = _load_channels_types()
