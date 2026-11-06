"""Vertebra pub/sub backend adapter for Litestar channels plugin."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import anyio

from vertebra.server._channels import ChannelBus
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable

__all__ = (
    "ChannelsBackend",
    "swap_channels_backend",
)


class ChannelsBackend(ChannelBus):
    """Litestar channels pub/sub backend adapter with cancellation shielding and capability bridges."""

    __slots__ = ()

    async def unsubscribe(self, channels: Iterable[str]) -> None:
        """Remove subscription topic slots with shielded cancellation cleanup."""
        with anyio.CancelScope(shield=True):
            await super().unsubscribe(channels)

    async def on_shutdown(self) -> None:
        """Flush and clear active subscriptions with shielded cancellation cleanup."""
        with anyio.CancelScope(shield=True):
            await super().on_shutdown()

    async def stream_events(self) -> AsyncGenerator[tuple[str, bytes], None]:
        """Yield (channel, data) events with shielded cleanup on SSE disconnect."""
        queue = self._register_stream_queue()
        try:
            while self._running:
                item = await queue.get()
                if item is None:
                    break
                channel, payload = item
                if channel in self._subscribed_channels:
                    yield channel, payload
        finally:
            with anyio.CancelScope(shield=True):
                self._unregister_stream_queue(queue)


def swap_channels_backend(
    app: Any,
    backend: ChannelsBackend | ChannelBus | None = None,
) -> ChannelsBackend | ChannelBus | None:
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
            current_backend = getattr(plugin, "_backend", None)
            is_memory_backend = (
                _MemoryChannelsBackendCls is not None and isinstance(current_backend, _MemoryChannelsBackendCls)
            ) or type(current_backend).__name__ == "MemoryChannelsBackend"
            if is_memory_backend or backend is not None:
                history = int(
                    getattr(current_backend, "_max_history_length", 0) or getattr(current_backend, "history", 0) or 0
                )
                replacement = backend if backend is not None else ChannelsBackend(history=history)
                plugin._backend = replacement
                return replacement
            if isinstance(current_backend, (ChannelsBackend, ChannelBus)):
                return current_backend
    return None


def _load_channels_types() -> tuple[type[Any] | None, type[Any] | None]:
    """Load Litestar ChannelsPlugin and MemoryChannelsBackend and register ChannelBus."""
    ch_mod = import_optional("litestar.channels")
    mem_mod = import_optional("litestar.channels.backends.memory")
    if ch_mod is None or mem_mod is None:
        return None, None
    register_fn = getattr(ch_mod.ChannelsBackend, "register", None)
    if callable(register_fn):
        register_fn(ChannelBus)
        register_fn(ChannelsBackend)
    return ch_mod.ChannelsPlugin, mem_mod.MemoryChannelsBackend


_ChannelsPluginCls, _MemoryChannelsBackendCls = _load_channels_types()
