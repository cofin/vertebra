"""Vertebra pub/sub backend adapter for Litestar channels plugin."""

from __future__ import annotations

from typing import Any

from vertebra.framework.channels import ChannelBus
from vertebra.utils.module_loader import import_optional

ChannelsBackend = ChannelBus

__all__ = (
    "ChannelBus",
    "ChannelsBackend",
    "swap_litestar_channels_backend",
)


def swap_litestar_channels_backend(app: Any, backend: ChannelBus | None = None) -> ChannelBus | None:
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
                history = int(getattr(current_backend, "_history", 0) or 0)
                replacement = backend if backend is not None else ChannelBus(history=history)
                plugin._backend = replacement
                return replacement
            if isinstance(current_backend, ChannelBus):
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
    return ch_mod.ChannelsPlugin, mem_mod.MemoryChannelsBackend


_ChannelsPluginCls, _MemoryChannelsBackendCls = _load_channels_types()
