"""Vertebra pub/sub backend adapter for Litestar channels plugin."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import anyio

from vertebra.framework.channels import create_channels_backend
from vertebra.server._channels import ChannelBus
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable

    from vertebra.db.driver import (
        AsyncChangeSource,
        AsyncNotificationSource,
        SyncNotificationSource,
    )

__all__ = (
    "ChannelsBackend",
    "create_channels_plugin",
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


def create_channels_plugin(
    backend: str | ChannelsBackend | ChannelBus | None = "memory",
    *,
    channels: Iterable[str] | None = None,
    arbitrary_channels_allowed: bool = True,
    history: int = 0,
    notification_source: AsyncNotificationSource | None = None,
    sync_notification_source: SyncNotificationSource | None = None,
    change_source: AsyncChangeSource | None = None,
    change_feed: Any = None,
    node_id: str | None = None,
    url: str | None = None,
    maxsize: int = 256,
    create_ws_route_handlers: bool = False,
    ws_handler_base_path: str = "/",
    **kwargs: Any,
) -> Any:
    """Create a Litestar ChannelsPlugin backed by Vertebra ChannelsBackend across 5 notification transports."""
    ch_mod = import_optional("litestar.channels")
    if ch_mod is None:
        msg = "litestar.channels is required to create ChannelsPlugin"
        raise ImproperConfigurationError(msg)
    plugin_cls = ch_mod.ChannelsPlugin

    if isinstance(backend, ChannelsBackend):
        resolved_backend = backend
    elif isinstance(backend, ChannelBus):
        resolved_backend = ChannelsBackend(
            backend=backend,
            history=history,
            notification_source=notification_source,
            sync_notification_source=sync_notification_source,
            change_source=change_source,
            change_feed=change_feed,
            node_id=node_id,
            maxsize=maxsize,
        )
    else:
        resolved = create_channels_backend(
            backend=backend,
            url=url,
            history=history,
            notification_source=notification_source,
            sync_notification_source=sync_notification_source,
            change_source=change_source,
            change_feed=change_feed,
            node_id=node_id,
            maxsize=maxsize,
            **kwargs,
        )
        resolved_backend = ChannelsBackend(
            backend=resolved,
            history=history,
            notification_source=resolved.notification_source,
            sync_notification_source=resolved.sync_notification_source,
            change_source=resolved.change_source,
            change_feed=resolved.change_feed,
            node_id=resolved.node_id,
            maxsize=maxsize,
        )

    return plugin_cls(
        backend=resolved_backend,
        channels=channels,
        arbitrary_channels_allowed=arbitrary_channels_allowed,
        create_ws_route_handlers=create_ws_route_handlers,
        ws_handler_base_path=ws_handler_base_path,
    )


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
