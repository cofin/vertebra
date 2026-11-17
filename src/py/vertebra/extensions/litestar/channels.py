"""Vertebra pub/sub backend adapter for Litestar channels plugin."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import anyio

from vertebra.framework.channels import create_channels_backend
from vertebra.server._channels import ChannelBus
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable

    from litestar.channels.backends.base import ChannelsBackend as _LitestarChannelsBackend

    from vertebra.db.driver import (
        AsyncChangeSource,
        AsyncNotificationSource,
        SyncNotificationSource,
    )

__all__ = (
    "ChannelsBackend",
    "create_channels_plugin",
)

_CACHED_CHANNELS_BACKEND_CLS: type[Any] | None = None


class _ChannelsBackendImpl(ChannelBus):
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


if TYPE_CHECKING:

    class ChannelsBackend(_ChannelsBackendImpl, _LitestarChannelsBackend):
        """Litestar channels pub/sub backend adapter with cancellation shielding and capability bridges."""


def _get_channels_backend_cls() -> type[ChannelsBackend]:
    global _CACHED_CHANNELS_BACKEND_CLS
    if _CACHED_CHANNELS_BACKEND_CLS is None:
        base_mod = ensure_module("litestar.channels.backends.base", extra="litestar")
        base_cls = base_mod.ChannelsBackend
        _CACHED_CHANNELS_BACKEND_CLS = type(
            "ChannelsBackend",
            (_ChannelsBackendImpl, base_cls),
            {
                "__module__": __name__,
                "__doc__": _ChannelsBackendImpl.__doc__,
                "__slots__": (),
            },
        )
    return cast("type[ChannelsBackend]", _CACHED_CHANNELS_BACKEND_CLS)


def __getattr__(name: str) -> Any:
    """Lazily bind ``ChannelsBackend`` to ``litestar.channels.backends.base.ChannelsBackend``."""
    if name == "ChannelsBackend":
        return _get_channels_backend_cls()
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    """Return module attributes including lazily bound ``ChannelsBackend``."""
    return sorted(set(globals()) | set(__all__))


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
    ch_mod = ensure_module("litestar.channels", extra="litestar")
    plugin_cls = ch_mod.ChannelsPlugin
    channels_backend_cls = _get_channels_backend_cls()

    if isinstance(backend, channels_backend_cls):
        resolved_backend = backend
    elif isinstance(backend, ChannelBus):
        resolved_backend = channels_backend_cls(
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
        resolved_backend = channels_backend_cls(
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
