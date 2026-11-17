"""Vertebra pub/sub channel backend adapter for FastAPI applications."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.framework.channels import ChannelsBackend as FrameworkChannelsBackend
from vertebra.framework.channels import create_channels_backend
from vertebra.server._channels import ChannelBus

if TYPE_CHECKING:
    from vertebra.db.driver import (
        AsyncChangeSource,
        AsyncNotificationSource,
        SyncNotificationSource,
    )

__all__ = (
    "ChannelsBackend",
    "attach_channels",
)


class ChannelsBackend(FrameworkChannelsBackend):
    """FastAPI pub/sub channel backend with local fan-out and capability bridges."""

    __slots__ = ()


def attach_channels(
    app: Any,
    backend: str | ChannelsBackend | ChannelBus | None = None,
    *,
    url: str | None = None,
    history: int = 0,
    notification_source: AsyncNotificationSource | None = None,
    sync_notification_source: SyncNotificationSource | None = None,
    change_source: AsyncChangeSource | None = None,
    change_feed: Any = None,
    node_id: str | None = None,
    maxsize: int = 256,
    **kwargs: Any,
) -> ChannelsBackend | ChannelBus:
    """Attach a ChannelsBackend instance to ``app.state.channels``."""
    if isinstance(backend, (ChannelsBackend, ChannelBus)):
        resolved = backend
    elif (
        isinstance(backend, str)
        or url is not None
        or notification_source is not None
        or sync_notification_source is not None
    ):
        raw_backend = create_channels_backend(
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
        resolved = ChannelsBackend(
            backend=raw_backend,
            history=history,
            notification_source=raw_backend.notification_source,
            sync_notification_source=raw_backend.sync_notification_source,
            change_source=raw_backend.change_source,
            change_feed=raw_backend.change_feed,
            node_id=raw_backend.node_id,
            maxsize=maxsize,
        )
    else:
        resolved = ChannelsBackend()

    state = getattr(app, "state", None)
    if state is not None:
        if isinstance(state, dict):
            state["channels"] = resolved
        else:
            state.channels = resolved
    return resolved
