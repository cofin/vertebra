"""Vertebra pub/sub channel backend adapter for Starlette applications."""

from __future__ import annotations

from typing import Any

from vertebra.server._channels import ChannelBus

__all__ = (
    "ChannelsBackend",
    "attach_channels",
)


class ChannelsBackend(ChannelBus):
    """Starlette pub/sub channel backend with local fan-out and capability bridges."""

    __slots__ = ()


def attach_channels(
    app: Any,
    backend: ChannelsBackend | ChannelBus | None = None,
) -> ChannelsBackend | ChannelBus:
    """Attach a ChannelsBackend instance to ``app.state.channels``."""
    resolved = backend if backend is not None else ChannelsBackend()
    state = getattr(app, "state", None)
    if state is not None:
        if isinstance(state, dict):
            state["channels"] = resolved
        else:
            state.channels = resolved
    return resolved
