"""Vertebra pub/sub channel backend adapter for Starlette applications."""

from __future__ import annotations

from typing import Any

from vertebra.db.channels import DatabaseChannelBackend

__all__ = (
    "ChannelsBackend",
    "attach_channels",
)


class ChannelsBackend(DatabaseChannelBackend):
    """Starlette pub/sub channel backend backed by Valkey, Redis, PostgreSQL, Oracle, or SHM."""

    __slots__ = ()


def attach_channels(
    app: Any,
    backend: ChannelsBackend | DatabaseChannelBackend | None = None,
) -> ChannelsBackend | DatabaseChannelBackend:
    """Attach a ChannelsBackend instance to ``app.state.channels``."""
    resolved = backend if backend is not None else ChannelsBackend()
    state = getattr(app, "state", None)
    if state is not None:
        if isinstance(state, dict):
            state["channels"] = resolved
        else:
            state.channels = resolved
    return resolved
