"""Channels pub/sub backend for Flask applications."""

from __future__ import annotations

import msgspec

from vertebra.extensions.litestar.channels import ChannelsBackend

__all__ = (
    "ChannelMessage",
    "ChannelsBackend",
)


class ChannelMessage(msgspec.Struct, frozen=True, gc=False):
    """Typed pub/sub message frame."""

    channel: str
    data: bytes
