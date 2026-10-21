"""Framework pub/sub channel bus backed by SHM ring buffers, Valkey, or Redis."""

from __future__ import annotations

from vertebra.extensions.litestar.channels import (
    ChannelsBackend,
    VertebraChannelsBackend,
    swap_litestar_channels_backend,
)

ChannelBus = ChannelsBackend

__all__ = (
    "ChannelBus",
    "ChannelsBackend",
    "VertebraChannelsBackend",
    "swap_litestar_channels_backend",
)
