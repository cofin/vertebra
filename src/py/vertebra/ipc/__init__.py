"""IPC surface backed by the Rust core extension."""

from __future__ import annotations

from vertebra import _vertebra
from vertebra.ipc.channel import BufferView, Channel, ChannelError, SerializationError, ZeroCopyChannel

ShmRegion = _vertebra.ShmRegion
BufferPool = _vertebra.BufferPool
Buffer = _vertebra.Buffer
ChannelHandle = _vertebra.Channel
SpscRing = _vertebra.SpscRing
MpmcRing = _vertebra.MpmcRing
BufferChannel = _vertebra.BufferChannel

__all__ = [
    "Buffer",
    "BufferChannel",
    "BufferPool",
    "BufferView",
    "Channel",
    "ChannelError",
    "ChannelHandle",
    "MpmcRing",
    "SerializationError",
    "ShmRegion",
    "SpscRing",
    "ZeroCopyChannel",
]
