"""IPC surface backed by the Rust core extension."""

from vertebra import _vertebra
from vertebra.ipc.async_transport import AsyncShmTransport, PollingTransport, create_transport
from vertebra.ipc.channel import BufferView, Channel, ChannelError, SerializationError, ZeroCopyChannel

ShmRegion = _vertebra.ShmRegion
BufferPool = _vertebra.BufferPool
Buffer = _vertebra.Buffer
ChannelHandle = _vertebra.Channel
SpscRing = _vertebra.SpscRing
MpmcRing = _vertebra.MpmcRing
BufferChannel = _vertebra.BufferChannel

__all__ = (
    "AsyncShmTransport",
    "Buffer",
    "BufferChannel",
    "BufferPool",
    "BufferView",
    "Channel",
    "ChannelError",
    "ChannelHandle",
    "MpmcRing",
    "PollingTransport",
    "SerializationError",
    "ShmRegion",
    "SpscRing",
    "ZeroCopyChannel",
    "create_transport",
)
