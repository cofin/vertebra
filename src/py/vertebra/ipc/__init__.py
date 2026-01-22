"""IPC primitives for shared-memory communication.

This module provides IPC channel wrappers backed by the Rust core extension:

Native Types (from vertebra-core via PyO3):
    ShmRegion - Shared memory region with buffer protocol
    BufferPool - Pool of reusable shared buffers
    Buffer - Single shared buffer
    ChannelHandle - Low-level bidirectional SPSC channel
    SpscRing - Single-producer single-consumer ring buffer
    MpmcRing - Multi-producer multi-consumer ring buffer
    BufferChannel - High-performance buffer-based channel

Python Wrappers:
    Channel - High-level request/response channel
    ZeroCopyChannel - BufferChannel wrapper returning BufferView objects
    BufferView - Zero-copy view into a shared buffer

Async Transport:
    AsyncShmTransport - Async transport using eventfd/kqueue
    PollingTransport - Fallback polling-based transport
    create_transport - Factory for creating appropriate transport

Exceptions:
    ChannelError - Channel operation failures
    SerializationError - Serialization/deserialization failures
"""

from vertebra import _vertebra
from vertebra.exceptions import ChannelError, SerializationError
from vertebra.ipc.async_transport import AsyncShmTransport, PollingTransport, create_transport
from vertebra.ipc.channel import BufferView, Channel, ZeroCopyChannel

ShmRegion = _vertebra.ShmRegion
BufferPool = _vertebra.BufferPool
Buffer = _vertebra.Buffer
ChannelHandle = _vertebra.Channel
SpscRing = _vertebra.SpscRing
MpmcRing = _vertebra.MpmcRing
BufferChannel = _vertebra.BufferChannel

__all__ = (
    # Async transport
    "AsyncShmTransport",
    "Buffer",
    "BufferChannel",
    "BufferPool",
    "BufferView",
    # Python wrappers
    "Channel",
    # Exceptions
    "ChannelError",
    "ChannelHandle",
    "MpmcRing",
    "PollingTransport",
    "SerializationError",
    # Native types (Rust core)
    "ShmRegion",
    "SpscRing",
    "ZeroCopyChannel",
    "create_transport",
)
