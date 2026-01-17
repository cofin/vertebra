"""IPC surface backed by the Rust core extension."""

from __future__ import annotations

from vertebra import _vertebra

ShmRegion = _vertebra.ShmRegion
BufferPool = _vertebra.BufferPool
Buffer = _vertebra.Buffer
SpscRing = _vertebra.SpscRing
MpmcRing = _vertebra.MpmcRing
BufferChannel = _vertebra.BufferChannel

__all__ = [
    "Buffer",
    "BufferChannel",
    "BufferPool",
    "MpmcRing",
    "ShmRegion",
    "SpscRing",
]
