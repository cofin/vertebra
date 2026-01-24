"""Protocol definitions for IPC control plane.

This module provides zero-parse-cost message headers for IPC communication.
Headers use a fixed 16-byte format that can be read directly from shared memory
without parsing overhead.

Wire Format
-----------
Messages are structured as:

    ┌─────────────────────┬─────────────────────┐
    │  Header (16 bytes)  │  MessagePack Payload │
    └─────────────────────┴─────────────────────┘

The header contains:
- id: 64-bit correlation ID for multiplexing
- flags: 32-bit flags (priority, compression, etc.)
- payload_len: 32-bit length of the MessagePack payload
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntFlag
from typing import Self

__all__ = ["HEADER_SIZE", "ControlMessageKind", "Flags", "RequestHeader", "ResponseFlags", "ResponseHeader"]

# Header format: little-endian u64 + u32 + u32 = 16 bytes
_HEADER_STRUCT = struct.Struct("<QII")
HEADER_SIZE = _HEADER_STRUCT.size  # 16 bytes


class Flags(IntFlag):
    """Request flags for IPC messages."""

    NONE = 0
    """No special flags."""

    PRIORITY = 1 << 0
    """Request requires immediate processing (skip queue)."""

    COMPRESSED = 1 << 1
    """Payload is compressed."""

    FIRE_AND_FORGET = 1 << 2
    """Request expects no response."""

    BATCH = 1 << 3
    """Request is part of a batch."""

    OK = 1 << 4
    """Response indicates success."""

    ERROR = 1 << 5
    """Response contains an error."""


class ResponseFlags(IntFlag):
    """Response flags for IPC messages."""

    NONE = Flags.NONE
    OK = Flags.OK
    ERROR = Flags.ERROR
    COMPRESSED = Flags.COMPRESSED


@dataclass(frozen=True, slots=True)
class RequestHeader:
    """Fixed-size request header (16 bytes).

    This header is designed for zero-copy reads directly from shared memory.
    All fields are little-endian for consistent cross-platform behavior.
    """

    id: int
    """Correlation ID for request/response matching in multiplexed scenarios."""

    flags: int
    """Request flags (see Flags class for constants)."""

    payload_len: int
    """Length of the MessagePack payload following this header."""

    @classmethod
    def from_bytes(cls, data: bytes | memoryview) -> Self:
        """Parse header from bytes (zero-copy from memoryview).

        Args:
            data: At least 16 bytes of header data.

        Returns:
            Parsed header.

        Raises:
            ValueError: If data is too short.
        """
        if len(data) < HEADER_SIZE:
            msg = f"Header requires {HEADER_SIZE} bytes, got {len(data)}"
            raise ValueError(msg)
        id_, flags, payload_len = _HEADER_STRUCT.unpack_from(data)
        return cls(id=id_, flags=flags, payload_len=payload_len)

    def to_bytes(self) -> bytes:
        """Serialize header to bytes.

        Returns:
            16-byte header.
        """
        return _HEADER_STRUCT.pack(self.id, self.flags, self.payload_len)

    def write_to(self, buffer: bytearray | memoryview, offset: int = 0) -> None:
        """Write header to buffer at offset (zero-copy).

        Args:
            buffer: Writable buffer.
            offset: Byte offset to write at.
        """
        _HEADER_STRUCT.pack_into(buffer, offset, self.id, self.flags, self.payload_len)


@dataclass(frozen=True, slots=True)
class ResponseHeader:
    """Fixed-size response header (16 bytes)."""

    id: int
    """Correlation ID (matches the request)."""

    flags: int
    """Response flags."""

    payload_len: int
    """Length of the MessagePack payload following this header."""

    @classmethod
    def from_bytes(cls, data: bytes | memoryview) -> Self:
        """Parse header from bytes."""
        if len(data) < HEADER_SIZE:
            msg = f"Header requires {HEADER_SIZE} bytes, got {len(data)}"
            raise ValueError(msg)
        id_, flags, payload_len = _HEADER_STRUCT.unpack_from(data)
        return cls(id=id_, flags=flags, payload_len=payload_len)

    def to_bytes(self) -> bytes:
        """Serialize header to bytes."""
        return _HEADER_STRUCT.pack(self.id, self.flags, self.payload_len)

    def write_to(self, buffer: bytearray | memoryview, offset: int = 0) -> None:
        """Write header to buffer at offset."""
        _HEADER_STRUCT.pack_into(buffer, offset, self.id, self.flags, self.payload_len)

    @property
    def is_ok(self) -> bool:
        """Check if response indicates success."""
        return bool(self.flags & ResponseFlags.OK)

    @property
    def is_error(self) -> bool:
        """Check if response contains an error."""
        return bool(self.flags & ResponseFlags.ERROR)


class ControlMessageKind:
    """Control message types for broker coordination."""

    CONNECT = "connect"
    DISCONNECT = "disconnect"
    HEARTBEAT = "heartbeat"
    SHUTDOWN = "shutdown"

    _VALID = frozenset({CONNECT, DISCONNECT, HEARTBEAT, SHUTDOWN})

    @classmethod
    def is_valid(cls, kind: str) -> bool:
        """Check if kind is a valid control message type."""
        return kind in cls._VALID
