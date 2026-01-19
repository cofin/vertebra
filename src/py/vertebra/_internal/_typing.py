# ruff: noqa: BLE001
"""Type stubs and compatibility shims for optional native dependencies.

This module provides:
1. Stub classes for when native libraries aren't available
2. Centralized re-exports of native types with availability flags
3. Protocols for context managers with proper exit annotations

Import pattern:
    from vertebra._internal._typing import (
        SHM_AVAILABLE,
        ShmRing,
        ShmRingConfig,
        get_shm_load_error,
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import msgspec

if TYPE_CHECKING:
    from types import TracebackType


# ============================================================================
# Context Manager Protocols (with proper __exit__ annotations per PYI036)
# ============================================================================


@runtime_checkable
class SyncContextManager(Protocol):
    """Protocol for synchronous context managers with proper exit typing."""

    def __enter__(self) -> Any: ...

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> bool | None: ...


@runtime_checkable
class AsyncContextManager(Protocol):
    """Protocol for asynchronous context managers with proper exit typing."""

    async def __aenter__(self) -> Any: ...

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> bool | None: ...


# ============================================================================
# Shared Memory Type Stubs
# ============================================================================


class ShmRingConfigStub(msgspec.Struct, frozen=True, gc=False):
    """Stub for ShmRingConfig when native library unavailable."""

    name: str
    slot_size: int = 65536
    slots: int = 1024


class ShmRegionConfigStub(msgspec.Struct, frozen=True, gc=False):
    """Stub for ShmRegionConfig when native library unavailable."""

    name: str
    size: int


class ShmRingStub:
    """Stub for ShmRing when native library unavailable.

    All methods raise RuntimeError to indicate the library is missing.
    """

    __slots__ = ("_capacity", "_handle")

    def __init__(self, handle: object, capacity: int) -> None:
        msg = "Shared memory library unavailable"
        raise RuntimeError(msg)

    @classmethod
    def create(cls, config: ShmRingConfigStub) -> ShmRingStub:
        """Create a new shared memory ring buffer."""
        msg = "Shared memory library unavailable"
        raise RuntimeError(msg)

    @classmethod
    def open(cls, config: ShmRingConfigStub) -> ShmRingStub:
        """Open an existing shared memory ring buffer."""
        msg = "Shared memory library unavailable"
        raise RuntimeError(msg)

    @property
    def capacity(self) -> int:
        """Return the buffer capacity in bytes."""
        return 0

    def send(self, payload: bytes, flags: int = 0, timeout_ms: int = 0) -> None:
        """Send data to the ring buffer."""

    def recv(self, timeout_ms: int = 0) -> tuple[bytes, int]:
        """Receive data from the ring buffer."""
        return b"", 0

    def stats(self) -> dict[str, int]:
        """Return buffer statistics."""
        return {}

    def close(self) -> None:
        """Close the ring buffer handle."""


class ShmRegionStub:
    """Stub for ShmRegion when native library unavailable."""

    __slots__ = ("_handle",)

    def __init__(self, handle: object) -> None:
        msg = "Shared memory library unavailable"
        raise RuntimeError(msg)

    @classmethod
    def create(cls, config: ShmRegionConfigStub) -> ShmRegionStub:
        """Create a new shared memory region."""
        msg = "Shared memory library unavailable"
        raise RuntimeError(msg)

    @classmethod
    def open(cls, config: ShmRegionConfigStub) -> ShmRegionStub:
        """Open an existing shared memory region."""
        msg = "Shared memory library unavailable"
        raise RuntimeError(msg)

    def buffer(self) -> memoryview:
        """Return a memoryview of the shared memory region."""
        return memoryview(b"")

    def close(self) -> None:
        """Close the region handle."""


class ShmErrorStub(RuntimeError):
    """Stub exception for shared memory errors."""


# ============================================================================
# Load Real Types or Fall Back to Stubs
# ============================================================================

_SHM_LOAD_ERROR: Exception | None = None

def _shm_unlink_stub(name: str) -> None:
    """Stub for shm_unlink when native library unavailable."""
    msg = "Shared memory library unavailable"
    raise ShmErrorStub(msg)


try:
    from vertebra._internal.shm import SHM_AVAILABLE as _SHM_AVAILABLE
    from vertebra._internal.shm import ShmError as _ShmError
    from vertebra._internal.shm import ShmRegion as _ShmRegion
    from vertebra._internal.shm import ShmRegionConfig as _ShmRegionConfig
    from vertebra._internal.shm import ShmRing as _ShmRing
    from vertebra._internal.shm import ShmRingConfig as _ShmRingConfig
    from vertebra._internal.shm import shm_unlink as _shm_unlink

    SHM_AVAILABLE: bool = _SHM_AVAILABLE
    ShmRing = _ShmRing
    ShmRingConfig = _ShmRingConfig
    ShmRegion = _ShmRegion
    ShmRegionConfig = _ShmRegionConfig
    ShmError = _ShmError
    shm_unlink = _shm_unlink
except Exception as exc:
    _SHM_LOAD_ERROR = exc
    SHM_AVAILABLE: bool = False  # type: ignore[no-redef]
    ShmRing = ShmRingStub  # type: ignore[assignment,misc]
    ShmRingConfig = ShmRingConfigStub  # type: ignore[assignment,misc]
    ShmRegion = ShmRegionStub  # type: ignore[assignment,misc]
    ShmRegionConfig = ShmRegionConfigStub  # type: ignore[assignment,misc]
    ShmError = ShmErrorStub  # type: ignore[assignment,misc]
    shm_unlink = _shm_unlink_stub


def get_shm_load_error() -> Exception | None:
    """Return the error that occurred when loading the shm module, if any."""
    return _SHM_LOAD_ERROR


__all__ = (
    # Availability flag
    "SHM_AVAILABLE",
    # Protocols
    "AsyncContextManager",
    # Real or stub types
    "ShmError",
    # Stubs (for explicit use)
    "ShmErrorStub",
    "ShmRegion",
    "ShmRegionConfig",
    "ShmRegionConfigStub",
    "ShmRegionStub",
    "ShmRing",
    "ShmRingConfig",
    "ShmRingConfigStub",
    "ShmRingStub",
    "SyncContextManager",
    # Helpers
    "get_shm_load_error",
    "shm_unlink",
)
