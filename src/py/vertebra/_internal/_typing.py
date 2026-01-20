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
    def create(cls, _config: ShmRingConfigStub) -> ShmRingStub:
        """Create a new shared memory ring buffer."""
        msg = "Shared memory library unavailable"
        raise RuntimeError(msg)

    @classmethod
    def open(cls, _config: ShmRingConfigStub) -> ShmRingStub:
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
    def create(cls, _config: ShmRegionConfigStub) -> ShmRegionStub:
        """Create a new shared memory region."""
        msg = "Shared memory library unavailable"
        raise RuntimeError(msg)

    @classmethod
    def open(cls, _config: ShmRegionConfigStub) -> ShmRegionStub:
        """Open an existing shared memory region."""
        msg = "Shared memory library unavailable"
        raise RuntimeError(msg)

    def buffer(self) -> memoryview:
        """Return a memoryview of the shared memory region."""
        return memoryview(b"")

    def close(self) -> None:
        """Close the region handle."""


class ShmStubError(RuntimeError):
    """Stub exception for shared memory errors."""


# ============================================================================
# Load Real Types or Fall Back to Stubs
# ============================================================================

_SHM_LOAD_ERROR: Exception | None = None


def _shm_unlink_stub(name: str) -> None:
    """Stub for shm_unlink when native library unavailable."""
    msg = "Shared memory library unavailable"
    raise ShmStubError(msg)


try:
    from vertebra._vertebra import ShmRegion as _ShmRegion
    from vertebra._vertebra import SpscRing as _SpscRing
    from vertebra._vertebra import shm_unlink as _shm_unlink

    SHM_AVAILABLE: bool = True
    # SpscRing replaces the old ShmRing (CFFI). API differs slightly:
    # - create_named(name, slot_size, slots) instead of create(ShmRingConfig(...))
    # - send_timeout(data, flags, timeout_ms) instead of send(data, flags, timeout_ms)
    # - try_recv(max_len) / recv_timeout(max_len, timeout_ms) instead of recv(timeout_ms)
    SpscRing = _SpscRing
    ShmRing = _SpscRing  # Alias for backward compatibility
    ShmRingConfig = ShmRingConfigStub  # Config no longer used; construct rings directly
    ShmRegion = _ShmRegion
    ShmRegionConfig = ShmRegionConfigStub  # Config no longer used; construct regions directly
    ShmError = ShmStubError  # PyO3 raises RuntimeError instead
    shm_unlink = _shm_unlink
except Exception as exc:
    _SHM_LOAD_ERROR = exc
    SHM_AVAILABLE: bool = False  # type: ignore[no-redef]
    SpscRing = ShmRingStub  # type: ignore[misc,assignment]
    ShmRing = ShmRingStub  # type: ignore[misc,assignment]
    ShmRingConfig = ShmRingConfigStub  # type: ignore[misc]
    ShmRegion = ShmRegionStub  # type: ignore[misc,assignment]
    ShmRegionConfig = ShmRegionConfigStub  # type: ignore[misc]
    ShmError = ShmStubError  # type: ignore[misc]
    shm_unlink = _shm_unlink_stub


def get_shm_load_error() -> Exception | None:
    """Return the error that occurred when loading the shm module, if any."""
    return _SHM_LOAD_ERROR


# ============================================================================
# Async Transport Types (PRD-10: Eventfd Async Transport)
# ============================================================================

_ASYNC_TRANSPORT_LOAD_ERROR: Exception | None = None


class AsyncRingStub:
    """Stub for AsyncRing when PyO3 native library unavailable."""

    def __init__(self, slot_size: int, slots: int) -> None:
        msg = "AsyncRing not available (vertebra._vertebra not installed)"
        raise RuntimeError(msg)

    @classmethod
    def create_named(cls, _name: str, _slot_size: int, _slots: int) -> AsyncRingStub:
        msg = "AsyncRing not available"
        raise RuntimeError(msg)

    @classmethod
    def open_named(cls, _name: str, _slot_size: int, _slots: int) -> AsyncRingStub:
        msg = "AsyncRing not available"
        raise RuntimeError(msg)

    def notifier_fd(self) -> int:
        return -1

    def send(self, data: bytes, flags: int | None = None) -> None:
        pass

    def try_send(self, data: bytes, flags: int | None = None) -> bool:
        return False

    def try_recv(self) -> bytes | None:
        return None

    def clear_notification(self) -> None:
        pass


class AsyncShmTransportStub:
    """Stub for AsyncShmTransport when async transport unavailable."""

    def __init__(self, ring: Any) -> None:
        msg = "AsyncShmTransport not available"
        raise RuntimeError(msg)

    async def send(self, data: bytes, flags: int = 0) -> None:
        pass

    async def recv(self) -> bytes:
        return b""

    def try_recv(self) -> bytes | None:
        return None

    def close(self) -> None:
        pass


try:
    from vertebra._internal.async_transport import AsyncShmTransport as _AsyncShmTransport
    from vertebra._internal.async_transport import PollingTransport as _PollingTransport
    from vertebra._internal.async_transport import create_transport as _create_transport
    from vertebra._vertebra import AsyncRing as _AsyncRing

    ASYNC_TRANSPORT_AVAILABLE: bool = True
    AsyncRing = _AsyncRing
    AsyncShmTransport = _AsyncShmTransport
    PollingTransport = _PollingTransport
    create_transport = _create_transport
except Exception as exc:
    _ASYNC_TRANSPORT_LOAD_ERROR = exc
    ASYNC_TRANSPORT_AVAILABLE: bool = False  # type: ignore[no-redef]
    AsyncRing = AsyncRingStub  # type: ignore[assignment,misc]
    AsyncShmTransport = AsyncShmTransportStub  # type: ignore[assignment,misc]
    PollingTransport = AsyncShmTransportStub  # type: ignore[assignment,misc]

    def create_transport(ring: Any, *, prefer_eventfd: bool = True, poll_interval: float = 0.0001) -> Any:
        msg = "Async transport not available"
        raise RuntimeError(msg)


def get_async_transport_load_error() -> Exception | None:
    """Return the error that occurred when loading async transport, if any."""
    return _ASYNC_TRANSPORT_LOAD_ERROR


__all__ = (
    # Availability flags
    "ASYNC_TRANSPORT_AVAILABLE",
    "SHM_AVAILABLE",
    # Protocols
    "AsyncContextManager",
    # Async transport types (PRD-10)
    "AsyncRing",
    "AsyncRingStub",
    "AsyncShmTransport",
    "AsyncShmTransportStub",
    "PollingTransport",
    # CFFI shm types
    "ShmError",
    "ShmRegion",
    "ShmRegionConfig",
    "ShmRegionConfigStub",
    "ShmRegionStub",
    "ShmRing",
    "ShmRingConfig",
    "ShmRingConfigStub",
    "ShmRingStub",
    "ShmStubError",
    "SyncContextManager",
    "create_transport",
    # Helpers
    "get_async_transport_load_error",
    "get_shm_load_error",
    "shm_unlink",
)
