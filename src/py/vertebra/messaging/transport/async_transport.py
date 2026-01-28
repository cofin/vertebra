"""Native asyncio transport for shared-memory IPC.

This module provides async wrappers for the Rust-based ring buffers that
integrate natively with asyncio's event loop via file descriptor notifications.

Instead of using asyncio.to_thread() which adds 50-130μs overhead per call,
this transport uses loop.add_reader() to wait for data without blocking any threads.

For sub-20µs latency, this module implements hybrid spin-wait: spinning briefly
before falling back to add_reader() to capture immediate responses without
syscall overhead.

Example:
    from vertebra.messaging.transport.async_transport import AsyncShmTransport
    from vertebra._vertebra import AsyncRing

    ring = AsyncRing(65536, 1024)
    transport = AsyncShmTransport(ring)

    # Send data
    await transport.send(b"hello")

    # Receive data (hybrid spin-wait + event loop)
    data = await transport.recv()
"""

import asyncio
import contextlib
import os
import sys
from collections import deque
from typing import Protocol

# Default spin iterations for hybrid wait.
# PERF: Using iteration count instead of time.perf_counter() saves ~1-7µs per recv().
# perf_counter() costs ~100-300ns per call; with 10-25 iterations that adds up.
# Read from VERTEBRA_SPIN_ITERS environment variable (default: 50).
# ~50 iterations ≈ 5µs on typical hardware (calibrate for your target).
DEFAULT_SPIN_ITERS: int = int(os.environ.get("VERTEBRA_SPIN_ITERS", "50"))


class _AsyncRingProtocol(Protocol):
    """Protocol for AsyncRing from vertebra._vertebra."""

    def notifier_fd(self) -> int:
        """Return the file descriptor for event loop integration."""
        ...

    def send(self, data: bytes, flags: int | None = None) -> None:
        """Send data to the ring buffer."""
        ...

    def try_send(self, data: bytes, flags: int | None = None) -> bool:
        """Try to send data, returns True on success."""
        ...

    def try_recv(self) -> bytes | None:
        """Try to receive data non-blocking."""
        ...

    def try_recv_with_flags(self) -> tuple[bytes, int] | None:
        """Try to receive data with flags."""
        ...

    def clear_notification(self) -> None:
        """Clear pending notification."""
        ...

    def has_pending_notification(self) -> bool:
        """Check if there's a pending notification."""
        ...

    def payload_capacity(self) -> int:
        """Return the capacity of the ring buffer."""
        ...


class AsyncShmTransport:
    """Async wrapper for shared-memory ring buffer using eventfd/pipe notifications.

    This class integrates with asyncio's event loop using loop.add_reader()
    to wait for data without blocking any threads. When data arrives on the
    ring buffer, the producer signals via eventfd (Linux) or pipe (macOS/Windows),
    and the event loop wakes up to process the data.

    For sub-20µs latency, this transport implements hybrid spin-wait:
    1. Fast path: try_recv() immediate check
    2. Spin phase: busy-wait for spin_duration microseconds
    3. Slow path: add_reader() and await notification

    This achieves sub-20μs latency by capturing immediate responses without
    syscall overhead while preserving correct async behavior for longer waits.

    Attributes:
        _ring: The underlying AsyncRing from Rust
        _fd: File descriptor for event loop integration
        _waiters: Queue of pending recv() futures
        _registered: Whether the FD is registered with the event loop
        _spin_iters: Number of spin iterations before falling back to add_reader
    """

    __slots__ = ("_fd", "_loop", "_registered", "_ring", "_spin_iters", "_waiters")

    def __init__(self, ring: _AsyncRingProtocol, *, spin_iters: int | None = None) -> None:
        """Initialize the transport with an AsyncRing.

        Args:
            ring: AsyncRing instance from vertebra._vertebra
            spin_iters: Number of spin iterations before add_reader fallback.
                       Defaults to VERTEBRA_SPIN_ITERS env var or 50.
                       Set to 0 to disable spin-wait entirely.
        """
        self._ring = ring
        self._fd = ring.notifier_fd()
        self._waiters: deque[asyncio.Future[bytes]] = deque()
        self._registered = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._spin_iters = spin_iters if spin_iters is not None else DEFAULT_SPIN_ITERS

    @property
    def fd(self) -> int:
        """Get the notification file descriptor."""
        return self._fd

    @property
    def payload_capacity(self) -> int:
        """Get the maximum payload size per message."""
        return self._ring.payload_capacity()

    async def send(self, data: bytes, flags: int = 0) -> None:
        """Send data to the ring buffer.

        This is a blocking send that waits for space if the ring is full.
        The GIL is released during the wait.

        Args:
            data: Bytes to send
            flags: Optional message flags (default 0)
        """
        # The Rust send() releases the GIL while waiting
        self._ring.send(data, flags)

    def try_send(self, data: bytes, flags: int = 0) -> bool:
        """Try to send without blocking.

        Args:
            data: Bytes to send
            flags: Optional message flags (default 0)

        Returns:
            True if sent successfully, False if ring is full
        """
        return self._ring.try_send(data, flags)

    async def recv(self) -> bytes:
        """Receive data from the ring buffer.

        This method implements hybrid spin-wait for sub-20µs latency:
        1. Fast path: try_recv() immediate check
        2. Spin phase: iteration-based busy-wait checking try_recv()
        3. Slow path: add_reader() and await notification

        PERF: Uses iteration count instead of time.perf_counter() to avoid
        ~100-300ns syscall overhead per timer check.

        Returns:
            Received bytes

        Raises:
            RuntimeError: If transport is closed
        """
        # 1. Fast path: data already available
        data = self._ring.try_recv()
        if data is not None:
            return data

        # 2. Spin phase: iteration-based busy-wait to capture immediate responses
        # PERF: Avoids time.perf_counter() overhead (~100-300ns per call)
        for _ in range(self._spin_iters):
            data = self._ring.try_recv()
            if data is not None:
                return data

        # 3. Slow path: wait for notification via add_reader
        return await self._recv_slow_path()

    async def recv_with_flags(self) -> tuple[bytes, int]:
        """Receive data with message flags.

        This method implements hybrid spin-wait for sub-20µs latency:
        1. Fast path: try_recv_with_flags() immediate check
        2. Spin phase: iteration-based busy-wait
        3. Slow path: add_reader() and await notification

        PERF: Uses iteration count instead of time.perf_counter().

        Returns:
            Tuple of (data, flags)
        """
        # 1. Fast path
        result = self._ring.try_recv_with_flags()
        if result is not None:
            return result

        # 2. Spin phase: iteration-based busy-wait
        for _ in range(self._spin_iters):
            result = self._ring.try_recv_with_flags()
            if result is not None:
                return result

        # 3. Slow path - wait then get with flags
        data = await self._recv_slow_path()
        return data, 0  # Flags are consumed in the wait path

    async def _recv_slow_path(self) -> bytes:
        """Internal slow path: add_reader and await notification."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[bytes] = loop.create_future()
        self._waiters.append(future)

        if not self._registered:
            self._loop = loop
            loop.add_reader(self._fd, self._on_readable)
            self._registered = True

        return await future

    def try_recv(self) -> bytes | None:
        """Try to receive without waiting.

        Returns:
            Received bytes, or None if empty
        """
        return self._ring.try_recv()

    def _on_readable(self) -> None:
        """Called when the notification FD is readable (data available)."""
        # Clear the notification first to avoid spurious wakeups
        self._ring.clear_notification()

        # Try to satisfy all waiters
        while self._waiters:
            data = self._ring.try_recv()
            if data is None:
                break
            future = self._waiters.popleft()
            if not future.done():
                future.set_result(data)

        # Unregister if no more waiters (saves syscalls)
        if not self._waiters and self._registered:
            if self._loop is not None:
                with contextlib.suppress(ValueError, RuntimeError):
                    self._loop.remove_reader(self._fd)
            self._registered = False

    def close(self) -> None:
        """Close the transport and clean up.

        This cancels any pending recv() operations and unregisters the FD.
        """
        if self._registered:
            if self._loop is not None:
                with contextlib.suppress(ValueError, RuntimeError):
                    self._loop.remove_reader(self._fd)
            self._registered = False

        # Cancel pending waiters
        for future in self._waiters:
            if not future.done():
                future.cancel("Transport closed")
        self._waiters.clear()
        self._loop = None

    def __del__(self) -> None:
        """Cleanup on garbage collection."""
        with contextlib.suppress(Exception):
            self.close()


class UnsupportedPlatformError(RuntimeError):
    """Raised when the platform doesn't support notification-based IPC."""


def create_transport(ring: _AsyncRingProtocol, *, spin_iters: int | None = None) -> AsyncShmTransport:
    """Create the notification-based transport for shared-memory IPC.

    Uses AsyncShmTransport which integrates via add_reader() for
    notification-driven, low-latency operation.

    Args:
        ring: AsyncRing instance from vertebra._vertebra
        spin_iters: Number of spin iterations before add_reader fallback.
                   Defaults to VERTEBRA_SPIN_ITERS env var or 50.

    Returns:
        AsyncShmTransport instance

    Raises:
        UnsupportedPlatformError: If the platform doesn't support add_reader()
    """
    # Check if we can use add_reader()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop - caller will need to set up later
        return AsyncShmTransport(ring, spin_iters=spin_iters)

    # Check if the loop supports add_reader (selector-based loops do)
    if hasattr(loop, "add_reader"):
        # On Windows, ProactorEventLoop has add_reader but it doesn't work
        # with arbitrary FDs
        if sys.platform == "win32" and hasattr(loop, "_proactor"):
            msg = (
                "Windows ProactorEventLoop does not support notification-based IPC. "
                "Use SelectorEventLoop or run on Linux/macOS."
            )
            raise UnsupportedPlatformError(msg)
        return AsyncShmTransport(ring, spin_iters=spin_iters)

    msg = "Event loop does not support add_reader(). Notification-based IPC requires a selector-based event loop."
    raise UnsupportedPlatformError(msg)


__all__ = ("DEFAULT_SPIN_ITERS", "AsyncShmTransport", "UnsupportedPlatformError", "create_transport")
