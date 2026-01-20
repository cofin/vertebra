"""Native asyncio transport for shared-memory IPC.

This module provides async wrappers for the Rust-based ring buffers that
integrate natively with asyncio's event loop via file descriptor notifications.

Instead of using asyncio.to_thread() which adds 50-130μs overhead per call,
this transport uses loop.add_reader() to wait for data without blocking any threads.

Example:
    from vertebra._internal.async_transport import AsyncShmTransport
    from vertebra._vertebra import AsyncRing

    ring = AsyncRing(65536, 1024)
    transport = AsyncShmTransport(ring)

    # Send data
    await transport.send(b"hello")

    # Receive data (waits via event loop, not thread pool)
    data = await transport.recv()
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import threading
import time
from collections import deque
from typing import Protocol


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

    This achieves sub-100μs latency by eliminating thread pool overhead.

    Attributes:
        _ring: The underlying AsyncRing from Rust
        _fd: File descriptor for event loop integration
        _waiters: Queue of pending recv() futures
        _registered: Whether the FD is registered with the event loop
    """

    __slots__ = ("_fd", "_loop", "_registered", "_ring", "_waiters")

    def __init__(self, ring: _AsyncRingProtocol) -> None:
        """Initialize the transport with an AsyncRing.

        Args:
            ring: AsyncRing instance from vertebra._vertebra
        """
        self._ring = ring
        self._fd = ring.notifier_fd()
        self._waiters: deque[asyncio.Future[bytes]] = deque()
        self._registered = False
        self._loop: asyncio.AbstractEventLoop | None = None

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

        This method:
        1. Tries non-blocking recv first (fast path)
        2. If empty, registers with event loop and waits
        3. When notified, tries recv again

        Returns:
            Received bytes

        Raises:
            RuntimeError: If transport is closed
        """
        # Fast path: data already available
        data = self._ring.try_recv()
        if data is not None:
            return data

        # Slow path: wait for notification
        loop = asyncio.get_running_loop()
        future: asyncio.Future[bytes] = loop.create_future()
        self._waiters.append(future)

        if not self._registered:
            self._loop = loop
            loop.add_reader(self._fd, self._on_readable)
            self._registered = True

        return await future

    async def recv_with_flags(self) -> tuple[bytes, int]:
        """Receive data with message flags.

        Returns:
            Tuple of (data, flags)
        """
        # Fast path
        result = self._ring.try_recv_with_flags()
        if result is not None:
            return result

        # Slow path - wait then get with flags
        data = await self.recv()
        return data, 0  # Flags are consumed in the wait path

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


class PollingTransport:
    """Fallback transport using a single polling thread.

    This is used on platforms where eventfd is not available or when
    the event loop doesn't support add_reader() (e.g., ProactorEventLoop
    on Windows).

    The polling thread checks for data at a configurable interval and
    enqueues received data for async consumption.
    """

    __slots__ = ("_loop", "_poll_interval", "_queue", "_ring", "_running", "_thread")

    def __init__(
        self,
        ring: _AsyncRingProtocol,
        poll_interval: float = 0.0001,  # 100μs default
    ) -> None:
        """Initialize the polling transport.

        Args:
            ring: AsyncRing instance from vertebra._vertebra
            poll_interval: Polling interval in seconds (default 100μs)
        """
        self._ring = ring
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._running = True
        self._loop: asyncio.AbstractEventLoop | None = None
        self._poll_interval = poll_interval
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()

    @property
    def payload_capacity(self) -> int:
        """Get the maximum payload size per message."""
        return self._ring.payload_capacity()

    async def send(self, data: bytes, flags: int = 0) -> None:
        """Send data to the ring buffer."""
        self._ring.send(data, flags)

    def try_send(self, data: bytes, flags: int = 0) -> bool:
        """Try to send without blocking."""
        return self._ring.try_send(data, flags)

    async def recv(self) -> bytes:
        """Receive data from the ring buffer.

        Returns:
            Received bytes
        """
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return await self._queue.get()

    def try_recv(self) -> bytes | None:
        """Try to receive without waiting."""
        return self._ring.try_recv()

    def _poll_loop(self) -> None:
        """Background thread that polls for data."""
        while self._running:
            data = self._ring.try_recv()
            if data is not None and self._loop is not None:
                # Thread-safe enqueue
                self._loop.call_soon_threadsafe(self._queue.put_nowait, data)
            else:
                time.sleep(self._poll_interval)

    def close(self) -> None:
        """Stop the polling thread and clean up."""
        self._running = False
        if self._thread.is_alive():
            self._thread.join(timeout=1.0)
        self._loop = None


def create_transport(
    ring: _AsyncRingProtocol, *, prefer_eventfd: bool = True, poll_interval: float = 0.0001
) -> AsyncShmTransport | PollingTransport:
    """Create an appropriate transport for the current platform.

    On Linux with a selector-based event loop, uses AsyncShmTransport
    which integrates via add_reader(). On other platforms or with
    ProactorEventLoop, falls back to PollingTransport.

    Args:
        ring: AsyncRing instance from vertebra._vertebra
        prefer_eventfd: If True, try to use eventfd-based transport first
        poll_interval: Polling interval for fallback transport

    Returns:
        An appropriate transport instance
    """
    if not prefer_eventfd:
        return PollingTransport(ring, poll_interval=poll_interval)

    # Check if we can use add_reader()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop - caller will need to set up later
        return AsyncShmTransport(ring)

    # Check if the loop supports add_reader (selector-based loops do)
    if hasattr(loop, "add_reader"):
        # On Windows, ProactorEventLoop has add_reader but it doesn't work
        # with arbitrary FDs. We detect this by checking for _proactor attr.
        if sys.platform == "win32" and hasattr(loop, "_proactor"):
            return PollingTransport(ring, poll_interval=poll_interval)
        return AsyncShmTransport(ring)

    return PollingTransport(ring, poll_interval=poll_interval)


__all__ = ("AsyncShmTransport", "PollingTransport", "create_transport")
