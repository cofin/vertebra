"""Async SHM transport wrapper using ``asyncio.add_reader()``.

Registers the request notification FD with the asyncio event loop for
zero-poll wakeup. When the supervisor writes a batch to the SPSC ring
and triggers the eventfd, the event loop fires ``_on_readable`` which
drains all available batches into an ``asyncio.Queue``.

Usage::

    from vertebra._vertebra import get_request_notify_fd, clear_request_notification, try_get_batch
    transport = ShmAsyncTransport(get_request_notify_fd, clear_request_notification, try_get_batch)
    transport.register(loop)
    batch = await transport.get_batch()
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class ShmAsyncTransport:
    """Async wrapper for SHM transport using ``add_reader``."""

    __slots__ = ("_notify_fd_fn", "_clear_fn", "_try_get_fn", "_queue", "_fd")

    def __init__(
        self,
        notify_fd_fn: "Callable[[], int]",
        clear_fn: "Callable[[], None]",
        try_get_fn: "Callable[[], Any | None]",
    ) -> None:
        self._notify_fd_fn = notify_fd_fn
        self._clear_fn = clear_fn
        self._try_get_fn = try_get_fn
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self._fd: int = -1

    def register(self, loop: asyncio.AbstractEventLoop) -> None:
        """Register the request notification FD with the event loop."""
        self._fd = self._notify_fd_fn()
        if self._fd >= 0:
            loop.add_reader(self._fd, self._on_readable)

    def unregister(self, loop: asyncio.AbstractEventLoop) -> None:
        """Remove the FD from the event loop."""
        if self._fd >= 0:
            loop.remove_reader(self._fd)

    def _on_readable(self) -> None:
        """Called by the event loop when the notification FD is readable."""
        self._clear_fn()
        while True:
            batch = self._try_get_fn()
            if batch is None:
                break
            self._queue.put_nowait(batch)

    async def get_batch(self) -> Any:
        """Await the next batch from the SHM ring (zero-poll)."""
        return await self._queue.get()

    @property
    def pending(self) -> int:
        """Number of batches waiting in the queue."""
        return self._queue.qsize()
