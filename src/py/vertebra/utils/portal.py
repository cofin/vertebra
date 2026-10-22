"""Portal provider for calling async functions from synchronous contexts.

Provides a persistent background thread with an event loop to execute async
operations from synchronous contexts with a single thread-safe wakeup per call.
"""

from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import contextlib
import logging
import os
import threading
from collections.abc import Callable, Coroutine
from typing import Any, ClassVar

__all__ = ("Portal", "PortalProvider")

logger = logging.getLogger("vertebra.utils.portal")


class PortalProvider:
    """Manage a background thread with an event loop for async operations."""

    _default_provider: ClassVar[PortalProvider | None] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self) -> None:
        """Initialize the PortalProvider."""
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._ready_event: threading.Event = threading.Event()
        self._pid: int | None = None
        self._portal: Portal | None = None

    @classmethod
    def default(cls) -> Portal:
        """Return the default process-global Portal instance, creating it if needed."""
        current_pid = os.getpid()
        provider = cls._default_provider
        if provider is None or not provider.is_running or provider._pid != current_pid:
            with cls._lock:
                provider = cls._default_provider
                if provider is None or not provider.is_running or provider._pid != current_pid:
                    if provider is not None and provider.is_running:
                        provider.stop()
                    new_provider = cls()
                    new_provider.start()
                    atexit.register(new_provider.stop)
                    cls._default_provider = new_provider
                    provider = new_provider
        return provider.portal

    @property
    def portal(self) -> Portal:
        """Return the portal instance for calling async functions."""
        if self._portal is None:
            self._portal = Portal(self)
        return self._portal

    @property
    def is_running(self) -> bool:
        """Return True if the portal provider thread is running."""
        return self._loop_thread is not None and self._loop_thread.is_alive()

    @property
    def is_ready(self) -> bool:
        """Return True if the portal provider event loop is ready."""
        return self._ready_event.is_set()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """Return the running event loop."""
        if self._loop is None or not self.is_running:
            msg = "Portal provider not started. Call start() first."
            raise RuntimeError(msg)
        return self._loop

    def start(self) -> None:
        """Start the background thread and event loop."""
        if self._loop_thread is not None and self._loop_thread.is_alive():
            return

        self._ready_event.clear()
        self._loop_thread = threading.Thread(
            target=self._run_event_loop,
            name="vertebra-portal-loop",
            daemon=True,
        )
        self._loop_thread.start()
        self._ready_event.wait()
        self._pid = os.getpid()

    def stop(self) -> None:
        """Stop the background thread and event loop."""
        if self._loop is None or self._loop_thread is None:
            return

        self._loop.call_soon_threadsafe(self._loop.stop)
        self._loop_thread.join(timeout=5)

        if self._loop_thread.is_alive():
            logger.warning("Portal thread did not stop within 5 seconds, skipping loop.close()")
        else:
            self._loop.close()

        self._loop = None
        self._loop_thread = None
        self._ready_event.clear()
        self._pid = None

    def __enter__(self) -> Portal:
        """Start the portal provider and return its Portal instance."""
        self.start()
        return self.portal

    def __exit__(self, _exc_type: object, _exc_val: object, _exc_tb: object) -> None:
        """Stop the portal provider on context exit."""
        self.stop()

    def call[R](self, func: Callable[..., Coroutine[Any, Any, R]], *args: Any, **kwargs: Any) -> R:
        """Call an async function from a synchronous context with a single loop wakeup."""
        timeout: float = float(kwargs.pop("timeout", 300.0))
        if self._loop is None or not self.is_running:
            msg = "Portal provider not running. Call start() first."
            raise RuntimeError(msg)

        coro = self._async_caller(func, args, kwargs)
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            future.cancel()
            msg = f"Portal call timed out after {timeout} seconds"
            raise RuntimeError(msg) from None

    def _run_event_loop(self) -> None:
        if self._loop is None:
            self._loop = asyncio.new_event_loop()

        asyncio.set_event_loop(self._loop)
        self._ready_event.set()
        with contextlib.suppress(Exception):
            self._loop.run_forever()

    @staticmethod
    async def _async_caller[R](
        func: Callable[..., Coroutine[Any, Any, R]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> R:
        return await func(*args, **kwargs)


class Portal:
    """Portal for calling async functions using PortalProvider."""

    def __init__(self, provider: PortalProvider) -> None:
        """Initialize Portal with a provider."""
        self._provider = provider

    def call[R](self, func: Callable[..., Coroutine[Any, Any, R]], *args: Any, **kwargs: Any) -> R:
        """Call an async function using the portal provider."""
        return self._provider.call(func, *args, **kwargs)
