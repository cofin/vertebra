"""Portal provider for calling async functions from synchronous contexts.

Provides a persistent background thread with an event loop to execute async
operations from synchronous contexts with a single thread-safe wakeup per call.
"""

import asyncio
import atexit
import concurrent.futures
import logging
import os
import threading
from collections.abc import Callable, Coroutine
from typing import Any, ClassVar, Self, cast

__all__ = ("Portal", "PortalManager", "PortalProvider", "get_global_portal")

logger = logging.getLogger("vertebra.utils.portal")


class PortalProvider:
    """Manage a background thread with an event loop for async operations."""

    def __init__(self) -> None:
        """Initialize the PortalProvider."""
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._ready_event: threading.Event = threading.Event()
        self._pid: int | None = None

    @property
    def portal(self) -> "Portal":
        """Return the portal instance for calling async functions."""
        return Portal(self)

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
        """Return the underlying event loop."""
        if self._loop is None:
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

    def __enter__(self) -> "Portal":
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


class PortalManager:
    """Singleton manager for the global portal instance."""

    _instance: ClassVar["PortalManager | None"] = None
    _singleton_lock: ClassVar[threading.Lock] = threading.Lock()
    _initialized: bool = False

    def __new__(cls) -> Self:
        """Return the singleton instance of PortalManager."""
        if cls._instance is None:
            with cls._singleton_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cast("Self", cls._instance)

    def __init__(self) -> None:
        """Initialize the PortalManager singleton."""
        if hasattr(self, "_initialized") and self._initialized:
            return

        self._provider: PortalProvider | None = None
        self._portal: Portal | None = None
        self._lock = threading.Lock()
        self._pid: int | None = None
        self._atexit_registered: bool = False
        self._initialized = True

    @property
    def is_running(self) -> bool:
        """Return True if the global portal is running."""
        return self._provider is not None and self._provider.is_running

    @classmethod
    def get_instance(cls) -> "PortalManager":
        """Return the singleton instance."""
        return cls()

    def get_or_create_portal(self) -> Portal:
        """Return or create the global portal instance."""
        current_pid = os.getpid()
        if self._needs_restart(current_pid):
            with self._lock:
                if self._needs_restart(current_pid):
                    if self._provider is not None:
                        self._provider.stop()
                    self._provider = PortalProvider()
                    self._provider.start()
                    self._portal = Portal(self._provider)
                    self._pid = current_pid
                    self._register_atexit()

        return cast("Portal", self._portal)

    def stop(self) -> None:
        """Stop the global portal provider."""
        if self._provider is not None:
            self._provider.stop()
            self._provider = None
            self._portal = None
            self._pid = None

    def _register_atexit(self) -> None:
        if not self._atexit_registered:
            atexit.register(self._atexit_cleanup)
            self._atexit_registered = True

    def _atexit_cleanup(self) -> None:
        if self._provider is not None and self._provider.is_running:
            self.stop()

    def _needs_restart(self, current_pid: int) -> bool:
        provider_missing = self._provider is None or not self._provider.is_running
        portal_missing = self._portal is None
        pid_changed = self._pid is not None and self._pid != current_pid
        return portal_missing or provider_missing or pid_changed


def get_global_portal() -> Portal:
    """Return the global portal instance for async-to-sync bridging."""
    return PortalManager().get_or_create_portal()
