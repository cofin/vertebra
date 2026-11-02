"""Utilities for async/sync interoperability, loop-sharded resources, and free-threaded sync pools."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import contextvars
import functools
import os
import sys
import threading
import weakref
from collections.abc import Awaitable, Callable, Coroutine
from typing import TYPE_CHECKING, Any, ClassVar, cast

from vertebra.typing import UVLOOP_INSTALLED
from vertebra.utils.module_loader import import_optional
from vertebra.utils.portal import PortalProvider

if TYPE_CHECKING:
    from types import TracebackType

__all__ = (
    "CapacityLimiter",
    "LoopShardedResource",
    "SyncPool",
    "async_",
    "run_",
)


def _is_gil_disabled() -> bool:
    check = getattr(sys, "_is_gil_enabled", None)
    if callable(check):
        return not bool(check())
    return False


class LoopShardedResource[T]:
    """Lazily construct and cache one loop-affine resource instance per asyncio event loop."""

    __slots__ = ("_async_factory", "_by_loop", "_factory", "_lock")

    def __init__(
        self,
        factory: Callable[[], T] | None = None,
        *,
        async_factory: Callable[[], Awaitable[T]] | None = None,
    ) -> None:
        if factory is None and async_factory is None:
            msg = "LoopShardedResource requires either factory or async_factory"
            raise ValueError(msg)
        self._factory = factory
        self._async_factory = async_factory
        self._by_loop: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, T] = weakref.WeakKeyDictionary()
        self._lock = threading.Lock()

    @property
    def shard_count(self) -> int:
        """Return the number of active event-loop shards holding an initialized resource."""
        with self._lock:
            return len(self._by_loop)

    def get(self, loop: asyncio.AbstractEventLoop | None = None) -> T:
        """Return the resource bound to the current or specified event loop, creating it synchronously if needed."""
        target_loop = loop or asyncio.get_running_loop()
        existing = self._by_loop.get(target_loop)
        if existing is not None:
            return existing
        if self._factory is None:
            msg = "LoopShardedResource configured with async_factory must be accessed via await get_async()"
            raise RuntimeError(msg)
        with self._lock:
            existing = self._by_loop.get(target_loop)
            if existing is None:
                existing = self._factory()
                self._by_loop[target_loop] = existing
            return existing

    async def get_async(self, loop: asyncio.AbstractEventLoop | None = None) -> T:
        """Return the resource bound to the current or specified event loop, awaiting async_factory if needed."""
        target_loop = loop or asyncio.get_running_loop()
        existing = self._by_loop.get(target_loop)
        if existing is not None:
            return existing
        if self._async_factory is not None:
            created = await self._async_factory()
            with self._lock:
                existing = self._by_loop.get(target_loop)
                if existing is None:
                    self._by_loop[target_loop] = created
                    return created
                return existing
        return self.get(target_loop)

    def clear(self) -> None:
        """Remove all cached loop shards."""
        with self._lock:
            self._by_loop.clear()


class _LoopCompletionQueue:
    """Per-event-loop completion queue coalescing worker-thread wakeups into a single callback."""

    __slots__ = ("_items", "_lock", "_loop_ref", "_wake_armed")

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop_ref = weakref.ref(loop)
        self._items: list[tuple[asyncio.Future[Any], Any, BaseException | None]] = []
        self._lock = threading.Lock()
        self._wake_armed = False

    def complete(self, fut: asyncio.Future[Any], value: Any, exc: BaseException | None) -> None:
        """Append a completed future outcome and schedule a loop drain if none is currently armed."""
        should_wake = False
        with self._lock:
            self._items.append((fut, value, exc))
            if not self._wake_armed:
                self._wake_armed = True
                should_wake = True
        if should_wake:
            loop = self._loop_ref()
            if loop is not None and not loop.is_closed():
                with contextlib.suppress(RuntimeError):
                    loop.call_soon_threadsafe(self.drain)

    def drain(self) -> None:
        """Resolve all queued futures on the owning event loop thread."""
        while True:
            with self._lock:
                if not self._items:
                    self._wake_armed = False
                    return
                batch = self._items
                self._items = []
            for fut, value, exc in batch:
                if fut.done():
                    continue
                if exc is not None:
                    fut.set_exception(exc)
                else:
                    fut.set_result(value)


class SyncPool:
    """Dedicated worker pool for executing synchronous callables in parallel on Python 3.12+/3.14t."""

    _default_instance: ClassVar[SyncPool | None] = None
    _default_lock: ClassVar[threading.Lock] = threading.Lock()

    __slots__ = ("_executor", "_loop_queues", "_loop_queues_lock", "_max_workers", "_pid")

    def __init__(self, max_workers: int | None = None) -> None:
        cpu_count = os.cpu_count() or 4
        self._max_workers = max(1, max_workers if max_workers is not None else cpu_count * 2)
        self._executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._pid: int | None = None
        self._loop_queues: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, _LoopCompletionQueue] = (
            weakref.WeakKeyDictionary()
        )
        self._loop_queues_lock = threading.Lock()

    @property
    def max_workers(self) -> int:
        """Return the configured maximum worker thread count."""
        return self._max_workers

    @classmethod
    def default(cls) -> SyncPool:
        """Return the process-wide default SyncPool."""
        if cls._default_instance is None:
            with cls._default_lock:
                if cls._default_instance is None:
                    cls._default_instance = cls()
        return cls._default_instance

    def submit[**P, R](self, fn: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> concurrent.futures.Future[R]:
        """Submit a synchronous callable with contextvars propagation."""
        executor = self._ensure_executor()
        ctx = contextvars.copy_context()

        def _invoke() -> R:
            return ctx.run(fn, *args, **kwargs)

        return executor.submit(_invoke)

    async def submit_async[**P, R](self, fn: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        """Execute a synchronous callable on the pool and await its completion via coalesced loop wakeups."""
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[R] = loop.create_future()
        queue = self._get_loop_queue(loop)
        executor = self._ensure_executor()
        ctx = contextvars.copy_context()

        def _invoke() -> None:
            try:
                res = ctx.run(fn, *args, **kwargs)
            except BaseException as exc:
                queue.complete(fut, None, exc)
            else:
                queue.complete(fut, res, None)

        executor.submit(_invoke)
        return await fut

    def shutdown(self, *, wait: bool = True) -> None:
        """Shut down the underlying thread pool executor."""
        if self._executor is not None:
            with contextlib.suppress(RuntimeError, OSError):
                self._executor.shutdown(wait=wait)
            self._executor = None
            self._pid = None

    def _get_loop_queue(self, loop: asyncio.AbstractEventLoop) -> _LoopCompletionQueue:
        queue = self._loop_queues.get(loop)
        if queue is not None:
            return queue
        with self._loop_queues_lock:
            queue = self._loop_queues.get(loop)
            if queue is None:
                queue = _LoopCompletionQueue(loop)
                self._loop_queues[loop] = queue
            return queue

    def _ensure_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        current_pid = os.getpid()
        if self._executor is None or self._pid != current_pid:
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_workers,
                thread_name_prefix="vertebra-ft-sync",
            )
            self._pid = current_pid
        return self._executor


class CapacityLimiter:
    """Limit the number of concurrent operations using a per-loop asyncio.Semaphore."""

    def __init__(self, total_tokens: int) -> None:
        """Initialize the capacity limiter."""
        self._total_tokens = total_tokens
        self._semaphores_by_loop: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore] = (
            weakref.WeakKeyDictionary()
        )
        self._fallback_semaphore: asyncio.Semaphore | None = None
        self._pid: int | None = None

    @property
    def total_tokens(self) -> int:
        """Return the total number of tokens available."""
        return self._total_tokens

    @total_tokens.setter
    def total_tokens(self, value: int) -> None:
        self._total_tokens = value
        self._semaphores_by_loop.clear()
        self._fallback_semaphore = None
        self._pid = None

    async def acquire(self) -> None:
        """Acquire a token from the semaphore bound to the current event loop."""
        await self._semaphore.acquire()

    def release(self) -> None:
        """Release a token back to the semaphore bound to the current event loop."""
        self._semaphore.release()

    async def __aenter__(self) -> None:
        """Enter the async context manager."""
        await self.acquire()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the async context manager."""
        _ = (exc_type, exc_val, exc_tb)
        self.release()

    def __enter__(self) -> None:
        """Enter synchronous context (no-op for async limiter)."""
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit synchronous context."""
        _ = (exc_type, exc_val, exc_tb)
        self.release()

    @property
    def _semaphore(self) -> asyncio.Semaphore:
        current_pid = os.getpid()
        if self._pid != current_pid:
            self._semaphores_by_loop.clear()
            self._fallback_semaphore = None
            self._pid = current_pid

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._fallback_semaphore is None:
                self._fallback_semaphore = asyncio.Semaphore(self._total_tokens)
            return self._fallback_semaphore

        sem = self._semaphores_by_loop.get(loop)
        if sem is None:
            sem = asyncio.Semaphore(self._total_tokens)
            self._semaphores_by_loop[loop] = sem
        return sem


def run_[**ParamSpecT, ReturnT](
    async_function: Callable[ParamSpecT, Coroutine[Any, Any, ReturnT]],
) -> Callable[ParamSpecT, ReturnT]:
    """Convert an async function to a blocking function using the persistent portal when a loop is running."""
    return _RunWrapper(async_function)


def async_[**ParamSpecT, ReturnT](
    function: Callable[ParamSpecT, ReturnT],
    *,
    limiter: CapacityLimiter | None = None,
) -> Callable[ParamSpecT, Awaitable[ReturnT]]:
    """Convert a blocking function to an async one without redundant partial allocations."""
    return _AsyncWrapper(function, limiter)


class _RunWrapper[**ParamSpecT, ReturnT]:
    __slots__ = ("__dict__", "_function")

    def __init__(self, async_function: Callable[ParamSpecT, Coroutine[Any, Any, ReturnT]]) -> None:
        self._function = async_function
        functools.update_wrapper(self, async_function)

    def __call__(self, *args: ParamSpecT.args, **kwargs: ParamSpecT.kwargs) -> ReturnT:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            return PortalProvider.default().call(self._function, *args, **kwargs)

        if bool(UVLOOP_INSTALLED) and sys.platform != "win32" and not _is_gil_disabled():
            uvloop_mod = import_optional("uvloop")
            if uvloop_mod is not None and hasattr(uvloop_mod, "run"):
                return cast("ReturnT", uvloop_mod.run(self._function(*args, **kwargs)))
        return asyncio.run(self._function(*args, **kwargs))


class _AsyncWrapper[**ParamSpecT, ReturnT]:
    __slots__ = ("__dict__", "_function", "_limiter")

    def __init__(self, function: Callable[ParamSpecT, ReturnT], limiter: CapacityLimiter | None) -> None:
        self._function = function
        self._limiter = limiter
        functools.update_wrapper(self, function)

    async def __call__(self, *args: ParamSpecT.args, **kwargs: ParamSpecT.kwargs) -> ReturnT:
        if self._limiter is not None:
            async with self._limiter:
                return await self._dispatch(*args, **kwargs)
        return await self._dispatch(*args, **kwargs)

    async def _dispatch(self, *args: ParamSpecT.args, **kwargs: ParamSpecT.kwargs) -> ReturnT:
        if _is_gil_disabled():
            return await SyncPool.default().submit_async(self._function, *args, **kwargs)
        return await asyncio.to_thread(self._function, *args, **kwargs)
