"""Utilities for async/sync interoperability, loop-sharded resources, and free-threaded sync pools."""

import asyncio
import concurrent.futures
import contextvars
import functools
import inspect
import os
import sys
import threading
import weakref
from collections.abc import Awaitable, Callable, Coroutine
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import TYPE_CHECKING, Any, ClassVar, cast

from vertebra._typing import UVLOOP_INSTALLED
from vertebra.utils.module_loader import import_optional
from vertebra.utils.portal import get_global_portal

if TYPE_CHECKING:
    from types import TracebackType

__all__ = (
    "NO_VALUE",
    "CapacityLimiter",
    "FreeThreadedSyncPool",
    "LoopShardedResource",
    "NoValue",
    "async_",
    "async_to_sync",
    "await_",
    "ensure_async_",
    "get_next",
    "run_",
    "sync_to_async",
    "with_ensure_async_",
)


class NoValue:
    """Sentinel class for missing values."""


NO_VALUE = NoValue()


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


class FreeThreadedSyncPool:
    """Dedicated worker pool for executing synchronous callables in parallel on Python 3.12+/3.14t."""

    _default_instance: ClassVar["FreeThreadedSyncPool | None"] = None
    _default_lock: ClassVar[threading.Lock] = threading.Lock()

    __slots__ = ("_executor", "_max_workers", "_pid")

    def __init__(self, max_workers: int | None = None) -> None:
        cpu_count = os.cpu_count() or 4
        self._max_workers = max(1, max_workers if max_workers is not None else cpu_count * 2)
        self._executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._pid: int | None = None

    @property
    def max_workers(self) -> int:
        """Return the configured maximum worker thread count."""
        return self._max_workers

    @classmethod
    def default(cls) -> "FreeThreadedSyncPool":
        """Return the process-wide default FreeThreadedSyncPool."""
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
        """Execute a synchronous callable on the pool and await its completion on the active event loop."""
        loop = asyncio.get_running_loop()
        executor = self._ensure_executor()
        ctx = contextvars.copy_context()

        def _invoke() -> R:
            return ctx.run(fn, *args, **kwargs)

        return await loop.run_in_executor(executor, _invoke)

    def shutdown(self, *, wait: bool = True) -> None:
        """Shut down the underlying thread pool executor."""
        if self._executor is not None:
            self._executor.shutdown(wait=wait)
            self._executor = None
            self._pid = None

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
        exc_type: "type[BaseException] | None",
        exc_val: "BaseException | None",
        exc_tb: "TracebackType | None",
    ) -> None:
        """Exit the async context manager."""
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


def await_[**ParamSpecT, ReturnT](
    async_function: Callable[ParamSpecT, Coroutine[Any, Any, ReturnT]],
    raise_sync_error: bool = False,
) -> Callable[ParamSpecT, ReturnT]:
    """Convert an async function to a blocking one, running in the main async loop or global portal."""
    return _AwaitWrapper(async_function, raise_sync_error)


def async_[**ParamSpecT, ReturnT](
    function: Callable[ParamSpecT, ReturnT],
    *,
    limiter: CapacityLimiter | None = None,
) -> Callable[ParamSpecT, Awaitable[ReturnT]]:
    """Convert a blocking function to an async one without redundant partial allocations."""
    return _AsyncWrapper(function, limiter)


def ensure_async_[**ParamSpecT, ReturnT](
    function: Callable[ParamSpecT, Awaitable[ReturnT] | ReturnT],
) -> Callable[ParamSpecT, Awaitable[ReturnT]]:
    """Convert a function to an async one if it is not already."""
    if inspect.iscoroutinefunction(function):
        return function

    return _EnsureAsyncWrapper(function)


def with_ensure_async_[T](
    obj: AbstractContextManager[T] | AbstractAsyncContextManager[T],
) -> AbstractAsyncContextManager[T]:
    """Convert a context manager to an async one if it is not already."""
    if isinstance(obj, AbstractContextManager):
        return cast("AbstractAsyncContextManager[T]", _ContextManagerWrapper(obj))
    return obj


async def get_next(iterable: Any, default: Any = NO_VALUE, *_args: Any) -> Any:
    """Return the next item from an async iterator."""
    if isinstance(default, NoValue):
        return await anext(iterable)
    return await anext(iterable, default)


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
            portal = get_global_portal()
            return portal.call(self._function, *args, **kwargs)

        if bool(UVLOOP_INSTALLED) and sys.platform != "win32" and not _is_gil_disabled():
            uvloop_mod = import_optional("uvloop")
            if uvloop_mod is not None:
                uvloop_mod.install()
        return asyncio.run(self._function(*args, **kwargs))


class _AwaitWrapper[**ParamSpecT, ReturnT]:
    __slots__ = ("__dict__", "_function", "_raise_sync_error")

    def __init__(
        self,
        async_function: Callable[ParamSpecT, Coroutine[Any, Any, ReturnT]],
        raise_sync_error: bool,
    ) -> None:
        self._function = async_function
        self._raise_sync_error = raise_sync_error
        functools.update_wrapper(self, async_function)

    def __call__(self, *args: ParamSpecT.args, **kwargs: ParamSpecT.kwargs) -> ReturnT:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._raise_sync_error:
                msg = "Cannot run async function"
                raise RuntimeError(msg) from None
            return get_global_portal().call(self._function, *args, **kwargs)

        if loop.is_running():
            try:
                current_task = asyncio.current_task(loop=loop)
            except RuntimeError:
                current_task = None

            if current_task is not None:
                if self._raise_sync_error:
                    msg = (
                        "await_ cannot be called from within an async task running on the same event loop. "
                        "Use 'await' instead."
                    )
                    raise RuntimeError(msg)
                return get_global_portal().call(self._function, *args, **kwargs)
            future = asyncio.run_coroutine_threadsafe(self._function(*args, **kwargs), loop)
            return future.result()

        if self._raise_sync_error:
            msg = "Cannot run async function"
            raise RuntimeError(msg)
        return get_global_portal().call(self._function, *args, **kwargs)


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
            return await FreeThreadedSyncPool.default().submit_async(self._function, *args, **kwargs)
        return await asyncio.to_thread(self._function, *args, **kwargs)


class _EnsureAsyncWrapper[**ParamSpecT, ReturnT]:
    __slots__ = ("__dict__", "_function")

    def __init__(self, function: Callable[ParamSpecT, Awaitable[ReturnT] | ReturnT]) -> None:
        self._function = function
        functools.update_wrapper(self, function)

    async def __call__(self, *args: ParamSpecT.args, **kwargs: ParamSpecT.kwargs) -> ReturnT:
        result = self._function(*args, **kwargs)
        if inspect.isawaitable(result):
            return await cast("Awaitable[ReturnT]", result)
        return result


class _ContextManagerWrapper[T]:
    def __init__(self, cm: AbstractContextManager[T]) -> None:
        self._cm = cm

    async def __aenter__(self) -> T:
        return self._cm.__enter__()

    async def __aexit__(
        self,
        exc_type: "type[BaseException] | None",
        exc_val: "BaseException | None",
        exc_tb: "TracebackType | None",
    ) -> bool | None:
        return self._cm.__exit__(exc_type, exc_val, exc_tb)


async_to_sync = run_
sync_to_async = async_
