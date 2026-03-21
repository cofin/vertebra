"""Utilities for async/sync interoperability in Vertebra.

Provides converters between async and sync calling conventions,
managing concurrency limits, and handling context managers. Aligned with vtb.
"""

import asyncio
import concurrent.futures
import functools
import inspect
import os
import sys
from collections.abc import Awaitable, Callable, Coroutine
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import TYPE_CHECKING, Any, TypeVar, cast

from typing_extensions import ParamSpec

from vertebra._typing import UVLOOP_INSTALLED
from vertebra.utils.portal import get_global_portal

if TYPE_CHECKING:
    from types import TracebackType

ReturnT = TypeVar("ReturnT")
ParamSpecT = ParamSpec("ParamSpecT")
T = TypeVar("T")


class NoValue:
    """Sentinel class for missing values."""


NO_VALUE = NoValue()


class CapacityLimiter:
    """Limits the number of concurrent operations using a semaphore."""

    def __init__(self, total_tokens: int) -> None:
        """Initialize the capacity limiter."""
        self._total_tokens = total_tokens
        self._semaphore_instance: asyncio.Semaphore | None = None
        self._pid: int | None = None

    @property
    def _semaphore(self) -> asyncio.Semaphore:
        """Lazy initialization of asyncio.Semaphore with per-process tracking."""
        current_pid = os.getpid()
        if self._semaphore_instance is None or self._pid != current_pid:
            self._semaphore_instance = asyncio.Semaphore(self._total_tokens)
            self._pid = current_pid
        return self._semaphore_instance

    async def acquire(self) -> None:
        """Acquire a token from the semaphore."""
        await self._semaphore.acquire()

    def release(self) -> None:
        """Release a token back to the semaphore."""
        self._semaphore.release()

    @property
    def total_tokens(self) -> int:
        """Get the total number of tokens available."""
        return self._total_tokens

    @total_tokens.setter
    def total_tokens(self, value: int) -> None:
        self._total_tokens = value
        self._semaphore_instance = None
        self._pid = None

    async def __aenter__(self) -> None:
        """Async context manager entry."""
        await self.acquire()

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        """Async context manager exit."""
        self.release()


_default_limiter = CapacityLimiter(1000)


class _RunWrapper[**ParamSpecT, ReturnT]:
    __slots__ = ("__dict__", "_function")

    def __init__(self, async_function: Callable[ParamSpecT, Coroutine[Any, Any, ReturnT]]) -> None:
        self._function = async_function
        functools.update_wrapper(self, async_function)

    def __call__(self, *args: ParamSpecT.args, **kwargs: ParamSpecT.kwargs) -> ReturnT:
        partial_f = functools.partial(self._function, *args, **kwargs)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            if loop.is_running():
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, partial_f())
                    return future.result()
            return asyncio.run(partial_f())

        if bool(UVLOOP_INSTALLED) and sys.platform != "win32":
            import uvloop

            uvloop.install()
        return asyncio.run(partial_f())


def run_[**ParamSpecT, ReturnT](
    async_function: Callable[ParamSpecT, Coroutine[Any, Any, ReturnT]],
) -> Callable[ParamSpecT, ReturnT]:
    """Convert an async function to a blocking function using asyncio.run()."""
    return _RunWrapper(async_function)


def await_[**ParamSpecT, ReturnT](
    async_function: Callable[ParamSpecT, Coroutine[Any, Any, ReturnT]], raise_sync_error: bool = False
) -> Callable[ParamSpecT, ReturnT]:
    """Convert an async function to a blocking one, running in the main async loop."""
    return _AwaitWrapper(async_function, raise_sync_error)


def async_[**ParamSpecT, ReturnT](
    function: Callable[ParamSpecT, ReturnT], *, limiter: CapacityLimiter | None = None
) -> Callable[ParamSpecT, Awaitable[ReturnT]]:
    """Convert a blocking function to an async one using asyncio.to_thread()."""
    return _AsyncWrapper(function, limiter)


def ensure_async_[**ParamSpecT, ReturnT](
    function: Callable[ParamSpecT, Awaitable[ReturnT] | ReturnT],
) -> Callable[ParamSpecT, Awaitable[ReturnT]]:
    """Convert a function to an async one if it is not already."""
    if inspect.iscoroutinefunction(function):
        return function

    return _EnsureAsyncWrapper(function)


class _AwaitWrapper[**ParamSpecT, ReturnT]:
    __slots__ = ("__dict__", "_function", "_raise_sync_error")

    def __init__(
        self, async_function: Callable[ParamSpecT, Coroutine[Any, Any, ReturnT]], raise_sync_error: bool
    ) -> None:
        self._function = async_function
        self._raise_sync_error = raise_sync_error
        functools.update_wrapper(self, async_function)

    def __call__(self, *args: ParamSpecT.args, **kwargs: ParamSpecT.kwargs) -> ReturnT:
        partial_f = functools.partial(self._function, *args, **kwargs)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._raise_sync_error:
                msg = "Cannot run async function"
                raise RuntimeError(msg) from None
            portal = get_global_portal()
            typed_partial = cast("Callable[[], Coroutine[Any, Any, ReturnT]]", partial_f)
            return portal.call(typed_partial)
        if loop.is_running():
            try:
                current_task = asyncio.current_task(loop=loop)
            except RuntimeError:
                current_task = None

            if current_task is not None:
                if self._raise_sync_error:
                    msg = "await_ cannot be called from within an async task running on the same event loop. Use 'await' instead."
                    raise RuntimeError(msg)
                portal = get_global_portal()
                typed_partial = cast("Callable[[], Coroutine[Any, Any, ReturnT]]", partial_f)
                return portal.call(typed_partial)
            future = asyncio.run_coroutine_threadsafe(partial_f(), loop)
            return future.result()
        if self._raise_sync_error:
            msg = "Cannot run async function"
            raise RuntimeError(msg)
        portal = get_global_portal()
        typed_partial = cast("Callable[[], Coroutine[Any, Any, ReturnT]]", partial_f)
        return portal.call(typed_partial)


class _AsyncWrapper[**ParamSpecT, ReturnT]:
    __slots__ = ("__dict__", "_function", "_limiter")

    def __init__(self, function: Callable[ParamSpecT, ReturnT], limiter: CapacityLimiter | None) -> None:
        self._function = function
        self._limiter = limiter
        functools.update_wrapper(self, function)

    async def __call__(self, *args: ParamSpecT.args, **kwargs: ParamSpecT.kwargs) -> ReturnT:
        partial_f = functools.partial(self._function, *args, **kwargs)
        if self._limiter is not None:
            async with self._limiter:
                return await asyncio.to_thread(partial_f)
        return await asyncio.to_thread(partial_f)


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
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        return self._cm.__exit__(exc_type, exc_val, exc_tb)


def with_ensure_async_[T](
    obj: AbstractContextManager[T] | AbstractAsyncContextManager[T],
) -> AbstractAsyncContextManager[T]:
    """Convert a context manager to an async one if it is not already."""
    if isinstance(obj, AbstractContextManager):
        return cast("AbstractAsyncContextManager[T]", _ContextManagerWrapper(obj))
    return obj


async def get_next(iterable: Any, default: Any = NO_VALUE, *args: Any) -> Any:
    """Return the next item from an async iterator."""
    if isinstance(default, NoValue):
        return await anext(iterable)
    return await anext(iterable, default)
