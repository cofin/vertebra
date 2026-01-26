"""Background task scheduling for native apps."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, get_args, get_origin, get_type_hints

from vertebra.di import Inject, ProviderCompiler

TaskHandler = Callable[..., Any | Awaitable[Any]]
TaskT = TypeVar("TaskT")


class BackgroundTaskError(RuntimeError):
    """Raised when background task scheduling fails."""


class BackgroundTaskHandle:
    """Handle for a scheduled background task."""

    __slots__ = ("_task",)

    def __init__(self, task: asyncio.Task[None]) -> None:
        self._task = task

    @property
    def task(self) -> asyncio.Task[None]:
        return self._task

    def cancel(self) -> None:
        self._task.cancel()

    async def wait(self) -> None:
        await self._task


class BackgroundTasks:
    """Schedule and manage background tasks with optional DI support."""

    __slots__ = ("_compiler", "_tasks")

    def __init__(self, *, compiler: ProviderCompiler | None = None) -> None:
        self._tasks: set[asyncio.Task[None]] = set()
        self._compiler = compiler

    def schedule(self, handler: TaskHandler, *args: Any, **kwargs: Any) -> BackgroundTaskHandle:
        task = asyncio.create_task(self._run(handler, args, kwargs))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return BackgroundTaskHandle(task)

    async def shutdown(self) -> None:
        if not self._tasks:
            return
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _run(self, handler: TaskHandler, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        request_cache: dict[type[Any], Any] = {}
        call_args, call_kwargs = await _resolve_call(handler, args, kwargs, self._compiler, request_cache)
        result = handler(*call_args, **call_kwargs)
        if inspect.isawaitable(result):
            await result


def _unwrap_inject(annotation: Any) -> type[Any] | None:
    if annotation is inspect.Signature.empty:
        return None
    if isinstance(annotation, str):
        return None
    origin = get_origin(annotation)
    if origin is Inject:
        args = get_args(annotation)
        dependency = args[0] if args else None
        return dependency if isinstance(dependency, type) else None
    return None


def _safe_type_hints(handler: TaskHandler) -> dict[str, Any]:
    try:
        return get_type_hints(handler, globalns=getattr(handler, "__globals__", None), include_extras=True)
    except (NameError, TypeError, AttributeError):
        return {}


async def _resolve_call(
    handler: TaskHandler,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    compiler: ProviderCompiler | None,
    request_cache: dict[type[Any], Any],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    signature = inspect.signature(handler)
    hints = _safe_type_hints(handler)
    bound = signature.bind_partial(*args, **kwargs)

    for name, param in signature.parameters.items():
        if name in bound.arguments or name in ("self", "cls"):
            continue
        annotation = hints.get(name, param.annotation)
        dependency = _unwrap_inject(annotation)
        if dependency is None:
            continue
        if compiler is None:
            msg = "background task requires DI but no ProviderCompiler was supplied"
            raise BackgroundTaskError(msg)
        runner = compiler.compile(dependency)
        bound.arguments[name] = await runner(request_cache)

    return bound.args, bound.kwargs


__all__ = ("BackgroundTaskError", "BackgroundTaskHandle", "BackgroundTasks")
