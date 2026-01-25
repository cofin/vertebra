"""Event dispatcher for native app workflows."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
import inspect
from typing import Any, TypeVar, get_args, get_origin, get_type_hints

import msgspec

from vertebra.di import Inject, ProviderCompiler

EventT = TypeVar("EventT")
EventHandlerFunc = Callable[..., Any | Awaitable[Any]]


class EventError(RuntimeError):
    """Raised when event dispatch fails."""


class EventHandler(msgspec.Struct, frozen=True, array_like=True):
    event_type: type[Any]
    handler: EventHandlerFunc
    dependency_runners: tuple[Callable[[dict[type[Any], Any]], Awaitable[Any]], ...] = ()


class EventBus:
    """Simple pub/sub event dispatcher with optional DI support."""

    __slots__ = ("_handlers", "_compiler")

    def __init__(self, *, compiler: ProviderCompiler | None = None) -> None:
        self._handlers: dict[type[Any], list[EventHandler]] = {}
        self._compiler = compiler

    def on(self, event_type: type[EventT]) -> Callable[[EventHandlerFunc], EventHandlerFunc]:
        def decorator(handler: EventHandlerFunc) -> EventHandlerFunc:
            spec = self._build_handler(event_type, handler)
            self._handlers.setdefault(event_type, []).append(spec)
            return handler

        return decorator

    def handlers(self, event_type: type[Any]) -> tuple[EventHandler, ...]:
        return tuple(self._handlers.get(event_type, ()))

    def publish(self, event: Any) -> None:
        handlers = self._handlers.get(type(event), [])
        if not handlers:
            return
        for handler in handlers:
            self._dispatch_sync(handler, event)

    async def publish_async(self, event: Any) -> None:
        handlers = self._handlers.get(type(event), [])
        if not handlers:
            return
        request_cache: dict[type[Any], Any] = {}
        for handler in handlers:
            await self._dispatch_async(handler, event, request_cache)

    def _build_handler(self, event_type: type[Any], handler: EventHandlerFunc) -> EventHandler:
        dependencies = _collect_dependencies(handler, event_type)
        if dependencies:
            if self._compiler is None:
                msg = "event handler requires DI but no ProviderCompiler was supplied"
                raise EventError(msg)
            runners = tuple(self._compiler.compile(dep) for dep in dependencies)
        else:
            runners = ()

        return EventHandler(event_type=event_type, handler=handler, dependency_runners=runners)

    def _dispatch_sync(self, handler: EventHandler, event: Any) -> None:
        if handler.dependency_runners:
            msg = "event handler requires DI but no ProviderCompiler was supplied"
            raise EventError(msg)
        result = handler.handler(event)
        if inspect.isawaitable(result):
            msg = "async event handler used with publish(); use publish_async()"
            raise EventError(msg)

    async def _dispatch_async(
        self,
        handler: EventHandler,
        event: Any,
        request_cache: dict[type[Any], Any],
    ) -> None:
        args = [event]
        for runner in handler.dependency_runners:
            args.append(await runner(request_cache))
        result = handler.handler(*args)
        if inspect.isawaitable(result):
            await result


def _collect_dependencies(handler: EventHandlerFunc, event_type: type[Any]) -> tuple[type[Any], ...]:
    signature = inspect.signature(handler)
    hints = _safe_type_hints(handler)
    dependencies: list[type[Any]] = []
    skip_first = True

    for name, param in signature.parameters.items():
        if name in ("self", "cls"):
            continue
        if skip_first:
            skip_first = False
            continue
        annotation = hints.get(name, param.annotation)
        dependency = _unwrap_inject(annotation)
        if dependency is None:
            raise EventError(f"handler for {event_type.__name__} must use Inject[] for dependencies")
        dependencies.append(dependency)

    return tuple(dependencies)


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


def _safe_type_hints(handler: EventHandlerFunc) -> dict[str, Any]:
    try:
        return get_type_hints(
            handler,
            globalns=getattr(handler, "__globals__", None),
            include_extras=True,
        )
    except (NameError, TypeError, AttributeError):
        return {}


__all__ = ("EventBus", "EventError", "EventHandler")
