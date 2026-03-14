"""Native application container for Vertebra."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from vertebra.web.routing import RouteDef, Router

LifecycleHook = Callable[[], Any | Awaitable[Any]]


class App:
    """Native application container for routing and lifecycle hooks."""

    __slots__ = (
        "_routes",
        "_routers",
        "_shutdown_hooks",
        "_startup_hooks",
    )

    def __init__(self) -> None:
        self._routers: list[Router] = []
        self._routes: list[RouteDef] = []
        self._startup_hooks: list[LifecycleHook] = []
        self._shutdown_hooks: list[LifecycleHook] = []

    @property
    def routes(self) -> tuple[RouteDef, ...]:
        return tuple(self._routes)

    @property
    def routers(self) -> tuple[Router, ...]:
        return tuple(self._routers)

    def register(self, target: Router | Callable[..., Any]) -> Any:
        if isinstance(target, Router):
            self._routers.append(target)
            self._routes.extend(target.routes)
            return target
        if callable(target):
            routes = getattr(target, "__vertebra_routes__", None)
            if routes:
                self._routes.extend(routes)
                return target
        msg = "register expects a Router or decorated handler"
        raise TypeError(msg)

    def on_startup(self, fn: LifecycleHook | None = None) -> Any:
        if fn is None:

            def decorator(func: LifecycleHook) -> LifecycleHook:
                self._startup_hooks.append(func)
                return func

            return decorator
        self._startup_hooks.append(fn)
        return fn

    def on_shutdown(self, fn: LifecycleHook | None = None) -> Any:
        if fn is None:

            def decorator(func: LifecycleHook) -> LifecycleHook:
                self._shutdown_hooks.append(func)
                return func

            return decorator
        self._shutdown_hooks.append(fn)
        return fn

    async def startup(self) -> None:
        await _run_hooks(self._startup_hooks)

    async def shutdown(self) -> None:
        await _run_hooks(self._shutdown_hooks)


async def _run_hooks(hooks: list[LifecycleHook]) -> None:
    for hook in hooks:
        result = hook()
        if inspect.isawaitable(result):
            await result


__all__ = ("App",)
