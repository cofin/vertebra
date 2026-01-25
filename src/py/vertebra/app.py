"""Native application container for Vertebra."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from types import MappingProxyType
from typing import Any, TypeVar

from vertebra.controllers import Controller, ControllerRoute
from vertebra.di import ProviderCompiler, ProviderRegistry, Scope
from vertebra.routing import RouteDef, Router
from vertebra.state import NativeState, register_state

StateT = TypeVar("StateT", bound=NativeState)
LifecycleHook = Callable[[], Any | Awaitable[Any]]


class Vertebra:
    """Native application container for routing, state, and lifecycle hooks."""

    __slots__ = (
        "_controllers",
        "_controller_routes",
        "_provider_compiler",
        "_providers",
        "_routes",
        "_routers",
        "_shutdown_hooks",
        "_startup_hooks",
        "_states",
    )

    def __init__(self) -> None:
        self._providers = ProviderRegistry()
        self._provider_compiler: ProviderCompiler | None = None
        self._routers: list[Router] = []
        self._controllers: list[type[Controller]] = []
        self._routes: list[RouteDef] = []
        self._controller_routes: list[ControllerRoute] = []
        self._states: dict[type[NativeState], NativeState] = {}
        self._startup_hooks: list[LifecycleHook] = []
        self._shutdown_hooks: list[LifecycleHook] = []

    def provide(
        self,
        provider: type[Any] | Callable[..., Any] | None = None,
        *,
        scope: Scope = "singleton",
    ) -> Any:
        return self._providers.provide(provider, scope=scope)

    @property
    def compiler(self) -> ProviderCompiler:
        if self._provider_compiler is None:
            self._provider_compiler = self._providers.compile()
        return self._provider_compiler

    @property
    def routes(self) -> tuple[RouteDef, ...]:
        return tuple(self._routes)

    @property
    def controller_routes(self) -> tuple[ControllerRoute, ...]:
        return tuple(self._controller_routes)

    @property
    def routers(self) -> tuple[Router, ...]:
        return tuple(self._routers)

    @property
    def controllers(self) -> tuple[type[Controller], ...]:
        return tuple(self._controllers)

    @property
    def states(self) -> MappingProxyType[type[NativeState], NativeState]:
        return MappingProxyType(self._states)

    def register(self, target: Router | type[Controller] | Controller) -> Any:
        if isinstance(target, Router):
            self._routers.append(target)
            self._routes.extend(target.routes)
            return target

        controller_cls: type[Controller] | None = None
        if inspect.isclass(target) and issubclass(target, Controller):
            controller_cls = target
        elif isinstance(target, Controller):
            controller_cls = target.__class__

        if controller_cls is None:
            raise TypeError("register expects a Router or Controller")

        self._controllers.append(controller_cls)
        controller_routes = controller_cls.controller_routes()
        self._controller_routes.extend(controller_routes)
        self._routes.extend(route.route for route in controller_routes)
        return target

    def state(self, state_cls: type[StateT], *, name: str | None = None, **kwargs: Any) -> StateT:
        if not issubclass(state_cls, NativeState):
            raise TypeError("state_cls must be a NativeState subclass")

        existing = self._states.get(state_cls)
        if existing is not None:
            return existing  # type: ignore[return-value]

        state = state_cls(**kwargs)
        self._states[state_cls] = state
        register_state(state, name or state_cls.__name__)
        return state

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
        _ = self.compiler
        await _run_hooks(self._startup_hooks)

    async def shutdown(self) -> None:
        await _run_hooks(self._shutdown_hooks)


async def _run_hooks(hooks: list[LifecycleHook]) -> None:
    for hook in hooks:
        result = hook()
        if inspect.isawaitable(result):
            await result


App = Vertebra


__all__ = ("App", "Vertebra")
