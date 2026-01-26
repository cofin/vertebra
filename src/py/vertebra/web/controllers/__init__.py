"""Controller base class for native app routing."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from vertebra.web.routing import RouteDef


class ControllerRoute(msgspec.Struct, frozen=True, array_like=True):
    route: RouteDef
    guards: tuple[Any, ...] = ()
    middleware: tuple[Any, ...] = ()


class Controller:
    """Base class for grouping related route handlers."""

    path: str = ""
    guards: Sequence[Any] = ()
    middleware: Sequence[Any] = ()

    @classmethod
    def routes(cls) -> tuple[RouteDef, ...]:
        return tuple(route.with_prefix(cls.path) for route in _collect_routes(cls))

    @classmethod
    def controller_routes(cls) -> tuple[ControllerRoute, ...]:
        guards = tuple(cls.guards)
        middleware = tuple(cls.middleware)
        return tuple(ControllerRoute(route=route, guards=guards, middleware=middleware) for route in cls.routes())


def _collect_routes(controller_cls: type[Controller]) -> list[RouteDef]:
    routes: list[RouteDef] = []

    for base in reversed(controller_cls.mro()):
        if base is Controller:
            continue
        for attr in base.__dict__.values():
            handler = _unwrap_handler(attr)
            if handler is None:
                continue
            route_defs = getattr(handler, "__vertebra_routes__", None)
            if route_defs:
                routes.extend(route_defs)

    return routes


def _unwrap_handler(attr: Any) -> Callable[..., Any] | None:
    if isinstance(attr, staticmethod):
        return attr.__func__
    if isinstance(attr, classmethod):
        return attr.__func__
    if callable(attr):
        return attr
    return None


__all__ = ("Controller", "ControllerRoute")
