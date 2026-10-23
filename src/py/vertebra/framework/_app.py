"""Application container for Vertebra.

Provides routing, decorator registration, sub-application mounting,
handler ID indexing, and startup/shutdown lifecycle hooks.
"""

import inspect
from collections.abc import Callable
from typing import Any

import msgspec

from vertebra._typing import LifecycleHook
from vertebra.framework._router import (
    RouteDef,
    Router,
    _compile_path_template,
    _make_handler_id,
    _normalize_path,
    _normalize_prefix,
)
from vertebra.framework.openapi import OpenAPIConfig, OpenAPISpecBuilder

__all__ = ("App", "HandlerInfo", "LifecycleHook", "_make_handler_id")


class HandlerInfo(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Metadata stored per registered handler, serializable via msgspec."""

    handler_id: int
    method: str
    path: str
    handler_name: str


class App:
    """High-performance application container for routing and lifecycle hooks."""

    __slots__ = (
        "_handler_index",
        "_handlers",
        "_mounts",
        "_openapi_builder",
        "_openapi_config",
        "_routers",
        "_routes",
        "_shutdown_hooks",
        "_startup_hooks",
    )

    def __init__(self) -> None:
        self._routers: list[Router] = []
        self._routes: list[RouteDef] = []
        self._startup_hooks: list[LifecycleHook] = []
        self._shutdown_hooks: list[LifecycleHook] = []
        self._handler_index: dict[int, HandlerInfo] = {}
        self._handlers: dict[int, Callable[..., Any]] = {}
        self._mounts: list[tuple[str, Any, str]] = []
        self._openapi_config: Any = None
        self._openapi_builder: Any = None

    @property
    def routes(self) -> tuple[RouteDef, ...]:
        """Return all registered route definitions in registration order."""
        return tuple(self._routes)

    @property
    def routers(self) -> tuple[Router, ...]:
        """Return all registered routers."""
        return tuple(self._routers)

    @property
    def handler_index(self) -> dict[int, HandlerInfo]:
        """Mapping from stable uint64 handler_id to HandlerInfo."""
        return dict(self._handler_index)

    @property
    def mounts(self) -> tuple[tuple[str, Any, str], ...]:
        """All mounted sub-applications as (prefix, app, protocol) tuples."""
        return tuple(self._mounts)

    def add_route(self, route: RouteDef) -> None:
        """Append a single route definition and index its handler."""
        self._routes.append(route)
        handler_id = _make_handler_id(route.method, route.path)
        info = HandlerInfo(
            handler_id=handler_id,
            method=route.method.upper(),
            path=route.path,
            handler_name=getattr(route.handler, "__qualname__", repr(route.handler)),
        )
        self._handler_index[handler_id] = info
        self._handlers[handler_id] = route.handler

    def register(self, target: Router | Callable[..., Any]) -> Any:
        """Register a Router or a route-decorated callable."""
        if isinstance(target, Router):
            self._routers.append(target)
            for route in target.routes:
                self.add_route(route)
            return target
        if callable(target):
            routes = getattr(target, "__vertebra_routes__", None)
            if routes:
                for route in routes:
                    self.add_route(route)
                return target
        msg = "register expects a Router or decorated handler"
        raise TypeError(msg)

    def get(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a GET handler."""
        return self._route("GET", path)

    def post(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a POST handler."""
        return self._route("POST", path)

    def put(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a PUT handler."""
        return self._route("PUT", path)

    def delete(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a DELETE handler."""
        return self._route("DELETE", path)

    def patch(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a PATCH handler."""
        return self._route("PATCH", path)

    def mount(self, prefix: str, sub_app: Any, *, protocol: str = "auto") -> None:
        """Mount a WSGI or ASGI sub-application under a path prefix."""
        self._mounts.append((_normalize_prefix(prefix), sub_app, protocol))

    def enable_docs(self, config: Any = None) -> Any:
        """Enable OpenAPI schema and Scalar documentation endpoints."""
        cfg = config or OpenAPIConfig()
        builder = OpenAPISpecBuilder.from_app(self, config=cfg)
        self._openapi_config = cfg
        self._openapi_builder = builder

        @self.get(cfg.schema_path)
        def _openapi_schema() -> dict[str, Any]:
            return builder.build()

        @self.get(cfg.scalar.path)
        def _scalar_docs() -> str:
            return (
                f"<!doctype html><html><head><title>{cfg.title}</title><meta charset='utf-8' />"
                f"<meta name='viewport' content='width=device-width, initial-scale=1' /></head>"
                f"<body><script id='api-reference' data-url='{cfg.schema_path}'></script>"
                f"<script src='https://cdn.jsdelivr.net/npm/@scalar/api-reference'></script></body></html>"
            )

        return builder

    def on_startup(self, fn: LifecycleHook | None = None) -> Any:
        """Register a startup lifecycle hook directly or as a decorator."""
        if fn is None:

            def decorator(func: LifecycleHook) -> LifecycleHook:
                self._startup_hooks.append(func)
                return func

            return decorator
        self._startup_hooks.append(fn)
        return fn

    def on_shutdown(self, fn: LifecycleHook | None = None) -> Any:
        """Register a shutdown lifecycle hook directly or as a decorator."""
        if fn is None:

            def decorator(func: LifecycleHook) -> LifecycleHook:
                self._shutdown_hooks.append(func)
                return func

            return decorator
        self._shutdown_hooks.append(fn)
        return fn

    async def startup(self) -> None:
        """Execute all registered startup hooks sequentially."""
        await self._run_hooks(self._startup_hooks)

    async def shutdown(self) -> None:
        """Execute all registered shutdown hooks sequentially."""
        await self._run_hooks(self._shutdown_hooks)

    def _route(self, method: str, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        normalized = _normalize_path(path)

        def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
            segments = _compile_path_template(normalized)
            route = RouteDef(method=method.upper(), path=normalized, handler=handler, segments=segments)
            self.add_route(route)
            return handler

        return decorator

    @staticmethod
    async def _run_hooks(hooks: list[LifecycleHook]) -> None:
        """Execute sync or async lifecycle hooks in order."""
        for hook in hooks:
            result = hook()
            if inspect.isawaitable(result):
                await result
