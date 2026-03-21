"""Application containers for Vertebra.

App: Low-level lifecycle container (routing + startup/shutdown hooks).
VertebraApp: User-facing application with decorator routing and handler ID management.
"""

import inspect
from collections.abc import Callable
from typing import Any

import msgspec

from vertebra._typing import LifecycleHook
from vertebra.framework._router import RouteDef, Router, _compile_path_template, _normalize_path, _normalize_prefix

# ---------------------------------------------------------------------------
# Low-level lifecycle container
# ---------------------------------------------------------------------------


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
    def routes(self) -> "tuple[RouteDef, ...]":
        return tuple(self._routes)

    @property
    def routers(self) -> "tuple[Router, ...]":
        return tuple(self._routers)

    def register(self, target: "Router | Callable[..., Any]") -> Any:
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

    def on_startup(self, fn: "LifecycleHook | None" = None) -> Any:
        if fn is None:

            def decorator(func: "LifecycleHook") -> "LifecycleHook":
                self._startup_hooks.append(func)
                return func

            return decorator
        self._startup_hooks.append(fn)
        return fn

    def on_shutdown(self, fn: "LifecycleHook | None" = None) -> Any:
        if fn is None:

            def decorator(func: "LifecycleHook") -> "LifecycleHook":
                self._shutdown_hooks.append(func)
                return func

            return decorator
        self._shutdown_hooks.append(fn)
        return fn

    async def startup(self) -> None:
        await _run_hooks(self._startup_hooks)

    async def shutdown(self) -> None:
        await _run_hooks(self._shutdown_hooks)


async def _run_hooks(hooks: "list[LifecycleHook]") -> None:
    for hook in hooks:
        result = hook()
        if inspect.isawaitable(result):
            await result


# ---------------------------------------------------------------------------
# Handler ID generation (FNV-1a, matches Rust route_table.rs)
# ---------------------------------------------------------------------------

_FNV_OFFSET = 0xCBF29CE484222325
_FNV_PRIME = 0x100000001B3
_MASK_64 = 0xFFFFFFFFFFFFFFFF


def _fnv1a_write(h: int, data: bytes) -> int:
    """FNV-1a hash update: process each byte of *data*."""
    for b in data:
        h ^= b
        h = (h * _FNV_PRIME) & _MASK_64
    return h


def _make_handler_id(method: str, path: str) -> int:
    """Derive a stable uint64 handler ID from method and path.

    Uses FNV-1a matching the Rust implementation in route_table.rs.
    Rust's ``Hash for str`` writes the string bytes followed by 0xFF.
    Rust's ``Hash for u8`` writes a single byte.

    Total byte sequence: method_upper | 0xFF | 0x20 | path | 0xFF
    """
    h = _FNV_OFFSET
    h = _fnv1a_write(h, method.upper().encode())
    h = _fnv1a_write(h, b"\xff")
    h = _fnv1a_write(h, b" ")
    h = _fnv1a_write(h, path.encode())
    h = _fnv1a_write(h, b"\xff")
    return h


class HandlerInfo(msgspec.Struct, frozen=True, array_like=True):
    """Metadata stored per registered handler.

    Serializable via msgspec for future FFI transmission.
    """

    handler_id: int
    method: str
    path: str
    handler_name: str


# ---------------------------------------------------------------------------
# User-facing application container
# ---------------------------------------------------------------------------


class VertebraApp:
    """High-performance application container with decorator-style routing.

    Usage::

        app = VertebraApp()

        @app.get("/hello")
        def hello() -> str:
            return "world"

    The app is passed to the CLI as ``vertebra run mymodule:app``.
    """

    __slots__ = ("_app", "_handler_index", "_handlers", "_mounts")

    def __init__(self) -> None:
        self._app: App = App()
        self._handler_index: dict[int, HandlerInfo] = {}
        self._handlers: dict[int, Callable[..., Any]] = {}
        self._mounts: list[tuple[str, Any, str]] = []

    def get(self, path: str) -> "Callable[[Callable[..., Any]], Callable[..., Any]]":
        """Register a GET handler."""
        return self._route("GET", path)

    def post(self, path: str) -> "Callable[[Callable[..., Any]], Callable[..., Any]]":
        """Register a POST handler."""
        return self._route("POST", path)

    def put(self, path: str) -> "Callable[[Callable[..., Any]], Callable[..., Any]]":
        """Register a PUT handler."""
        return self._route("PUT", path)

    def delete(self, path: str) -> "Callable[[Callable[..., Any]], Callable[..., Any]]":
        """Register a DELETE handler."""
        return self._route("DELETE", path)

    def patch(self, path: str) -> "Callable[[Callable[..., Any]], Callable[..., Any]]":
        """Register a PATCH handler."""
        return self._route("PATCH", path)

    def _route(self, method: str, path: str) -> "Callable[[Callable[..., Any]], Callable[..., Any]]":
        normalized = _normalize_path(path)
        handler_id = _make_handler_id(method, normalized)

        def decorator(handler: "Callable[..., Any]") -> "Callable[..., Any]":
            segments = _compile_path_template(normalized)
            route = RouteDef(method=method.upper(), path=normalized, handler=handler, segments=segments)

            self._app._routes.append(route)  # noqa: SLF001

            info = HandlerInfo(
                handler_id=handler_id,
                method=method.upper(),
                path=normalized,
                handler_name=getattr(handler, "__qualname__", repr(handler)),
            )
            self._handler_index[handler_id] = info
            self._handlers[handler_id] = handler

            return handler

        return decorator

    def mount(self, prefix: str, sub_app: Any, *, protocol: str = "auto") -> None:
        """Mount a WSGI or ASGI sub-application under a path prefix."""
        self._mounts.append((_normalize_prefix(prefix), sub_app, protocol))

    @property
    def routes(self) -> "tuple[RouteDef, ...]":
        """All registered routes, in registration order."""
        return tuple(self._app._routes)  # noqa: SLF001

    @property
    def handler_index(self) -> "dict[int, HandlerInfo]":
        """Mapping from stable uint64 handler_id to HandlerInfo."""
        return dict(self._handler_index)

    @property
    def mounts(self) -> "tuple[tuple[str, Any, str], ...]":
        """All mounted sub-applications as (prefix, app, protocol) tuples."""
        return tuple(self._mounts)

    def on_startup(self, fn: Any = None) -> Any:
        """Register a startup lifecycle hook."""
        return self._app.on_startup(fn)

    def on_shutdown(self, fn: Any = None) -> Any:
        """Register a shutdown lifecycle hook."""
        return self._app.on_shutdown(fn)

    async def startup(self) -> None:
        """Run all registered startup hooks."""
        await self._app.startup()

    async def shutdown(self) -> None:
        """Run all registered shutdown hooks."""
        await self._app.shutdown()


__all__ = ("App", "HandlerInfo", "LifecycleHook", "VertebraApp", "_make_handler_id")
