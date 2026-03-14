"""VertebraApp: primary user-facing application container."""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Callable
from typing import Any

import msgspec

from vertebra.core.lifecycle import App
from vertebra.web.routing import RouteDef, _compile_path_template, _normalize_path, _normalize_prefix


class HandlerInfo(msgspec.Struct, frozen=True, array_like=True):
    """Metadata stored per registered handler.

    Serializable via msgspec for future FFI transmission.
    """

    handler_id: int
    method: str
    path: str
    handler_name: str


def _make_handler_id(method: str, path: str) -> int:
    """Derive a stable uint64 handler ID from method and path.

    Uses SHA-256 so the result is deterministic across interpreter restarts
    and not subject to Python's randomized hash seed (PYTHONHASHSEED).
    """
    key = f"{method.upper()}:{_normalize_path(path)}".encode()
    digest = hashlib.sha256(key).digest()
    return struct.unpack_from(">Q", digest)[0]


class VertebraApp:
    """High-performance application container with decorator-style routing.

    Usage::

        app = VertebraApp()

        @app.get("/hello")
        def hello() -> str:
            return "world"

    The app is passed to the CLI as ``vertebra run mymodule:app``.
    """

    __slots__ = ("_app", "_handler_index", "_mounts")

    def __init__(self) -> None:
        self._app: App = App()
        self._handler_index: dict[int, HandlerInfo] = {}
        self._mounts: list[tuple[str, Any]] = []

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

    def _route(self, method: str, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        normalized = _normalize_path(path)
        handler_id = _make_handler_id(method, normalized)

        def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
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

            return handler

        return decorator

    def mount(self, prefix: str, sub_app: Any) -> None:
        """Mount a WSGI or ASGI sub-application under a path prefix."""
        self._mounts.append((_normalize_prefix(prefix), sub_app))

    @property
    def routes(self) -> tuple[RouteDef, ...]:
        """All registered routes, in registration order."""
        return tuple(self._app._routes)  # noqa: SLF001

    @property
    def handler_index(self) -> dict[int, HandlerInfo]:
        """Mapping from stable uint64 handler_id to HandlerInfo."""
        return dict(self._handler_index)

    @property
    def mounts(self) -> tuple[tuple[str, Any], ...]:
        """All mounted sub-applications."""
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


__all__ = ("HandlerInfo", "VertebraApp", "_make_handler_id")
