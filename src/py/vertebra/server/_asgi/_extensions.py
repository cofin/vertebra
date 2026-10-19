"""ASGI framework extensions — shared utilities and FastAPI/Litestar integrations."""

import functools
import logging
import re
from typing import Any

from vertebra._typing import module_available
from vertebra.server._asgi._queues import AsgiResponse
from vertebra.utils.serializers._json import encode_json

__all__ = [
    "FastApiExtension",
    "HeaderFilter",
    "LitestarExtension",
    "compile_guard_pattern",
    "get_extensions",
    "is_static_route",
    "register_extension",
]

log = logging.getLogger(__name__)

_EXTENSIONS: list[Any] = []
_FASTAPI_AVAILABLE = module_available("fastapi")
_LITESTAR_AVAILABLE = module_available("litestar")

_OPENAPI_PATHS: frozenset[str] = frozenset(
    {
        "/openapi.json",
        "/docs",
        "/docs/oauth2-redirect",
        "/redoc",
    }
)


def register_extension(extension: Any) -> None:
    """Register a framework extension with AsgiAdapter's extension chain."""
    _EXTENSIONS.append(extension)


def get_extensions() -> list[Any]:
    """Return the list of registered extensions."""
    return list(_EXTENSIONS)


@functools.lru_cache(maxsize=512)
def compile_guard_pattern(pattern: str) -> re.Pattern[str]:
    """Compile and cache a route guard pattern."""
    regex = re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", pattern)
    regex = re.sub(r":(\w+)", r"(?P<\1>[^/]+)", regex)
    return re.compile(rf"^{regex}$")


def is_static_route(path: str) -> bool:
    """Return True if the path contains no variable segments."""
    return "{" not in path and ":" not in path


class HeaderFilter:
    """Fast-path check for a required header name/value pair."""

    __slots__ = ("_name_bytes", "_value_bytes")

    def __init__(self, name: str, value: str) -> None:
        self._name_bytes = name.lower().encode("latin-1")
        self._value_bytes = value.encode("latin-1")

    def matches(self, headers_bytes: list[tuple[bytes, bytes]]) -> bool:
        """Return True if the required header is present."""
        return any(name == self._name_bytes and value == self._value_bytes for name, value in headers_bytes)


class FastApiExtension:
    """Vertebra extension for FastAPI ASGI applications."""

    def can_accelerate(self, app: Any) -> bool:
        """Return True if the given application is a FastAPI instance."""
        if not _FASTAPI_AVAILABLE:
            return False
        try:
            from fastapi import FastAPI

            return isinstance(app, FastAPI)
        except Exception:
            return False

    async def try_handle(self, app: Any, request: Any) -> AsgiResponse | None:
        """Attempt direct handler invocation, returning None to fall through."""
        _ = (app, request)
        return None

    def extract_routes(self, app: Any) -> dict[str, Any]:
        """Extract static and dynamic routes registered on a FastAPI app."""
        if not _FASTAPI_AVAILABLE:
            return {}
        try:
            routes: dict[str, Any] = {}
            for route in getattr(getattr(app, "router", None), "routes", []):
                path = getattr(route, "path", None)
                if path:
                    routes[path] = route
            return routes
        except Exception:
            log.debug("FastAPI route extraction failed", exc_info=True)
            return {}


class LitestarExtension:
    """Vertebra extension for Litestar ASGI applications."""

    def can_accelerate(self, app: Any) -> bool:
        """Return True if the given application is a Litestar instance."""
        if not _LITESTAR_AVAILABLE:
            return False
        try:
            from litestar import Litestar

            return isinstance(app, Litestar)
        except Exception:
            return False

    async def try_handle(self, app: Any, request: Any) -> AsgiResponse | None:
        """Attempt direct handler invocation, returning None to fall through."""
        try:
            from litestar import Litestar

            if not isinstance(app, Litestar):
                return None
            route_handler, params = self._resolve_handler(app, request)
            if route_handler is None:
                return None
            result = await route_handler(**params)
            if isinstance(result, dict):
                body = encode_json(result, as_bytes=True)
                return AsgiResponse(status=200, headers=[("content-type", "application/json")], body=body)
            if isinstance(result, (str, bytes)):
                body = result.encode("utf-8") if isinstance(result, str) else result
                return AsgiResponse(status=200, headers=[("content-type", "text/plain")], body=body)
            return None
        except Exception:
            log.debug("Litestar direct invocation failed, falling back to ASGI", exc_info=True)
            return None

    def extract_routes(self, app: Any) -> dict[str, Any]:
        """Extract route handlers keyed by path pattern from a Litestar app."""
        if not _LITESTAR_AVAILABLE:
            return {}
        try:
            routes: dict[str, Any] = {}
            for route in getattr(app, "routes", []):
                pattern = getattr(route, "path", None)
                handler = getattr(route, "route_handler", None)
                if pattern and handler:
                    routes[pattern] = handler
            return routes
        except Exception:
            log.debug("Litestar route extraction failed", exc_info=True)
            return {}

    def _resolve_handler(self, app: Any, request: Any) -> tuple[Any, dict[str, Any]]:
        try:
            for route in getattr(app, "routes", []):
                handler = getattr(route, "route_handler", None)
                path = getattr(route, "path", None)
                if path == request.path and handler is not None:
                    fn = getattr(handler, "fn", None)
                    if fn is not None and not getattr(handler, "middleware", None):
                        return fn.value if hasattr(fn, "value") else fn, {}
        except Exception:
            pass
        return None, {}


if _FASTAPI_AVAILABLE:
    register_extension(FastApiExtension())

if _LITESTAR_AVAILABLE:
    register_extension(LitestarExtension())
