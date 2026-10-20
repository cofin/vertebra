"""ASGI 3.0 wire extension helpers and framework integration extensions."""

from __future__ import annotations

import functools
import logging
import re
from collections.abc import Awaitable, Callable
from typing import Any

from vertebra._typing import module_available
from vertebra.server._asgi._litestar_router import extract_litestar_route_entries
from vertebra.server._asgi._queues import AsgiResponse
from vertebra.utils.serializers._json import encode_json_bytes

__all__ = (
    "DebugResponse",
    "EarlyHintResponse",
    "FastApiExtension",
    "HeaderFilter",
    "LitestarExtension",
    "PathSendResponse",
    "TrailerResponse",
    "WebSocketHttpResponse",
    "ZeroCopySendResponse",
    "compile_guard_pattern",
    "get_extensions",
    "is_static_route",
    "register_extension",
)

log = logging.getLogger(__name__)

type AsgiSendCallable = Callable[[dict[str, Any]], Awaitable[None]]

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


class ZeroCopySendResponse:
    """ASGI http.response.zerocopysend helper transmitting a buffer-protocol view without copying."""

    __slots__ = ("body", "headers", "more_body", "status")

    def __init__(
        self,
        body: memoryview | bytes | bytearray,
        *,
        status: int = 200,
        headers: list[tuple[bytes, bytes]] | None = None,
        more_body: bool = False,
    ) -> None:
        self.body: memoryview = body if isinstance(body, memoryview) else memoryview(body)
        self.status: int = status
        self.headers: list[tuple[bytes, bytes]] = headers or []
        self.more_body: bool = more_body

    async def send(self, send: AsgiSendCallable) -> None:
        """Emit http.response.start followed by http.response.zerocopysend."""
        await send(
            {
                "type": "http.response.start",
                "status": self.status,
                "headers": self.headers,
            }
        )
        await send(
            {
                "type": "http.response.zerocopysend",
                "body": self.body,
                "more_body": self.more_body,
            }
        )


class PathSendResponse:
    """ASGI http.response.pathsend helper delegating file and byte-range streaming to Rust."""

    __slots__ = ("end", "headers", "path", "start", "status")

    def __init__(
        self,
        path: str,
        *,
        status: int = 200,
        headers: list[tuple[bytes, bytes]] | None = None,
        start: int | None = None,
        end: int | None = None,
    ) -> None:
        self.path: str = path
        self.status: int = status
        self.headers: list[tuple[bytes, bytes]] = headers or []
        self.start: int | None = start
        self.end: int | None = end

    async def send(self, send: AsgiSendCallable) -> None:
        """Emit http.response.start followed by http.response.pathsend."""
        await send(
            {
                "type": "http.response.start",
                "status": self.status,
                "headers": self.headers,
            }
        )
        payload: dict[str, Any] = {
            "type": "http.response.pathsend",
            "path": self.path,
        }
        if self.start is not None:
            payload["start"] = self.start
        if self.end is not None:
            payload["end"] = self.end
        await send(payload)


class TrailerResponse:
    """ASGI http.response.trailers helper emitting trailing HTTP/2 headers."""

    __slots__ = ("headers", "more_trailers")

    def __init__(
        self,
        headers: list[tuple[bytes, bytes]],
        *,
        more_trailers: bool = False,
    ) -> None:
        self.headers: list[tuple[bytes, bytes]] = headers
        self.more_trailers: bool = more_trailers

    async def send(self, send: AsgiSendCallable) -> None:
        """Emit http.response.trailers."""
        await send(
            {
                "type": "http.response.trailers",
                "headers": self.headers,
                "more_trailers": self.more_trailers,
            }
        )


class EarlyHintResponse:
    """ASGI http.response.early_hint helper emitting 103 Early Hints Link headers."""

    __slots__ = ("links",)

    def __init__(self, links: list[bytes]) -> None:
        self.links: list[bytes] = links

    async def send(self, send: AsgiSendCallable) -> None:
        """Emit http.response.early_hint."""
        await send(
            {
                "type": "http.response.early_hint",
                "links": self.links,
            }
        )


class DebugResponse:
    """ASGI http.response.debug helper attaching diagnostic metadata to the response."""

    __slots__ = ("info",)

    def __init__(self, info: dict[str, Any]) -> None:
        self.info: dict[str, Any] = info

    async def send(self, send: AsgiSendCallable) -> None:
        """Emit http.response.debug."""
        await send(
            {
                "type": "http.response.debug",
                "info": self.info,
            }
        )


class WebSocketHttpResponse:
    """ASGI websocket.http.response helper rejecting a WebSocket handshake with an HTTP response."""

    __slots__ = ("body", "headers", "status")

    def __init__(
        self,
        *,
        status: int = 403,
        headers: list[tuple[bytes, bytes]] | None = None,
        body: bytes = b"",
    ) -> None:
        self.status: int = status
        self.headers: list[tuple[bytes, bytes]] = headers or []
        self.body: bytes = body

    async def send(self, send: AsgiSendCallable) -> None:
        """Emit websocket.http.response.start and websocket.http.response.body."""
        await send(
            {
                "type": "websocket.http.response.start",
                "status": self.status,
                "headers": self.headers,
            }
        )
        await send(
            {
                "type": "websocket.http.response.body",
                "body": self.body,
                "more_body": False,
            }
        )


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
                body = encode_json_bytes(result)
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
            return {entry.path_template: entry.route_handler for entry in extract_litestar_route_entries(app)}
        except Exception:
            log.debug("Litestar route extraction failed", exc_info=True)
            return {}

    def _resolve_handler(self, app: Any, request: Any) -> tuple[Any, dict[str, Any]]:
        try:
            for entry in extract_litestar_route_entries(app):
                if entry.path_template == request.path:
                    handler = entry.route_handler
                    fn = getattr(handler, "fn", None)
                    if fn is not None and not getattr(handler, "middleware", None):
                        return fn.value if hasattr(fn, "value") else fn, {}
        except Exception:
            pass
        return None, {}


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


if _FASTAPI_AVAILABLE:
    register_extension(FastApiExtension())

if _LITESTAR_AVAILABLE:
    register_extension(LitestarExtension())
