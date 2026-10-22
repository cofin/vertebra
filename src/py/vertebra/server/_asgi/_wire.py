"""ASGI 3.0 wire-extension response helpers and route classification utilities."""

import re
from collections.abc import Buffer, Callable, Iterable
from pathlib import Path
from typing import Any

__all__ = (
    "HeaderFilter",
    "PathSendResponse",
    "TrailerResponse",
    "WebSocketHttpResponse",
    "ZeroCopySendResponse",
    "compile_guard_pattern",
    "is_static_route",
)

_PARAM_RE = re.compile(r"\{[^}]+\}")


class ZeroCopySendResponse:
    """ASGI 3.0 callable that sends a buffer via ``http.response.zerocopysend``."""

    __slots__ = ("_buffer", "_content_type", "_headers", "_status")

    def __init__(
        self,
        buffer: Buffer,
        *,
        status: int = 200,
        headers: Iterable[tuple[bytes, bytes]] | None = None,
        content_type: bytes = b"application/octet-stream",
    ) -> None:
        self._buffer = buffer
        self._status = status
        self._content_type = content_type
        self._headers = list(headers) if headers is not None else []

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Any],
        send: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Execute the ASGI response using zerocopysend when advertised."""
        del receive
        mv = memoryview(self._buffer)
        hdrs = list(self._headers)
        if not any(k.lower() == b"content-type" for k, _ in hdrs):
            hdrs.append((b"content-type", self._content_type))
        if not any(k.lower() == b"content-length" for k, _ in hdrs):
            hdrs.append((b"content-length", str(mv.nbytes).encode("ascii")))

        await send(
            {
                "type": "http.response.start",
                "status": self._status,
                "headers": hdrs,
            }
        )

        extensions = scope.get("extensions")
        if isinstance(extensions, dict) and "http.response.zerocopysend" in extensions:
            await send(
                {
                    "type": "http.response.zerocopysend",
                    "buffer": mv,
                    "more_body": False,
                }
            )
        else:
            await send(
                {
                    "type": "http.response.body",
                    "body": bytes(mv),
                    "more_body": False,
                }
            )


class PathSendResponse:
    """ASGI 3.0 callable that offloads file serving via ``http.response.pathsend``."""

    __slots__ = ("_content_type", "_headers", "_path", "_status")

    def __init__(
        self,
        path: str | Path,
        *,
        status: int = 200,
        headers: Iterable[tuple[bytes, bytes]] | None = None,
        content_type: bytes = b"application/octet-stream",
    ) -> None:
        self._path = Path(path)
        self._status = status
        self._content_type = content_type
        self._headers = list(headers) if headers is not None else []

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Any],
        send: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Execute the ASGI response using pathsend when advertised."""
        del receive
        hdrs = list(self._headers)
        if not any(k.lower() == b"content-type" for k, _ in hdrs):
            hdrs.append((b"content-type", self._content_type))

        await send(
            {
                "type": "http.response.start",
                "status": self._status,
                "headers": hdrs,
            }
        )

        extensions = scope.get("extensions")
        if isinstance(extensions, dict) and "http.response.pathsend" in extensions:
            await send(
                {
                    "type": "http.response.pathsend",
                    "path": str(self._path),
                }
            )
        else:
            data = self._path.read_bytes()
            await send(
                {
                    "type": "http.response.body",
                    "body": data,
                    "more_body": False,
                }
            )


class TrailerResponse:
    """ASGI 3.0 callable that emits HTTP response trailers via ``http.response.trailers``."""

    __slots__ = ("_body", "_headers", "_status", "_trailers")

    def __init__(
        self,
        body: bytes,
        *,
        trailers: Iterable[tuple[bytes, bytes]],
        status: int = 200,
        headers: Iterable[tuple[bytes, bytes]] | None = None,
    ) -> None:
        self._body = body
        self._trailers = list(trailers)
        self._status = status
        self._headers = list(headers) if headers is not None else []

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Any],
        send: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Send response headers, body, and trailing headers."""
        del receive
        extensions = scope.get("extensions")
        supports_trailers = isinstance(extensions, dict) and "http.response.trailers" in extensions

        hdrs = list(self._headers)
        if supports_trailers and self._trailers and not any(k.lower() == b"trailer" for k, _ in hdrs):
            names = b", ".join(k for k, _ in self._trailers)
            hdrs.append((b"trailer", names))

        await send(
            {
                "type": "http.response.start",
                "status": self._status,
                "headers": hdrs,
                "trailers": bool(supports_trailers and self._trailers),
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": self._body,
                "more_body": False,
            }
        )
        if supports_trailers and self._trailers:
            await send(
                {
                    "type": "http.response.trailers",
                    "headers": self._trailers,
                    "more_trailers": False,
                }
            )


class WebSocketHttpResponse:
    """ASGI 3.0 callable that rejects a WebSocket handshake with an HTTP response."""

    __slots__ = ("_body", "_headers", "_status")

    def __init__(
        self,
        body: bytes = b"",
        *,
        status: int = 403,
        headers: Iterable[tuple[bytes, bytes]] | None = None,
    ) -> None:
        self._body = body
        self._status = status
        self._headers = list(headers) if headers is not None else []

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Any],
        send: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Send websocket.http.response.start + body if supported, else websocket.close."""
        del receive
        extensions = scope.get("extensions")
        if isinstance(extensions, dict) and "websocket.http.response" in extensions:
            hdrs = list(self._headers)
            if not any(k.lower() == b"content-length" for k, _ in hdrs):
                hdrs.append((b"content-length", str(len(self._body)).encode("ascii")))
            await send(
                {
                    "type": "websocket.http.response.start",
                    "status": self._status,
                    "headers": hdrs,
                }
            )
            await send(
                {
                    "type": "websocket.http.response.body",
                    "body": self._body,
                    "more_body": False,
                }
            )
        else:
            await send({"type": "websocket.close", "code": 1008})


def compile_guard_pattern(pattern: str) -> re.Pattern[str]:
    """Compile a route guard pattern into a regular expression."""
    escaped = re.sub(r"\{[^}]+\}", r"[^/]+", pattern)
    return re.compile(f"^{escaped}$")


def is_static_route(path: str) -> bool:
    """Return True if the route path has no dynamic segments."""
    return not bool(_PARAM_RE.search(path))


class HeaderFilter:
    """Header filter for pre-dispatch guard checks."""

    __slots__ = ("_blocked", "_required")

    def __init__(
        self,
        required: dict[str, str] | None = None,
        blocked: set[str] | None = None,
    ) -> None:
        self._required: dict[str, str] = {k.lower(): v for k, v in (required or {}).items()}
        self._blocked: set[str] = {k.lower() for k in (blocked or set())}

    def check(self, headers: dict[str, str]) -> tuple[bool, int]:
        """Validate ``headers`` against required and blocked rules."""
        lower = {k.lower(): v for k, v in headers.items()}
        for name in self._blocked:
            if name in lower:
                return False, 403
        for name, expected in self._required.items():
            actual = lower.get(name)
            if actual is None:
                return False, 401
            if expected and actual != expected:
                return False, 403
        return True, 200
