"""ASGI 3.0 TechEmpower data, cache, and object-storage benchmark application.

Exports both a raw ASGI callable for Granian and Uvicorn baselines and a
Vertebra ``App`` mounting the ASGI callable at ``/``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import parse_qs

from tools.benchmarks.apps._data_backend import (
    JSON_HELLO_PAYLOAD,
    DataBackend,
    assert_free_threaded_runtime,
    clamp_query_count,
    get_default_backend,
)
from vertebra.framework._app import App
from vertebra.utils.serializers import encode_json_bytes

__all__ = (
    "ASGIReceive",
    "ASGIScope",
    "ASGISend",
    "asgi_app",
    "create_asgi_data_app",
    "create_vertebra_asgi_app",
    "vertebra_asgi_app",
)

ASGIScope = dict[str, Any]
ASGIReceive = Callable[[], Awaitable[dict[str, Any]]]
ASGISend = Callable[[dict[str, Any]], Awaitable[None]]

_JSON_CONTENT_TYPE = b"application/json"
_TEXT_CONTENT_TYPE = b"text/plain"
_BINARY_CONTENT_TYPE = b"application/octet-stream"
_OBJECT_PREFIX = "/object/"


def _query_count(raw_query_string: bytes | bytearray | str, key: str) -> int:
    """Decode and clamp a TechEmpower count parameter from an ASGI query string."""
    query_string = (
        raw_query_string.decode("latin-1")
        if isinstance(raw_query_string, (bytes, bytearray))
        else str(raw_query_string)
    )
    if not query_string:
        return 1
    values = parse_qs(query_string).get(key)
    return clamp_query_count(values[0] if values else None)


async def _send_response(send: ASGISend, status: int, content_type: bytes, body: bytes) -> None:
    """Send a complete ASGI HTTP response."""
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                [b"content-type", content_type],
                [b"content-length", str(len(body)).encode("ascii")],
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


async def _receive_body(receive: ASGIReceive) -> bytes:
    """Collect the complete ASGI HTTP request body."""
    chunks = bytearray()
    while True:
        message = await receive()
        chunks.extend(message.get("body", b""))
        if not message.get("more_body", False):
            break
    return bytes(chunks)


def create_asgi_data_app(
    backend: DataBackend | None = None,
) -> Callable[[ASGIScope, ASGIReceive, ASGISend], Awaitable[None]]:
    """Create an ASGI 3.0 callable backed by ``DataBackend``.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        ASGI callable handling ``lifespan`` events and HTTP requests for
        TechEmpower data, cache, object-storage, and health routes.
    """
    resolved_backend = get_default_backend() if backend is None else backend

    async def _asgi_data_app(scope: ASGIScope, receive: ASGIReceive, send: ASGISend) -> None:
        """Dispatch ASGI lifespan and HTTP requests."""
        scope_type = scope.get("type")
        if scope_type == "lifespan":
            while True:
                message = await receive()
                msg_type = message.get("type")
                if msg_type == "lifespan.startup":
                    try:
                        assert_free_threaded_runtime(strict=resolved_backend.config.strict_free_threading)
                    except Exception as exc:
                        await send({"type": "lifespan.startup.failed", "message": str(exc)})
                        return
                    await send({"type": "lifespan.startup.complete"})
                elif msg_type == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
            return

        if scope_type != "http":
            return

        path = str(scope.get("path", "/"))
        method = str(scope.get("method", "GET")).upper()
        raw_qs = scope.get("query_string", b"")

        if path == "/json":
            await _send_response(send, 200, _JSON_CONTENT_TYPE, JSON_HELLO_PAYLOAD)
            return

        if path == "/db":
            row = await resolved_backend.get_world()
            await _send_response(send, 200, _JSON_CONTENT_TYPE, encode_json_bytes(row))
            return

        if path == "/queries":
            count = _query_count(raw_qs, "queries")
            rows = await resolved_backend.get_worlds(count)
            await _send_response(send, 200, _JSON_CONTENT_TYPE, encode_json_bytes(rows))
            return

        if path == "/updates":
            count = _query_count(raw_qs, "queries")
            rows = await resolved_backend.update_worlds(count)
            await _send_response(send, 200, _JSON_CONTENT_TYPE, encode_json_bytes(rows))
            return

        if path == "/cached-queries":
            count = _query_count(raw_qs, "count")
            rows = await resolved_backend.get_cached_worlds(count)
            await _send_response(send, 200, _JSON_CONTENT_TYPE, encode_json_bytes(rows))
            return

        if path == "/health":
            await _send_response(send, 200, _TEXT_CONTENT_TYPE, b"ok")
            return

        if path.startswith(_OBJECT_PREFIX):
            key = path.removeprefix(_OBJECT_PREFIX)
            if not key:
                await _send_response(send, 404, _TEXT_CONTENT_TYPE, b"Not Found")
                return
            if method == "GET":
                payload = await resolved_backend.get_object(key)
                if payload is None:
                    await _send_response(send, 404, _TEXT_CONTENT_TYPE, b"Not Found")
                    return
                await _send_response(send, 200, _BINARY_CONTENT_TYPE, payload)
                return
            if method == "PUT":
                body = await _receive_body(receive)
                size = await resolved_backend.put_object(key, body)
                await _send_response(send, 200, _JSON_CONTENT_TYPE, encode_json_bytes({"key": key, "size": size}))
                return
            await _send_response(send, 405, _TEXT_CONTENT_TYPE, b"Method Not Allowed")
            return

        await _send_response(send, 404, _TEXT_CONTENT_TYPE, b"Not Found")

    return _asgi_data_app


def create_vertebra_asgi_app(backend: DataBackend | None = None) -> App:
    """Create a Vertebra ``App`` mounting the ASGI data callable at ``/``.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        Configured ``App`` with a free-threaded startup check and root ASGI mount.
    """
    resolved_backend = get_default_backend() if backend is None else backend
    app = App()

    @app.on_startup
    def _verify_free_threaded_startup() -> None:
        """Enforce strict free-threading when configured on the backend."""
        assert_free_threaded_runtime(strict=resolved_backend.config.strict_free_threading)

    app.mount("/", create_asgi_data_app(resolved_backend), protocol="asgi")
    return app


asgi_app: Callable[[ASGIScope, ASGIReceive, ASGISend], Awaitable[None]] = create_asgi_data_app()
vertebra_asgi_app: App = create_vertebra_asgi_app()
