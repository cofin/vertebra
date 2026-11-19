"""WSGI TechEmpower data, cache, and object-storage benchmark application.

Exports both a raw WSGI callable and a Vertebra ``App`` mounting the WSGI
callable at ``/``.
"""

from __future__ import annotations

from collections.abc import Callable
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
    "StartResponse",
    "create_vertebra_wsgi_app",
    "create_wsgi_data_app",
    "vertebra_wsgi_app",
    "wsgi_app",
)

StartResponse = Any

_OBJECT_PREFIX = "/object/"


def _query_count(query_string: str, key: str) -> int:
    """Extract and clamp a TechEmpower count parameter from a WSGI query string."""
    if not query_string:
        return 1
    values = parse_qs(query_string).get(key)
    return clamp_query_count(values[0] if values else None)


def _respond(
    start_response: StartResponse,
    body: bytes,
    content_type: str,
    status: str = "200 OK",
) -> list[bytes]:
    """Emit WSGI status, headers, and response body."""
    headers = [("Content-Type", content_type), ("Content-Length", str(len(body)))]
    start_response(status, headers)
    return [body]


def _read_wsgi_body(environ: dict[str, Any]) -> bytes:
    """Read the request body from ``wsgi.input`` honoring ``CONTENT_LENGTH``."""
    stream = environ.get("wsgi.input")
    if stream is None:
        return b""
    raw_length = environ.get("CONTENT_LENGTH")
    if raw_length is not None and str(raw_length).strip() != "":
        length = int(str(raw_length).strip())
        if length <= 0:
            return b""
        return bytes(stream.read(length))
    return bytes(stream.read())


def create_wsgi_data_app(backend: DataBackend | None = None) -> Callable[[dict[str, Any], Any], list[bytes]]:
    """Create a synchronous WSGI callable backed by ``DataBackend``.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        WSGI callable handling TechEmpower data, cache, object-storage, and
        health routes via ``DataBackend`` sync methods.
    """
    resolved_backend = get_default_backend() if backend is None else backend

    def _wsgi_data_app(environ: dict[str, Any], start_response: StartResponse) -> list[bytes]:
        """Dispatch WSGI requests to ``DataBackend`` sync operations."""
        path = str(environ.get("PATH_INFO", "/"))
        method = str(environ.get("REQUEST_METHOD", "GET")).upper()
        query_string = str(environ.get("QUERY_STRING", "") or "")

        if path == "/json":
            return _respond(start_response, JSON_HELLO_PAYLOAD, "application/json")

        if path == "/db":
            row = resolved_backend.get_world_sync()
            return _respond(start_response, encode_json_bytes(row), "application/json")

        if path == "/queries":
            count = _query_count(query_string, "queries")
            rows = resolved_backend.get_worlds_sync(count)
            return _respond(start_response, encode_json_bytes(rows), "application/json")

        if path == "/updates":
            count = _query_count(query_string, "queries")
            rows = resolved_backend.update_worlds_sync(count)
            return _respond(start_response, encode_json_bytes(rows), "application/json")

        if path == "/cached-queries":
            count = _query_count(query_string, "count")
            rows = resolved_backend.get_cached_worlds_sync(count)
            return _respond(start_response, encode_json_bytes(rows), "application/json")

        if path == "/health":
            return _respond(start_response, b"ok", "text/plain")

        if path.startswith(_OBJECT_PREFIX):
            key = path.removeprefix(_OBJECT_PREFIX)
            if not key:
                return _respond(start_response, b"Not Found", "text/plain", "404 Not Found")
            if method == "GET":
                payload = resolved_backend.get_object_sync(key)
                if payload is None:
                    return _respond(start_response, b"Not Found", "text/plain", "404 Not Found")
                return _respond(start_response, payload, "application/octet-stream")
            if method == "PUT":
                body = _read_wsgi_body(environ)
                size = resolved_backend.put_object_sync(key, body)
                return _respond(start_response, encode_json_bytes({"key": key, "size": size}), "application/json")
            return _respond(start_response, b"Method Not Allowed", "text/plain", "405 Method Not Allowed")

        return _respond(start_response, b"Not Found", "text/plain", "404 Not Found")

    return _wsgi_data_app


def create_vertebra_wsgi_app(backend: DataBackend | None = None) -> App:
    """Create a Vertebra ``App`` mounting the WSGI data callable at ``/``.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        Configured ``App`` with a free-threaded startup check and root WSGI mount.
    """
    resolved_backend = get_default_backend() if backend is None else backend
    app = App()

    @app.on_startup
    def _verify_free_threaded_startup() -> None:
        """Enforce strict free-threading when configured on the backend."""
        assert_free_threaded_runtime(strict=resolved_backend.config.strict_free_threading)

    app.mount("/", create_wsgi_data_app(resolved_backend), protocol="wsgi")
    return app


wsgi_app: Callable[[dict[str, Any], Any], list[bytes]] = create_wsgi_data_app()
vertebra_wsgi_app: App = create_vertebra_wsgi_app()
