"""RSGI v1.6 TechEmpower data, cache, and object-storage benchmark application.

Exports both a raw RSGI v1.6 callable for Granian baselines and a Vertebra
``App`` mounting the RSGI callable at ``/``.
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
    "create_rsgi_data_app",
    "create_vertebra_rsgi_app",
    "rsgi_app",
    "vertebra_rsgi_app",
)

_JSON_HEADERS: list[tuple[str, str]] = [("content-type", "application/json")]
_TEXT_HEADERS: list[tuple[str, str]] = [("content-type", "text/plain")]
_BINARY_HEADERS: list[tuple[str, str]] = [("content-type", "application/octet-stream")]
_OBJECT_PREFIX = "/object/"


def _query_count(query_string: str, key: str) -> int:
    """Extract and clamp a TechEmpower count parameter from a query string."""
    if not query_string:
        return 1
    values = parse_qs(query_string).get(key)
    return clamp_query_count(values[0] if values else None)


def create_rsgi_data_app(backend: DataBackend | None = None) -> Callable[[Any, Any], Awaitable[None]]:
    """Create an RSGI v1.6 async callable backed by ``DataBackend``.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        RSGI v1.6 async callable handling HTTP requests for TechEmpower data,
        cache, object-storage, and health routes.
    """
    resolved_backend = get_default_backend() if backend is None else backend

    async def _rsgi_data_app(scope: Any, protocol: Any) -> None:
        """Dispatch RSGI HTTP requests to ``DataBackend`` operations."""
        if getattr(scope, "proto", "http") != "http":
            return

        path = str(scope.path)
        method = str(getattr(scope, "method", "GET")).upper()
        query_string = str(getattr(scope, "query_string", "") or "")

        if path == "/json":
            protocol.response_bytes(200, _JSON_HEADERS, JSON_HELLO_PAYLOAD)
            return

        if path == "/db":
            row = await resolved_backend.get_world()
            protocol.response_bytes(200, _JSON_HEADERS, encode_json_bytes(row))
            return

        if path == "/queries":
            count = _query_count(query_string, "queries")
            rows = await resolved_backend.get_worlds(count)
            protocol.response_bytes(200, _JSON_HEADERS, encode_json_bytes(rows))
            return

        if path == "/updates":
            count = _query_count(query_string, "queries")
            rows = await resolved_backend.update_worlds(count)
            protocol.response_bytes(200, _JSON_HEADERS, encode_json_bytes(rows))
            return

        if path == "/cached-queries":
            count = _query_count(query_string, "count")
            rows = await resolved_backend.get_cached_worlds(count)
            protocol.response_bytes(200, _JSON_HEADERS, encode_json_bytes(rows))
            return

        if path == "/health":
            protocol.response_bytes(200, _TEXT_HEADERS, b"ok")
            return

        if path.startswith(_OBJECT_PREFIX):
            key = path.removeprefix(_OBJECT_PREFIX)
            if not key:
                protocol.response_bytes(404, _TEXT_HEADERS, b"Not Found")
                return
            if method == "GET":
                payload = await resolved_backend.get_object(key)
                if payload is None:
                    protocol.response_bytes(404, _TEXT_HEADERS, b"Not Found")
                    return
                protocol.response_bytes(200, _BINARY_HEADERS, payload)
                return
            if method == "PUT":
                body = bytes(await protocol())
                size = await resolved_backend.put_object(key, body)
                protocol.response_bytes(200, _JSON_HEADERS, encode_json_bytes({"key": key, "size": size}))
                return
            protocol.response_bytes(405, _TEXT_HEADERS, b"Method Not Allowed")
            return

        protocol.response_bytes(404, _TEXT_HEADERS, b"Not Found")

    return _rsgi_data_app


def create_vertebra_rsgi_app(backend: DataBackend | None = None) -> App:
    """Create a Vertebra ``App`` mounting the RSGI data callable at ``/``.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        Configured ``App`` with a free-threaded startup check and root RSGI mount.
    """
    resolved_backend = get_default_backend() if backend is None else backend
    app = App()

    @app.on_startup
    def _verify_free_threaded_startup() -> None:
        """Enforce strict free-threading when configured on the backend."""
        assert_free_threaded_runtime(strict=resolved_backend.config.strict_free_threading)

    app.mount("/", create_rsgi_data_app(resolved_backend), protocol="rsgi")
    return app


rsgi_app: Callable[[Any, Any], Awaitable[None]] = create_rsgi_data_app()
vertebra_rsgi_app: App = create_vertebra_rsgi_app()
