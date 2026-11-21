"""Vertebra Native data, cache, and object-storage benchmark application.

Exposes standardized TechEmpower and object-storage endpoints over Vertebra's
native ``App`` router backed by the shared ``DataBackend``.
"""

from __future__ import annotations

from tools.benchmarks.apps._data_backend import (
    JSON_HELLO_PAYLOAD,
    DataBackend,
    assert_free_threaded_runtime,
    clamp_query_count,
    get_default_backend,
)
from vertebra._vertebra import Response
from vertebra.framework._app import App
from vertebra.utils.serializers import encode_json_bytes

__all__ = ("create_native_data_app", "native_data_app")

_JSON_HEADERS: list[tuple[str, str]] = [("content-type", "application/json")]
_TEXT_HEADERS: list[tuple[str, str]] = [("content-type", "text/plain")]
_BINARY_HEADERS: list[tuple[str, str]] = [("content-type", "application/octet-stream")]


def create_native_data_app(backend: DataBackend | None = None) -> App:
    """Create a Vertebra native ``App`` wired to a pluggable ``DataBackend``.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        Configured ``App`` with TechEmpower data, cache, object-storage, and
        health endpoints plus a free-threading startup check.
    """
    resolved_backend = get_default_backend() if backend is None else backend
    app = App()

    @app.on_startup
    def _verify_free_threaded_startup() -> None:
        """Enforce strict free-threading when configured on the backend."""
        assert_free_threaded_runtime(strict=resolved_backend.config.strict_free_threading)

    @app.get("/json")
    def json_endpoint() -> Response:
        """Return the TechEmpower JSON hello payload."""
        return Response(200, JSON_HELLO_PAYLOAD, _JSON_HEADERS)

    @app.get("/db")
    async def db_endpoint() -> Response:
        """Return a single randomly selected World row as JSON."""
        row = await resolved_backend.get_world()
        return Response(200, encode_json_bytes(row), _JSON_HEADERS)

    @app.get("/queries")
    async def queries_endpoint(queries: str | None = None) -> Response:
        """Return ``queries`` randomly selected World rows as JSON."""
        count = clamp_query_count(queries)
        rows = await resolved_backend.get_worlds(count)
        return Response(200, encode_json_bytes(rows), _JSON_HEADERS)

    @app.get("/updates")
    async def updates_endpoint(queries: str | None = None) -> Response:
        """Read and update ``queries`` random World rows and return them as JSON."""
        count = clamp_query_count(queries)
        rows = await resolved_backend.update_worlds(count)
        return Response(200, encode_json_bytes(rows), _JSON_HEADERS)

    @app.get("/cached-queries")
    async def cached_queries_endpoint(count: str | None = None) -> Response:
        """Return ``count`` cached World rows as JSON."""
        resolved_count = clamp_query_count(count)
        rows = await resolved_backend.get_cached_worlds(resolved_count)
        return Response(200, encode_json_bytes(rows), _JSON_HEADERS)

    @app.get("/object/{key}")
    async def get_object_endpoint(key: str) -> Response:
        """Fetch an object payload by key or return HTTP 404 when absent."""
        payload = await resolved_backend.get_object(key)
        if payload is None:
            return Response(404, b"Not Found", _TEXT_HEADERS)
        return Response(200, payload, _BINARY_HEADERS)

    @app.put("/object/{key}")
    async def put_object_endpoint(key: str, body: bytes = b"") -> Response:
        """Store an object payload by key and return its persisted byte length."""
        size = await resolved_backend.put_object(key, body)
        return Response(200, encode_json_bytes({"key": key, "size": size}), _JSON_HEADERS)

    @app.get("/health")
    def health_endpoint() -> Response:
        """Return a lightweight readiness payload."""
        return Response(200, b"ok", _TEXT_HEADERS)

    return app


native_data_app: App = create_native_data_app()
