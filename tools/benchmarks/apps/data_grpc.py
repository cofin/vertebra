"""Vertebra gRPC ASGI and native H2C unary benchmark applications.

Exposes both gRPC benchmark surfaces backed by the shared ``DataBackend``:
- ``grpc_asgi_app``: ``GrpcServiceRegistry`` ASGI dispatcher emitting full
  HTTP/2 trailers (``trailers: True``, ``grpc-status: 0``), mounted at ``/``
  with ``protocol="asgi"``.
- ``grpc_native_app``: Native unary ``Procedure`` registrations on ``App``
  mounted at ``/vertebra.rpc.ProcedureService/<name>`` for ``VERTEBRA_H2C=1``
  benchmarking.
"""

from __future__ import annotations

from typing import Final

import msgspec

from tools.benchmarks.apps._data_backend import (
    DataBackend,
    WorldRow,
    assert_free_threaded_runtime,
    clamp_query_count,
    get_default_backend,
)
from vertebra.extensions.grpc import GrpcServiceRegistry, grpc_method
from vertebra.framework._app import App
from vertebra.framework.procedures import Procedure

__all__ = (
    "GRPC_PACKAGE",
    "GRPC_SERVICE",
    "CountRequest",
    "EmptyRequest",
    "JsonMessageResponse",
    "ObjectKeyRequest",
    "ObjectPayloadResponse",
    "PutObjectRequest",
    "PutObjectResponse",
    "WorldListResponse",
    "create_grpc_asgi_app",
    "create_grpc_native_app",
    "create_grpc_registry",
    "grpc_asgi_app",
    "grpc_native_app",
    "grpc_registry",
)

GRPC_PACKAGE: Final[str] = "vertebra.bench"
GRPC_SERVICE: Final[str] = "BenchmarkService"


class EmptyRequest(msgspec.Struct, frozen=True, gc=False):
    """Empty unary gRPC request message."""


class CountRequest(msgspec.Struct, frozen=True, gc=False):
    """Unary gRPC request specifying a TechEmpower query or update count."""

    count: int = 1


class ObjectKeyRequest(msgspec.Struct, frozen=True, gc=False):
    """Unary gRPC request identifying an object storage key."""

    key: str


class PutObjectRequest(msgspec.Struct, frozen=True, gc=False):
    """Unary gRPC request carrying an object key and binary payload."""

    key: str
    payload: bytes


class JsonMessageResponse(msgspec.Struct, frozen=True, gc=False):
    """Unary gRPC response carrying the TechEmpower JSON greeting message."""

    message: str


class WorldListResponse(msgspec.Struct, frozen=True, gc=False):
    """Unary gRPC response wrapping a list of TechEmpower World rows."""

    worlds: list[WorldRow]


class ObjectPayloadResponse(msgspec.Struct, frozen=True, gc=False):
    """Unary gRPC response returning an object storage payload and presence flag."""

    key: str
    payload: bytes
    found: bool


class PutObjectResponse(msgspec.Struct, frozen=True, gc=False):
    """Unary gRPC response confirming a stored object key and byte length."""

    key: str
    size: int


_EMPTY_REQUEST: Final[EmptyRequest] = EmptyRequest()
_DEFAULT_COUNT_REQUEST: Final[CountRequest] = CountRequest()
_HELLO_RESPONSE: Final[JsonMessageResponse] = JsonMessageResponse(message="Hello, World!")


def create_grpc_registry(backend: DataBackend | None = None) -> GrpcServiceRegistry:
    """Create a ``GrpcServiceRegistry`` populated with all 7 benchmark unary RPCs.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        Configured ``GrpcServiceRegistry`` serving
        ``/vertebra.bench.BenchmarkService/*``.
    """
    resolved_backend = get_default_backend() if backend is None else backend
    registry = GrpcServiceRegistry()

    @grpc_method(service=GRPC_SERVICE, package=GRPC_PACKAGE, name="GetJson")
    async def get_json(req: EmptyRequest) -> JsonMessageResponse:
        """Return the TechEmpower JSON greeting message."""
        _ = req
        return _HELLO_RESPONSE

    @grpc_method(service=GRPC_SERVICE, package=GRPC_PACKAGE, name="GetDb")
    async def get_db(req: EmptyRequest) -> WorldRow:
        """Return a single randomly selected World row."""
        _ = req
        return await resolved_backend.get_world()

    @grpc_method(service=GRPC_SERVICE, package=GRPC_PACKAGE, name="GetQueries")
    async def get_queries(req: CountRequest) -> WorldListResponse:
        """Return a clamped batch of randomly selected World rows."""
        count = clamp_query_count(req.count)
        rows = await resolved_backend.get_worlds(count)
        return WorldListResponse(worlds=rows)

    @grpc_method(service=GRPC_SERVICE, package=GRPC_PACKAGE, name="UpdateWorlds")
    async def update_worlds(req: CountRequest) -> WorldListResponse:
        """Read and update a clamped batch of random World rows."""
        count = clamp_query_count(req.count)
        rows = await resolved_backend.update_worlds(count)
        return WorldListResponse(worlds=rows)

    @grpc_method(service=GRPC_SERVICE, package=GRPC_PACKAGE, name="GetCachedQueries")
    async def get_cached_queries(req: CountRequest) -> WorldListResponse:
        """Return a clamped batch of cached World rows."""
        count = clamp_query_count(req.count)
        rows = await resolved_backend.get_cached_worlds(count)
        return WorldListResponse(worlds=rows)

    @grpc_method(service=GRPC_SERVICE, package=GRPC_PACKAGE, name="GetObject")
    async def get_object(req: ObjectKeyRequest) -> ObjectPayloadResponse:
        """Fetch an object payload by key, reporting ``found=False`` when absent."""
        payload = await resolved_backend.get_object(req.key)
        if payload is None:
            return ObjectPayloadResponse(key=req.key, payload=b"", found=False)
        return ObjectPayloadResponse(key=req.key, payload=payload, found=True)

    @grpc_method(service=GRPC_SERVICE, package=GRPC_PACKAGE, name="PutObject")
    async def put_object(req: PutObjectRequest) -> PutObjectResponse:
        """Store an object payload by key and return its persisted byte length."""
        size = await resolved_backend.put_object(req.key, req.payload)
        return PutObjectResponse(key=req.key, size=size)

    for handler in (
        get_json,
        get_db,
        get_queries,
        update_worlds,
        get_cached_queries,
        get_object,
        put_object,
    ):
        registry.register(handler)

    return registry


def create_grpc_asgi_app(backend: DataBackend | None = None) -> App:
    """Create a Vertebra ``App`` mounting the gRPC ASGI dispatcher at ``/``.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        Configured ``App`` with ``GrpcServiceRegistry.handle_asgi`` mounted via
        ``protocol="asgi"`` and the free-threading startup check attached.
    """
    resolved_backend = get_default_backend() if backend is None else backend
    registry = create_grpc_registry(resolved_backend)
    app = App()

    @app.on_startup
    def _verify_free_threaded_startup() -> None:
        """Enforce strict free-threading when configured on the backend."""
        assert_free_threaded_runtime(strict=resolved_backend.config.strict_free_threading)

    app.mount("/", registry.handle_asgi, protocol="asgi")
    return app


def create_grpc_native_app(backend: DataBackend | None = None) -> App:
    """Create a Vertebra ``App`` exposing native unary ``Procedure`` gRPC routes.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        Configured ``App`` with ``/vertebra.rpc.ProcedureService/<name>`` POST
        routes for ``VERTEBRA_H2C=1`` benchmarking and the free-threading
        startup check attached.
    """
    resolved_backend = get_default_backend() if backend is None else backend
    app = App()

    @app.on_startup
    def _verify_free_threaded_startup() -> None:
        """Enforce strict free-threading when configured on the backend."""
        assert_free_threaded_runtime(strict=resolved_backend.config.strict_free_threading)

    async def get_json(payload: EmptyRequest = _EMPTY_REQUEST) -> JsonMessageResponse:
        """Return the TechEmpower JSON greeting message."""
        _ = payload
        return _HELLO_RESPONSE

    async def get_db(payload: EmptyRequest = _EMPTY_REQUEST) -> WorldRow:
        """Return a single randomly selected World row."""
        _ = payload
        return await resolved_backend.get_world()

    async def get_queries(payload: CountRequest = _DEFAULT_COUNT_REQUEST) -> WorldListResponse:
        """Return a clamped batch of randomly selected World rows."""
        count = clamp_query_count(payload.count)
        rows = await resolved_backend.get_worlds(count)
        return WorldListResponse(worlds=rows)

    async def update_worlds(payload: CountRequest = _DEFAULT_COUNT_REQUEST) -> WorldListResponse:
        """Read and update a clamped batch of random World rows."""
        count = clamp_query_count(payload.count)
        rows = await resolved_backend.update_worlds(count)
        return WorldListResponse(worlds=rows)

    async def get_cached_queries(payload: CountRequest = _DEFAULT_COUNT_REQUEST) -> WorldListResponse:
        """Return a clamped batch of cached World rows."""
        count = clamp_query_count(payload.count)
        rows = await resolved_backend.get_cached_worlds(count)
        return WorldListResponse(worlds=rows)

    async def get_object(payload: ObjectKeyRequest) -> ObjectPayloadResponse:
        """Fetch an object payload by key, reporting ``found=False`` when absent."""
        data = await resolved_backend.get_object(payload.key)
        if data is None:
            return ObjectPayloadResponse(key=payload.key, payload=b"", found=False)
        return ObjectPayloadResponse(key=payload.key, payload=data, found=True)

    async def put_object(payload: PutObjectRequest) -> PutObjectResponse:
        """Store an object payload by key and return its persisted byte length."""
        size = await resolved_backend.put_object(payload.key, payload.payload)
        return PutObjectResponse(key=payload.key, size=size)

    procedures = (
        Procedure(
            name="get_json",
            kind="query",
            fn=get_json,
            input_type=EmptyRequest,
            output_type=JsonMessageResponse,
        ),
        Procedure(
            name="get_db",
            kind="query",
            fn=get_db,
            input_type=EmptyRequest,
            output_type=WorldRow,
        ),
        Procedure(
            name="get_queries",
            kind="query",
            fn=get_queries,
            input_type=CountRequest,
            output_type=WorldListResponse,
        ),
        Procedure(
            name="update_worlds",
            kind="mutation",
            fn=update_worlds,
            input_type=CountRequest,
            output_type=WorldListResponse,
        ),
        Procedure(
            name="get_cached_queries",
            kind="query",
            fn=get_cached_queries,
            input_type=CountRequest,
            output_type=WorldListResponse,
        ),
        Procedure(
            name="get_object",
            kind="query",
            fn=get_object,
            input_type=ObjectKeyRequest,
            output_type=ObjectPayloadResponse,
        ),
        Procedure(
            name="put_object",
            kind="mutation",
            fn=put_object,
            input_type=PutObjectRequest,
            output_type=PutObjectResponse,
        ),
    )

    for procedure in procedures:
        app.register(procedure)

    return app


grpc_registry: GrpcServiceRegistry = create_grpc_registry()
grpc_asgi_app: App = create_grpc_asgi_app()
grpc_native_app: App = create_grpc_native_app()
