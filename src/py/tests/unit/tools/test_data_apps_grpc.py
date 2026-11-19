"""Unit tests for the Vertebra gRPC ASGI and native H2C benchmark apps."""

from __future__ import annotations

import asyncio
import sys
from typing import Any

import pytest
from tools.benchmarks.apps._data_backend import WORLD_ROW_COUNT, DataBackend, DataBackendConfig, WorldRow
from tools.benchmarks.apps.data_grpc import (
    GRPC_PACKAGE,
    GRPC_SERVICE,
    CountRequest,
    EmptyRequest,
    JsonMessageResponse,
    ObjectKeyRequest,
    ObjectPayloadResponse,
    PutObjectRequest,
    PutObjectResponse,
    WorldListResponse,
    create_grpc_asgi_app,
    create_grpc_native_app,
    create_grpc_registry,
    grpc_asgi_app,
    grpc_native_app,
    grpc_registry,
)

from vertebra._vertebra import Response
from vertebra.extensions.grpc import GrpcServiceRegistry, GrpcStatus, decode_lpm_frames, encode_lpm_frame
from vertebra.utils.serializers import decode_json, decode_msgpack, encode_json_bytes, encode_msgpack_bytes

EXPECTED_GRPC_PATHS: tuple[str, ...] = (
    f"/{GRPC_PACKAGE}.{GRPC_SERVICE}/GetJson",
    f"/{GRPC_PACKAGE}.{GRPC_SERVICE}/GetDb",
    f"/{GRPC_PACKAGE}.{GRPC_SERVICE}/GetQueries",
    f"/{GRPC_PACKAGE}.{GRPC_SERVICE}/UpdateWorlds",
    f"/{GRPC_PACKAGE}.{GRPC_SERVICE}/GetCachedQueries",
    f"/{GRPC_PACKAGE}.{GRPC_SERVICE}/GetObject",
    f"/{GRPC_PACKAGE}.{GRPC_SERVICE}/PutObject",
)

EXPECTED_NATIVE_PROCEDURES: tuple[str, ...] = (
    "get_json",
    "get_db",
    "get_queries",
    "update_worlds",
    "get_cached_queries",
    "get_object",
    "put_object",
)


class _GrpcStubRequest:
    """Minimal request stub for invoking native App gRPC procedure handlers."""

    def __init__(self, body: bytes, content_type: str = "application/grpc+json", request_id: int = 1) -> None:
        self.body = body
        self.headers = {"content-type": content_type}
        self.request_id = request_id


async def _invoke_asgi_grpc(
    registry: GrpcServiceRegistry,
    path: str,
    payload: Any,
    content_type: bytes = b"application/grpc+msgpack",
) -> tuple[dict[str, Any], bytes, dict[bytes, bytes]]:
    """Dispatch a unary LPM-framed gRPC request through ``GrpcServiceRegistry.handle_asgi``."""
    framed = encode_lpm_frame(encode_msgpack_bytes(payload))
    sent_messages: list[dict[str, Any]] = []
    delivered = False

    async def receive() -> dict[str, Any]:
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": framed, "more_body": False}
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent_messages.append(message)

    scope: dict[str, Any] = {
        "type": "http",
        "http_version": "2",
        "method": "POST",
        "path": path,
        "headers": [(b"content-type", content_type)],
    }
    await registry.handle_asgi(scope, receive, send)

    start_msg = next(msg for msg in sent_messages if msg["type"] == "http.response.start")
    body_bytes = b"".join(msg.get("body", b"") for msg in sent_messages if msg["type"] == "http.response.body")
    trailer_msg = next(msg for msg in sent_messages if msg["type"] == "http.response.trailers")
    trailers = dict(trailer_msg.get("headers", []))
    return start_msg, body_bytes, trailers


def _decode_single_lpm_msgpack(body: bytes, target_type: type[Any]) -> Any:
    """Decode a single LPM-framed MsgPack payload into ``target_type``."""
    frames = decode_lpm_frames(body)
    assert len(frames) == 1
    return decode_msgpack(frames[0], type=target_type)


def _decode_single_lpm_json(body: bytes, target_type: type[Any]) -> Any:
    """Decode a single LPM-framed JSON payload into ``target_type``."""
    frames = decode_lpm_frames(body)
    assert len(frames) == 1
    return decode_json(frames[0], type=target_type)


def _find_route_handler(app: Any, method: str, path: str) -> Any:
    """Locate a registered handler on a Vertebra ``App`` by HTTP method and path."""
    for route in app.routes:
        if route.method.upper() == method.upper() and route.path == path:
            return route.handler
    msg = f"Route {method} {path} not found on app"
    raise AssertionError(msg)


def test_grpc_registry_registers_all_seven_paths() -> None:
    """Verify ``create_grpc_registry`` registers all 7 expected BenchmarkService paths."""
    backend = DataBackend(DataBackendConfig())
    try:
        registry = create_grpc_registry(backend)
        assert set(registry.methods.keys()) == set(EXPECTED_GRPC_PATHS)
        assert set(grpc_registry.methods.keys()) == set(EXPECTED_GRPC_PATHS)
    finally:
        backend.close()


def test_grpc_asgi_app_mounts_registry_at_root() -> None:
    """Verify ``create_grpc_asgi_app`` mounts the ASGI gRPC dispatcher at root."""
    backend = DataBackend(DataBackendConfig())
    try:
        app = create_grpc_asgi_app(backend)
        assert len(app.mounts) == 1
        prefix, sub_app, protocol = app.mounts[0]
        assert prefix in {"", "/"}
        assert callable(sub_app)
        assert protocol == "asgi"
        assert len(grpc_asgi_app.mounts) == 1
    finally:
        backend.close()


def test_grpc_asgi_get_json_and_get_db() -> None:
    """Verify ``GetJson`` and ``GetDb`` emit HTTP/2 trailers and valid payloads."""
    backend = DataBackend(DataBackendConfig())
    try:
        registry = create_grpc_registry(backend)

        async def _exercise() -> None:
            start_json, body_json, trailers_json = await _invoke_asgi_grpc(
                registry,
                f"/{GRPC_PACKAGE}.{GRPC_SERVICE}/GetJson",
                EmptyRequest(),
            )
            assert start_json["status"] == 200
            assert start_json["trailers"] is True
            assert trailers_json[b"grpc-status"] == b"0"
            msg = _decode_single_lpm_msgpack(body_json, JsonMessageResponse)
            assert msg == JsonMessageResponse(message="Hello, World!")

            start_db, body_db, trailers_db = await _invoke_asgi_grpc(
                registry,
                f"/{GRPC_PACKAGE}.{GRPC_SERVICE}/GetDb",
                EmptyRequest(),
            )
            assert start_db["status"] == 200
            assert start_db["trailers"] is True
            assert trailers_db[b"grpc-status"] == b"0"
            row = _decode_single_lpm_msgpack(body_db, WorldRow)
            assert 1 <= row.id <= WORLD_ROW_COUNT
            assert 1 <= row.randomNumber <= WORLD_ROW_COUNT

        asyncio.run(_exercise())
    finally:
        backend.close()


@pytest.mark.parametrize(
    ("method_name", "requested_count", "expected_count"),
    [
        ("GetQueries", 5, 5),
        ("GetQueries", 0, 1),
        ("GetQueries", -7, 1),
        ("GetQueries", 999, 500),
        ("UpdateWorlds", 3, 3),
        ("UpdateWorlds", 0, 1),
        ("GetCachedQueries", 4, 4),
        ("GetCachedQueries", 600, 500),
    ],
)
def test_grpc_asgi_world_batch_methods_and_clamping(
    method_name: str,
    requested_count: int,
    expected_count: int,
) -> None:
    """Verify ``GetQueries``, ``UpdateWorlds``, and ``GetCachedQueries`` clamping and trailers."""
    backend = DataBackend(DataBackendConfig())
    try:
        registry = create_grpc_registry(backend)

        async def _exercise() -> None:
            start, body, trailers = await _invoke_asgi_grpc(
                registry,
                f"/{GRPC_PACKAGE}.{GRPC_SERVICE}/{method_name}",
                CountRequest(count=requested_count),
            )
            assert start["status"] == 200
            assert start["trailers"] is True
            assert trailers[b"grpc-status"] == b"0"
            resp = _decode_single_lpm_msgpack(body, WorldListResponse)
            assert len(resp.worlds) == expected_count
            assert all(1 <= row.id <= WORLD_ROW_COUNT for row in resp.worlds)

        asyncio.run(_exercise())
    finally:
        backend.close()


def test_grpc_asgi_put_and_get_object_found_and_missing() -> None:
    """Verify ``PutObject`` and ``GetObject`` handle stored and missing keys cleanly."""
    backend = DataBackend(DataBackendConfig())
    try:
        registry = create_grpc_registry(backend)

        async def _exercise() -> None:
            start_missing, body_missing, trailers_missing = await _invoke_asgi_grpc(
                registry,
                f"/{GRPC_PACKAGE}.{GRPC_SERVICE}/GetObject",
                ObjectKeyRequest(key="missing-key"),
            )
            assert start_missing["trailers"] is True
            assert trailers_missing[b"grpc-status"] == b"0"
            missing_resp = _decode_single_lpm_msgpack(body_missing, ObjectPayloadResponse)
            assert missing_resp == ObjectPayloadResponse(key="missing-key", payload=b"", found=False)

            sample = b"vertebra-grpc-payload"
            start_put, body_put, trailers_put = await _invoke_asgi_grpc(
                registry,
                f"/{GRPC_PACKAGE}.{GRPC_SERVICE}/PutObject",
                PutObjectRequest(key="bench-key", payload=sample),
            )
            assert start_put["trailers"] is True
            assert trailers_put[b"grpc-status"] == b"0"
            put_resp = _decode_single_lpm_msgpack(body_put, PutObjectResponse)
            assert put_resp == PutObjectResponse(key="bench-key", size=len(sample))

            start_get, body_get, trailers_get = await _invoke_asgi_grpc(
                registry,
                f"/{GRPC_PACKAGE}.{GRPC_SERVICE}/GetObject",
                ObjectKeyRequest(key="bench-key"),
            )
            assert start_get["trailers"] is True
            assert trailers_get[b"grpc-status"] == b"0"
            get_resp = _decode_single_lpm_msgpack(body_get, ObjectPayloadResponse)
            assert get_resp == ObjectPayloadResponse(key="bench-key", payload=sample, found=True)

        asyncio.run(_exercise())
    finally:
        backend.close()


def test_grpc_asgi_unknown_path_returns_unimplemented() -> None:
    """Verify unknown gRPC paths return ``GrpcStatus.UNIMPLEMENTED`` trailers cleanly."""
    backend = DataBackend(DataBackendConfig())
    try:
        registry = create_grpc_registry(backend)

        async def _exercise() -> None:
            start, body, trailers = await _invoke_asgi_grpc(
                registry,
                f"/{GRPC_PACKAGE}.{GRPC_SERVICE}/DoesNotExist",
                EmptyRequest(),
            )
            assert start["status"] == 200
            assert start["trailers"] is True
            assert body == b""
            assert trailers[b"grpc-status"] == str(int(GrpcStatus.UNIMPLEMENTED)).encode("ascii")

        asyncio.run(_exercise())
    finally:
        backend.close()


def test_grpc_native_app_mounts_all_procedure_routes() -> None:
    """Verify ``create_grpc_native_app`` mounts all 7 ``/vertebra.rpc.ProcedureService/*`` routes."""
    backend = DataBackend(DataBackendConfig())
    try:
        app = create_grpc_native_app(backend)
        expected_paths = {f"/vertebra.rpc.ProcedureService/{name}" for name in EXPECTED_NATIVE_PROCEDURES}
        mounted_grpc_paths = {
            route.path
            for route in app.routes
            if route.method.upper() == "POST" and route.path.startswith("/vertebra.rpc.ProcedureService/")
        }
        assert expected_paths.issubset(mounted_grpc_paths)

        module_grpc_paths = {
            route.path
            for route in grpc_native_app.routes
            if route.method.upper() == "POST" and route.path.startswith("/vertebra.rpc.ProcedureService/")
        }
        assert expected_paths.issubset(module_grpc_paths)
    finally:
        backend.close()


@pytest.mark.parametrize(
    ("proc_name", "request_payload", "response_type"),
    [
        ("get_json", EmptyRequest(), JsonMessageResponse),
        ("get_db", EmptyRequest(), WorldRow),
        ("get_queries", CountRequest(count=5), WorldListResponse),
        ("update_worlds", CountRequest(count=3), WorldListResponse),
        ("get_cached_queries", CountRequest(count=4), WorldListResponse),
    ],
)
def test_grpc_native_app_procedure_invocation_returns_framed_response(
    proc_name: str,
    request_payload: Any,
    response_type: type[Any],
) -> None:
    """Verify native ProcedureService routes return LPM-framed ``Response`` for ``application/grpc+json``."""
    backend = DataBackend(DataBackendConfig())
    try:
        app = create_grpc_native_app(backend)
        handler = _find_route_handler(app, "POST", f"/vertebra.rpc.ProcedureService/{proc_name}")
        framed_req = encode_lpm_frame(encode_json_bytes(request_payload))
        req = _GrpcStubRequest(body=framed_req, content_type="application/grpc+json")

        response = asyncio.run(handler(req))
        assert isinstance(response, Response)
        assert response.status == 200
        headers = dict(response.headers)
        assert headers[b"content-type"] == b"application/grpc+json"
        assert headers[b"grpc-status"] == b"0"

        decoded = _decode_single_lpm_json(bytes(response.body), response_type)
        if proc_name == "get_json":
            assert decoded == JsonMessageResponse(message="Hello, World!")
        elif proc_name == "get_db":
            assert 1 <= decoded.id <= WORLD_ROW_COUNT
        else:
            assert len(decoded.worlds) == request_payload.count
    finally:
        backend.close()


def test_grpc_native_app_object_procedures_and_count_clamping() -> None:
    """Verify native ``put_object``/``get_object`` and count clamping over ``application/grpc+json``."""
    backend = DataBackend(DataBackendConfig())
    try:
        app = create_grpc_native_app(backend)
        put_handler = _find_route_handler(app, "POST", "/vertebra.rpc.ProcedureService/put_object")
        get_handler = _find_route_handler(app, "POST", "/vertebra.rpc.ProcedureService/get_object")
        queries_handler = _find_route_handler(app, "POST", "/vertebra.rpc.ProcedureService/get_queries")

        async def _exercise() -> None:
            missing_req = _GrpcStubRequest(
                body=encode_lpm_frame(encode_json_bytes(ObjectKeyRequest(key="absent"))),
            )
            missing_resp = await get_handler(missing_req)
            assert missing_resp.status == 200
            assert _decode_single_lpm_json(bytes(missing_resp.body), ObjectPayloadResponse) == ObjectPayloadResponse(
                key="absent",
                payload=b"",
                found=False,
            )

            blob = b"native-h2c-object-payload"
            put_req = _GrpcStubRequest(
                body=encode_lpm_frame(encode_json_bytes(PutObjectRequest(key="item-1", payload=blob))),
            )
            put_resp = await put_handler(put_req)
            assert put_resp.status == 200
            assert _decode_single_lpm_json(bytes(put_resp.body), PutObjectResponse) == PutObjectResponse(
                key="item-1",
                size=len(blob),
            )

            found_req = _GrpcStubRequest(
                body=encode_lpm_frame(encode_json_bytes(ObjectKeyRequest(key="item-1"))),
            )
            found_resp = await get_handler(found_req)
            assert found_resp.status == 200
            assert _decode_single_lpm_json(bytes(found_resp.body), ObjectPayloadResponse) == ObjectPayloadResponse(
                key="item-1",
                payload=blob,
                found=True,
            )

            clamped_low_req = _GrpcStubRequest(
                body=encode_lpm_frame(encode_json_bytes(CountRequest(count=0))),
            )
            clamped_low_resp = await queries_handler(clamped_low_req)
            assert len(_decode_single_lpm_json(bytes(clamped_low_resp.body), WorldListResponse).worlds) == 1

            clamped_high_req = _GrpcStubRequest(
                body=encode_lpm_frame(encode_json_bytes(CountRequest(count=999))),
            )
            clamped_high_resp = await queries_handler(clamped_high_req)
            assert len(_decode_single_lpm_json(bytes(clamped_high_resp.body), WorldListResponse).worlds) == 500

        asyncio.run(_exercise())
    finally:
        backend.close()


def test_grpc_apps_enforce_strict_free_threading_on_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify both gRPC apps enforce strict free-threading during startup when configured."""
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: True, raising=False)
    strict_backend = DataBackend(DataBackendConfig(strict_free_threading=True))
    try:
        asgi_app = create_grpc_asgi_app(strict_backend)
        native_app = create_grpc_native_app(strict_backend)
        with pytest.raises(RuntimeError, match="GIL disabled"):
            asyncio.run(asgi_app.startup())
        with pytest.raises(RuntimeError, match="GIL disabled"):
            asyncio.run(native_app.startup())
    finally:
        strict_backend.close()
