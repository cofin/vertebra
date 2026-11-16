"""Unit tests for the Rust-backed AsgiScope and ASGI 3.0 wire extensions."""

from pathlib import Path
from typing import Any, cast

import pytest

from vertebra._vertebra import AsgiProtocol, AsgiScope, Request, drive_asgi_batch, init_transport
from vertebra.server._asgi._runner import FLAG_FINAL, FLAG_TRAILERS, AsgiRunner
from vertebra.server._asgi._wire import (
    PathSendResponse,
    TrailerResponse,
    WebSocketHttpResponse,
    ZeroCopySendResponse,
)


def test_asgi_scope_fixed_slots_and_extras() -> None:
    """Verify AsgiScope stores known ASGI keys in fixed slots and supports arbitrary extras."""
    scope = AsgiScope.from_parts(
        method="POST",
        path="/api/items",
        query_string="page=2",
        headers=[(b"content-type", b"application/json"), (b"x-custom", b"abc")],
        root_path="/api",
        http_version="2",
        scheme="https",
    )

    assert scope["type"] == "http"
    assert scope["asgi"] == {"version": "3.0", "spec_version": "2.4"}
    assert scope["http_version"] == "2"
    assert scope["method"] == "POST"
    assert scope["scheme"] == "https"
    assert scope["path"] == "/api/items"
    assert scope["raw_path"] == b"/api/items"
    assert scope["query_string"] == b"page=2"
    assert scope["root_path"] == "/api"
    assert scope["headers"] == [
        (b"content-type", b"application/json"),
        (b"x-custom", b"abc"),
    ]
    assert scope["client"] == ("127.0.0.1", 0)
    assert scope["server"] == ("127.0.0.1", 8000)
    assert scope["state"] == {}

    assert "method" in scope
    assert "nonexistent" not in scope
    assert scope.get("method") == "POST"
    assert scope.get("nonexistent", "fallback") == "fallback"

    scope["path_params"] = {"id": "42"}
    assert scope["path_params"] == {"id": "42"}

    scope["custom_middleware_flag"] = True
    assert "custom_middleware_flag" in scope
    assert scope["custom_middleware_flag"] is True

    popped = scope.pop("custom_middleware_flag")
    assert popped is True
    assert "custom_middleware_flag" not in scope

    copied = scope.copy()
    assert isinstance(copied, dict)
    assert copied["method"] == "POST"
    assert copied["path_params"] == {"id": "42"}


def test_asgi_scope_advertises_extensions() -> None:
    """Verify AsgiScope advertises pathsend, zerocopysend, trailers, and conditional tls."""
    req = Request.from_parts(
        "GET",
        "/v1/users",
        b"",
        request_id=7,
        handler_id=1,
        headers=[("Host", "example.com")],
    )
    scope = req.asgi_scope("/v1")
    assert scope["path"] == "/users"
    assert scope["root_path"] == "/v1"
    extensions = scope["extensions"]
    assert "http.response.pathsend" in extensions
    assert "http.response.zerocopysend" in extensions
    assert "http.response.trailers" in extensions
    assert "tls" not in extensions
    assert "http.response.early_hints" not in extensions
    assert "http.response.debug" not in extensions
    assert "http.response.push" not in extensions

    https_scope = AsgiScope.from_parts(method="GET", path="/secure", scheme="https")
    assert "tls" in https_scope["extensions"]


@pytest.mark.anyio
async def test_zero_copy_send_response_extension() -> None:
    """Verify ZeroCopySendResponse emits http.response.zerocopysend when supported."""
    payload = memoryview(b"zero-copy-binary-payload")
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(msg: dict[str, Any]) -> None:
        sent.append(msg)

    scope = AsgiScope.from_parts(method="GET", path="/blob")
    resp = ZeroCopySendResponse(payload, content_type=b"application/octet-stream")
    await resp(cast("Any", scope), receive, send)

    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["status"] == 200
    assert sent[1]["type"] == "http.response.zerocopysend"
    assert bytes(sent[1]["buffer"]) == b"zero-copy-binary-payload"
    assert sent[1]["more_body"] is False


@pytest.mark.anyio
async def test_path_send_response_extension(tmp_path: Path) -> None:
    """Verify PathSendResponse emits http.response.pathsend when supported."""
    sample = tmp_path / "static.txt"
    sample.write_bytes(b"hello-from-disk")
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(msg: dict[str, Any]) -> None:
        sent.append(msg)

    scope = AsgiScope.from_parts(method="GET", path="/static.txt")
    resp = PathSendResponse(sample, content_type=b"text/plain")
    await resp(cast("Any", scope), receive, send)

    assert sent[0]["type"] == "http.response.start"
    assert sent[1] == {"type": "http.response.pathsend", "path": str(sample)}


@pytest.mark.anyio
async def test_trailer_response_extension() -> None:
    """Verify TrailerResponse advertises trailers and sends http.response.trailers."""
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(msg: dict[str, Any]) -> None:
        sent.append(msg)

    scope = AsgiScope.from_parts(method="GET", path="/digest")
    resp = TrailerResponse(
        b"streamed-body",
        trailers=[(b"grpc-status", b"0"), (b"x-checksum", b"deadbeef")],
    )
    await resp(cast("Any", scope), receive, send)

    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["trailers"] is True
    assert sent[1]["type"] == "http.response.body"
    assert sent[2]["type"] == "http.response.trailers"
    assert sent[2]["headers"] == [(b"grpc-status", b"0"), (b"x-checksum", b"deadbeef")]


@pytest.mark.anyio
async def test_asgi_runner_lifespan_state_and_zerocopysend_integration() -> None:
    """Verify AsgiRunner injects lifespan state copy and collects zerocopysend bodies."""
    lifespan_state = {"db_pool": "active", "counter": 1}

    async def app(scope: Any, receive: Any, send: Any) -> None:
        assert scope["state"]["db_pool"] == "active"
        scope["state"]["counter"] = 99
        resp = ZeroCopySendResponse(b"zero-copy-payload", status=201)
        await resp(scope, receive, send)

    runner = AsgiRunner(app, lifespan_state=lifespan_state)
    try:
        req = Request.from_parts("GET", "/test", b"", request_id=42, handler_id=1)
        result = await runner.run_single(req)

        assert result.status == 201
        assert result.body == b"zero-copy-payload"
        assert lifespan_state["counter"] == 1
    finally:
        runner.close()


@pytest.mark.anyio
async def test_asgi_trailers_forwarded_to_native_response() -> None:
    """Verify AsgiRunner encodes and forwards trailer frames with FLAG_TRAILERS via submit_raw_response."""

    async def app(scope: Any, receive: Any, send: Any) -> None:
        resp = TrailerResponse(
            b"payload-with-trailers",
            trailers=[(b"grpc-status", b"0"), (b"x-digest", b"sha256:abcd")],
        )
        await resp(scope, receive, send)

    runner = AsgiRunner(app)
    try:
        req = Request.from_parts("POST", "/rpc.Service/Call", b"", request_id=88, handler_id=1)
        result = await runner.run_single(req)

        assert result.status == 200
        assert result.trailers == [(b"grpc-status", b"0"), (b"x-digest", b"sha256:abcd")]
        assert len(runner.last_trailer_frames) == 1
        raw_frame = runner.last_trailer_frames[0]
        flags_in_frame = int.from_bytes(raw_frame[10:12], "little")
        assert flags_in_frame & FLAG_TRAILERS == FLAG_TRAILERS
        assert flags_in_frame & FLAG_FINAL == FLAG_FINAL
        assert b"grpc-status" in raw_frame
        assert b"sha256:abcd" in raw_frame
    finally:
        runner.close()


@pytest.mark.anyio
async def test_asgi_zerocopysend_retains_buffer_without_bytes_copy() -> None:
    """Verify http.response.zerocopysend retains the memoryview buffer and passes it to Response.from_buffer."""
    backing = bytearray(b"zero-copy-shared-memory-view")
    source_view = memoryview(backing)

    async def app(scope: Any, receive: Any, send: Any) -> None:
        resp = ZeroCopySendResponse(source_view, status=200)
        await resp(scope, receive, send)

    runner = AsgiRunner(app)
    try:
        req = Request.from_parts("GET", "/zero-copy", b"", request_id=55, handler_id=1)
        asgi_resp = await runner.run_single(req)

        assert asgi_resp.zero_copy_buffer is not None
        assert isinstance(asgi_resp.body, memoryview)
        assert asgi_resp.zero_copy_buffer.obj is backing

        native_resp = runner._to_native_response(asgi_resp, 55)
        assert native_resp.status == 200
        assert native_resp.body == b"zero-copy-shared-memory-view"
        assert native_resp.request_id == 55
    finally:
        runner.close()


@pytest.mark.anyio
async def test_websocket_http_response_rejection_helper() -> None:
    """Verify WebSocketHttpResponse sends websocket.http.response.start and body."""
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "websocket.connect"}

    async def send(msg: dict[str, Any]) -> None:
        sent.append(msg)

    scope: dict[str, Any] = {
        "type": "websocket",
        "path": "/ws",
        "extensions": {"websocket.http.response": {}},
    }
    rejection = WebSocketHttpResponse(b"Forbidden", status=403)
    await rejection(scope, receive, send)

    assert sent[0]["type"] == "websocket.http.response.start"
    assert sent[0]["status"] == 403
    assert sent[1]["type"] == "websocket.http.response.body"
    assert sent[1]["body"] == b"Forbidden"


@pytest.mark.anyio
async def test_asgi_protocol_zerocopysend_pathsend_and_lifespan_batch(tmp_path: Path) -> None:
    """Verify AsgiProtocol handles zerocopysend, pathsend, root_path, and isolated lifespan_state."""
    init_transport()
    asset = tmp_path / "hello.txt"
    asset.write_bytes(b"disk-bytes-payload")

    req_zc = Request.from_parts("POST", "/sub/echo", b"zero-copy-view", request_id=701)
    proto_zc = AsgiProtocol(b"", request_id=701)
    await proto_zc.send({"type": "http.response.start", "status": 200, "headers": []})
    await proto_zc.send(
        {
            "type": "http.response.zerocopysend",
            "buffer": req_zc.body_view,
            "more_body": False,
        }
    )
    resp_zc = proto_zc.take_response()
    assert resp_zc.body == b"zero-copy-view"

    proto_path = AsgiProtocol(b"", request_id=702)
    await proto_path.send({"type": "http.response.start", "status": 200, "headers": []})
    await proto_path.send({"type": "http.response.pathsend", "path": str(asset)})
    resp_path = proto_path.take_response()
    assert resp_path.body == b"disk-bytes-payload"

    seen_paths_and_states: list[tuple[str, str, int]] = []
    shared_state = {"count": 10}

    async def mounted_app(scope: Any, receive: Any, send: Any) -> None:
        await receive()
        scope["state"]["count"] += 1
        seen_paths_and_states.append((scope["path"], scope["root_path"], scope["state"]["count"]))
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"done"})

    inline_done = drive_asgi_batch(
        mounted_app,
        [
            Request.from_parts("GET", "/v2/items", b"", request_id=703),
            Request.from_parts("GET", "/v2", b"", request_id=704),
        ],
        root_path="/v2",
        lifespan_state=shared_state,
    )
    assert inline_done == 2
    assert shared_state["count"] == 10
    assert seen_paths_and_states == [("/items", "/v2", 11), ("/", "/v2", 11)]
