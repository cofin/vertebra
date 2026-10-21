"""Unit tests for 20-slot AsgiScope, zero-double-alloc setdefault, and ASGI 3.0 wire extensions."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from vertebra._vertebra import AsgiScope, stream_file_chunks
from vertebra.server._asgi._adapter import AsgiAdapter
from vertebra.server._asgi._extensions import (
    DebugResponse,
    EarlyHintResponse,
    PathSendResponse,
    TrailerResponse,
    WebSocketHttpResponse,
    ZeroCopySendResponse,
)
from vertebra.server._asgi._queues import AsgiSend


def test_asgi_scope_20_known_keys_and_zero_extras() -> None:
    """Verify AsgiScope stores all 14 standard and 6 Litestar v2/v3 keys in inline slots."""
    default_scope = AsgiScope()
    assert len(default_scope.known_keys()) == 20

    scope = AsgiScope.from_parts(
        method="GET",
        path="/users/42",
        query_string="active=true",
        headers=[(b"host", b"localhost:8000")],
        root_path="",
    )
    assert len(scope) == 14
    assert scope["type"] == "http"
    assert scope["method"] == "GET"
    assert scope["path"] == "/users/42"
    assert scope["raw_path"] == b"/users/42"
    assert scope["query_string"] == b"active=true"
    assert "app" not in scope
    assert "connection" not in scope

    hot_keys = (
        ("app", "app_instance"),
        ("litestar_app", "ls_instance"),
        ("path_params", {"user_id": "42"}),
        ("route_handler", "get_user_handler"),
        ("path_template", "/users/{user_id:str}"),
        ("connection", "reusable_request_conn"),
    )
    for key, val in hot_keys:
        scope[key] = val
        assert key in scope
        assert scope[key] == val

    assert len(scope) == 20
    assert scope.has_extras() is False


def test_asgi_scope_setdefault_state_zero_double_allocation() -> None:
    """Verify scope.setdefault('state', default_dict) reuses the caller's dict when state_cache is empty."""
    scope = AsgiScope.from_parts(method="GET", path="/state")
    caller_state: dict[str, Any] = {"db_pool": "ready"}
    returned = scope.setdefault("state", caller_state)
    assert returned is caller_state
    assert scope["state"] is caller_state

    second_candidate: dict[str, Any] = {"db_pool": "overwritten"}
    cached = scope.setdefault("state", second_candidate)
    assert cached is caller_state
    assert cached["db_pool"] == "ready"


def test_asgi_scope_advertised_extensions() -> None:
    """Verify scope['extensions'] advertises all supported ASGI 3.0 wire extensions."""
    scope = AsgiScope.from_parts(method="GET", path="/ext")
    extensions = scope["extensions"]
    expected = {
        "http.response.pathsend",
        "http.response.zerocopysend",
        "http.response.trailers",
        "http.response.early_hint",
        "http.response.debug",
        "http.response.sse",
        "websocket.http.response",
    }
    assert expected.issubset(set(extensions.keys()))


def test_asgi_send_progressive_streaming_and_unary_fastpath() -> None:
    """Verify AsgiSend flushes immediately on more_body=True and stays unary on single-chunk more_body=False."""
    flushed_frames: list[tuple[int, bytes, bool]] = []

    def on_chunk(status: int, chunk: bytes, final: bool) -> None:
        flushed_frames.append((status, chunk, final))

    loop = asyncio.new_event_loop()
    try:
        streaming_sender = AsgiSend(on_chunk=on_chunk)
        loop.run_until_complete(streaming_sender({"type": "http.response.start", "status": 200, "headers": []}))
        loop.run_until_complete(streaming_sender({"type": "http.response.body", "body": b"part-1:", "more_body": True}))
        assert len(flushed_frames) == 1
        assert flushed_frames[0] == (200, b"part-1:", False)

        loop.run_until_complete(streaming_sender({"type": "http.response.body", "body": b"part-2", "more_body": False}))
        assert len(flushed_frames) == 2
        assert flushed_frames[1] == (200, b"part-2", True)
        assert streaming_sender.response.body == b"part-1:part-2"
        assert streaming_sender.response.streaming is True

        flushed_frames.clear()
        unary_sender = AsgiSend(on_chunk=on_chunk)
        loop.run_until_complete(unary_sender({"type": "http.response.start", "status": 201, "headers": []}))
        loop.run_until_complete(unary_sender({"type": "http.response.body", "body": b"single", "more_body": False}))
        assert flushed_frames == []
        assert unary_sender.response.body == b"single"
        assert unary_sender.response.streaming is False
    finally:
        loop.close()


def test_asgi_wire_extensions_and_lifespan_state_isolation(tmp_path: Path) -> None:
    """Verify zerocopysend, pathsend, trailers, early_hint, debug, websocket.http.response, and lifespan state."""
    asset = tmp_path / "data.bin"
    asset.write_bytes(b"0123456789abcdef")

    chunks = stream_file_chunks(str(asset), 4, 12, 4)
    assert b"".join(chunks) == b"456789ab"

    lifespan_state: dict[str, Any] = {"shared_key": "initial"}

    async def app(scope: Any, receive: Any, send: Any) -> None:
        _ = receive
        path = scope["path"]
        scope["state"]["per_request"] = path
        if path == "/zerocopy":
            view = memoryview(b"zero-copy-payload")
            await ZeroCopySendResponse(view, status=200, headers=[(b"x-zc", b"1")]).send(send)
        elif path == "/pathsend":
            await PathSendResponse(str(asset), status=200, start=2, end=10).send(send)
        elif path == "/trailers":
            await EarlyHintResponse([b"</app.js>; rel=preload; as=script"]).send(send)
            await DebugResponse({"route": "/trailers"}).send(send)
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"trailer", b"grpc-status")],
                    "trailers": True,
                }
            )
            await send({"type": "http.response.body", "body": b"payload", "more_body": False})
            await TrailerResponse([(b"grpc-status", b"0")]).send(send)
        elif path == "/ws-reject":
            await WebSocketHttpResponse(status=403, body=b"Forbidden", headers=[(b"x-ws", b"denied")]).send(send)

    adapter = AsgiAdapter(app, lifespan_state=lifespan_state)
    loop = asyncio.new_event_loop()
    try:
        zc_resp = loop.run_until_complete(adapter.handle_path("/zerocopy"))
        assert zc_resp.status == 200
        assert zc_resp.body == b"zero-copy-payload"
        assert zc_resp.zero_copy_view is not None
        assert "per_request" not in lifespan_state

        ps_resp = loop.run_until_complete(adapter.handle_path("/pathsend"))
        assert ps_resp.status == 200
        assert ps_resp.body == b"23456789"
        assert ps_resp.file_path == str(asset)
        assert ps_resp.file_range == (2, 10)

        tr_resp = loop.run_until_complete(adapter.handle_path("/trailers"))
        assert tr_resp.status == 200
        assert tr_resp.body == b"payload"
        assert tr_resp.trailers == [(b"grpc-status", b"0")]
        assert tr_resp.early_hints == [[(b"link", b"</app.js>; rel=preload; as=script")]]
        assert tr_resp.debug_info == {"route": "/trailers"}

        ws_resp = loop.run_until_complete(adapter.handle_path("/ws-reject"))
        assert ws_resp.status == 403
        assert ws_resp.body == b"Forbidden"
    finally:
        loop.close()
