"""Tests for DevMiddleware WebSocket broadcast."""

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from vertebra.framework import App, WelcomePageRenderer
from vertebra.framework.dev import DevMiddleware, TypeBridge, _ws_clients, broadcast_hmr_event
from vertebra.utils.serializers import decode_json, encode_json


@pytest.fixture()
def type_bridge(tmp_path: Path) -> TypeBridge:
    (tmp_path / "types.d.ts").write_text("export type Foo = string;")
    return TypeBridge(tmp_path)


@pytest.fixture()
def middleware(type_bridge: TypeBridge) -> DevMiddleware:
    app = AsyncMock()
    return DevMiddleware(app, type_bridge)


def test_broadcast_hmr_event_to_empty_clients() -> None:
    _ws_clients.clear()
    broadcast_hmr_event({"type": "test"})


def test_broadcast_hmr_event_delivers_to_queue() -> None:
    _ws_clients.clear()
    q: asyncio.Queue[dict] = asyncio.Queue()
    _ws_clients.add(q)
    try:
        broadcast_hmr_event({"type": "update", "path": "/a"})
        assert not q.empty()
        event = q.get_nowait()
        assert event["type"] == "update"
    finally:
        _ws_clients.discard(q)


def test_broadcast_hmr_event_to_multiple_clients() -> None:
    _ws_clients.clear()
    q1: asyncio.Queue[dict] = asyncio.Queue()
    q2: asyncio.Queue[dict] = asyncio.Queue()
    _ws_clients.add(q1)
    _ws_clients.add(q2)
    try:
        broadcast_hmr_event({"type": "full-reload", "reason": "test"})
        assert q1.get_nowait()["type"] == "full-reload"
        assert q2.get_nowait()["type"] == "full-reload"
    finally:
        _ws_clients.clear()


def test_broadcast_hmr_event_drops_on_full_queue() -> None:
    _ws_clients.clear()
    q: asyncio.Queue[dict] = asyncio.Queue(maxsize=1)
    q.put_nowait({"type": "old"})
    _ws_clients.add(q)
    try:
        broadcast_hmr_event({"type": "new"})
        assert q.get_nowait()["type"] == "old"
    finally:
        _ws_clients.discard(q)


def test_invalidate_and_broadcast_broadcasts_types_update(middleware: DevMiddleware) -> None:
    _ws_clients.clear()
    q: asyncio.Queue[dict] = asyncio.Queue()
    _ws_clients.add(q)
    try:
        middleware.type_bridge.invalidate()
        event = q.get_nowait()
        assert event["type"] == "types-update"
        assert "version" in event
        assert "timestamp" in event
    finally:
        _ws_clients.discard(q)


@pytest.mark.anyio()
async def test_handle_ws_accept_and_connected_message(middleware: DevMiddleware) -> None:
    send = AsyncMock()
    messages = [{"type": "websocket.disconnect"}]
    receive = AsyncMock(side_effect=messages)

    await middleware.handle_ws({}, receive, send)

    send.assert_any_call({"type": "websocket.accept"})
    calls = [c.args[0] for c in send.call_args_list]
    connected_calls = [c for c in calls if c.get("type") == "websocket.send"]
    assert len(connected_calls) >= 1
    data = decode_json(connected_calls[0]["text"])
    assert data["type"] == "connected"


@pytest.mark.anyio()
async def test_handle_ws_cleanup_on_disconnect(middleware: DevMiddleware) -> None:
    send = AsyncMock()
    receive = AsyncMock(side_effect=[{"type": "websocket.disconnect"}])

    before = len(_ws_clients)
    await middleware.handle_ws({}, receive, send)
    assert len(_ws_clients) == before


@pytest.mark.anyio()
async def test_handle_ws_ping_pong(middleware: DevMiddleware) -> None:
    send = AsyncMock()
    messages = [
        {"type": "websocket.receive", "text": encode_json({"type": "ping"})},
        {"type": "websocket.disconnect"},
    ]
    receive = AsyncMock(side_effect=messages)

    await middleware.handle_ws({}, receive, send)

    calls = [c.args[0] for c in send.call_args_list]
    pong_calls = [c for c in calls if c.get("type") == "websocket.send" and "pong" in c.get("text", "")]
    assert len(pong_calls) == 1


@pytest.mark.anyio()
async def test_dev_middleware_serves_wheel_bundled_client_and_hmr_subpath(middleware: DevMiddleware) -> None:
    """DevMiddleware serves /__vertebra/client/index.js and /__vertebra/client/hmr.js from the wheel bundle."""
    for req_path in ("/__vertebra/client/index.js", "/__vertebra/client/hmr.js"):
        send = AsyncMock()
        await middleware({"type": "http", "path": req_path}, AsyncMock(), send)
        sent = [c.args[0] for c in send.call_args_list]
        start = next(m for m in sent if m["type"] == "http.response.start")
        body = next(m for m in sent if m["type"] == "http.response.body")
        assert start["status"] == 200
        assert len(body["body"]) > 50


@pytest.mark.anyio()
async def test_dev_middleware_injects_hmr_script_into_welcome_page(type_bridge: TypeBridge) -> None:
    """DevMiddleware injects the /__vertebra/hmr script into the WelcomePageRenderer HTML response."""
    app = App(debug=True)
    welcome_resp = WelcomePageRenderer.make_handler(app)()

    async def asgi_welcome_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        del scope, receive
        raw_headers = [
            (
                k if isinstance(k, bytes) else k.encode("latin-1"),
                v if isinstance(v, bytes) else v.encode("latin-1"),
            )
            for k, v in welcome_resp.headers
        ]
        await send({"type": "http.response.start", "status": welcome_resp.status, "headers": raw_headers})
        await send({"type": "http.response.body", "body": bytes(welcome_resp.body), "more_body": False})

    dev_mw = DevMiddleware(asgi_welcome_app, type_bridge)
    send = AsyncMock()
    await dev_mw({"type": "http", "method": "GET", "path": "/"}, AsyncMock(), send)
    sent = [c.args[0] for c in send.call_args_list]
    start = next(m for m in sent if m["type"] == "http.response.start")
    body = next(m for m in sent if m["type"] == "http.response.body")
    assert start["status"] == 200
    assert (b"content-type", b"text/html; charset=utf-8") in start["headers"]
    assert b"/__vertebra/client/hmr.js" in body["body"]
    assert b"VERTEBRA" in body["body"]
