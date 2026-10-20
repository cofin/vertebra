"""Unit tests for the Python ASGI WebSocket session and pre-upgrade http.response rejection."""

import asyncio
from typing import Any

import pytest

from vertebra.server._asgi._websocket import (
    WebSocketDisconnect,
    WebSocketHandshakeResult,
    run_asgi_websocket,
)


def test_websocket_disconnect_exception_attributes() -> None:
    """Verify WebSocketDisconnect stores close code and reason."""
    err = WebSocketDisconnect(code=1008, reason="policy violation")
    assert err.code == 1008
    assert err.reason == "policy violation"


@pytest.mark.anyio
async def test_asgi_websocket_accepts_subprotocol_and_exchanges_frames() -> None:
    """Verify standard websocket.connect -> websocket.accept -> receive/send -> close."""
    incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=16)
    outgoing: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=16)
    handshake_future: asyncio.Future[WebSocketHandshakeResult] = asyncio.get_running_loop().create_future()

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        assert scope["type"] == "websocket"
        connect_msg = await receive()
        assert connect_msg["type"] == "websocket.connect"
        await send(
            {
                "type": "websocket.accept",
                "subprotocol": "chat.v1",
                "headers": [(b"x-session", b"abc")],
            }
        )
        msg = await receive()
        assert msg == {"type": "websocket.receive", "text": "ping"}
        await send({"type": "websocket.send", "text": "pong"})
        await send({"type": "websocket.close", "code": 1000, "reason": "done"})

    await incoming.put({"type": "websocket.receive", "text": "ping"})
    await incoming.put({"type": "websocket.disconnect", "code": 1000})

    scope: dict[str, Any] = {
        "type": "websocket",
        "path": "/ws",
        "subprotocols": ["chat.v1"],
        "headers": [],
    }

    await run_asgi_websocket(
        app,
        scope,
        incoming=incoming,
        outgoing=outgoing,
        handshake_future=handshake_future,
    )

    decision = await handshake_future
    assert decision.accepted is True
    assert decision.subprotocol == "chat.v1"
    assert decision.headers == [(b"x-session", b"abc")]

    sent_frame = await outgoing.get()
    assert sent_frame == {"type": "websocket.send", "text": "pong"}
    close_frame = await outgoing.get()
    assert close_frame == {"type": "websocket.close", "code": 1000, "reason": "done"}


@pytest.mark.anyio
async def test_asgi_websocket_pre_upgrade_http_response_rejection() -> None:
    """Verify websocket.http.response.start + body rejects handshake with custom status and body."""
    incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=16)
    outgoing: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=16)
    handshake_future: asyncio.Future[WebSocketHandshakeResult] = asyncio.get_running_loop().create_future()

    async def rejecting_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        del scope
        msg = await receive()
        assert msg["type"] == "websocket.connect"
        await send(
            {
                "type": "websocket.http.response.start",
                "status": 403,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send(
            {
                "type": "websocket.http.response.body",
                "body": b'{"error":"unauthorized"}',
                "more_body": False,
            }
        )

    scope: dict[str, Any] = {
        "type": "websocket",
        "path": "/ws/private",
        "headers": [],
        "extensions": {"websocket.http.response": {}},
    }

    await run_asgi_websocket(
        rejecting_app,
        scope,
        incoming=incoming,
        outgoing=outgoing,
        handshake_future=handshake_future,
    )

    decision = await handshake_future
    assert decision.accepted is False
    assert decision.status == 403
    assert decision.body == b'{"error":"unauthorized"}'
