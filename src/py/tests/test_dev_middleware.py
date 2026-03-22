"""Tests for DevMiddleware WebSocket broadcast."""

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from vertebra.framework._dev_middleware import DevMiddleware, _ws_clients, broadcast_hmr_event
from vertebra.framework._type_bridge import TypeBridge


@pytest.fixture()
def type_bridge(tmp_path: Path) -> TypeBridge:
    (tmp_path / "types.d.ts").write_text("export type Foo = string;")
    return TypeBridge(tmp_path)


@pytest.fixture()
def middleware(type_bridge: TypeBridge) -> DevMiddleware:
    app = AsyncMock()
    return DevMiddleware(app, type_bridge)


class TestBroadcastHmrEvent:
    def test_broadcast_to_empty_clients(self) -> None:
        _ws_clients.clear()
        broadcast_hmr_event({"type": "test"})
        # No error

    def test_broadcast_delivers_to_queue(self) -> None:
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

    def test_broadcast_to_multiple_clients(self) -> None:
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

    def test_broadcast_drops_on_full_queue(self) -> None:
        _ws_clients.clear()
        q: asyncio.Queue[dict] = asyncio.Queue(maxsize=1)
        q.put_nowait({"type": "old"})
        _ws_clients.add(q)
        try:
            # Should not raise
            broadcast_hmr_event({"type": "new"})
            # Old event still there, new was dropped
            assert q.get_nowait()["type"] == "old"
        finally:
            _ws_clients.discard(q)


class TestInvalidateAndBroadcast:
    def test_invalidate_broadcasts_types_update(self, middleware: DevMiddleware) -> None:
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


class TestHandleWs:
    @pytest.mark.anyio()
    async def test_ws_accept_and_connected_message(self, middleware: DevMiddleware) -> None:
        send = AsyncMock()
        messages = [{"type": "websocket.disconnect"}]
        receive = AsyncMock(side_effect=messages)

        await middleware.handle_ws({}, receive, send)

        # First call: accept
        send.assert_any_call({"type": "websocket.accept"})
        # Second call: connected message
        calls = [c.args[0] for c in send.call_args_list]
        connected_calls = [c for c in calls if c.get("type") == "websocket.send"]
        assert len(connected_calls) >= 1
        data = json.loads(connected_calls[0]["text"])
        assert data["type"] == "connected"

    @pytest.mark.anyio()
    async def test_ws_cleanup_on_disconnect(self, middleware: DevMiddleware) -> None:
        send = AsyncMock()
        receive = AsyncMock(side_effect=[{"type": "websocket.disconnect"}])

        before = len(_ws_clients)
        await middleware.handle_ws({}, receive, send)
        assert len(_ws_clients) == before  # Client removed on disconnect

    @pytest.mark.anyio()
    async def test_ws_ping_pong(self, middleware: DevMiddleware) -> None:
        send = AsyncMock()
        messages = [
            {"type": "websocket.receive", "text": json.dumps({"type": "ping"})},
            {"type": "websocket.disconnect"},
        ]
        receive = AsyncMock(side_effect=messages)

        await middleware.handle_ws({}, receive, send)

        calls = [c.args[0] for c in send.call_args_list]
        pong_calls = [c for c in calls if c.get("type") == "websocket.send" and "pong" in c.get("text", "")]
        assert len(pong_calls) == 1
