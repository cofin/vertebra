import pytest
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from vertebra.server.worker import AsgiWorker, WorkerConfig
from litestar import websocket

@pytest.mark.unit
@pytest.mark.anyio
async def test_vertebra_worker_direct_websocket() -> None:
    @websocket("/ws")
    async def handler(socket: Any) -> None:
        pass

    # Mock native handler for WS
    async def native_ws_handler(socket):
        # socket is our VertebraWebSocket wrapper
        msg = socket.receive_text()
        await socket.send_text(f"echo: {msg}")

    worker = AsgiWorker(MagicMock(), WorkerConfig(worker_id="0"))
    handler_id = 123
    worker._native_runners = {handler_id: native_ws_handler}
    
    # Test WS_MESSAGE
    message = {
        "type": "ws.message",
        "id": "req-ws",
        "handler_id": handler_id,
        "text": "hello"
    }
    
    writer = AsyncMock()
    # Call via _handle_frame to trigger dispatch
    await worker._handle_frame(message, {}, writer)
    
    writer.send.assert_called_once()
    args = writer.send.call_args[0][0]
    assert args["type"] == "ws.message"
    assert args["text"] == "echo: hello"
