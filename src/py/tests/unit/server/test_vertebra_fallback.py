from unittest.mock import AsyncMock, MagicMock

import pytest
from litestar import get

from vertebra.core.worker import AsgiWorker, WorkerConfig


@pytest.mark.unit
@pytest.mark.anyio
async def test_vertebra_worker_fallback_logic() -> None:
    # Handler for native route
    @get("/native", sync_to_thread=False)
    def native_handler() -> dict:
        return {"type": "native"}

    # Standard ASGI app for fallback
    async def asgi_app(scope, receive, send) -> None:
        if scope["type"] == "http":
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b'{"type": "legacy"}', "more_body": False})

    worker = AsgiWorker(asgi_app, WorkerConfig())

    # Setup one native runner
    from vertebra.extensions.litestar.compiler import PipelineCompiler

    compiler = PipelineCompiler()
    # Mocking handler object enough for compiler
    handler_obj = MagicMock()
    handler_obj.fn = native_handler.fn
    handler_obj.resolve_guards.return_value = []
    handler_obj.resolve_dependencies.return_value = {}

    runner = compiler.compile_route(handler_obj)
    handler_id = id(native_handler.fn)
    worker._native_runners = {handler_id: runner}

    # 1. Test Native Path
    native_message = {
        "type": "direct.http",
        "id": "req-native",
        "handler_id": handler_id,
        "path": "/native",
        "method": "GET",
    }
    writer = AsyncMock()
    await worker._handle_frame(native_message, {}, writer)

    body_args = writer.send.call_args_list[1][0][0]
    import json

    assert json.loads(body_args["body"]) == {"type": "native"}

    # 2. Test Legacy Fallback Path
    # The broker would send a standard http.request.start if it doesn't have a handler_id
    legacy_message = {
        "type": "http.request.start",
        "id": "req-legacy",
        "method": "GET",
        "path": "/legacy",
        "headers": [],
    }
    writer = AsyncMock()
    # We need to test the standard ASGI flow through _handle_frame
    # _handle_frame for http.request.start calls _start_request
    await worker._handle_frame(legacy_message, {}, writer)

    # The ASGI app runs in a background task, we might need to wait or poll
    # For simplicity, we check if a context was created
    # Wait, the worker sends response to writer.
    # We'll wait a bit for the task to finish
    import asyncio

    await asyncio.sleep(0.1)

    assert writer.send.call_count >= 2
    body_args = writer.send.call_args_list[1][0][0]
    assert body_args["body"] == b'{"type": "legacy"}'
