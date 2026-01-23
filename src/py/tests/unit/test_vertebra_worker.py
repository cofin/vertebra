import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from vertebra.server.worker import AsgiWorker, WorkerConfig
from vertebra.integration.litestar import LitestarRoute
from litestar import get

@pytest.mark.unit
@pytest.mark.anyio
async def test_vertebra_worker_direct_http() -> None:
    # We want to test that when the worker receives a DIRECT_HTTP message,
    # it calls the compiled runner instead of standard ASGI flow.
    
    @get("/hello", sync_to_thread=False)
    def handler() -> dict:
        return {"hello": "world"}
    
    app = MagicMock()
    # Mocking what the worker expects from a Litestar-like app
    app.routes = [] 
    
    worker = AsgiWorker(app, WorkerConfig(worker_id="0"))
    
    # Manually inject a native route for testing
    from vertebra.integration.litestar.compiler import PipelineCompiler
    compiler = PipelineCompiler()
    runner = compiler.compile_route(handler)
    
    handler_id = id(handler.fn)
    worker._native_runners = {handler_id: runner}
    
    # Mock message
    message = {
        "type": "direct.http", # We'll define this string or use opcode
        "id": "req-1",
        "handler_id": handler_id,
        "path": "/hello",
        "method": "GET",
        "headers": [],
        "body": b""
    }
    
    # Mock response writer
    writer = AsyncMock()
    
    # We need to test the internal _handle_direct_http or similar
    await worker._handle_direct_http(message, writer)
    
    # Check that writer.send was called with the result
    assert writer.send.call_count == 2
    
    # First call: http.response.start
    start_args = writer.send.call_args_list[0][0][0]
    assert start_args["id"] == "req-1"
    assert start_args["type"] == "http.response.start"
    assert start_args["status"] == 200
    
    # Second call: http.response.body
    body_args = writer.send.call_args_list[1][0][0]
    assert body_args["id"] == "req-1"
    assert body_args["type"] == "http.response.body"
    assert body_args["body"] == b'{"hello": "world"}'
