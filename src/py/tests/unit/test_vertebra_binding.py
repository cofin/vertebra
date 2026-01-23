import pytest
from litestar import post

from vertebra.integration.litestar import VertebraRequest


@pytest.mark.unit
def test_vertebra_request_binding() -> None:
    message = {
        "method": "POST",
        "path": "/hello",
        "headers": [["content-type", "application/json"]],
        "body": b'{"name": "cody"}',
    }

    request = VertebraRequest(message)
    assert request.method == "POST"
    assert request.headers["content-type"] == "application/json"

    # Test body parsing
    data = request.json()
    assert data == {"name": "cody"}


@pytest.mark.unit
@pytest.mark.anyio
async def test_vertebra_worker_with_request_object() -> None:
    # This test will ensure the worker uses the real request object
    # and can pass its data to handlers.
    from unittest.mock import AsyncMock, MagicMock

    from vertebra.server.worker import AsgiWorker, WorkerConfig

    @post("/echo", sync_to_thread=False)
    def handler(data: dict) -> dict:
        return data

    worker = AsgiWorker(MagicMock(), WorkerConfig())

    from vertebra.integration.litestar.compiler import PipelineCompiler

    compiler = PipelineCompiler()
    # We need to tweak the compiler to use VertebraRequest specifically or
    # ensure it's compatible.
    runner = compiler.compile_route(handler)

    worker._native_runners = {id(handler.fn): runner}

    message = {
        "type": "direct.http",
        "id": "req-1",
        "handler_id": id(handler.fn),
        "path": "/echo",
        "method": "POST",
        "headers": [["content-type", "application/json"]],
        "body": b'{"key": "value"}',
    }

    writer = AsyncMock()
    # Execute
    await worker._handle_direct_http(message, writer)

    assert writer.send.call_count == 2
    body_args = writer.send.call_args_list[1][0][0]
    assert body_args["body"] == b'{"key": "value"}'


@pytest.mark.unit
@pytest.mark.anyio
async def test_vertebra_worker_request_injection() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from litestar import get

    from vertebra.server.worker import AsgiWorker, WorkerConfig

    @get("/path", sync_to_thread=False)
    def handler(request: VertebraRequest) -> dict:
        return {"path": request.path}

    worker = AsgiWorker(MagicMock(), WorkerConfig())
    from vertebra.integration.litestar.compiler import PipelineCompiler

    compiler = PipelineCompiler()
    runner = compiler.compile_route(handler)
    worker._native_runners = {id(handler.fn): runner}

    message = {
        "type": "direct.http",
        "id": "req-2",
        "handler_id": id(handler.fn),
        "path": "/actual-path",
        "method": "GET",
        "headers": [],
        "body": b"",
    }

    writer = AsyncMock()
    await worker._handle_direct_http(message, writer)

    body_args = writer.send.call_args_list[1][0][0]
    assert body_args["body"] == b'{"path": "/actual-path"}'
