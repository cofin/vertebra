from __future__ import annotations

import asyncio

import msgspec
import pytest

from vertebra.server.worker import AsgiWorker, WorkerConfig


async def app(scope, receive, send):
    assert scope["type"] == "http"
    message = await receive()
    body = message.get("body", b"")
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [(b"content-type", b"text/plain")],
    })
    await send({
        "type": "http.response.body",
        "body": body or b"ok",
        "more_body": False,
    })


@pytest.mark.anyio
async def test_asgi_worker_roundtrip() -> None:
    worker = AsgiWorker(app, WorkerConfig(host="127.0.0.1", port=0))
    await worker.start()
    assert worker._server is not None
    port = worker._server.sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    encoder = msgspec.msgpack.Encoder()
    decoder = msgspec.msgpack.Decoder()
    payload = {
        "id": "req-1",
        "method": "GET",
        "path": "/",
        "query": "",
        "headers": [("x-test", "1")],
        "body": b"hello",
        "scheme": "http",
        "httpVersion": "1.1",
    }
    encoded = encoder.encode(payload)
    writer.write(len(encoded).to_bytes(4, "big") + encoded)
    await writer.drain()

    header = await reader.readexactly(4)
    size = int.from_bytes(header, "big")
    response_payload = await reader.readexactly(size)
    response = decoder.decode(response_payload)
    assert response["status"] == 200
    assert response["body"] == b"hello"

    writer.close()
    await writer.wait_closed()
    await worker.stop()
