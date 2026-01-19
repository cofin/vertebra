from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import msgspec
import pytest

from vertebra._internal._typing import SHM_AVAILABLE, ShmRing, ShmRingConfig, get_shm_load_error, shm_unlink
from vertebra.server.worker import AsgiWorker, WorkerConfig


def _cleanup_asgi_shm(name: str, worker_id: str, client_id: str = "test-client") -> None:
    """Clean up shared memory from previous test runs."""
    if not SHM_AVAILABLE:
        return
    suffixes = [
        f".asgi.{worker_id}.control",
        f".asgi.{worker_id}.client.{client_id}.req",
        f".asgi.{worker_id}.client.{client_id}.resp",
    ]
    for suffix in suffixes:
        with contextlib.suppress(Exception):
            shm_unlink(f"/{name}{suffix}")


async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
    assert scope["type"] == "http"
    message = await receive()
    body = message.get("body", b"")
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": body or b"ok", "more_body": False})


def _skip_if_missing() -> None:
    if not SHM_AVAILABLE:
        reason = get_shm_load_error() or "shared memory library unavailable"
        pytest.skip(f"Shared memory bindings unavailable: {reason}")


async def _register_client(
    name: str, worker_id: str, slot_size: int, slots: int
) -> tuple[ShmRing, ShmRing, msgspec.msgpack.Encoder, msgspec.msgpack.Decoder]:
    encoder = msgspec.msgpack.Encoder()
    decoder = msgspec.msgpack.Decoder()
    control = ShmRing.open(ShmRingConfig(name=f"/{name}.asgi.{worker_id}.control", slot_size=slot_size, slots=slots))
    client_id = "test-client"
    req_name = f"/{name}.asgi.{worker_id}.client.{client_id}.req"
    resp_name = f"/{name}.asgi.{worker_id}.client.{client_id}.resp"
    req_ring = ShmRing.create(ShmRingConfig(name=req_name, slot_size=slot_size, slots=slots))
    resp_ring = ShmRing.create(ShmRingConfig(name=resp_name, slot_size=slot_size, slots=slots))
    register = {
        "id": 1,
        "kind": "register_client",
        "client_id": client_id,
        "req_ring": req_name,
        "resp_ring": resp_name,
    }
    control.send(encoder.encode(register), 0, 1000)
    control.close()
    # Use asyncio.to_thread so the event loop can continue while we wait
    data, _flags = await asyncio.to_thread(resp_ring.recv, 1000)
    response = decoder.decode(data)
    assert response.get("ok") is True
    return req_ring, resp_ring, encoder, decoder


@pytest.mark.anyio
async def test_asgi_worker_roundtrip() -> None:
    _skip_if_missing()
    name = "vertebra-asgi-test"
    worker_id = "0"
    slot_size = 4096
    slots = 64

    # Clean up any stale shared memory from previous runs
    _cleanup_asgi_shm(name, worker_id)

    worker = AsgiWorker(app, WorkerConfig(name=name, worker_id=worker_id, slot_size=slot_size, slots=slots))
    # Run serve_forever in background so it polls for control messages
    serve_task = asyncio.create_task(worker.serve_forever())
    # Give the worker time to start and enter the polling loop
    for _ in range(50):
        await asyncio.sleep(0.01)
        if worker._running:
            break

    req_ring, resp_ring, encoder, decoder = await _register_client(name, worker_id, slot_size, slots)
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
    req_ring.send(encoder.encode(payload), 0, 1000)

    status = None
    body = b""
    while True:
        # Use asyncio.to_thread so the worker can poll while we wait
        data, _flags = await asyncio.to_thread(resp_ring.recv, 1000)
        response = decoder.decode(data)
        if response["type"] == "http.response.start":
            status = response["status"]
        elif response["type"] == "http.response.body":
            body += response.get("body", b"")
            if not response.get("more_body", False):
                break

    assert status == 200
    assert body == b"hello"

    req_ring.close()
    resp_ring.close()
    await worker.stop()
    serve_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await serve_task
    # Clean up shared memory after test completes
    _cleanup_asgi_shm(name, worker_id)


@pytest.mark.anyio
async def test_asgi_worker_lifespan() -> None:
    _skip_if_missing()
    # Clean up any stale shared memory from previous runs
    _cleanup_asgi_shm("vertebra-asgi-test", "1")

    startup = asyncio.Event()
    shutdown = asyncio.Event()

    async def lifespan_app(scope, receive, send) -> None:
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    startup.set()
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    shutdown.set()
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        else:
            await send({"type": "http.response.start", "status": 204, "headers": []})
            await send({"type": "http.response.body", "body": b"", "more_body": False})

    worker = AsgiWorker(lifespan_app, WorkerConfig(name="vertebra-asgi-test", worker_id="1"))
    await worker.start()
    assert startup.is_set()
    await worker.stop()
    assert shutdown.is_set()
    # Clean up shared memory after test completes
    _cleanup_asgi_shm("vertebra-asgi-test", "1")
