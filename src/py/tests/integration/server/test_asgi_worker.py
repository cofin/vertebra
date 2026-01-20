from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import msgspec
import pytest

from tests.helpers.shm import skip_if_shm_unavailable, tracked_shm_cleanup, unique_shm_name
from vertebra._vertebra import SpscRing
from vertebra.server.worker import AsgiWorker, WorkerConfig

pytestmark = pytest.mark.integration


async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
    assert scope["type"] == "http"
    message = await receive()
    body = message.get("body", b"")
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": body or b"ok", "more_body": False})


def _skip_if_missing() -> None:
    skip_if_shm_unavailable()


async def _register_client(
    name: str, worker_id: str, slot_size: int, slots: int, created_names: list[str]
) -> tuple[SpscRing, SpscRing, msgspec.msgpack.Encoder, msgspec.msgpack.Decoder]:
    encoder = msgspec.msgpack.Encoder()
    decoder = msgspec.msgpack.Decoder()
    control_name = f"/{name}.asgi.{worker_id}.control"
    control = SpscRing.open_named(control_name, slot_size, slots)
    client_id = "test-client"
    req_name = f"/{name}.asgi.{worker_id}.client.{client_id}.req"
    resp_name = f"/{name}.asgi.{worker_id}.client.{client_id}.resp"
    req_ring = SpscRing.create_named(req_name, slot_size, slots)
    resp_ring = SpscRing.create_named(resp_name, slot_size, slots)
    created_names.append(req_name)
    created_names.append(resp_name)
    register = {
        "id": 1,
        "kind": "register_client",
        "client_id": client_id,
        "req_ring": req_name,
        "resp_ring": resp_name,
    }
    control.send_timeout(encoder.encode(register), 0, 1000)
    control.close()
    # Poll with retry - worker may not have processed registration yet
    max_len = slot_size
    data = None
    for _ in range(50):  # Retry up to 50 times
        with contextlib.suppress(TimeoutError):
            data = await asyncio.wait_for(asyncio.to_thread(resp_ring.recv_timeout, max_len, 100), timeout=2.0)
        if data is not None:
            break
        await asyncio.sleep(0.05)  # Give worker time to poll
    if data is None:
        pytest.fail("_register_client: resp_ring.recv returned None after retries")
    response = decoder.decode(data)
    assert response.get("ok") is True
    return req_ring, resp_ring, encoder, decoder


@pytest.mark.anyio
async def test_asgi_worker_roundtrip() -> None:
    _skip_if_missing()

    # Use unique name to prevent collision with other tests
    name = unique_shm_name("vertebra-asgi-test")
    worker_id = "0"
    slot_size = 4096
    slots = 64

    with tracked_shm_cleanup() as created_names:
        # The worker creates its own control ring, we just need to track it
        control_name = f"/{name}.asgi.{worker_id}.control"
        created_names.append(control_name)

        worker = AsgiWorker(app, WorkerConfig(name=name, worker_id=worker_id, slot_size=slot_size, slots=slots))
        # Run serve_forever in background so it polls for control messages
        serve_task = asyncio.create_task(worker.serve_forever())
        # Give the worker time to start and enter the polling loop
        for _ in range(50):
            await asyncio.sleep(0.01)
            if worker._running:
                break

        req_ring, resp_ring, encoder, decoder = await _register_client(name, worker_id, slot_size, slots, created_names)
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
        req_ring.send_timeout(encoder.encode(payload), 0, 1000)

        status = None
        body = b""
        while True:
            # Poll with retry - worker may not have processed request yet
            data = None
            for _ in range(50):
                with contextlib.suppress(TimeoutError):
                    data = await asyncio.wait_for(
                        asyncio.to_thread(resp_ring.recv_timeout, slot_size, 100), timeout=2.0
                    )
                if data is not None:
                    break
                await asyncio.sleep(0.05)
            if data is None:
                pytest.fail("test_asgi_worker_roundtrip: resp_ring.recv returned None after retries")
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


@pytest.mark.anyio
async def test_asgi_worker_lifespan() -> None:
    _skip_if_missing()

    # Use unique name to prevent collision with other tests
    name = unique_shm_name("vertebra-asgi-test")
    worker_id = "1"

    with tracked_shm_cleanup() as created_names:
        control_name = f"/{name}.asgi.{worker_id}.control"
        created_names.append(control_name)

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

        worker = AsgiWorker(lifespan_app, WorkerConfig(name=name, worker_id=worker_id))
        await worker.start()
        assert startup.is_set()
        await worker.stop()
        assert shutdown.is_set()
