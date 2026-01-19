from __future__ import annotations

import asyncio

import msgspec
import pytest

from vertebra.server.worker import AsgiWorker, WorkerConfig

try:
    from vertebra._internal.shm import SHM_AVAILABLE, ShmRing, ShmRingConfig
except Exception as exc:  # pragma: no cover
    SHM_AVAILABLE = False
    ShmRing = None  # type: ignore[assignment]
    ShmRingConfig = None  # type: ignore[assignment]
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


async def app(scope, receive, send) -> None:
    assert scope["type"] == "http"
    message = await receive()
    body = message.get("body", b"")
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": body or b"ok", "more_body": False})


def _skip_if_missing() -> None:
    if ShmRing is None or not SHM_AVAILABLE:
        reason = _IMPORT_ERROR or "shared memory library unavailable"
        pytest.skip(f"Shared memory bindings unavailable: {reason}")


def _register_client(name: str, worker_id: str, slot_size: int, slots: int) -> tuple[ShmRing, ShmRing, msgspec.msgpack.Encoder, msgspec.msgpack.Decoder]:
    encoder = msgspec.msgpack.Encoder()
    decoder = msgspec.msgpack.Decoder()
    control = ShmRing.open(ShmRingConfig(name=f"/{name}.asgi.{worker_id}.control", slot_size=slot_size, slots=slots))
    client_id = "test-client"
    req_name = f"/{name}.asgi.{worker_id}.client.{client_id}.req"
    resp_name = f"/{name}.asgi.{worker_id}.client.{client_id}.resp"
    req_ring = ShmRing.create(ShmRingConfig(name=req_name, slot_size=slot_size, slots=slots))
    resp_ring = ShmRing.create(ShmRingConfig(name=resp_name, slot_size=slot_size, slots=slots))
    register = {"id": 1, "kind": "register_client", "client_id": client_id, "req_ring": req_name, "resp_ring": resp_name}
    control.send(encoder.encode(register), 0, 1000)
    data, _flags = resp_ring.recv(1000)
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

    worker = AsgiWorker(app, WorkerConfig(name=name, worker_id=worker_id, slot_size=slot_size, slots=slots))
    await worker.start()

    req_ring, resp_ring, encoder, decoder = _register_client(name, worker_id, slot_size, slots)
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
        data, _flags = resp_ring.recv(1000)
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


@pytest.mark.anyio
async def test_asgi_worker_lifespan() -> None:
    _skip_if_missing()
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
