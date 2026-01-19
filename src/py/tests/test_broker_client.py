from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import msgspec
import pytest

from vertebra._internal._typing import SHM_AVAILABLE, ShmRing, ShmRingConfig, get_shm_load_error, shm_unlink
from vertebra.resources import BrokerClient, BrokerClientConfig


def _cleanup_shm(name: str) -> None:
    """Clean up shared memory from previous test runs."""
    if not SHM_AVAILABLE:
        return
    for suffix in (".control", ".client.test-client.req", ".client.test-client.resp"):
        with contextlib.suppress(Exception):
            shm_unlink(f"/{name}{suffix}")


@pytest.mark.anyio
async def test_broker_client_roundtrip() -> None:
    if not SHM_AVAILABLE:
        reason = get_shm_load_error() or "shared memory library unavailable"
        pytest.skip(f"Shared memory bindings unavailable: {reason}")
    encoder = msgspec.msgpack.Encoder()
    decoder = msgspec.msgpack.Decoder()

    name = "vertebra-broker-test"
    # Clean up any stale shared memory from previous runs
    _cleanup_shm(name)
    control = ShmRing.create(ShmRingConfig(name=f"/{name}.control", slot_size=4096, slots=64))

    async def broker_task() -> None:
        data, _flags = await asyncio.to_thread(control.recv, 1000)
        register = decoder.decode(data)
        req_name = register["req_ring"]
        resp_name = register["resp_ring"]
        req_ring = ShmRing.open(ShmRingConfig(name=req_name, slot_size=4096, slots=64))
        resp_ring = ShmRing.open(ShmRingConfig(name=resp_name, slot_size=4096, slots=64))
        resp_ring.send(encoder.encode({"ok": True, "id": register["id"]}), 0, 1000)

        # Handle 3 requests: acquire, release, health
        for _ in range(3):
            data, _flags = await asyncio.to_thread(req_ring.recv, 1000)
            request = decoder.decode(data)
            if request["kind"] == "acquire":
                response: dict[str, Any] = {"ok": True, "token": "1", "id": request["id"]}
            elif request["kind"] == "release" or request["kind"] == "health":
                response = {"ok": True, "id": request["id"]}
            else:
                response = {"ok": False, "error": {"message": "unknown"}, "id": request["id"]}
            resp_ring.send(encoder.encode(response), 0, 1000)

        req_ring.close()
        resp_ring.close()

    task = asyncio.create_task(broker_task())

    client = BrokerClient(BrokerClientConfig(name=name, slot_size=4096, slots=64, client_id="test-client"))
    token = await client.acquire("default")
    assert token == "1"
    ok = await client.release("default", token)
    assert ok is True
    healthy = await client.health("default")
    assert healthy is True
    await client.close()

    await task
    control.close()
    # Clean up shared memory after test completes
    _cleanup_shm(name)
