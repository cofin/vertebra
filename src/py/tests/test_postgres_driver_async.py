from __future__ import annotations

import asyncio

import msgspec
import pytest

from vertebra.drivers.postgres import AsyncPostgresDriver, SyncPostgresDriverConfig
from vertebra.resources import BrokerClientConfig

try:
    from vertebra._internal.shm import SHM_AVAILABLE, ShmRing, ShmRingConfig
except Exception as exc:  # pragma: no cover
    ShmRing = None  # type: ignore[assignment]
    ShmRingConfig = None  # type: ignore[assignment]
    SHM_AVAILABLE = False
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


@pytest.mark.anyio
async def test_postgres_driver_async_fetch() -> None:
    if ShmRing is None or not SHM_AVAILABLE:
        reason = _IMPORT_ERROR or "shared memory library unavailable"
        pytest.skip(f"Shared memory bindings unavailable: {reason}")
    encoder = msgspec.msgpack.Encoder()
    decoder = msgspec.msgpack.Decoder()

    name = "vertebra-broker-test"
    control = ShmRing.create(ShmRingConfig(name=f"/{name}.control", slot_size=4096, slots=64))

    async def broker_task() -> None:
        data, _flags = await asyncio.to_thread(control.recv, 1000)
        register = decoder.decode(data)
        req_ring = ShmRing.open(ShmRingConfig(name=register["req_ring"], slot_size=4096, slots=64))
        resp_ring = ShmRing.open(ShmRingConfig(name=register["resp_ring"], slot_size=4096, slots=64))
        resp_ring.send(encoder.encode({"ok": True, "id": register["id"]}), 0, 1000)

        # acquire
        data, _flags = await asyncio.to_thread(req_ring.recv, 1000)
        request = decoder.decode(data)
        resp_ring.send(encoder.encode({"ok": True, "token": "token", "id": request["id"]}), 0, 1000)
        # execute
        data, _flags = await asyncio.to_thread(req_ring.recv, 1000)
        request = decoder.decode(data)
        resp_ring.send(
            encoder.encode({
                "ok": True,
                "result": {"columns": ["value"], "rows": [[42]], "rowcount": 1},
                "id": request["id"],
            }),
            0,
            1000,
        )
        # release
        data, _flags = await asyncio.to_thread(req_ring.recv, 1000)
        request = decoder.decode(data)
        resp_ring.send(encoder.encode({"ok": True, "id": request["id"]}), 0, 1000)

        req_ring.close()
        resp_ring.close()

    task = asyncio.create_task(broker_task())

    client_config = BrokerClientConfig(name=name, slot_size=4096, slots=64, client_id="test-client")
    config = SyncPostgresDriverConfig(pool="default", client=client_config)
    driver = AsyncPostgresDriver(config)
    conn = await driver.connect()
    rows = await conn.fetch("select 1")
    assert rows == [(42,)]
    await conn.close()

    await task
    control.close()
