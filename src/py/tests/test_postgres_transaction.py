from __future__ import annotations

import asyncio
from typing import Any

import msgspec
import pytest

from tests.helpers.shm import skip_if_shm_unavailable
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
async def test_postgres_transaction_context() -> None:
    if ShmRing is None or not SHM_AVAILABLE:
        reason = _IMPORT_ERROR or "shared memory library unavailable"
        pytest.skip(f"Shared memory bindings unavailable: {reason}")
    skip_if_shm_unavailable()
    encoder = msgspec.msgpack.Encoder()
    decoder = msgspec.msgpack.Decoder()
    queries: list[str] = []

    name = "vertebra-broker-test"
    control = ShmRing.create(ShmRingConfig(name=f"/{name}.control", slot_size=4096, slots=64))

    async def broker_task() -> None:
        data, _flags = await asyncio.to_thread(control.recv, 1000)
        register = decoder.decode(data)
        req_ring = ShmRing.open(ShmRingConfig(name=register["req_ring"], slot_size=4096, slots=64))
        resp_ring = ShmRing.open(ShmRingConfig(name=register["resp_ring"], slot_size=4096, slots=64))
        resp_ring.send(encoder.encode({"ok": True, "id": register["id"]}), 0, 1000)

        for _ in range(5):
            data, _flags = await asyncio.to_thread(req_ring.recv, 1000)
            request = decoder.decode(data)
            kind = request.get("kind")
            if kind == "acquire":
                response: dict[str, Any] = {"ok": True, "token": "token", "id": request["id"]}
            elif kind == "begin":
                queries.append("BEGIN")
                response = {"ok": True, "id": request["id"]}
            elif kind == "execute":
                queries.append(str(request.get("sql")))
                response = {"ok": True, "result": {"columns": [], "rows": [], "rowcount": 0}, "id": request["id"]}
            elif kind == "commit":
                queries.append("COMMIT")
                response = {"ok": True, "id": request["id"]}
            elif kind == "release":
                response = {"ok": True, "id": request["id"]}
            else:
                response = {"ok": False, "error": {"message": "unknown"}, "id": request["id"]}
            resp_ring.send(encoder.encode(response), 0, 1000)

        req_ring.close()
        resp_ring.close()

    task = asyncio.create_task(broker_task())

    client_config = BrokerClientConfig(name=name, slot_size=4096, slots=64, client_id="test-client")
    config = SyncPostgresDriverConfig(pool="default", client=client_config)
    driver = AsyncPostgresDriver(config)
    conn = await driver.connect()
    async with conn.transaction():
        await conn.execute("SELECT 1")
    await conn.close()

    await task
    control.close()

    assert queries[0] == "BEGIN"
    assert "SELECT 1" in queries
    assert queries[-1] == "COMMIT"
