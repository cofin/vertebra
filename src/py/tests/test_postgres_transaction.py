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
    from tests.helpers.shm import tracked_shm_cleanup, unique_shm_name

    skip_if_shm_unavailable()
    encoder = msgspec.msgpack.Encoder()
    decoder = msgspec.msgpack.Decoder()
    queries: list[str] = []

    name = unique_shm_name("vertebra-broker-test")
    
    with tracked_shm_cleanup() as created_names:
        control_name = f"/{name}.control"
        created_names.append(control_name)
        control = ShmRing.create(ShmRingConfig(name=control_name, slot_size=4096, slots=64))

        async def broker_task() -> None:
            try:
                data, _flags = await asyncio.to_thread(control.recv, 1000)
                register = decoder.decode(data)
                
                req_name = register["req_ring"]
                resp_name = register["resp_ring"]
                created_names.append(req_name)
                created_names.append(resp_name)
                
                req_ring = ShmRing.open(ShmRingConfig(name=req_name, slot_size=4096, slots=64))
                resp_ring = ShmRing.open(ShmRingConfig(name=resp_name, slot_size=4096, slots=64))
                
                try:
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
                finally:
                    req_ring.close()
                    resp_ring.close()
            except Exception:
                pass

        task = asyncio.create_task(broker_task())

        client_config = BrokerClientConfig(
            name=name, 
            slot_size=4096, 
            slots=64, 
            client_id="test-client",
            use_async_transport=False
        )
        config = SyncPostgresDriverConfig(pool="default", client=client_config)
        driver = AsyncPostgresDriver(config)
        
        try:
            conn = await driver.connect()
            try:
                async with conn.transaction():
                    await conn.execute("SELECT 1")
            finally:
                await conn.close()
        finally:
            await task
            control.close()

    assert queries[0] == "BEGIN"
    assert "SELECT 1" in queries
    assert queries[-1] == "COMMIT"
