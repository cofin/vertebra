from __future__ import annotations

import asyncio

import msgspec
import pytest

from tests.helpers.shm import skip_if_shm_unavailable
from vertebra.drivers.sqlite import AsyncSqliteDriver, SyncSqliteDriverConfig
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
async def test_sqlite_driver_async_fetch() -> None:
    if ShmRing is None or not SHM_AVAILABLE:
        reason = _IMPORT_ERROR or "shared memory library unavailable"
        pytest.skip(f"Shared memory bindings unavailable: {reason}")
    from tests.helpers.shm import tracked_shm_cleanup, unique_shm_name

    skip_if_shm_unavailable()
    encoder = msgspec.msgpack.Encoder()
    decoder = msgspec.msgpack.Decoder()

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
                            "result": {"columns": ["value"], "rows": [[1], [2]], "rowcount": 2},
                            "id": request["id"],
                        }),
                        0,
                        1000,
                    )
                    # release
                    data, _flags = await asyncio.to_thread(req_ring.recv, 1000)
                    request = decoder.decode(data)
                    resp_ring.send(encoder.encode({"ok": True, "id": request["id"]}), 0, 1000)
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
        config = SyncSqliteDriverConfig(pool="default", client=client_config)
        driver = AsyncSqliteDriver(config)
        
        try:
            conn = await driver.connect()
            try:
                rows = await conn.fetch("select 1")
                assert rows == [(1,), (2,)]
            finally:
                await conn.close()
        finally:
            await task
            control.close()
