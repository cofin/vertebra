import asyncio

import msgspec
import pytest

from tests.helpers.shm import skip_if_shm_unavailable
from vertebra.drivers.postgres import AsyncPostgresDriver, SyncPostgresDriverConfig
from vertebra.ipc.channel import decode_request, encode_response
from vertebra.resources import BrokerClientConfig

pytestmark = pytest.mark.integration

try:
    from vertebra.typing import SpscRing
except ImportError as exc:  # pragma: no cover
    SpscRing = None  # type: ignore[assignment]
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


@pytest.mark.anyio
async def test_postgres_driver_async_fetch() -> None:
    if SpscRing is None:
        pytest.skip(f"Shared memory bindings unavailable: {_IMPORT_ERROR}")
    from tests.helpers.shm import tracked_shm_cleanup, unique_shm_name

    skip_if_shm_unavailable()
    encoder = msgspec.msgpack.Encoder()
    decoder = msgspec.msgpack.Decoder()

    name = unique_shm_name("vertebra-broker-test")
    slot_size = 4096
    slots = 64

    with tracked_shm_cleanup() as created_names:
        control_name = f"/{name}.control"
        created_names.append(control_name)
        control = SpscRing.create_named(control_name, slot_size, slots)

        async def broker_task() -> None:
            try:
                data = await asyncio.to_thread(control.recv_timeout, slot_size, 1000)
                if data is None:
                    return
                register = decoder.decode(data)

                req_name = register["req_ring"]
                resp_name = register["resp_ring"]
                created_names.append(req_name)
                created_names.append(resp_name)

                req_ring = SpscRing.open_named(req_name, slot_size, slots)
                resp_ring = SpscRing.open_named(resp_name, slot_size, slots)

                try:
                    resp_ring.send_timeout(encoder.encode({"ok": True, "id": register["id"]}), 0, 1000)

                    # acquire
                    data = await asyncio.to_thread(req_ring.recv_timeout, slot_size, 1000)
                    if data is None:
                        return
                    correlation_id, _flags, _procedure, payload = decode_request(data)
                    request = decoder.decode(payload)
                    resp_ring.send_timeout(
                        encode_response(encoder.encode({"ok": True, "token": "token", "id": request["id"]}), correlation_id=correlation_id),
                        0,
                        1000,
                    )
                    # execute
                    data = await asyncio.to_thread(req_ring.recv_timeout, slot_size, 1000)
                    if data is None:
                        return
                    correlation_id, _flags, _procedure, payload = decode_request(data)
                    request = decoder.decode(payload)
                    resp_ring.send_timeout(
                        encode_response(encoder.encode({
                            "ok": True,
                            "result": {"columns": ["value"], "rows": [[42]], "rowcount": 1},
                            "id": request["id"],
                        }), correlation_id=correlation_id),
                        0,
                        1000,
                    )
                    # release
                    data = await asyncio.to_thread(req_ring.recv_timeout, slot_size, 1000)
                    if data is None:
                        return
                    correlation_id, _flags, _procedure, payload = decode_request(data)
                    request = decoder.decode(payload)
                    resp_ring.send_timeout(
                        encode_response(encoder.encode({"ok": True, "id": request["id"]}), correlation_id=correlation_id),
                        0,
                        1000,
                    )

                except Exception:  # noqa: S110
                    pass
            except Exception:  # noqa: S110
                pass

        task = asyncio.create_task(broker_task())

        # Use prefer_eventfd=False because the mock broker uses SpscRing (no eventfd)
        # while AsyncBrokerClient would use AsyncShmTransport waiting on eventfd by default.
        client_config = BrokerClientConfig(
            name=name, slot_size=slot_size, slots=slots, client_id="test-client", prefer_eventfd=False
        )
        config = SyncPostgresDriverConfig(pool="default", client=client_config)
        driver = AsyncPostgresDriver(config)

        try:
            conn = await driver.connect()
            try:
                rows = await conn.fetch("select 1")
                assert rows == [(42,)]
            finally:
                await conn.close()
        finally:
            await task
