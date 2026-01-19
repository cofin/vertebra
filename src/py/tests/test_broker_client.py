from __future__ import annotations

import asyncio
from typing import Any

import msgspec
import pytest

from tests.helpers.shm import tracked_shm_cleanup, unique_shm_name


def _skip_if_async_ring_unavailable() -> None:
    """Skip test if AsyncRing (PyO3) is not available."""
    try:
        from vertebra._vertebra import AsyncRing  # noqa: F401
    except ImportError:
        pytest.skip("AsyncRing (PyO3) not available")


@pytest.mark.anyio
async def test_broker_client_roundtrip() -> None:
    _skip_if_async_ring_unavailable()

    from vertebra._internal.async_transport import PollingTransport
    from vertebra._vertebra import AsyncRing
    from vertebra.resources import BrokerClient, BrokerClientConfig

    encoder = msgspec.msgpack.Encoder()
    decoder = msgspec.msgpack.Decoder()

    # Use unique name to prevent collision with other tests
    name = unique_shm_name("vertebra-broker-test")

    with tracked_shm_cleanup() as created_names:
        control_name = f"/{name}.control"
        # Broker creates control ring
        control = AsyncRing.create_named(control_name, 4096, 64)
        created_names.append(control_name)

        broker_ready = asyncio.Event()
        broker_error: Exception | None = None

        async def broker_task() -> None:
            nonlocal broker_error
            try:
                # Use PollingTransport because eventfd notifications don't work
                # across different AsyncRing handles (each handle has its own eventfd).
                # When the client sends to control, it signals client's eventfd, not ours.
                control_transport = PollingTransport(control, poll_interval=0.001)

                broker_ready.set()

                # Receive registration
                try:
                    data = await asyncio.wait_for(control_transport.recv(), timeout=10.0)
                except asyncio.TimeoutError:
                    pytest.fail("broker_task: control_transport.recv timed out")
                    return

                register = decoder.decode(data)
                req_name = register["req_ring"]
                resp_name = register["resp_ring"]
                created_names.append(req_name)
                created_names.append(resp_name)

                # Open client's rings - must use polling for same reason
                req_ring = AsyncRing.open_named(req_name, 4096, 64)
                resp_ring = AsyncRing.open_named(resp_name, 4096, 64)
                req_transport = PollingTransport(req_ring, poll_interval=0.001)
                resp_transport = PollingTransport(resp_ring, poll_interval=0.001)

                # Send registration ack
                await resp_transport.send(encoder.encode({"ok": True, "id": register["id"]}))

                # Handle 3 requests: acquire, release, health
                for _ in range(3):
                    try:
                        data = await asyncio.wait_for(req_transport.recv(), timeout=10.0)
                    except asyncio.TimeoutError:
                        pytest.fail("broker_task: req_transport.recv timed out")
                        return

                    request = decoder.decode(data)
                    if request["kind"] == "acquire":
                        response: dict[str, Any] = {"ok": True, "token": "1", "id": request["id"]}
                    elif request["kind"] == "release" or request["kind"] == "health":
                        response = {"ok": True, "id": request["id"]}
                    else:
                        response = {"ok": False, "error": {"message": "unknown"}, "id": request["id"]}
                    await resp_transport.send(encoder.encode(response))

                control_transport.close()
                req_transport.close()
                resp_transport.close()
            except Exception as e:
                broker_error = e

        task = asyncio.create_task(broker_task())

        # Wait for broker to be ready
        await asyncio.wait_for(broker_ready.wait(), timeout=5.0)

        # BrokerClient also uses PollingTransport internally when available.
        # However, since the client creates the response ring and the broker
        # opens it, the broker's send() won't notify the client's eventfd.
        # We need to ensure the client uses polling too.
        #
        # For now, force CFFI mode which uses blocking recv with timeouts.
        # This is the correct approach until the architecture supports
        # cross-handle notification via shared memory futex.
        client = BrokerClient(BrokerClientConfig(name=name, slot_size=4096, slots=64, client_id="test-client"))
        client._use_async_transport = False  # Force CFFI blocking mode for client

        token = await client.acquire("default")
        assert token == "1"
        ok = await client.release("default", token)
        assert ok is True
        healthy = await client.health("default")
        assert healthy is True
        await client.close()

        await asyncio.wait_for(task, timeout=10.0)
        if broker_error is not None:
            raise broker_error
