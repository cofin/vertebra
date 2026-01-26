from unittest.mock import AsyncMock, MagicMock

import pytest

from vertebra.core.serializers import MessagePackCodec
from vertebra.core.worker import AsgiWorker, ClientState, WorkerConfig
from vertebra.messaging.protocol import HEADER_SIZE, RequestHeader, ResponseHeader


@pytest.mark.unit
@pytest.mark.anyio
async def test_worker_decodes_request_header() -> None:
    app = MagicMock()
    worker = AsgiWorker(app, WorkerConfig())
    codec = MessagePackCodec()

    # Mock client state
    client_id = "test-client"
    req_ring = MagicMock()
    resp_ring = MagicMock()
    client = ClientState(id=client_id, request=req_ring, response=resp_ring)

    # Create a payload with a header
    body_dict = {"type": "vertebra.ping", "id": "req-1"}
    body_bytes = codec.encode(body_dict)
    header = RequestHeader(id=1, flags=0, payload_len=len(body_bytes))
    full_payload = header.to_bytes() + body_bytes

    # Mock _handle_ping so we can verify it gets called
    worker._handle_ping = AsyncMock()

    await worker._handle_client_payload(client_id, client, full_payload)

    # Assert _handle_ping called with body_dict
    worker._handle_ping.assert_awaited_once()
    args = worker._handle_ping.await_args[0]
    assert args[0] == body_dict


@pytest.mark.unit
@pytest.mark.anyio
async def test_worker_encodes_response_header() -> None:
    app = MagicMock()
    worker = AsgiWorker(app, WorkerConfig())

    # Mock client state
    MagicMock()
    resp_ring = MagicMock()

    # Use ResponseWriter directly
    from vertebra.core.worker import ResponseWriter

    writer = ResponseWriter(resp_ring, worker._codec, 5000)

    # Send a response
    await writer.send({"id": "req-1", "ok": True})

    # Verify resp_ring.send called with header + payload (AsyncRing uses send, not send_timeout)
    assert resp_ring.send.call_count == 1
    sent_bytes = resp_ring.send.call_args[0][0]

    assert len(sent_bytes) > HEADER_SIZE, "Payload too short to contain header"

    # Verify header
    header = ResponseHeader.from_bytes(sent_bytes[:HEADER_SIZE])
    assert header.payload_len == len(sent_bytes) - HEADER_SIZE

    # Decode body
    codec = MessagePackCodec()
    body = codec.decode(sent_bytes[HEADER_SIZE:])
    assert body == {"id": "req-1", "ok": True}
