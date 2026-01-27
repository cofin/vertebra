import os
import sys
import time

import msgspec

# Ensure we can import vertebra
# sys.path.insert(0, os.path.abspath("src/py"))

try:
    from vertebra._vertebra import AsyncRing, shm_unlink
    from vertebra.messaging.protocol import Flags, RequestHeader
except ImportError:
    sys.exit(1)


def run_server() -> None:

    # Create rings
    slot_size = 65536
    slots = 16

    # Use names for now since we are simulating the Rust behavior of passing FDs
    # but we need to pass FDs to python.
    # So we create named rings, open them, get their FDs (if possible), or just use names for this test?
    # Worker expects FDs in managed mode.
    # AsyncRing.notifier_fd() returns the notification FD.
    # Does it expose the SHM FD? No, not easily in Python binding yet.
    # But wait, we added `from_raw_fd_handle` to open FROM fd.
    # We didn't add `to_raw_fd` or `shm_handle` to GET the fd.
    # `AsyncRing` struct in Rust has `region: Arc<ShmRegion>`.
    # We need to expose `region_fd()` on `AsyncRing` or similar to support this mock test.
    # OR we can update the test to use the legacy named mode?
    # The worker only enters managed mode if `VERTEBRA_REQ_SHM_FD` is set.

    # Let's test the legacy named mode for now to verify the worker logic itself
    # (parsing, scope construction, response).
    # Since we verified FD passing in the previous flow (ipc-worker-bridge) with a dummy script.

    # Actually, we can test managed mode if we can get FDs.
    # ShmRegion.handle() returns RawFd.
    # But AsyncRing wraps it.

    # For this test, let's use legacy named mode to keep it simple and verify logic.
    # We can pass `VERTEBRA_WORKER_ID` and ensure names match.

    worker_id = "0"
    name = "mock-server"
    req_name = f"/{name}.asgi.{worker_id}.client.mock.req"
    resp_name = f"/{name}.asgi.{worker_id}.client.mock.resp"
    ctrl_name = f"/{name}.asgi.{worker_id}.control"

    try:
        shm_unlink(req_name)
        shm_unlink(resp_name)
        shm_unlink(ctrl_name)
    except:
        pass

    ctrl_ring = AsyncRing.create_named(ctrl_name, slot_size, slots)
    req_ring = AsyncRing.create_named(req_name, slot_size, slots)
    resp_ring = AsyncRing.create_named(resp_name, slot_size, slots)

    # Start worker process
    env = os.environ.copy()
    env["VERTEBRA_WORKER_ID"] = worker_id

    # We use subprocess to run the worker
    cmd = [
        sys.executable,
        "-m",
        "vertebra.core.worker",
        "--app",
        "tools.test_app:app",
        "--name",
        name,
        "--host",
        "127.0.0.1",
        "--port",
        "8000",
    ]

    import subprocess

    worker_proc = subprocess.Popen(cmd, env=env)

    try:
        time.sleep(2)

        # 1. Register client
        codec = msgspec.msgpack.Encoder()

        reg_msg = {"kind": "register_client", "client_id": "mock", "req_ring": req_name, "resp_ring": resp_name}
        payload = codec.encode(reg_msg)
        header = RequestHeader(id=1, flags=0, payload_len=len(payload))
        ctrl_ring.send(header.to_bytes() + payload, 0)

        # Wait for ack on resp_ring? No, ack comes on resp_ring if we provided it?
        # `_send_control_ack` uses `response_ring`.

        # Read ack
        ack_data = None
        for _ in range(10):
            ack_data = resp_ring.try_recv()
            if ack_data:
                break
            time.sleep(0.1)

        if not ack_data:
            sys.exit(1)

        # 2. Send Request
        req_msg = {
            "id": "req-1",
            "method": "GET",
            "path": "/hello",
            "headers": [["host", "127.0.0.1:8000"]],
            "type": "http.request.start",
        }
        req_payload = codec.encode(req_msg)
        req_header = RequestHeader(id=2, flags=0, payload_len=len(req_payload))
        req_ring.send(req_header.to_bytes() + req_payload, 0)

        # Body end
        body_msg = {"id": "req-1", "type": "http.request", "body": b"", "more_body": False}
        body_payload = codec.encode(body_msg)
        body_header = RequestHeader(id=2, flags=0, payload_len=len(body_payload))
        req_ring.send(body_header.to_bytes() + body_payload, 0)

        # 3. Read Response
        decoder = msgspec.msgpack.Decoder()

        start_received = False
        body_received = False

        for _ in range(20):
            while True:
                chunk = resp_ring.try_recv()
                if not chunk:
                    break

                # Parse header
                # Note: Response also has header? Yes, ResponseHeader.
                # But PyAsyncRing.try_recv returns BYTES (payload only? No, full message? No, AsyncRing.try_recv returns payload)
                # Rust AsyncRing.try_recv returns `Ok(Some(len))`.
                # Python `try_recv` returns `bytes`.
                # Does it strip the header?
                # `channel.rs`: `recv` reads `SlotHeader` (internal) and copies payload.
                # `RequestHeader` is part of the payload from `AsyncRing`'s perspective?
                # The Rust server sends `RequestHeader` + `MsgPack` AS the payload.
                # The worker reads it.
                # The worker sends `ResponseHeader` + `MsgPack`.
                # So we need to parse it.

                header_bytes = chunk[:16]  # HEADER_SIZE
                # We can't parse ResponseHeader from python easily without struct or the class we just added?
                # We added ResponseHeader class in previous step.
                from vertebra.messaging.protocol import ResponseHeader as RespHeader

                RespHeader.from_bytes(header_bytes)
                msg_bytes = chunk[16:]
                msg = decoder.decode(msg_bytes)

                if msg.get("type") == "http.response.start":
                    start_received = True
                    assert msg["status"] == 200
                elif msg.get("type") == "http.response.body":
                    body_received = True
                    assert msg["body"] == b'{"hello":"world"}'

            if start_received and body_received:
                break
            time.sleep(0.1)

        if not (start_received and body_received):
            sys.exit(1)

    finally:
        worker_proc.terminate()
        worker_proc.wait()

        try:
            shm_unlink(req_name)
            shm_unlink(resp_name)
            shm_unlink(ctrl_name)
        except:
            pass


if __name__ == "__main__":
    run_server()
