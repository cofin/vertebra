"""End-to-end streaming acceptance tests against live embedded and SHM vertebra-server."""

import socket

import pytest

from tests.integration.server.conftest import LiveServerHandle


@pytest.mark.parametrize(
    ("route_path", "expected_first", "expected_second"),
    [
        ("/asgi/sse", b"asgi-first", b"asgi-second"),
        ("/rsgi/stream", b"data: rsgi-first\n\n", b"data: rsgi-second\n\n"),
        ("/native/sse", b"data: native-first\n\n", b"data: native-second\n\n"),
    ],
)
def test_vertebra_server_progressive_streaming_before_gate_release(
    live_vertebra_server: LiveServerHandle,
    route_path: str,
    expected_first: bytes,
    expected_second: bytes,
) -> None:
    """Verify first stream chunk arrives over TCP while the async gate remains open."""
    stream_id = f"{live_vertebra_server.topology}-{route_path.strip('/').replace('/', '-')}"
    request_head = (
        f"GET {route_path}?id={stream_id} HTTP/1.1\r\n"
        f"Host: {live_vertebra_server.host}:{live_vertebra_server.port}\r\n"
        "Accept: text/event-stream\r\n"
        "Connection: close\r\n\r\n"
    ).encode("ascii")

    with socket.create_connection((live_vertebra_server.host, live_vertebra_server.port), timeout=5.0) as stream_sock:
        stream_sock.settimeout(5.0)
        stream_sock.sendall(request_head)

        received_before_gate = _recv_until_contains(stream_sock, expected_first)
        assert received_before_gate.startswith(b"HTTP/1.1 200")
        header_part = received_before_gate.split(b"\r\n\r\n", 1)[0].lower()
        assert b"content-type: text/event-stream" in header_part
        assert expected_second not in received_before_gate

        _release_gate(live_vertebra_server, stream_id)

        remaining = _recv_until_eof(stream_sock)
        combined = received_before_gate + remaining
        assert expected_second in combined


def _recv_until_contains(sock: socket.socket, marker: bytes) -> bytes:
    """Read from sock until marker appears in the accumulated byte stream."""
    buf = bytearray()
    while marker not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            msg = f"Socket closed before marker {marker!r} arrived; received={bytes(buf)!r}"
            raise AssertionError(msg)
        buf.extend(chunk)
    return bytes(buf)


def _recv_until_eof(sock: socket.socket) -> bytes:
    """Read remaining bytes from sock until EOF."""
    buf = bytearray()
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf.extend(chunk)
    return bytes(buf)


def _release_gate(handle: LiveServerHandle, stream_id: str) -> None:
    """Open the server-side asyncio.Event gate for stream_id over a separate HTTP connection."""
    req = (
        f"POST /gate/open?id={stream_id} HTTP/1.1\r\n"
        f"Host: {handle.host}:{handle.port}\r\n"
        "Content-Length: 0\r\n"
        "Connection: close\r\n\r\n"
    ).encode("ascii")
    with socket.create_connection((handle.host, handle.port), timeout=5.0) as gate_sock:
        gate_sock.settimeout(5.0)
        gate_sock.sendall(req)
        resp = _recv_until_eof(gate_sock)
        assert resp.startswith(b"HTTP/1.1 200"), f"Gate release failed: {resp!r}"
        assert b"opened" in resp
