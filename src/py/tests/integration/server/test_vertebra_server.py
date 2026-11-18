"""End-to-end streaming acceptance tests against live embedded and SHM vertebra-server."""

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.integration.server.conftest import LiveServerHandle, _reserve_ephemeral_port
from vertebra.supervisor import EmbeddedPythonRuntime, find_server_binary


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


@pytest.mark.parametrize("topology", ["embedded", "shm"])
def test_vertebra_server_aborts_startup_when_lifespan_fails(
    tmp_path: Path,
    topology: str,
) -> None:
    """Both embedded and SHM server topologies abort startup with non-zero exit when lifespan fails."""
    repo_root = Path(__file__).resolve().parents[5]
    try:
        binary_path = find_server_binary()
    except SystemExit:
        pytest.skip("vertebra-server binary not built yet")

    app_file = tmp_path / "failing_lifespan_app.py"
    app_file.write_text(
        "async def app(scope, receive, send):\n"
        "    if scope['type'] == 'lifespan':\n"
        "        await receive()\n"
        "        await send({'type': 'lifespan.startup.failed', 'message': 'startup broken'})\n"
        "        return\n"
        "    await send({'type': 'http.response.start', 'status': 200, 'headers': []})\n"
        "    await send({'type': 'http.response.body', 'body': b'ok', 'more_body': False})\n",
        encoding="utf-8",
    )

    port = _reserve_ephemeral_port()
    env = dict(os.environ)
    env["VERTEBRA_SERVER_BINARY"] = str(binary_path)
    EmbeddedPythonRuntime.detect().apply(env)
    env["VERTEBRA_SHM_PROFILE"] = "compact"
    py_paths = [str(tmp_path), str(repo_root / "src" / "py"), *(p for p in sys.path if p)]
    env["PYTHONPATH"] = os.pathsep.join(py_paths)

    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "from vertebra.cli import cli; cli()",
            "run",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--workers",
            "1",
            "--topology",
            topology,
            "--no-banner",
            "failing_lifespan_app:app",
        ],
        cwd=str(repo_root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    start_time = time.monotonic()
    try:
        stdout, stderr = proc.communicate(timeout=10.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        msg = f"vertebra-server ({topology}) hung or respawned infinitely on failing lifespan"
        raise AssertionError(msg) from None

    elapsed = time.monotonic() - start_time
    assert proc.returncode != 0, f"Expected non-zero exit for {topology}; stdout={stdout} stderr={stderr}"
    assert elapsed < 10.0
    combined_output = f"{stdout}\n{stderr}"
    assert "No module named '_socket'" not in combined_output
    assert "startup broken" in combined_output or "WorkerStartupError" in combined_output or "78" in combined_output
