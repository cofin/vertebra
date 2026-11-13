"""Fixtures launching embedded and SHM vertebra-server binaries over loopback sockets."""

import os
import signal
import socket
import subprocess
import sys
import textwrap
import time
from collections.abc import Iterator
from pathlib import Path

import msgspec
import pytest

import vertebra.cli as _vtb_cli
from vertebra.supervisor._supervisor import find_server_binary

__all__ = (
    "LiveServerHandle",
    "live_vertebra_server",
)

_ = _vtb_cli

_STREAMING_FIXTURE_MODULE = textwrap.dedent(
    """\
    import asyncio
    from collections.abc import AsyncIterator
    from typing import Any

    from vertebra._vertebra import RsgiHttpProtocol, RsgiScope
    from vertebra.framework._app import App
    from vertebra.server._asgi._sse import SseEvent, SseResponse

    _GATES: dict[str, asyncio.Event] = {}


    def _gate_for(stream_id: str) -> asyncio.Event:
        gate = _GATES.get(stream_id)
        if gate is None:
            gate = asyncio.Event()
            _GATES[stream_id] = gate
        return gate


    async def _asgi_sse_app(scope: Any, receive: Any, send: Any) -> None:
        raw_qs = scope.get("query_string", b"")
        qs = raw_qs.decode("latin-1") if isinstance(raw_qs, bytes) else str(raw_qs)
        stream_id = qs.removeprefix("id=") or "default-asgi"
        gate = _gate_for(stream_id)

        async def _events() -> AsyncIterator[SseEvent]:
            yield SseEvent(data="asgi-first", id="1", event="tick")
            await gate.wait()
            yield SseEvent(data="asgi-second", id="2", event="tick")

        resp = SseResponse(_events())
        await resp(scope, receive, send)


    class _RsgiStreamApp:
        async def __rsgi__(self, scope: RsgiScope, protocol: RsgiHttpProtocol) -> None:
            qs = scope.query_string or ""
            stream_id = qs.removeprefix("id=") or "default-rsgi"
            gate = _gate_for(stream_id)
            stream = protocol.response_stream(
                200,
                [("content-type", "text/event-stream"), ("cache-control", "no-cache")],
            )
            await stream.send_str("data: rsgi-first\\n\\n")
            await gate.wait()
            await stream.send_bytes(b"data: rsgi-second\\n\\n")


    app = App()


    @app.get("/health")
    def health() -> bytes:
        return b"ok"


    @app.post("/gate/open")
    async def open_gate(id: str = "default") -> bytes:
        _gate_for(id).set()
        return b"opened"


    @app.get("/native/sse")
    async def native_sse(id: str = "default-native") -> tuple[int, dict[str, str], AsyncIterator[bytes]]:
        gate = _gate_for(id)

        async def _chunks() -> AsyncIterator[bytes]:
            yield b"data: native-first\\n\\n"
            await gate.wait()
            yield b"data: native-second\\n\\n"

        return (
            200,
            {"content-type": "text/event-stream", "cache-control": "no-cache"},
            _chunks(),
        )


    app.mount("/asgi", _asgi_sse_app, protocol="asgi")
    app.mount("/rsgi", _RsgiStreamApp(), protocol="rsgi")
    """
)


class LiveServerHandle(msgspec.Struct, frozen=True):
    """Connection metadata for a running vertebra-server subprocess."""

    host: str
    port: int
    topology: str


@pytest.fixture(scope="module", params=["embedded", "shm"])
def live_vertebra_server(
    request: pytest.FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[LiveServerHandle]:
    """Start vertebra-server in embedded or SHM mode against the streaming fixture app."""
    topology = str(request.param)
    repo_root = Path(__file__).resolve().parents[5]
    try:
        binary_path = find_server_binary()
    except SystemExit:
        pytest.skip("vertebra-server binary not built yet (https://github.com/cofin/vertebra/issues/82)")

    app_dir = tmp_path_factory.mktemp(f"vtb_srv_{topology}")
    module_name = f"vtb_live_stream_app_{topology}"
    (app_dir / f"{module_name}.py").write_text(_STREAMING_FIXTURE_MODULE, encoding="utf-8")

    port = _reserve_ephemeral_port()
    env = dict(os.environ)
    env["VERTEBRA_SERVER_BINARY"] = str(binary_path)
    env["PYTHONHOME"] = sys.base_prefix
    env["VERTEBRA_SHM_PROFILE"] = "compact"
    py_paths = [str(app_dir), str(repo_root / "src" / "py"), *(p for p in sys.path if p)]
    env["PYTHONPATH"] = os.pathsep.join(py_paths)

    cmd = [
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
        f"{module_name}:app",
    ]
    proc = subprocess.Popen(
        cmd,
        cwd=str(repo_root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_until_healthy("127.0.0.1", port, proc)
        yield LiveServerHandle(host="127.0.0.1", port=port, topology=topology)
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2.0)


def _reserve_ephemeral_port() -> int:
    """Reserve and release an ephemeral localhost TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_until_healthy(host: str, port: int, proc: subprocess.Popen[str]) -> None:
    """Poll GET /health until HTTP 200 is returned or fail with subprocess output."""
    deadline = time.monotonic() + 15.0
    request_bytes = f"GET /health HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n".encode("ascii")
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            stdout, stderr = proc.communicate(timeout=2.0)
            msg = f"vertebra-server exited prematurely (code={proc.returncode}):\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
            raise RuntimeError(msg)
        try:
            with socket.create_connection((host, port), timeout=0.5) as conn:
                conn.sendall(request_bytes)
                raw = conn.recv(4096)
                if raw.startswith(b"HTTP/1.1 200") and b"ok" in raw:
                    return
        except OSError:
            pass
        time.sleep(0.05)

    proc.kill()
    stdout, stderr = proc.communicate(timeout=2.0)
    msg = f"Timed out waiting for vertebra-server on {host}:{port}:\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    raise TimeoutError(msg)
