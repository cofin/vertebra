"""Fixtures launching embedded and SHM vertebra-server binaries over loopback sockets."""

import contextlib
import errno
import os
import selectors
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
from vertebra.supervisor._supervisor import EmbeddedPythonRuntime, find_server_binary

__all__ = (
    "LiveServerHandle",
    "live_vertebra_server",
)

_ = _vtb_cli

_STREAMING_FIXTURE_MODULE = textwrap.dedent(
    """\
    import asyncio
    import threading
    from collections.abc import AsyncIterator
    from typing import Any

    from vertebra._vertebra import RsgiHttpProtocol, RsgiScope
    from vertebra.framework._app import App
    from vertebra.server._asgi._sse import SseEvent, SseResponse


    class _CrossThreadGate:
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self._is_set = False
            self._waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = []

        async def wait(self) -> None:
            loop = asyncio.get_running_loop()
            ev = asyncio.Event()
            with self._lock:
                if self._is_set:
                    return
                self._waiters.append((loop, ev))
            await ev.wait()

        def set(self) -> None:
            with self._lock:
                self._is_set = True
                waiters = list(self._waiters)
                self._waiters.clear()
            for loop, ev in waiters:
                loop.call_soon_threadsafe(ev.set)


    _GATES_LOCK = threading.Lock()
    _GATES: dict[str, _CrossThreadGate] = {}


    def _gate_for(stream_id: str) -> _CrossThreadGate:
        with _GATES_LOCK:
            gate = _GATES.get(stream_id)
            if gate is None:
                gate = _CrossThreadGate()
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
    EmbeddedPythonRuntime.detect().apply(env)
    env["VERTEBRA_SHM_PROFILE"] = "compact"
    vtb_pkg_root = str(Path(_vtb_cli.__file__).resolve().parents[2])
    py_paths = [str(app_dir), vtb_pkg_root, *(p for p in sys.path if p), str(repo_root / "src" / "py")]
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
        start_new_session=True,
    )
    try:
        _wait_until_healthy("127.0.0.1", port, proc)
        yield LiveServerHandle(host="127.0.0.1", port=port, topology=topology)
    finally:
        if proc.poll() is None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=2.0)


def _reserve_ephemeral_port() -> int:
    """Reserve and release an ephemeral localhost TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _probe_health_nonblocking(
    selector: selectors.BaseSelector,
    host: str,
    port: int,
    request_bytes: bytes,
    deadline: float,
) -> bool:
    """Attempt one non-blocking HTTP /health handshake bounded by ``deadline``."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setblocking(False)
    try:
        err = sock.connect_ex((host, port))
        if err not in (0, errno.EINPROGRESS, errno.EALREADY, errno.EWOULDBLOCK):
            return False
        selector.register(sock, selectors.EVENT_WRITE)
        while time.monotonic() < deadline:
            remaining = min(0.05, max(0.0, deadline - time.monotonic()))
            events = selector.select(timeout=remaining)
            for key, _ in events:
                if key.fileobj is sock:
                    so_err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                    if so_err != 0:
                        return False
                    sock.sendall(request_bytes)
                    selector.modify(sock, selectors.EVENT_READ)
                    break
            else:
                continue
            break
        else:
            return False

        response = bytearray()
        while time.monotonic() < deadline:
            remaining = min(0.05, max(0.0, deadline - time.monotonic()))
            events = selector.select(timeout=remaining)
            for key, _ in events:
                if key.fileobj is sock:
                    chunk = sock.recv(4096)
                    if not chunk:
                        return response.startswith(b"HTTP/1.1 200") and b"ok" in response
                    response.extend(chunk)
                    if response.startswith(b"HTTP/1.1 200") and b"ok" in response:
                        return True
        return False
    except OSError:
        return False
    finally:
        with contextlib.suppress(KeyError, ValueError):
            selector.unregister(sock)
        sock.close()


def _wait_until_healthy(host: str, port: int, proc: subprocess.Popen[str]) -> None:
    """Wait for HTTP 200 /health readiness via selector events or fail with subprocess output."""
    deadline = time.monotonic() + 15.0
    request_bytes = f"GET /health HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n".encode("ascii")
    captured: dict[str, list[bytes]] = {"stdout": [], "stderr": []}
    with selectors.DefaultSelector() as selector:
        for stream_name, pipe in (("stdout", proc.stdout), ("stderr", proc.stderr)):
            if pipe is not None:
                os.set_blocking(pipe.fileno(), False)
                selector.register(pipe, selectors.EVENT_READ, data=stream_name)

        while time.monotonic() < deadline:
            if proc.poll() is not None:
                stdout_tail, stderr_tail = proc.communicate(timeout=2.0)
                stdout = b"".join(captured["stdout"]).decode("utf-8", errors="replace") + stdout_tail
                stderr = b"".join(captured["stderr"]).decode("utf-8", errors="replace") + stderr_tail
                msg = (
                    f"vertebra-server exited prematurely (code={proc.returncode}):\n"
                    f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
                )
                raise RuntimeError(msg)

            if _probe_health_nonblocking(selector, host, port, request_bytes, min(deadline, time.monotonic() + 0.5)):
                return

            remaining = min(0.05, max(0.0, deadline - time.monotonic()))
            for key, _ in selector.select(timeout=remaining):
                if isinstance(key.data, str):
                    fd = key.fd
                    try:
                        chunk = os.read(fd, 4096)
                    except BlockingIOError:
                        continue
                    if not chunk:
                        selector.unregister(key.fileobj)
                    else:
                        captured[key.data].append(chunk)

    proc.kill()
    stdout_tail, stderr_tail = proc.communicate(timeout=2.0)
    stdout = b"".join(captured["stdout"]).decode("utf-8", errors="replace") + stdout_tail
    stderr = b"".join(captured["stderr"]).decode("utf-8", errors="replace") + stderr_tail
    msg = f"Timed out waiting for vertebra-server on {host}:{port}:\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    raise TimeoutError(msg)
