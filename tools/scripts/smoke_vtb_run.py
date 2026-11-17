"""Isolated wheel smoke harness for packaged ``vtb run`` execution outside the repository."""

from __future__ import annotations

import argparse
import http.client
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "SmokeScenario",
    "VtbRunSmoke",
    "main",
)

_LIBPYTHON_DLL_RE = re.compile(r"^python3\d+t?\.dll$", re.IGNORECASE)

_SAMPLE_APP_SOURCE = '''"""Self-contained smoke test app executed outside the Vertebra repo."""

import asyncio
from typing import Any

from vertebra import App
from vertebra.framework.queues import QueueService

app = App()
app.state["vertebra_queue_service"] = QueueService()


@app.get("/health")
async def health() -> dict[str, str]:
    """Return a unary JSON health payload."""
    return {"status": "ok"}


async def asgi_sse_app(
    scope: dict[str, Any],
    receive: Any,
    send: Any,
) -> None:
    """Progressive ASGI streaming application emitting two spaced SSE events."""
    if scope["type"] == "lifespan":
        msg = await receive()
        if msg["type"] == "lifespan.startup":
            await send({"type": "lifespan.startup.complete"})
            msg = await receive()
        if msg["type"] == "lifespan.shutdown":
            await send({"type": "lifespan.shutdown.complete"})
        return

    if scope["type"] != "http":
        return

    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"content-type", b"text/event-stream"),
                (b"cache-control", b"no-cache"),
            ],
        }
    )
    await send({"type": "http.response.body", "body": b"data: chunk-1\\n\\n", "more_body": True})
    await asyncio.sleep(0.25)
    await send({"type": "http.response.body", "body": b"data: chunk-2\\n\\n", "more_body": False})
'''


class SmokeScenario(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for a single wheel-installed ``vtb run`` smoke scenario."""

    name: str
    args: tuple[str, ...]
    request_path: str
    posix_only: bool = False
    expect_progressive: bool = False
    expected_status: int = 200
    expected_substring: str = ""


class VtbRunSmoke:
    """Verify wheel-installed ``vtb run`` outside the source repository across server topologies."""

    def __init__(self, vtb_executable: Path, workdir: Path) -> None:
        self.vtb_executable = vtb_executable
        self.workdir = workdir

    @staticmethod
    def default_scenarios(*, platform: str | None = None) -> tuple[SmokeScenario, ...]:
        """Return the smoke matrix filtered for the target operating system."""
        active_platform = platform if platform is not None else sys.platform
        scenarios = (
            SmokeScenario(
                name="embedded_unary",
                args=("run", "--topology", "embedded", "--no-banner", "smoke_app:app"),
                request_path="/health",
                expected_substring="ok",
            ),
            SmokeScenario(
                name="progressive_sse",
                args=("run", "--topology", "embedded", "--interface", "asgi", "--no-banner", "smoke_app:asgi_sse_app"),
                request_path="/stream",
                expect_progressive=True,
                expected_substring="chunk-2",
            ),
            SmokeScenario(
                name="shm_workers",
                args=("run", "--topology", "shm", "--workers", "2", "--no-banner", "smoke_app:app"),
                request_path="/health",
                posix_only=True,
                expected_substring="ok",
            ),
            SmokeScenario(
                name="worker_only_healthz",
                args=("run", "--worker-only", "--no-banner", "smoke_app:app"),
                request_path="/healthz",
                expected_substring="ok",
            ),
        )
        if active_platform == "win32":
            return tuple(item for item in scenarios if not item.posix_only)
        return scenarios

    @staticmethod
    def verify_wheel_archive(wheel_path: Path, *, platform: str | None = None) -> list[str]:
        """Verify the core wheel includes ``vertebra/bin/vertebra-server`` and omits ``libpython``."""
        active_platform = platform if platform is not None else sys.platform
        expected_binary = (
            "vertebra/bin/vertebra-server.exe" if active_platform == "win32" else "vertebra/bin/vertebra-server"
        )
        with zipfile.ZipFile(wheel_path) as archive:
            names = archive.namelist()
        if expected_binary not in names:
            msg = f"Wheel {wheel_path.name} is missing packaged server binary {expected_binary!r}"
            raise ValueError(msg)
        bundled_libpython = [
            name
            for name in names
            if Path(name).name.startswith("libpython") or bool(_LIBPYTHON_DLL_RE.match(Path(name).name))
        ]
        if bundled_libpython:
            msg = f"Wheel {wheel_path.name} unexpectedly bundled libpython artifacts: {bundled_libpython}"
            raise ValueError(msg)
        return [name for name in names if name.startswith("vertebra/bin/")]

    def write_sample_app(self) -> Path:
        """Write a self-contained ``smoke_app.py`` outside the repository tree."""
        self.workdir.mkdir(parents=True, exist_ok=True)
        app_path = self.workdir / "smoke_app.py"
        app_path.write_text(_SAMPLE_APP_SOURCE, encoding="utf-8")
        return app_path

    @staticmethod
    def find_free_port() -> int:
        """Reserve and return an available IPv4 loopback TCP port."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            sock.listen(1)
            port = int(sock.getsockname()[1])
        return port

    @staticmethod
    def format_early_exit(
        scenario: SmokeScenario,
        returncode: int | None,
        stdout: str,
        stderr: str,
    ) -> str:
        """Format diagnostic context when a smoke server process exits before readiness."""
        return (
            f"Smoke scenario {scenario.name!r} exited prematurely with code {returncode}.\n"
            f"--- STDOUT ---\n{stdout.strip() or '<empty>'}\n"
            f"--- STDERR ---\n{stderr.strip() or '<empty>'}"
        )

    def _spawn_env(self) -> dict[str, str]:
        """Build an isolated subprocess environment exercising the wheel-packaged binary."""
        env = dict(os.environ)
        env.pop("VERTEBRA_SERVER_BINARY", None)
        env.pop("PYTHONPATH", None)
        env["VERTEBRA_SHM_PROFILE"] = "compact"
        return env

    def _request_once(
        self,
        scenario: SmokeScenario,
        port: int,
    ) -> tuple[int, str, float | None, float]:
        """Execute a single HTTP request, optionally measuring progressive SSE chunk arrival."""
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10.0)
        try:
            start = time.monotonic()
            conn.request("GET", scenario.request_path)
            resp = conn.getresponse()
            if not scenario.expect_progressive:
                body = resp.read().decode("utf-8", errors="replace")
                return resp.status, body, None, time.monotonic() - start

            chunks: list[bytes] = []
            first_chunk_elapsed: float | None = None
            while True:
                chunk = resp.read1(64)
                if not chunk:
                    break
                if first_chunk_elapsed is None:
                    first_chunk_elapsed = time.monotonic() - start
                chunks.append(chunk)
            total_elapsed = time.monotonic() - start
            return resp.status, b"".join(chunks).decode("utf-8", errors="replace"), first_chunk_elapsed, total_elapsed
        finally:
            conn.close()

    def _terminate_process(self, proc: subprocess.Popen[str]) -> None:
        """Gracefully terminate the running ``vtb run`` process tree."""
        if proc.poll() is not None:
            return
        if os.name == "nt":
            proc.terminate()
        else:
            proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5.0)

    def run_scenario(self, scenario: SmokeScenario, *, startup_timeout_s: float = 20.0) -> None:
        """Launch ``vtb run`` for ``scenario``, assert HTTP/streaming behavior, and verify clean drain."""
        port = self.find_free_port()
        cmd = [str(self.vtb_executable), *scenario.args, "--host", "127.0.0.1", "--port", str(port)]
        proc = subprocess.Popen(
            cmd,
            cwd=str(self.workdir),
            env=self._spawn_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            deadline = time.monotonic() + startup_timeout_s
            last_error: Exception | None = None
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    stdout, stderr = proc.communicate(timeout=2.0)
                    raise RuntimeError(self.format_early_exit(scenario, proc.returncode, stdout, stderr))
                try:
                    status, body, first_elapsed, total_elapsed = self._request_once(scenario, port)
                except OSError as exc:
                    last_error = exc
                    time.sleep(0.1)
                    continue
                if status != scenario.expected_status:
                    msg = f"Scenario {scenario.name!r} returned HTTP {status}, expected {scenario.expected_status}: {body!r}"
                    raise AssertionError(msg)
                if scenario.expected_substring and scenario.expected_substring not in body:
                    msg = f"Scenario {scenario.name!r} response missing {scenario.expected_substring!r}: {body!r}"
                    raise AssertionError(msg)
                if scenario.expect_progressive and (first_elapsed is None or total_elapsed - first_elapsed < 0.08):
                    msg = (
                        f"Scenario {scenario.name!r} buffered SSE chunks instead of streaming progressively "
                        f"(first={first_elapsed}, total={total_elapsed})"
                    )
                    raise AssertionError(msg)
                break
            else:
                stdout = ""
                stderr = ""
                self._terminate_process(proc)
                if proc.stdout is not None:
                    stdout = proc.stdout.read()
                if proc.stderr is not None:
                    stderr = proc.stderr.read()
                detail = f"Timed out waiting for scenario {scenario.name!r} on port {port} (last_error={last_error})"
                raise TimeoutError(f"{detail}\n{self.format_early_exit(scenario, proc.returncode, stdout, stderr)}")
        finally:
            self._terminate_process(proc)

    def run_all(
        self,
        *,
        wheel_path: Path | None = None,
        wheel_paths: Sequence[Path] = (),
        verify_wheel_only: bool = False,
    ) -> None:
        """Run optional wheel inspection followed by every platform-applicable smoke scenario."""
        all_wheels: list[Path] = []
        if wheel_path is not None:
            all_wheels.append(wheel_path)
        all_wheels.extend(wheel_paths)
        for path in all_wheels:
            self.verify_wheel_archive(path)
        if verify_wheel_only:
            return
        self.write_sample_app()
        for scenario in self.default_scenarios():
            self.run_scenario(scenario)


def _resolve_wheel_paths(wheel: Path | None, wheel_dir: Path | None) -> tuple[Path, ...]:
    """Resolve one or more built ``vertebra_core-*.whl`` paths from direct flag or directory."""
    resolved: list[Path] = []
    if wheel is not None:
        resolved.append(wheel.resolve())
    if wheel_dir is not None:
        candidates = sorted(wheel_dir.resolve().glob("vertebra_core-*.whl"))
        if not candidates:
            msg = f"Expected at least 1 vertebra_core-*.whl in {wheel_dir}, found 0"
            raise FileNotFoundError(msg)
        resolved.extend(candidates)
    return tuple(dict.fromkeys(resolved))


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI options for isolated ``vtb run`` wheel smoke verification."""
    parser = argparse.ArgumentParser(description="Smoke test a wheel-installed vtb run executable.")
    parser.add_argument("--vtb", type=Path, required=True, help="Path to installed vtb CLI executable")
    parser.add_argument("--wheel", type=Path, default=None, help="Path to vertebra_core wheel to inspect")
    parser.add_argument("--wheel-dir", type=Path, default=None, help="Directory containing vertebra_core-*.whl")
    parser.add_argument("--workdir", type=Path, default=None, help="External temporary app directory")
    parser.add_argument(
        "--verify-wheel-only", action="store_true", help="Inspect wheel archive without running scenarios"
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run wheel verification and isolated ``vtb run`` smoke scenarios."""
    args = _parse_args(argv)
    wheel_paths = _resolve_wheel_paths(args.wheel, args.wheel_dir)
    if args.workdir is not None:
        smoke = VtbRunSmoke(vtb_executable=args.vtb.resolve(), workdir=args.workdir.resolve())
        smoke.run_all(wheel_paths=wheel_paths, verify_wheel_only=args.verify_wheel_only)
        return 0

    with tempfile.TemporaryDirectory(prefix="vtb-smoke-") as tmp_dir:
        smoke = VtbRunSmoke(vtb_executable=args.vtb.resolve(), workdir=Path(tmp_dir))
        smoke.run_all(wheel_paths=wheel_paths, verify_wheel_only=args.verify_wheel_only)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
