"""SSR execution backends for Vertebra.

Two modes:
- Native: rquickjs QuickJS pool in the vertebra-bundler Rust crate (default)
- Sidecar: Managed Node/Bun subprocess using Inertia SSR protocol (fallback)
"""

import json
import logging
import os
import shutil
import signal
import socket
import subprocess
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@dataclass
class SsrResult:
    """Result of an SSR render call."""

    head: list[str]
    body: str


class SsrClient(Protocol):
    """Unified SSR interface -- both native and sidecar implement this."""

    def render(self, page: dict[str, Any]) -> SsrResult: ...
    def invalidate(self, paths: list[str]) -> None: ...
    def stop(self) -> None: ...


class NativeSsrClient:
    """SSR via embedded QuickJS in the vertebra-bundler crate."""

    def __init__(self, bundle_path: str, pool_size: int = 4) -> None:
        from vertebra._bundler import RolldownBundler

        self._bundler = RolldownBundler()
        self._bundler.init_ssr(bundle_path, pool_size)

    def render(self, page: dict[str, Any]) -> SsrResult:
        result = self._bundler.ssr_render(
            page.get("component", ""),
            json.dumps(page),
        )
        return SsrResult(head=result["head"], body=result["body"])

    def invalidate(self, paths: list[str]) -> None:
        self._bundler.ssr_invalidate(paths)

    def stop(self) -> None:
        self._bundler.ssr_shutdown()


class SidecarSsrClient:
    """SSR via standard Inertia SSR server (Node/Bun subprocess).

    Manages the lifecycle of a Node/Bun process running the Inertia
    SSR server. Communicates via HTTP POST to /render.
    """

    _STARTUP_TIMEOUT: float = 30.0
    _SHUTDOWN_TIMEOUT: float = 5.0

    def __init__(self, entry: str, port: int = 13714) -> None:
        self._entry = entry
        self._port = port
        self._process: subprocess.Popen[str] | None = None
        self._start()

    @property
    def _base_url(self) -> str:
        return f"http://127.0.0.1:{self._port}"

    def _start(self) -> None:
        runtime = shutil.which("bun") or shutil.which("node")
        if not runtime:
            msg = "Node.js or Bun required for SSR sidecar mode"
            raise RuntimeError(msg)

        cmd = f"{runtime} {self._entry}"
        env = {**os.environ, "PORT": str(self._port)}

        self._process = subprocess.Popen(
            cmd,
            shell=True,  # noqa: S602
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            preexec_fn=os.setsid if hasattr(os, "setsid") else None,  # noqa: PLW1509
        )

        if not self._wait_for_health():
            self.stop()
            msg = f"SSR server failed to start on port {self._port}"
            raise RuntimeError(msg)

        logger.info("SSR sidecar ready on port %d", self._port)

    def render(self, page: dict[str, Any]) -> SsrResult:
        import urllib.request

        data = json.dumps(page).encode()
        req = urllib.request.Request(
            f"{self._base_url}/render",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310
                result = json.loads(resp.read())
                return SsrResult(
                    head=result.get("head", []),
                    body=result.get("body", ""),
                )
        except Exception as e:
            msg = f"SSR sidecar render failed: {e}"
            raise RuntimeError(msg) from e

    def invalidate(self, paths: list[str]) -> None:
        pass  # Sidecar handles its own module invalidation

    def stop(self) -> None:
        # Try graceful shutdown via /shutdown endpoint
        try:
            import urllib.request

            req = urllib.request.Request(
                f"{self._base_url}/shutdown",
                data=b"",
                method="POST",
            )
            urllib.request.urlopen(req, timeout=1)  # noqa: S310
        except Exception:  # noqa: BLE001
            pass

        process = self._process
        if process is None or process.poll() is not None:
            return
        self._process = None

        try:
            if hasattr(os, "killpg"):
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            else:
                process.terminate()
            process.wait(timeout=self._SHUTDOWN_TIMEOUT)
        except subprocess.TimeoutExpired:
            try:
                if hasattr(os, "killpg"):
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                else:
                    process.kill()
            except (OSError, ProcessLookupError):
                pass
        except (OSError, ProcessLookupError):
            pass

    def _wait_for_health(self, timeout: float | None = None) -> bool:
        if timeout is None:
            timeout = self._STARTUP_TIMEOUT
        deadline = time.monotonic() + timeout
        interval = 0.1
        while time.monotonic() < deadline:
            if self._process is not None and self._process.poll() is not None:
                return False
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                try:
                    s.connect(("127.0.0.1", self._port))
                    return True
                except (ConnectionRefusedError, OSError):
                    pass
            time.sleep(interval)
            interval = min(interval * 1.5, 1.0)
        return False


def create_ssr_client(
    *,
    mode: str = "native",
    entry: str | None = None,
    pool_size: int = 4,
    sidecar_port: int = 13714,
) -> SsrClient | None:
    """Factory: create the appropriate SSR client based on mode.

    Falls back from native to sidecar if native extension unavailable.
    """
    if not entry:
        return None

    if mode == "native":
        try:
            return NativeSsrClient(entry, pool_size)
        except ImportError:
            logger.warning("Native SSR (vertebra._bundler) unavailable. Falling back to sidecar mode.")
            return SidecarSsrClient(entry, sidecar_port)
        except Exception:  # noqa: BLE001
            logger.exception("Native SSR init failed, falling back to sidecar")
            return SidecarSsrClient(entry, sidecar_port)

    return SidecarSsrClient(entry, sidecar_port)
