"""Vite fallback subprocess manager for Vertebra.

When the native Rust bundler is unavailable (or the user explicitly
configures ``bundler = "vite"``), this module manages a Vite dev-server
subprocess and provides a one-shot ``run_vite_build`` helper for
production builds.
"""

import contextlib
import logging
import os
import shutil
import signal
import socket
import subprocess
import time
from pathlib import Path

__all__ = ("DevServerProcess", "run_vite_build")

logger = logging.getLogger(__name__)

_STARTUP_TIMEOUT = 30.0
_SHUTDOWN_TIMEOUT = 5.0


def _resolve_vite_argv(*args: str) -> list[str]:
    """Resolve the command argument list for invoking Vite."""
    vite_bin = shutil.which("vite")
    if vite_bin:
        return [vite_bin, *args]

    npx_bin = shutil.which("npx")
    if npx_bin:
        return [npx_bin, "vite", *args]

    msg = "Neither 'vite' nor 'npx' found on PATH (Vite not found)"
    raise RuntimeError(msg)


class DevServerProcess:
    """Manage a Vite dev server subprocess lifecycle."""

    __slots__ = ("_port", "_process", "_project_dir")

    def __init__(self, project_dir: Path, port: int) -> None:
        self._project_dir = project_dir
        self._port = port
        self._process: subprocess.Popen[str] | None = None

    @property
    def proxy_url(self) -> str:
        """URL to proxy requests to while the dev server is running."""
        return f"http://127.0.0.1:{self._port}"

    @property
    def is_running(self) -> bool:
        """Return ``True`` if the subprocess is still alive."""
        return self._process is not None and self._process.poll() is None

    def start(self) -> None:
        """Start the Vite dev server and block until it accepts connections."""
        argv = self._resolve_argv()
        env = {**os.environ, "BROWSER": "none", "PORT": str(self._port)}

        self._process = subprocess.Popen(
            argv,
            cwd=self._project_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )

        if not self._wait_for_ready():
            self.stop()
            msg = f"Vite failed to start on port {self._port}"
            raise RuntimeError(msg)

        logger.info("Vite dev server ready on port %d", self._port)

    def stop(self) -> None:
        """Gracefully stop the Vite subprocess, force-killing on timeout."""
        process = self._process
        if process is None or process.poll() is not None:
            self._process = None
            return

        self._process = None
        logger.info("Stopping Vite (pid %d)", process.pid)

        try:
            if hasattr(os, "killpg"):
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            else:
                process.terminate()
        except (OSError, ProcessLookupError):
            return

        try:
            process.wait(timeout=_SHUTDOWN_TIMEOUT)
        except subprocess.TimeoutExpired:
            try:
                if hasattr(os, "killpg"):
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                else:
                    process.kill()
            except (OSError, ProcessLookupError):
                pass
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=2)

    def _resolve_argv(self) -> list[str]:
        return _resolve_vite_argv("--port", str(self._port))

    def _resolve_command(self) -> str:
        return " ".join(self._resolve_argv())

    def _wait_for_ready(self, timeout: float = _STARTUP_TIMEOUT) -> bool:
        deadline = time.monotonic() + timeout
        interval = 0.1

        while time.monotonic() < deadline:
            if self._process is not None and self._process.poll() is not None:
                return False

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                try:
                    s.connect(("127.0.0.1", self._port))
                except (ConnectionRefusedError, OSError):
                    pass
                else:
                    return True

            time.sleep(interval)
            interval = min(interval * 1.5, 1.0)

        return False


def run_vite_build(project_dir: Path) -> None:
    """Run a one-shot Vite production build."""
    cmd = _resolve_vite_argv("build")
    result = subprocess.run(cmd, cwd=project_dir, check=False)
    if result.returncode != 0:
        msg = f"Vite build exited with status {result.returncode}"
        raise RuntimeError(msg)
