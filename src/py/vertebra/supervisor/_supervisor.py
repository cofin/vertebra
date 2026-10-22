"""Process supervisor for the Vertebra server.

Manages the lifecycle of the Rust HTTP server binary.
"""

import contextlib
import logging
import os
import signal
import subprocess
import threading
from pathlib import Path

from vertebra.cli.banner import render_shutdown
from vertebra.cli.tui import LogBuffer, run_dev_tui, stderr_reader
from vertebra.server.platform import ShmAllocationPolicy

__all__ = (
    "build_server_command",
    "find_repo_root",
    "find_server_binary",
    "run_supervisor",
)

logger = logging.getLogger("vertebra.supervisor")


def run_supervisor(
    *,
    server_cmd: list[str],
    env: dict[str, str],
    drain_timeout_ms: int = 5000,
    shutdown_timeout_ms: int = 30000,
    tui: bool = False,
) -> int:
    """Spawn the Rust server binary and wait for it to exit.

    When ``tui=True``, stderr is piped and fed into a Rich Live display
    for the dev TUI experience. When ``tui=False`` (default), stderr is
    inherited from the parent process.
    """
    if not server_cmd:
        msg = "server_cmd must not be empty"
        raise ValueError(msg)

    ShmAllocationPolicy().validate_environment()

    full_env = os.environ | env
    full_env["VERTEBRA_DRAIN_TIMEOUT_MS"] = str(drain_timeout_ms)
    full_env["VERTEBRA_SHUTDOWN_TIMEOUT_MS"] = str(shutdown_timeout_ms)

    if tui:
        return _run_with_tui(server_cmd, full_env, shutdown_timeout_ms)

    proc = subprocess.Popen(server_cmd, env=full_env)
    try:
        return proc.wait()
    except KeyboardInterrupt:
        with contextlib.suppress(Exception):
            render_shutdown()

        try:
            return proc.wait(timeout=shutdown_timeout_ms / 1000)
        except subprocess.TimeoutExpired:
            logger.warning("Server did not exit after timeout, sending SIGTERM")
            proc.send_signal(signal.SIGTERM)
            return proc.wait(timeout=5)


def find_repo_root(start: Path) -> Path | None:
    """Find the repository root by looking for pyproject.toml."""
    for parent in [start, *start.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return None


def find_server_binary() -> str:
    """Find the vertebra-server binary."""
    if env_path := os.getenv("VERTEBRA_SERVER_BINARY"):
        return env_path

    pkg_bin = Path(__file__).resolve().parent.parent / "bin" / "vertebra-server"
    if pkg_bin.exists():
        return str(pkg_bin)

    pkg_bin_exe = pkg_bin.with_suffix(".exe")
    if pkg_bin_exe.exists():
        return str(pkg_bin_exe)

    repo_root = find_repo_root(Path(__file__).resolve())
    if repo_root:
        candidates = [
            repo_root / "target" / "release" / "vertebra-server",
            repo_root / "src" / "rs" / "target" / "release" / "vertebra-server",
            repo_root / "target" / "debug" / "vertebra-server",
            repo_root / "src" / "rs" / "target" / "debug" / "vertebra-server",
        ]
        for candidate in candidates:
            if candidate.exists():
                return str(candidate)

    msg = "Unable to locate vertebra-server binary. Please set VERTEBRA_SERVER_BINARY."
    raise SystemExit(msg)


def build_server_command(binary: str | None = None) -> list[str]:
    """Build command to start the Rust server."""
    server_bin = binary or find_server_binary()
    return [server_bin]


def _run_with_tui(
    server_cmd: list[str],
    env: dict[str, str],
    shutdown_timeout_ms: int,
) -> int:
    """Run the server with TUI mode using piped stderr and a Rich Live display."""
    log_buffer = LogBuffer()
    stop_event = threading.Event()

    proc = subprocess.Popen(
        server_cmd,
        env=env,
        stderr=subprocess.PIPE,
        text=True,
    )

    reader_thread = threading.Thread(
        target=stderr_reader,
        args=(proc.stderr, log_buffer, stop_event),
        daemon=True,
        name="vertebra-stderr-reader",
    )
    reader_thread.start()

    host = env.get("VERTEBRA_HOST", "127.0.0.1")
    port = int(env.get("VERTEBRA_PORT", "8000"))

    try:
        live = run_dev_tui(log_buffer=log_buffer, host=host, port=port)
        with live:
            proc.wait()
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        if proc.stderr:
            proc.stderr.close()
        reader_thread.join(timeout=2)

        with contextlib.suppress(Exception):
            render_shutdown()

        try:
            exit_code = proc.wait(timeout=shutdown_timeout_ms / 1000)
        except subprocess.TimeoutExpired:
            logger.warning("Server did not exit after timeout, sending SIGTERM")
            proc.send_signal(signal.SIGTERM)
            exit_code = proc.wait(timeout=5)

    return exit_code
