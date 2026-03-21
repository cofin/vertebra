"""Process supervisor for the Vertebra server.

Manages the lifecycle of the Rust HTTP server binary.
"""

import logging
import os
import signal
import subprocess
from pathlib import Path

logger = logging.getLogger("vertebra.supervisor")


def run_supervisor(
    *,
    server_cmd: list[str],
    env: dict[str, str],
    drain_timeout_ms: int = 5000,
    shutdown_timeout_ms: int = 30000,
) -> int:
    """Spawn the Rust server binary and wait for it to exit."""
    if not server_cmd:
        msg = "server_cmd must not be empty"
        raise ValueError(msg)

    full_env = os.environ | env
    full_env["VERTEBRA_DRAIN_TIMEOUT_MS"] = str(drain_timeout_ms)
    full_env["VERTEBRA_SHUTDOWN_TIMEOUT_MS"] = str(shutdown_timeout_ms)

    proc = subprocess.Popen(server_cmd, env=full_env)
    try:
        return proc.wait()
    except KeyboardInterrupt:
        # Child already received SIGINT (same process group). Give it time to drain.
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


__all__ = (
    "build_server_command",
    "find_server_binary",
    "run_supervisor",
)
