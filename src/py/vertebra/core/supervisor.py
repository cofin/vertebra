"""Process supervisor for Bun server + shared-memory broker.

Manages the lifecycle of:
- Bun HTTP server (engine)
- Shared-memory broker
- Python workers

Features:
- Native Rust process supervision (GIL-free)
- Structured shutdown sequencing
- Signal propagation (SIGTERM, SIGINT)
"""

import logging
import os
import sys
from pathlib import Path
from typing import Any

from vertebra._vertebra import NativeSupervisor

logger = logging.getLogger("vertebra.supervisor")


class Supervisor:
    """Process supervisor backed by Rust NativeSupervisor.

    Manages processes with:
    - Ordered startup
    - Structured shutdown
    - Signal propagation
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self._native = NativeSupervisor()
        self._config = config or {}

    def start(self, engine_path: str, engine_args: list[str], workers: int = 1) -> None:
        """Start the engine and workers."""
        logger.info("Starting supervisor with engine: %s %s", engine_path, engine_args)
        # config passed as Python object, Rust handles it opaque or extracts values
        self._native.start(engine_path, engine_args, sys.executable, self._config, workers)
        logger.info("Supervisor started")

    def wait(self) -> int:
        """Wait for shutdown signal or process exit."""
        return self._native.wait_for_shutdown()

    def stop(self) -> None:
        """Stop all processes."""
        self._native.stop()


def run_supervisor(
    *,
    server_cmd: list[str],
    env: dict[str, str],
    workers: int = 1,
    config: dict[str, Any] | None = None,
    drain_timeout_ms: int = 5000,
    shutdown_timeout_ms: int = 30000,
) -> int:
    """Run the supervisor.

    Args:
        server_cmd: Command to start the server engine.
        env: Environment variables for the supervised processes.
        workers: Number of Python workers.
        config: Runtime config.
        drain_timeout_ms: Drain timeout in milliseconds before shutdown.
        shutdown_timeout_ms: Shutdown timeout in milliseconds.

    Returns:
        Exit code.
    """
    if not server_cmd:
        msg = "server_cmd must not be empty"
        raise ValueError(msg)

    # Apply env vars to current process for supervisor inheritance.
    env_update = dict(env)
    env_update["VERTEBRA_DRAIN_TIMEOUT_MS"] = str(drain_timeout_ms)
    env_update["VERTEBRA_SHUTDOWN_TIMEOUT_MS"] = str(shutdown_timeout_ms)
    os.environ.update(env_update)

    supervisor = Supervisor(config)

    try:
        supervisor.start(server_cmd[0], server_cmd[1:], workers=workers)
        exit_code = supervisor.wait()
    except KeyboardInterrupt:
        supervisor.stop()
        exit_code = 130
    except Exception:
        logger.exception("Supervisor error")
        supervisor.stop()
        exit_code = 1

    return exit_code


def find_repo_root(start: Path) -> Path | None:
    """Find the repository root by looking for pyproject.toml."""
    for parent in [start, *start.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return None


def find_server_binary() -> str:
    """Find the vertebra-server binary."""
    # 1. Check environment override
    if env_path := os.getenv("VERTEBRA_SERVER_BINARY"):
        return env_path

    # 2. Check adjacent to this file (installed package layout)
    # Expected: vertebra/bin/vertebra-server
    pkg_bin = Path(__file__).resolve().parent.parent / "bin" / "vertebra-server"
    if pkg_bin.exists():
        return str(pkg_bin)

    # Windows extension check
    pkg_bin_exe = pkg_bin.with_suffix(".exe")
    if pkg_bin_exe.exists():
        return str(pkg_bin_exe)

    # 3. Check repo target (dev mode)
    repo_root = find_repo_root(Path(__file__).resolve())
    if repo_root:
        # Check common target locations
        candidates = [
            repo_root / "target" / "release" / "vertebra-server",
            repo_root / "src" / "rs" / "target" / "release" / "vertebra-server",
            # Debug fallback
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
