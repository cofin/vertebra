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
    bun_cmd: list[str],
    env: dict[str, str],
    workers: int = 1,
    config: dict[str, Any] | None = None,
    drain_timeout_ms: int = 5000,
    shutdown_timeout_ms: int = 30000,
) -> int:
    """Run the supervisor.

    Args:
        bun_cmd: Command to start the Bun engine.
        env: Environment variables for the supervised processes.
        workers: Number of Python workers.
        config: Runtime config.
        drain_timeout_ms: Drain timeout in milliseconds before shutdown.
        shutdown_timeout_ms: Shutdown timeout in milliseconds.

    Returns:
        Exit code.
    """
    if not bun_cmd:
        msg = "bun_cmd must not be empty"
        raise ValueError(msg)

    # Apply env vars to current process for supervisor inheritance.
    env_update = dict(env)
    env_update["VERTEBRA_DRAIN_TIMEOUT_MS"] = str(drain_timeout_ms)
    env_update["VERTEBRA_SHUTDOWN_TIMEOUT_MS"] = str(shutdown_timeout_ms)
    os.environ.update(env_update)

    supervisor = Supervisor(config)

    try:
        supervisor.start(bun_cmd[0], bun_cmd[1:], workers=workers)
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


def find_broker_entrypoint() -> str:
    """Find the broker TypeScript entrypoint."""
    package_root = Path(__file__).resolve().parent
    repo_root = find_repo_root(package_root)
    if repo_root:
        # Check for compiled engine first (future PRD)
        # Check source
        entry = repo_root / "src" / "ts" / "src" / "server" / "index.ts"
        if entry.exists():
            return str(entry)
    msg = "Unable to locate server entrypoint for Bun engine"
    raise SystemExit(msg)


def shutil_which(name: str) -> str | None:
    """Find executable in PATH."""
    path = os.getenv("PATH")
    if not path:
        return None
    for entry in path.split(os.pathsep):
        candidate = Path(entry) / name
        if candidate.exists():
            return str(candidate)
    return None


def build_bun_command(engine: str | None = None, bun: str | None = None) -> list[str]:
    """Build command to start the Bun engine."""
    bun_bin = bun or shutil_which("bun")
    if not bun_bin:
        msg = "bun binary not found; install Bun or add it to PATH"
        raise SystemExit(msg)

    engine_entry = engine or find_broker_entrypoint()
    return [bun_bin, "run", engine_entry]
