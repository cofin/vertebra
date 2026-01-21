"""Process supervisor for Bun server + shared-memory broker.

Manages the lifecycle of:
- Bun HTTP server (engine)
- Shared-memory broker

Features:
- Process group management for clean shutdown
- Structured shutdown sequencing (drain → stop → kill)
- Health checks with configurable intervals
- Restart policies with exponential backoff
- Signal propagation (SIGTERM, SIGINT, SIGHUP)
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import logging
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from vertebra._vertebra import NativeSupervisor

if TYPE_CHECKING:
    from types import FrameType

logger = logging.getLogger("vertebra.supervisor")


class ProcessState(enum.Enum):
    """State machine for supervised processes."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"


class SupervisorState(enum.Enum):
    """State machine for the supervisor itself."""

    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    DRAINING = "draining"
    STOPPING = "stopping"
    STOPPED = "stopped"


@dataclass(slots=True)
class RestartPolicy:
    """Restart policy with exponential backoff."""

    max_restarts: int = 5
    base_delay_ms: int = 200
    max_delay_ms: int = 30000
    reset_after_ms: int = 60000

    def compute_delay(self, attempt: int) -> float:
        """Compute delay in seconds for the given attempt."""
        delay_ms = min(self.max_delay_ms, self.base_delay_ms * (2 ** max(0, attempt - 1)))
        return float(delay_ms / 1000.0)


@dataclass(slots=True)
class HealthCheckConfig:
    """Health check configuration."""

    enabled: bool = True
    interval_ms: int = 5000
    timeout_ms: int = 3000
    unhealthy_threshold: int = 3


@dataclass(slots=True)
class ProcessSpec:
    """Specification for a supervised process."""

    name: str
    cmd: list[str]
    env: dict[str, str] = field(default_factory=dict)
    restart_policy: RestartPolicy = field(default_factory=RestartPolicy)
    health_check: HealthCheckConfig = field(default_factory=HealthCheckConfig)
    critical: bool = True  # If critical process dies, supervisor stops
    shutdown_timeout_ms: int = 30000
    drain_timeout_ms: int = 5000


@dataclass(slots=True)
class ProcessHandle:
    """Runtime handle for a supervised process."""

    spec: ProcessSpec
    proc: subprocess.Popen[bytes] | None = None
    state: ProcessState = ProcessState.STOPPED
    restarts: int = 0
    last_restart: float = 0.0
    consecutive_failures: int = 0
    started_at: float | None = None


class Supervisor:
    """Process supervisor with structured lifecycle management.

    Manages multiple processes as a group with:
    - Ordered startup (broker first, then engine)
    - Structured shutdown (drain → stop → kill)
    - Health monitoring and automatic restarts
    - Signal propagation to child processes
    """

    def __init__(
        self, processes: list[ProcessSpec], *, drain_timeout_ms: int = 5000, shutdown_timeout_ms: int = 30000
    ) -> None:
        self._specs = processes
        self._handles: dict[str, ProcessHandle] = {spec.name: ProcessHandle(spec=spec) for spec in processes}
        self._state = SupervisorState.IDLE
        self._drain_timeout = drain_timeout_ms / 1000.0
        self._shutdown_timeout = shutdown_timeout_ms / 1000.0
        self._health_task: asyncio.Task[None] | None = None
        self._shutdown_event = asyncio.Event()
        self._original_handlers: dict[int, Any] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._native = NativeSupervisor()

    @property
    def state(self) -> SupervisorState:
        return self._state

    def get_stats(self) -> dict[str, dict[str, int | str | float | None]]:
        """Get statistics for all processes."""
        return {
            name: {
                "state": handle.state.value,
                "restarts": handle.restarts,
                "pid": handle.proc.pid if handle.proc else None,
                "uptime_s": time.time() - handle.started_at if handle.started_at else None,
            }
            for name, handle in self._handles.items()
        }

    def start(self) -> None:
        """Start all processes in order."""
        if self._state != SupervisorState.IDLE:
            logger.warning("Supervisor already started, state=%s", self._state)
            return

        self._state = SupervisorState.STARTING
        logger.info("Starting supervisor with %d processes", len(self._specs))

        # Install signal handlers
        self._install_signal_handlers()

        # Start processes in order (broker first, then engine)
        for spec in self._specs:
            self._start_process(spec.name)

        self._state = SupervisorState.RUNNING
        logger.info("Supervisor running")

    def _start_process(self, name: str) -> bool:
        """Start a single process."""
        handle = self._handles.get(name)
        if not handle:
            logger.error("Unknown process: %s", name)
            return False

        if handle.state == ProcessState.RUNNING:
            logger.debug("Process %s already running", name)
            return True

        handle.state = ProcessState.STARTING
        spec = handle.spec

        try:
            # Merge environment
            env = os.environ.copy()
            env.update(spec.env)

            # Start process in new process group for clean shutdown
            kwargs: dict[str, Any] = {"env": env, "stdout": subprocess.PIPE, "stderr": subprocess.STDOUT}

            # Unix: use process groups
            if sys.platform != "win32":
                kwargs["start_new_session"] = True

            proc = subprocess.Popen(spec.cmd, **kwargs)
            handle.proc = proc
            handle.state = ProcessState.RUNNING
            handle.started_at = time.time()
            self._native.add_child(proc.pid)

            logger.info("Started process %s (pid=%d)", name, proc.pid)

            # Start output reader in background
            if proc.stdout:
                self._start_output_reader(name, proc)
        except OSError:
            logger.exception("Failed to start process %s", name)
            handle.state = ProcessState.FAILED
            handle.consecutive_failures += 1
            return False
        else:
            return True

    def _start_output_reader(self, name: str, proc: subprocess.Popen[bytes]) -> None:
        """Start a thread to read process output."""
        import threading

        def reader() -> None:
            if not proc.stdout:
                return
            for line in proc.stdout:
                with contextlib.suppress(OSError, ValueError):
                    text = line.decode("utf-8", errors="replace").rstrip()
                    logger.info("[%s] %s", name, text)

        thread = threading.Thread(target=reader, daemon=True)
        thread.start()

    def stop(self, sig: int = signal.SIGTERM) -> None:
        """Stop all processes with structured shutdown."""
        if self._state in (SupervisorState.STOPPING, SupervisorState.STOPPED):
            return

        logger.info("Stopping supervisor (signal=%d)", sig)
        self._state = SupervisorState.DRAINING

        # Phase 1: Drain (send SIGTERM and wait briefly)
        self._drain_processes(sig)

        self._state = SupervisorState.STOPPING

        # Phase 2: Wait for graceful termination
        self._wait_for_termination()

        # Phase 3: Force kill remaining
        self._force_kill_remaining()

        # Restore signal handlers
        self._restore_signal_handlers()

        self._state = SupervisorState.STOPPED
        logger.info("Supervisor stopped")

    def _drain_processes(self, sig: int) -> None:
        """Send signal to all processes and wait for drain period."""
        for name, handle in reversed(list(self._handles.items())):
            if handle.proc and handle.proc.poll() is None:
                logger.debug("Draining process %s", name)
                self._send_signal(handle, sig)

        # Wait for drain timeout
        time.sleep(min(self._drain_timeout, 1.0))

    def _wait_for_termination(self) -> None:
        """Wait for processes to terminate gracefully."""
        deadline = time.time() + self._shutdown_timeout

        for name, handle in reversed(list(self._handles.items())):
            if not handle.proc:
                continue

            remaining = max(0, deadline - time.time())
            try:
                handle.proc.wait(timeout=remaining)
                logger.info("Process %s exited gracefully", name)
                handle.state = ProcessState.STOPPED
            except subprocess.TimeoutExpired:
                logger.warning("Process %s did not exit within timeout", name)

    def _force_kill_remaining(self) -> None:
        """Force kill any remaining processes."""
        for name, handle in self._handles.items():
            if handle.proc and handle.proc.poll() is None:
                logger.warning("Force killing process %s", name)
                self._send_signal(handle, signal.SIGKILL)
                with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                    handle.proc.wait(timeout=5)
                handle.state = ProcessState.STOPPED

    def _send_signal(self, handle: ProcessHandle, sig: int) -> None:
        """Send signal to a process (and its process group on Unix)."""
        if not handle.proc:
            return

        try:
            if sys.platform != "win32":
                # Send to process group
                os.killpg(os.getpgid(handle.proc.pid), sig)
            else:
                handle.proc.send_signal(sig)
        except (OSError, ProcessLookupError):
            pass

    def wait(self, primary: str | None = None) -> int:
        """Wait for processes to complete.

        Args:
            primary: If specified, return when this process exits.
                    Otherwise, wait for any process to exit.

        Returns:
            Exit code of the primary process, or first failed process.
        """
        # Wait for any critical process to exit
        while self._state == SupervisorState.RUNNING:
            # Wait for native events (100ms timeout)
            # This handles signals and child reaping
            try:
                event = self._native.wait_next_event(100)
            except Exception:
                logger.exception("Error in native supervisor wait")
                time.sleep(0.1)
                continue

            event_type = event.get("type")

            if event_type == "exit":
                pid = event["pid"]
                code = event["code"]

                # Find matching process
                for name, handle in self._handles.items():
                    if handle.proc and handle.proc.pid == pid:
                        logger.info("Process %s exited with code %d", name, code)
                        handle.state = ProcessState.STOPPED

                        # Check critical
                        if handle.spec.critical:
                            return cast("int", code)

                        # If primary exited
                        if primary and name == primary:
                            return cast("int", code)

                        # Non-critical: attempt restart
                        self._maybe_restart(name)
                        break

            elif event_type == "signal":
                signo = event["signo"]
                logger.info("Received signal %d via native supervisor", signo)
                self.stop(signo)
                return 128 + cast("int", signo)

            # Fallback: Check polling for processes not managed by native supervisor
            # or if the native supervisor missed the exit (e.g. process reaped by Python)
            for name, handle in self._handles.items():
                if handle.proc and handle.state == ProcessState.RUNNING:
                    retcode = handle.proc.poll()
                    if retcode is not None:
                        logger.info("Process %s exited with code %d (detected via poll)", name, retcode)
                        handle.state = ProcessState.STOPPED

                        # Check critical
                        if handle.spec.critical:
                            return retcode

                        # If primary exited
                        if primary and name == primary:
                            return retcode

                        # Non-critical: attempt restart
                        self._maybe_restart(name)

        return 0

    def _maybe_restart(self, name: str) -> None:
        """Attempt to restart a failed process if policy allows."""
        handle = self._handles.get(name)
        if not handle:
            return

        policy = handle.spec.restart_policy
        now = time.time()

        # Reset restart counter if stable for long enough
        if handle.last_restart and (now - handle.last_restart) > policy.reset_after_ms / 1000.0:
            handle.restarts = 0
            handle.consecutive_failures = 0

        # Check restart limit
        if handle.restarts >= policy.max_restarts:
            logger.error("Process %s exceeded restart limit (%d), not restarting", name, policy.max_restarts)
            handle.state = ProcessState.FAILED
            return

        # Compute backoff delay
        delay = policy.compute_delay(handle.restarts + 1)
        logger.info(
            "Restarting process %s in %.1fs (attempt %d/%d)", name, delay, handle.restarts + 1, policy.max_restarts
        )

        time.sleep(delay)

        handle.restarts += 1
        handle.last_restart = now
        handle.proc = None

        if self._start_process(name):
            logger.info("Process %s restarted successfully", name)
        else:
            logger.error("Failed to restart process %s", name)

    def _install_signal_handlers(self) -> None:
        """Install signal handlers for graceful shutdown."""

        def handler(sig: int, _frame: FrameType | None) -> None:
            logger.info("Received signal %d, initiating shutdown", sig)
            self.stop(sig)

        for sig in (signal.SIGTERM, signal.SIGINT):
            self._original_handlers[sig] = signal.signal(sig, cast("Any", handler))

        if sys.platform != "win32":
            # SIGHUP for reload (future)
            self._original_handlers[signal.SIGHUP] = signal.signal(signal.SIGHUP, cast("Any", handler))

    def _restore_signal_handlers(self) -> None:
        """Restore original signal handlers."""
        for sig, handler in self._original_handlers.items():
            with contextlib.suppress(OSError, ValueError):
                signal.signal(sig, handler)
        self._original_handlers.clear()


def run_supervisor(
    *,
    bun_cmd: list[str],
    broker_cmd: list[str],
    broker_config: str,
    env: dict[str, str],
    drain_timeout_ms: int = 5000,
    shutdown_timeout_ms: int = 30000,
) -> int:
    """Run the supervisor with broker and engine processes.

    Args:
        bun_cmd: Command to start the Bun engine.
        broker_cmd: Command to start the broker.
        broker_config: Path to broker configuration file.
        env: Environment variables to pass to processes.
        drain_timeout_ms: Time to wait for graceful drain.
        shutdown_timeout_ms: Total shutdown timeout.

    Returns:
        Exit code from the primary (bun) process.
    """
    # Define process specs - broker starts first, engine second
    processes = [
        ProcessSpec(
            name="broker",
            cmd=broker_cmd,
            env=env,
            critical=True,
            restart_policy=RestartPolicy(max_restarts=3),
            # Broker health is checked via IPC, not HTTP. The broker's Bun server
            # could expose an HTTP health endpoint if needed for production k8s probes.
            health_check=HealthCheckConfig(enabled=False),
        ),
        ProcessSpec(
            name="engine",
            cmd=bun_cmd,
            env=env,
            critical=True,
            restart_policy=RestartPolicy(max_restarts=5),
            health_check=HealthCheckConfig(enabled=True, interval_ms=10000),
        ),
    ]

    supervisor = Supervisor(processes, drain_timeout_ms=drain_timeout_ms, shutdown_timeout_ms=shutdown_timeout_ms)

    supervisor.start()
    exit_code = supervisor.wait(primary="engine")
    supervisor.stop()
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
        entry = repo_root / "src" / "ts" / "src" / "broker" / "serve.ts"
        if entry.exists():
            return str(entry)
    msg = "Unable to locate broker entrypoint for Bun broker"
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


def build_broker_command(config_path: str) -> list[str]:
    """Build command to start the broker."""
    bun = shutil_which("bun")
    if not bun:
        msg = "bun binary not found; install Bun or add it to PATH"
        raise SystemExit(msg)
    entry = os.getenv("VERTEBRA_BROKER_ENTRY") or find_broker_entrypoint()
    return [bun, entry, "--config", config_path]


def build_bun_command(engine: str, bun: str | None = None) -> list[str]:
    """Build command to start the Bun engine."""
    bun_bin = bun or shutil_which("bun")
    if not bun_bin:
        msg = "bun binary not found; install Bun or add it to PATH"
        raise SystemExit(msg)
    return [bun_bin, engine]
