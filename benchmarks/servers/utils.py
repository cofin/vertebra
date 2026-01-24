"""Utilities for benchmark execution.

Process management, health checks, and environment detection.
"""

from __future__ import annotations

import atexit
import contextlib
import os
import signal
import subprocess
import time
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from .config import ServerConfig

# Track all spawned processes for cleanup
_spawned_processes: list[subprocess.Popen] = []


def _cleanup_processes() -> None:
    """Cleanup all spawned processes on exit."""
    for proc in _spawned_processes:
        with contextlib.suppress(Exception):
            stop_server(proc, graceful_timeout=2.0)


atexit.register(_cleanup_processes)


class ServerStartupError(Exception):
    """Server failed to start."""


class ServerHealthCheckError(Exception):
    """Server health check failed."""


def start_server(
    config: ServerConfig, *, workers: int | None = None, verbose: bool = False, use_bench_venv: bool = True
) -> subprocess.Popen:
    """Start a server process.

    Args:
        config: Server configuration
        workers: Override worker count (modifies command if possible)
        verbose: Print server output
        use_bench_venv: Use benchmark venv instead of current environment

    Returns:
        Popen object for the running server

    Raises:
        ServerStartupError: If server fails to start
    """
    cmd = list(config.command)

    # Override worker count if specified
    if workers is not None:
        cmd = _modify_worker_count(cmd, workers)

    env = os.environ.copy()
    env.update(config.env)

    # Use benchmark venv if requested and available
    if use_bench_venv:
        try:
            from .venv_manager import BENCH_VENV_DIR, venv_exists

            if venv_exists():
                env["VIRTUAL_ENV"] = str(BENCH_VENV_DIR)
                env["PATH"] = f"{BENCH_VENV_DIR / 'bin'}:{env.get('PATH', '')}"
        except ImportError:
            pass  # venv_manager not available, use current env

    # Disable Python buffering for immediate output
    env["PYTHONUNBUFFERED"] = "1"

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE if not verbose else None,
            stderr=subprocess.PIPE if not verbose else None,
            env=env,
            preexec_fn=os.setsid if hasattr(os, "setsid") else None,
        )
    except FileNotFoundError as e:
        msg = f"Command not found: {cmd[0]}"
        if config.requires_install:
            msg += f"\nInstall with: pip install {' '.join(config.requires_install)}"
        raise ServerStartupError(msg) from e
    except Exception as e:
        msg = f"Failed to start {config.name}: {e}"
        raise ServerStartupError(msg) from e

    _spawned_processes.append(proc)

    # Wait for server to be ready
    try:
        wait_for_health(config.port, timeout=30.0, initial_delay=config.startup_delay)
    except ServerHealthCheckError as e:
        stop_server(proc)
        msg = f"{config.name} failed health check: {e}"
        raise ServerStartupError(msg) from e

    return proc


def stop_server(proc: subprocess.Popen, graceful_timeout: float = 5.0) -> None:
    """Stop a server process gracefully.

    Args:
        proc: Process to stop
        graceful_timeout: Seconds to wait before SIGKILL
    """
    if proc.poll() is not None:
        return  # Already stopped

    try:
        # Try graceful termination first
        if hasattr(os, "killpg"):
            # Kill the entire process group
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                proc.terminate()
        else:
            proc.terminate()

        try:
            proc.wait(timeout=graceful_timeout)
        except subprocess.TimeoutExpired:
            # Force kill
            if hasattr(os, "killpg"):
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    proc.kill()
            else:
                proc.kill()
            proc.wait(timeout=2.0)

    except Exception:
        pass  # Best effort cleanup

    # Remove from tracking
    if proc in _spawned_processes:
        _spawned_processes.remove(proc)


def wait_for_health(port: int, timeout: float = 30.0, initial_delay: float = 1.0, poll_interval: float = 0.1) -> None:
    """Wait for server to respond to health check.

    Args:
        port: Server port
        timeout: Maximum wait time
        initial_delay: Initial delay before first check
        poll_interval: Time between checks

    Raises:
        ServerHealthCheckError: If server doesn't respond in time
    """
    time.sleep(initial_delay)

    url = f"http://127.0.0.1:{port}/health"
    deadline = time.monotonic() + timeout

    last_error = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=2.0)
            if response.status_code == 200:
                return
            last_error = f"HTTP {response.status_code}"
        except httpx.ConnectError:
            last_error = "Connection refused"
        except httpx.TimeoutException:
            last_error = "Request timeout"
        except Exception as e:
            last_error = str(e)

        time.sleep(poll_interval)

    msg = f"Server on port {port} didn't respond after {timeout}s: {last_error}"
    raise ServerHealthCheckError(msg)


def is_port_in_use(port: int) -> bool:
    """Check if a port is already in use."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def _modify_worker_count(cmd: list[str], workers: int) -> list[str]:
    """Modify command to use specified worker count."""
    cmd = cmd.copy()

    # Handle different server argument formats
    for i, arg in enumerate(cmd):
        if arg in ("--workers", "-w"):
            if i + 1 < len(cmd):
                cmd[i + 1] = str(workers)
                return cmd
        elif arg.startswith("--workers="):
            cmd[i] = f"--workers={workers}"
            return cmd

    # If --workers not found, try to add it
    # This won't work for all servers, but we try
    return cmd


def detect_environment() -> dict[str, str]:
    """Detect system environment for benchmark metadata."""
    import platform
    import sys

    from .parsers import detect_oha_version

    return {
        "cpu": _get_cpu_info(),
        "cpu_cores": str(os.cpu_count() or 1),
        "os": platform.system(),
        "os_version": platform.release(),
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "oha_version": detect_oha_version() or "unknown",
    }


def _get_cpu_info() -> str:
    """Get CPU model name."""
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":")[1].strip()
    except (FileNotFoundError, PermissionError):
        pass

    try:
        result = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True, timeout=5, check=False
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    import platform

    return platform.processor() or "Unknown"


def format_rps(rps: float) -> str:
    """Format RPS with thousands separator."""
    return f"{rps:,.0f}"


def format_latency(ms: float) -> str:
    """Format latency in milliseconds."""
    if ms < 1:
        return f"{ms * 1000:.2f}µs"
    return f"{ms:.2f}ms"


def format_bytes(b: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(b) < 1024:
            return f"{b:.1f}{unit}"
        b /= 1024
    return f"{b:.1f}TB"
