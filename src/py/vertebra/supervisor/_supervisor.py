"""Process supervisor for the Vertebra server.

Manages the lifecycle of the Rust HTTP server binary.
"""

import contextlib
import logging
import os
import signal
import subprocess
import sys
import sysconfig
import tempfile
import threading
from collections.abc import MutableMapping
from dataclasses import dataclass
from pathlib import Path

from vertebra.cli.banner import render_shutdown
from vertebra.cli.tui import LogBuffer, run_dev_tui, stderr_reader
from vertebra.server.platform import SHMAllocationPolicy

__all__ = (
    "EmbeddedPythonRuntime",
    "build_server_command",
    "find_repo_root",
    "find_server_binary",
    "is_packaged_server_binary",
    "run_supervisor",
)

logger = logging.getLogger("vertebra.supervisor")


def is_packaged_server_binary(binary: str | Path) -> bool:
    """Return True when ``binary`` resolves inside ``<vertebra>/bin`` without explicit env override."""
    if os.getenv("VERTEBRA_SERVER_BINARY"):
        return False
    pkg_bin_dir = (Path(__file__).resolve().parent.parent / "bin").resolve()
    try:
        resolved = Path(binary).resolve()
    except OSError:
        return False
    return resolved.is_relative_to(pkg_bin_dir)


@dataclass(frozen=True, slots=True)
class EmbeddedPythonRuntime:
    """Host CPython embedding metadata exported to ``vertebra-server`` subprocesses."""

    python_home: Path
    library_dir: Path | None
    library_name: str | None
    is_shared: bool
    platform: str = sys.platform

    @classmethod
    def detect(cls) -> "EmbeddedPythonRuntime":
        """Inspect the running CPython interpreter for ``PYTHONHOME`` and shared ``libpython`` paths."""
        python_home = Path(sys.base_prefix)
        libdir_raw = sysconfig.get_config_var("LIBDIR")
        ldlibrary_raw = sysconfig.get_config_var("LDLIBRARY")
        enable_shared_raw = sysconfig.get_config_var("ENABLE_SHARED")
        plat = sys.platform

        library_dir = Path(libdir_raw) if isinstance(libdir_raw, str) and libdir_raw else None
        library_name = str(ldlibrary_raw) if isinstance(ldlibrary_raw, str) and ldlibrary_raw else None

        if plat == "win32" or os.name == "nt":
            resolved_dir = library_dir or python_home
            return cls(
                python_home=python_home,
                library_dir=resolved_dir,
                library_name=library_name,
                is_shared=True,
                platform="win32",
            )

        is_static_archive = library_name is not None and library_name.endswith(".a")
        is_explicit_shared = (
            enable_shared_raw is True
            or enable_shared_raw == 1
            or str(enable_shared_raw).strip().lower() in {"1", "true"}
        )
        is_explicit_static = (
            enable_shared_raw is False
            or enable_shared_raw == 0
            or str(enable_shared_raw).strip().lower() in {"0", "false"}
        )
        has_shared_path = (not is_static_archive) and library_dir is not None and library_name is not None
        if is_explicit_static or not has_shared_path:
            is_shared = False
        elif is_explicit_shared:
            is_shared = True
        else:
            assert library_dir is not None and library_name is not None
            is_shared = (Path(library_dir) / library_name).exists()
        return cls(
            python_home=python_home,
            library_dir=library_dir,
            library_name=library_name,
            is_shared=is_shared,
            platform=plat,
        )

    def require_shared_library(self, binary: str | Path) -> None:
        """Fail loudly when a packaged POSIX ``vertebra-server`` lacks a shared ``libpython`` runtime."""
        if self.platform == "win32" or not is_packaged_server_binary(binary):
            return
        if not self.is_shared or self.library_dir is None or not self.library_name:
            msg = (
                "Packaged vertebra-server requires a shared libpython installation "
                "(sysconfig LIBDIR/LDLIBRARY with ENABLE_SHARED=1), but this Python interpreter "
                "is statically linked or missing shared library metadata. Install a shared CPython "
                "build (for example via 'uv python install') or set VERTEBRA_SERVER_BINARY."
            )
            raise SystemExit(msg)

    def apply(self, env: MutableMapping[str, str]) -> MutableMapping[str, str]:
        """Export ``PYTHONHOME`` and prepend ``library_dir`` to the platform dynamic loader variable."""
        env["PYTHONHOME"] = str(self.python_home)
        if self.library_dir is None:
            return env
        lib_entry = str(self.library_dir)
        if self.platform == "win32":
            var_name = "PATH"
        elif self.platform == "darwin":
            var_name = "DYLD_LIBRARY_PATH"
        else:
            var_name = "LD_LIBRARY_PATH"
        existing = env.get(var_name, "")
        if not existing:
            env[var_name] = lib_entry
            return env
        parts = [part for part in existing.split(os.pathsep) if part]
        if parts and parts[0] == lib_entry:
            return env
        env[var_name] = os.pathsep.join([lib_entry, *parts])
        return env


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

    SHMAllocationPolicy().validate_environment()

    full_env = os.environ | env
    runtime = EmbeddedPythonRuntime.detect()
    if server_cmd:
        runtime.require_shared_library(server_cmd[0])
    runtime.apply(full_env)
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
    """Find the vertebra-server binary, honoring CARGO_TARGET_DIR and freshest release artifacts."""
    if env_path := os.getenv("VERTEBRA_SERVER_BINARY"):
        return env_path

    pkg_bin = Path(__file__).resolve().parent.parent / "bin" / "vertebra-server"
    if pkg_bin.exists():
        return str(pkg_bin)

    pkg_bin_exe = pkg_bin.with_suffix(".exe")
    if pkg_bin_exe.exists():
        return str(pkg_bin_exe)

    target_dirs: list[Path] = []
    cargo_target = os.getenv("CARGO_TARGET_DIR")
    if cargo_target:
        cargo_dir = Path(cargo_target)
        explicit_release = cargo_dir / "release" / "vertebra-server"
        if explicit_release.exists():
            return str(explicit_release)
        target_dirs.append(cargo_dir)

    repo_root = find_repo_root(Path(__file__).resolve())
    if repo_root:
        target_dirs.extend(
            [
                repo_root / "target",
                repo_root / "src" / "rs" / "target",
            ]
        )

    default_make_target = Path(tempfile.gettempdir()) / "vtb-cargo-target"
    if default_make_target not in target_dirs:
        target_dirs.append(default_make_target)

    for profile in ("release", "debug"):
        existing = [
            candidate for target_dir in target_dirs if (candidate := target_dir / profile / "vertebra-server").exists()
        ]
        if existing:
            newest = max(existing, key=lambda candidate: candidate.stat().st_mtime_ns)
            return str(newest)

    msg = "Unable to locate vertebra-server binary. Please set VERTEBRA_SERVER_BINARY."
    raise SystemExit(msg)


def build_server_command(binary: str | None = None) -> list[str]:
    """Build command to start the Rust server."""
    server_bin = binary or find_server_binary()
    EmbeddedPythonRuntime.detect().require_shared_library(server_bin)
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
