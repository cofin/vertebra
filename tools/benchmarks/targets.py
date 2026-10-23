"""Server targets and runtime topologies for Vertebra HTTP benchmarks."""

from __future__ import annotations

import glob
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from enum import StrEnum


class Topology(StrEnum):
    """Runtime topologies for Vertebra."""

    NATIVE = "native"
    EMBEDDED = "embedded"
    SHM = "shm"
    FREE_THREADED = "free-threaded"
    BASELINE = "baseline"


class Target:
    """A server target to benchmark."""

    __slots__ = (
        "_cmd_data",
        "_env_data",
        "category",
        "description",
        "host",
        "name",
        "port",
        "topology",
    )

    def __init__(
        self,
        name: str,
        cmd: list[str] | Callable[[], list[str]],
        topology: Topology,
        host: str = "127.0.0.1",
        port: int = 8000,
        env: dict[str, str] | Callable[[], dict[str, str]] | None = None,
        description: str = "",
        category: str = "vertebra",
    ) -> None:
        self.name = name
        self._cmd_data = cmd
        self.topology = topology
        self.host = host
        self.port = port
        self._env_data = env
        self.description = description
        self.category = category

    @property
    def cmd(self) -> list[str]:
        """Return the target command list, resolving callable factory if provided."""
        if callable(self._cmd_data):
            return self._cmd_data()
        return self._cmd_data

    @property
    def env(self) -> dict[str, str]:
        """Return the target environment dictionary, resolving callable factory if provided."""
        if callable(self._env_data):
            return self._env_data()
        return self._env_data if self._env_data is not None else {}


def _py(*args: str) -> list[str]:
    """Build a command list using the current Python interpreter."""
    return [sys.executable, *args]


def _vertebra_run(*args: str, workers: int | None = None, env_extra: dict[str, str] | None = None) -> list[str]:
    """Build a ``vtb run`` command using the console script in the current venv."""
    venv_bin = os.path.dirname(sys.executable)
    vtb = os.path.join(venv_bin, "vtb")
    if not os.path.isfile(vtb):
        found = shutil.which("vtb")
        if found:
            vtb = found
        else:
            msg = "vtb CLI not found — run `make install`"
            raise FileNotFoundError(msg)
    return [vtb, "run", *args, "--no-banner"]


def _ft_server_binary() -> str:
    """Return path to the 3.14t-linked vertebra-server binary.

    Built by ``make bench-ft`` via ``PYO3_PYTHON=... cargo build --target-dir target-ft``.
    """
    repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    binary = os.path.join(repo, "src", "rs", "target-ft", "release", "vertebra-server")
    return binary


def _ft_env() -> dict[str, str]:
    """Environment variables needed to run the 3.14t server binary."""
    uv = shutil.which("uv")
    if uv is None:
        return {}
    try:
        py_path = subprocess.check_output([uv, "python", "find", "3.14t"], text=True).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return {}
    lib_dir = os.path.join(os.path.dirname(py_path), "..", "lib")
    repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    src_py = os.path.join(repo, "src", "py")
    venv_ft = os.path.join(repo, ".venv-ft")

    site_packages = ""
    if os.path.isdir(venv_ft):
        matches = glob.glob(os.path.join(venv_ft, "lib", "python*", "site-packages"))
        if matches:
            site_packages = matches[0]

    paths = [p for p in [site_packages, src_py, repo] if p]
    return {
        "LD_LIBRARY_PATH": os.path.abspath(lib_dir),
        "PYTHONPATH": os.pathsep.join(paths),
    }


def _ft_run(*args: str) -> list[str]:
    """Build a command to run the 3.14t-linked vertebra-server binary."""
    binary = _ft_server_binary()
    if not os.path.isfile(binary):
        raise FileNotFoundError(f"3.14t server binary not found at {binary}; run 'make bench-ft' first")
    return [binary, *args]


ALL_TARGETS: dict[str, Target] = {
    "vtb-embedded-native": Target(
        name="vtb-embedded-native",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run("tools.benchmarks.apps.vertebra_app:native_app", "--host", "127.0.0.1", "--port", "8000"),
        port=8000,
        description="Vertebra embedded, native handlers",
    ),
    "vtb-embedded-asgi": Target(
        name="vtb-embedded-asgi",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:asgi_app", "--asgi", "--host", "127.0.0.1", "--port", "8001"
        ),
        port=8001,
        description="Vertebra embedded, ASGI adapter",
    ),
    "vtb-embedded-wsgi": Target(
        name="vtb-embedded-wsgi",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run("tools.benchmarks.apps.vertebra_app:wsgi_app", "--host", "127.0.0.1", "--port", "8002"),
        port=8002,
        description="Vertebra embedded, WSGI adapter",
    ),
    "vtb-shm-native-4w": Target(
        name="vtb-shm-native-4w",
        topology=Topology.SHM,
        cmd=_vertebra_run("tools.benchmarks.apps.vertebra_app:native_app", "--host", "127.0.0.1", "--port", "8011"),
        port=8011,
        env={"VERTEBRA_WORKERS": "4"},
        description="Vertebra SHM, native handlers, 4 workers",
    ),
    "vtb-shm-native-8w": Target(
        name="vtb-shm-native-8w",
        topology=Topology.SHM,
        cmd=_vertebra_run("tools.benchmarks.apps.vertebra_app:native_app", "--host", "127.0.0.1", "--port", "8014"),
        port=8014,
        env={"VERTEBRA_WORKERS": "8"},
        description="Vertebra SHM, native handlers, 8 workers",
    ),
    "vtb-shm-asgi-4w": Target(
        name="vtb-shm-asgi-4w",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:asgi_app", "--asgi", "--host", "127.0.0.1", "--port", "8012"
        ),
        port=8012,
        env={"VERTEBRA_WORKERS": "4"},
        description="Vertebra SHM, ASGI, 4 workers",
    ),
    "vtb-shm-asgi-8w": Target(
        name="vtb-shm-asgi-8w",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:asgi_app", "--asgi", "--host", "127.0.0.1", "--port", "8015"
        ),
        port=8015,
        env={"VERTEBRA_WORKERS": "8"},
        description="Vertebra SHM, ASGI, 8 workers",
    ),
    "vtb-shm-wsgi-4w": Target(
        name="vtb-shm-wsgi-4w",
        topology=Topology.SHM,
        cmd=_vertebra_run("tools.benchmarks.apps.vertebra_app:wsgi_app", "--host", "127.0.0.1", "--port", "8013"),
        port=8013,
        env={"VERTEBRA_WORKERS": "4"},
        description="Vertebra SHM, WSGI, 4 workers",
    ),
    "vtb-ft-native-4w": Target(
        name="vtb-ft-native-4w",
        topology=Topology.FREE_THREADED,
        cmd=_ft_run,
        port=8022,
        env=lambda: {
            **_ft_env(),
            "VERTEBRA_APP": "tools.benchmarks.apps.vertebra_app:native_app",
            "VERTEBRA_HOST": "127.0.0.1",
            "VERTEBRA_PORT": "8022",
            "VERTEBRA_WORKERS": "0",
            "VERTEBRA_CPU_COUNT": "4",
        },
        description="Vertebra FT, native handlers, 4 threads",
        category="freethreaded",
    ),
    "granian-asgi": Target(
        name="granian-asgi",
        topology=Topology.BASELINE,
        cmd=_py(
            "-m",
            "granian",
            "tools.benchmarks.apps.asgi:app",
            "--interface",
            "asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8003",
        ),
        port=8003,
        description="Granian ASGI",
        category="baseline",
    ),
    "uvicorn-httptools": Target(
        name="uvicorn-httptools",
        topology=Topology.BASELINE,
        cmd=_py(
            "-m",
            "uvicorn",
            "tools.benchmarks.apps.asgi:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8004",
            "--http",
            "httptools",
            "--loop",
            "uvloop",
        ),
        port=8004,
        description="Uvicorn httptools",
        category="baseline",
    ),
    "spikard": Target(
        name="spikard",
        topology=Topology.BASELINE,
        cmd=_py("-m", "tools.benchmarks.apps.spikard_app"),
        port=8005,
        env={"SPIKARD_HOST": "127.0.0.1", "SPIKARD_PORT": "8005"},
        description="Spikard",
        category="baseline",
    ),
}

VERTEBRA_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "vertebra"]
BASELINE_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "baseline"]
FREETHREADED_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "freethreaded"]
