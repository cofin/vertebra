"""Server targets and runtime topologies for Vertebra HTTP benchmarks."""

from __future__ import annotations

import asyncio
import glob
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from enum import StrEnum
from typing import Any

from tools.benchmarks.apps.asgi import STATIC_ROUTES


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


async def rsgi_app(scope: Any, protocol: Any) -> None:
    """Benchmark RSGI v1.6 application matching the standardized ASGI/native routes."""
    if getattr(scope, "proto", "http") != "http":
        return
    path = scope.path
    if path in STATIC_ROUTES:
        body, content_type = STATIC_ROUTES[path]
        protocol.response_bytes(200, [("content-type", content_type)], body)
        return
    if path == "/echo":
        body = await protocol()
        protocol.response_bytes(200, [("content-type", "text/plain")], bytes(body))
        return
    if path == "/io10":
        await asyncio.sleep(0.010)
        protocol.response_bytes(200, [("content-type", "text/plain")], b"ok")
        return
    protocol.response_bytes(404, [("content-type", "text/plain")], b"Not Found")


def _py(*args: str) -> list[str]:
    """Build a command list using the current Python interpreter."""
    return [sys.executable, *args]


def _vertebra_run(
    *args: str,
    topology: str | None = None,
    workers: int | None = None,
) -> list[str]:
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
    cmd = [vtb, "run", *args]
    if topology is not None:
        cmd.extend(["--topology", topology])
    if workers is not None:
        cmd.extend(["--workers", str(workers)])
    cmd.append("--no-banner")
    return cmd


def _ft_server_binary() -> str:
    """Return path to the free-threaded-linked vertebra-server binary."""
    repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(repo, "src", "rs", "target-ft", "release", "vertebra-server")


def _ft_env() -> dict[str, str]:
    """Environment variables needed to run the free-threaded (3.15t / 3.14t) server binary."""
    uv = shutil.which("uv")
    if uv is None:
        return {}
    explicit = os.environ.get("FT_PYTHON", "").strip()
    candidates = (explicit,) if explicit else ("3.15t", "3.14t")
    py_path = ""
    for candidate in candidates:
        try:
            py_path = subprocess.check_output([uv, "python", "find", candidate], text=True).strip()
            if py_path:
                break
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
    if not py_path:
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
    """Build a command to run the free-threaded-linked vertebra-server binary."""
    binary = _ft_server_binary()
    if not os.path.isfile(binary):
        msg = f"Free-threaded server binary not found at {binary}; run 'make bench-ft' first"
        raise FileNotFoundError(msg)
    return [binary, *args]


ALL_TARGETS: dict[str, Target] = {
    "vtb-embedded-native": Target(
        name="vtb-embedded-native",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:native_app",
            "--host",
            "127.0.0.1",
            "--port",
            "8000",
            topology="embedded",
            workers=1,
        ),
        port=8000,
        description="Vertebra embedded, native handlers, 1 worker",
    ),
    "vtb-embedded-native-4w": Target(
        name="vtb-embedded-native-4w",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:native_app",
            "--host",
            "127.0.0.1",
            "--port",
            "8016",
            topology="embedded",
            workers=4,
        ),
        port=8016,
        env={"VERTEBRA_WORKERS": "4"},
        description="Vertebra embedded SO_REUSEPORT, native handlers, 4 workers",
    ),
    "vtb-embedded-native-8w": Target(
        name="vtb-embedded-native-8w",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:native_app",
            "--host",
            "127.0.0.1",
            "--port",
            "8017",
            topology="embedded",
            workers=8,
        ),
        port=8017,
        env={"VERTEBRA_WORKERS": "8"},
        description="Vertebra embedded SO_REUSEPORT, native handlers, 8 workers",
    ),
    "vtb-embedded-asgi": Target(
        name="vtb-embedded-asgi",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:asgi_app",
            "--asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8001",
            topology="embedded",
            workers=1,
        ),
        port=8001,
        description="Vertebra embedded, ASGI adapter, 1 worker",
    ),
    "vtb-embedded-wsgi": Target(
        name="vtb-embedded-wsgi",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:wsgi_app",
            "--host",
            "127.0.0.1",
            "--port",
            "8002",
            topology="embedded",
            workers=1,
        ),
        port=8002,
        description="Vertebra embedded, WSGI adapter, 1 worker",
    ),
    "vtb-shm-native-1w": Target(
        name="vtb-shm-native-1w",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:native_app",
            "--host",
            "127.0.0.1",
            "--port",
            "8010",
            topology="shm",
            workers=1,
        ),
        port=8010,
        env={"VERTEBRA_WORKERS": "1"},
        description="Vertebra SHM, native handlers, 1 worker",
    ),
    "vtb-shm-native-4w": Target(
        name="vtb-shm-native-4w",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:native_app",
            "--host",
            "127.0.0.1",
            "--port",
            "8011",
            topology="shm",
            workers=4,
        ),
        port=8011,
        env={"VERTEBRA_WORKERS": "4"},
        description="Vertebra SHM, native handlers, 4 workers",
    ),
    "vtb-shm-native-8w": Target(
        name="vtb-shm-native-8w",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:native_app",
            "--host",
            "127.0.0.1",
            "--port",
            "8014",
            topology="shm",
            workers=8,
        ),
        port=8014,
        env={"VERTEBRA_WORKERS": "8"},
        description="Vertebra SHM, native handlers, 8 workers",
    ),
    "vtb-shm-asgi-1w": Target(
        name="vtb-shm-asgi-1w",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:asgi_app",
            "--asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8018",
            topology="shm",
            workers=1,
        ),
        port=8018,
        env={"VERTEBRA_WORKERS": "1"},
        description="Vertebra SHM, ASGI, 1 worker",
    ),
    "vtb-shm-asgi-4w": Target(
        name="vtb-shm-asgi-4w",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:asgi_app",
            "--asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8012",
            topology="shm",
            workers=4,
        ),
        port=8012,
        env={"VERTEBRA_WORKERS": "4"},
        description="Vertebra SHM, ASGI, 4 workers",
    ),
    "vtb-shm-asgi-8w": Target(
        name="vtb-shm-asgi-8w",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:asgi_app",
            "--asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8015",
            topology="shm",
            workers=8,
        ),
        port=8015,
        env={"VERTEBRA_WORKERS": "8"},
        description="Vertebra SHM, ASGI, 8 workers",
    ),
    "vtb-shm-wsgi-4w": Target(
        name="vtb-shm-wsgi-4w",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:wsgi_app",
            "--host",
            "127.0.0.1",
            "--port",
            "8013",
            topology="shm",
            workers=4,
        ),
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
            "VERTEBRA_THREADS": "4",
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
    "granian-rsgi": Target(
        name="granian-rsgi",
        topology=Topology.BASELINE,
        cmd=_py(
            "-m",
            "granian",
            "tools.benchmarks.targets:rsgi_app",
            "--interface",
            "rsgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8006",
        ),
        port=8006,
        description="Granian RSGI",
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
