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
        "adapter",
        "broker_mode",
        "category",
        "description",
        "host",
        "name",
        "pool_size",
        "port",
        "protocol",
        "topology",
        "transport",
        "workers",
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
        protocol: str = "http1",
        transport: str = "native",
        adapter: str = "none",
        broker_mode: str = "none",
        pool_size: int | None = None,
        workers: int = 1,
    ) -> None:
        self.name = name
        self._cmd_data = cmd
        self.topology = topology
        self.host = host
        self.port = port
        self._env_data = env
        self.description = description
        self.category = category
        self.protocol = protocol
        self.transport = transport
        self.adapter = adapter
        self.broker_mode = broker_mode
        self.pool_size = pool_size
        self.workers = workers

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


def apply_shm_broker_dsn(dsn: str, *, broker_mode: str = "shm_broker") -> str:
    """Append ``shm_broker=true`` to a database DSN when SHM broker pooling is enabled."""
    if broker_mode.strip().lower() != "shm_broker" or not dsn:
        return dsn
    if "shm_broker=" in dsn:
        return dsn
    separator = "&" if "?" in dsn else "?"
    return f"{dsn}{separator}shm_broker=true"


def _shm_broker_env(workers: int, total_budget: int = 40) -> Callable[[], dict[str, str]]:
    """Build a lazy environment factory for SHM broker targets with DSN parity."""

    def _factory() -> dict[str, str]:
        env: dict[str, str] = {
            "VERTEBRA_WORKERS": str(workers),
            "VTB_BENCH_WORKERS": str(workers),
            "VTB_BENCH_BROKER_MODE": "shm_broker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": str(total_budget),
        }
        dsn = os.environ.get("VTB_BENCH_DB_DSN", "").strip()
        if dsn:
            env["VTB_BENCH_DB_DSN"] = apply_shm_broker_dsn(dsn, broker_mode="shm_broker")
        return env

    return _factory


def _ft_data_env(
    app: str,
    port: int,
    threads: int,
    *,
    interface: str | None = None,
    total_budget: int = 40,
) -> Callable[[], dict[str, str]]:
    """Build a lazy environment factory for strict free-threaded data targets."""

    def _factory() -> dict[str, str]:
        env: dict[str, str] = {
            **_ft_env(),
            "VERTEBRA_APP": app,
            "VERTEBRA_HOST": "127.0.0.1",
            "VERTEBRA_PORT": str(port),
            "VERTEBRA_WORKERS": "0",
            "VERTEBRA_THREADS": str(threads),
            "VERTEBRA_CPU_COUNT": str(threads),
            "VERTEBRA_STRICT_FREE_THREADING": "1",
            "VTB_BENCH_WORKERS": "1",
            "VTB_BENCH_TOTAL_POOL_BUDGET": str(total_budget),
        }
        if interface is not None:
            env["VERTEBRA_INTERFACE"] = interface
        return env

    return _factory


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
    "vtb-embedded-rsgi": Target(
        name="vtb-embedded-rsgi",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.targets:rsgi_app",
            "--interface",
            "rsgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8100",
            topology="embedded",
            workers=1,
        ),
        port=8100,
        description="Vertebra embedded, RSGI adapter, 1 worker",
        category="vertebra",
        transport="rsgi",
        workers=1,
    ),
    "vtb-data-embedded-native-1w": Target(
        name="vtb-data-embedded-native-1w",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_native:native_data_app",
            "--host",
            "127.0.0.1",
            "--port",
            "8101",
            topology="embedded",
            workers=1,
        ),
        port=8101,
        env={
            "VERTEBRA_WORKERS": "1",
            "VTB_BENCH_WORKERS": "1",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Vertebra embedded data, native handlers, 1 worker",
        category="data",
        transport="native",
        broker_mode="none",
        pool_size=40,
        workers=1,
    ),
    "vtb-data-embedded-native-4w": Target(
        name="vtb-data-embedded-native-4w",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_native:native_data_app",
            "--host",
            "127.0.0.1",
            "--port",
            "8102",
            topology="embedded",
            workers=4,
        ),
        port=8102,
        env={
            "VERTEBRA_WORKERS": "4",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Vertebra embedded data, native handlers, 4 workers",
        category="data",
        transport="native",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "vtb-data-embedded-rsgi-4w": Target(
        name="vtb-data-embedded-rsgi-4w",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_rsgi:vertebra_rsgi_app",
            "--interface",
            "rsgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8103",
            topology="embedded",
            workers=4,
        ),
        port=8103,
        env={
            "VERTEBRA_WORKERS": "4",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Vertebra embedded data, RSGI adapter, 4 workers",
        category="data",
        transport="rsgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "vtb-data-embedded-asgi-4w": Target(
        name="vtb-data-embedded-asgi-4w",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_asgi:vertebra_asgi_app",
            "--asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8104",
            topology="embedded",
            workers=4,
        ),
        port=8104,
        env={
            "VERTEBRA_WORKERS": "4",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Vertebra embedded data, ASGI adapter, 4 workers",
        category="data",
        transport="asgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "vtb-data-embedded-wsgi-4w": Target(
        name="vtb-data-embedded-wsgi-4w",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_wsgi:vertebra_wsgi_app",
            "--interface",
            "wsgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8105",
            topology="embedded",
            workers=4,
        ),
        port=8105,
        env={
            "VERTEBRA_WORKERS": "4",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Vertebra embedded data, WSGI adapter, 4 workers",
        category="data",
        transport="wsgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "vtb-data-shm-native-4w": Target(
        name="vtb-data-shm-native-4w",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_native:native_data_app",
            "--host",
            "127.0.0.1",
            "--port",
            "8110",
            topology="shm",
            workers=4,
        ),
        port=8110,
        env={
            "VERTEBRA_WORKERS": "4",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Vertebra SHM data, native handlers, 4 workers per-worker pool",
        category="data",
        transport="native",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "vtb-data-shm-native-4w-broker": Target(
        name="vtb-data-shm-native-4w-broker",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_native:native_data_app",
            "--host",
            "127.0.0.1",
            "--port",
            "8111",
            topology="shm",
            workers=4,
        ),
        port=8111,
        env=_shm_broker_env(workers=4, total_budget=40),
        description="Vertebra SHM data, native handlers, 4 workers SHM broker pool",
        category="data",
        transport="native",
        broker_mode="shm_broker",
        pool_size=40,
        workers=4,
    ),
    "vtb-data-shm-rsgi-4w": Target(
        name="vtb-data-shm-rsgi-4w",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_rsgi:vertebra_rsgi_app",
            "--interface",
            "rsgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8112",
            topology="shm",
            workers=4,
        ),
        port=8112,
        env={
            "VERTEBRA_WORKERS": "4",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Vertebra SHM data, RSGI adapter, 4 workers per-worker pool",
        category="data",
        transport="rsgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "vtb-data-shm-rsgi-4w-broker": Target(
        name="vtb-data-shm-rsgi-4w-broker",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_rsgi:vertebra_rsgi_app",
            "--interface",
            "rsgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8113",
            topology="shm",
            workers=4,
        ),
        port=8113,
        env=_shm_broker_env(workers=4, total_budget=40),
        description="Vertebra SHM data, RSGI adapter, 4 workers SHM broker pool",
        category="data",
        transport="rsgi",
        broker_mode="shm_broker",
        pool_size=40,
        workers=4,
    ),
    "vtb-data-shm-asgi-4w": Target(
        name="vtb-data-shm-asgi-4w",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_asgi:vertebra_asgi_app",
            "--asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8114",
            topology="shm",
            workers=4,
        ),
        port=8114,
        env={
            "VERTEBRA_WORKERS": "4",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Vertebra SHM data, ASGI adapter, 4 workers per-worker pool",
        category="data",
        transport="asgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "vtb-data-shm-asgi-4w-broker": Target(
        name="vtb-data-shm-asgi-4w-broker",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_asgi:vertebra_asgi_app",
            "--asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8115",
            topology="shm",
            workers=4,
        ),
        port=8115,
        env=_shm_broker_env(workers=4, total_budget=40),
        description="Vertebra SHM data, ASGI adapter, 4 workers SHM broker pool",
        category="data",
        transport="asgi",
        broker_mode="shm_broker",
        pool_size=40,
        workers=4,
    ),
    "vtb-data-shm-wsgi-4w": Target(
        name="vtb-data-shm-wsgi-4w",
        topology=Topology.SHM,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_wsgi:vertebra_wsgi_app",
            "--interface",
            "wsgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8116",
            topology="shm",
            workers=4,
        ),
        port=8116,
        env={
            "VERTEBRA_WORKERS": "4",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Vertebra SHM data, WSGI adapter, 4 workers per-worker pool",
        category="data",
        transport="wsgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "vtb-grpc-asgi": Target(
        name="vtb-grpc-asgi",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_grpc:grpc_asgi_app",
            "--asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8120",
            topology="embedded",
            workers=1,
        ),
        port=8120,
        env={
            "VERTEBRA_H2C": "1",
            "VTB_BENCH_WORKERS": "1",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Vertebra gRPC over ASGI with HTTP/2 trailers",
        category="grpc",
        protocol="grpc",
        transport="grpc-asgi",
        pool_size=40,
        workers=1,
    ),
    "vtb-grpc-native-h2c": Target(
        name="vtb-grpc-native-h2c",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_grpc:grpc_native_app",
            "--host",
            "127.0.0.1",
            "--port",
            "8121",
            topology="embedded",
            workers=1,
        ),
        port=8121,
        env={
            "VERTEBRA_H2C": "1",
            "VTB_BENCH_WORKERS": "1",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Vertebra native H2C unary gRPC procedures",
        category="grpc",
        protocol="grpc",
        transport="grpc-native-h2c",
        pool_size=40,
        workers=1,
    ),
    "vtb-ft-data-native-1t": Target(
        name="vtb-ft-data-native-1t",
        topology=Topology.FREE_THREADED,
        cmd=_ft_run,
        port=8125,
        env=_ft_data_env("tools.benchmarks.apps.data_native:native_data_app", 8125, threads=1),
        description="Vertebra FT data, native handlers, 1 thread",
        category="data-freethreaded",
        transport="native",
        pool_size=40,
        workers=1,
    ),
    "vtb-ft-data-native-4t": Target(
        name="vtb-ft-data-native-4t",
        topology=Topology.FREE_THREADED,
        cmd=_ft_run,
        port=8126,
        env=_ft_data_env("tools.benchmarks.apps.data_native:native_data_app", 8126, threads=4),
        description="Vertebra FT data, native handlers, 4 threads",
        category="data-freethreaded",
        transport="native",
        pool_size=40,
        workers=1,
    ),
    "vtb-ft-data-native-8t": Target(
        name="vtb-ft-data-native-8t",
        topology=Topology.FREE_THREADED,
        cmd=_ft_run,
        port=8127,
        env=_ft_data_env("tools.benchmarks.apps.data_native:native_data_app", 8127, threads=8),
        description="Vertebra FT data, native handlers, 8 threads",
        category="data-freethreaded",
        transport="native",
        pool_size=40,
        workers=1,
    ),
    "vtb-ft-data-rsgi-4t": Target(
        name="vtb-ft-data-rsgi-4t",
        topology=Topology.FREE_THREADED,
        cmd=_ft_run,
        port=8128,
        env=_ft_data_env("tools.benchmarks.apps.data_rsgi:vertebra_rsgi_app", 8128, threads=4, interface="rsgi"),
        description="Vertebra FT data, RSGI adapter, 4 threads",
        category="data-freethreaded",
        transport="rsgi",
        pool_size=40,
        workers=1,
    ),
    "vtb-ft-data-asgi-4t": Target(
        name="vtb-ft-data-asgi-4t",
        topology=Topology.FREE_THREADED,
        cmd=_ft_run,
        port=8129,
        env=_ft_data_env("tools.benchmarks.apps.data_asgi:vertebra_asgi_app", 8129, threads=4, interface="asgi"),
        description="Vertebra FT data, ASGI adapter, 4 threads",
        category="data-freethreaded",
        transport="asgi",
        pool_size=40,
        workers=1,
    ),
    "granian-rsgi-data": Target(
        name="granian-rsgi-data",
        topology=Topology.BASELINE,
        cmd=_py(
            "-m",
            "granian",
            "tools.benchmarks.apps.data_rsgi:rsgi_app",
            "--interface",
            "rsgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8130",
            "--workers",
            "4",
        ),
        port=8130,
        env={
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Granian RSGI data baseline",
        category="data-baseline",
        transport="rsgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "granian-asgi-data": Target(
        name="granian-asgi-data",
        topology=Topology.BASELINE,
        cmd=_py(
            "-m",
            "granian",
            "tools.benchmarks.apps.data_asgi:asgi_app",
            "--interface",
            "asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8131",
            "--workers",
            "4",
        ),
        port=8131,
        env={
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Granian ASGI data baseline",
        category="data-baseline",
        transport="asgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "uvicorn-httptools-data": Target(
        name="uvicorn-httptools-data",
        topology=Topology.BASELINE,
        cmd=_py(
            "-m",
            "uvicorn",
            "tools.benchmarks.apps.data_asgi:asgi_app",
            "--host",
            "127.0.0.1",
            "--port",
            "8132",
            "--http",
            "httptools",
            "--loop",
            "uvloop",
            "--workers",
            "4",
        ),
        port=8132,
        env={
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Uvicorn httptools data baseline",
        category="data-baseline",
        transport="asgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "django-bolt-data": Target(
        name="django-bolt-data",
        topology=Topology.BASELINE,
        cmd=_py(
            "-m",
            "django",
            "runbolt",
            "--host",
            "127.0.0.1",
            "--port",
            "8140",
            "--processes",
            "4",
        ),
        port=8140,
        env={
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Django-Bolt data framework baseline",
        category="framework-baseline",
        transport="rust-actix",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "spikard-data": Target(
        name="spikard-data",
        topology=Topology.BASELINE,
        cmd=_py(
            "-c",
            "from tools.benchmarks.apps.data_frameworks import create_spikard_data_app; "
            "create_spikard_data_app().run(host='127.0.0.1', port=8141)",
        ),
        port=8141,
        env={
            "SPIKARD_HOST": "127.0.0.1",
            "SPIKARD_PORT": "8141",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Spikard data framework baseline",
        category="framework-baseline",
        transport="rust-spikard",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "litestar-vtb-data": Target(
        name="litestar-vtb-data",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_frameworks:litestar_app",
            "--asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8142",
            topology="embedded",
            workers=4,
        ),
        port=8142,
        env={
            "VERTEBRA_WORKERS": "4",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Litestar on Vertebra ASGI data framework baseline",
        category="framework-baseline",
        transport="asgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "litestar-granian-data": Target(
        name="litestar-granian-data",
        topology=Topology.BASELINE,
        cmd=_py(
            "-m",
            "granian",
            "tools.benchmarks.apps.data_frameworks:create_litestar_app",
            "--factory",
            "--interface",
            "asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8143",
            "--workers",
            "4",
        ),
        port=8143,
        env={
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Litestar on Granian ASGI data framework baseline",
        category="framework-baseline",
        transport="asgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "fastapi-vtb-data": Target(
        name="fastapi-vtb-data",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_frameworks:fastapi_app",
            "--asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8144",
            topology="embedded",
            workers=4,
        ),
        port=8144,
        env={
            "VERTEBRA_WORKERS": "4",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="FastAPI on Vertebra ASGI data framework baseline",
        category="framework-baseline",
        transport="asgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "fastapi-granian-data": Target(
        name="fastapi-granian-data",
        topology=Topology.BASELINE,
        cmd=_py(
            "-m",
            "granian",
            "tools.benchmarks.apps.data_frameworks:create_fastapi_app",
            "--factory",
            "--interface",
            "asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8145",
            "--workers",
            "4",
        ),
        port=8145,
        env={
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="FastAPI on Granian ASGI data framework baseline",
        category="framework-baseline",
        transport="asgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "fastapi-uvicorn-data": Target(
        name="fastapi-uvicorn-data",
        topology=Topology.BASELINE,
        cmd=_py(
            "-m",
            "uvicorn",
            "tools.benchmarks.apps.data_frameworks:create_fastapi_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            "8146",
            "--http",
            "httptools",
            "--loop",
            "uvloop",
            "--workers",
            "4",
        ),
        port=8146,
        env={
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="FastAPI on Uvicorn ASGI data framework baseline",
        category="framework-baseline",
        transport="asgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "django-vtb-data": Target(
        name="django-vtb-data",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_frameworks:django_app",
            "--asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8147",
            topology="embedded",
            workers=4,
        ),
        port=8147,
        env={
            "VERTEBRA_WORKERS": "4",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Django on Vertebra ASGI data framework baseline",
        category="framework-baseline",
        transport="asgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "flask-vtb-data": Target(
        name="flask-vtb-data",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_frameworks:flask_app",
            "--interface",
            "wsgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8148",
            topology="embedded",
            workers=4,
        ),
        port=8148,
        env={
            "VERTEBRA_WORKERS": "4",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Flask on Vertebra WSGI data framework baseline",
        category="framework-baseline",
        transport="wsgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "django-granian-data": Target(
        name="django-granian-data",
        topology=Topology.BASELINE,
        cmd=_py(
            "-m",
            "granian",
            "tools.benchmarks.apps.data_frameworks:create_django_app",
            "--factory",
            "--interface",
            "asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8149",
            "--workers",
            "4",
        ),
        port=8149,
        env={
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Regular Django ASGI on Granian data framework baseline",
        category="framework-baseline",
        transport="asgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "django-uvicorn-data": Target(
        name="django-uvicorn-data",
        topology=Topology.BASELINE,
        cmd=_py(
            "-m",
            "uvicorn",
            "tools.benchmarks.apps.data_frameworks:create_django_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            "8150",
            "--http",
            "httptools",
            "--loop",
            "uvloop",
            "--workers",
            "4",
        ),
        port=8150,
        env={
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Regular Django ASGI on Uvicorn data framework baseline",
        category="framework-baseline",
        transport="asgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "django-wsgi-vtb-data": Target(
        name="django-wsgi-vtb-data",
        topology=Topology.EMBEDDED,
        cmd=_vertebra_run(
            "tools.benchmarks.apps.data_frameworks:django_wsgi_app",
            "--interface",
            "wsgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8151",
            topology="embedded",
            workers=4,
        ),
        port=8151,
        env={
            "VERTEBRA_WORKERS": "4",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Regular Django WSGI on Vertebra data framework baseline",
        category="framework-baseline",
        transport="wsgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
    "django-wsgi-granian-data": Target(
        name="django-wsgi-granian-data",
        topology=Topology.BASELINE,
        cmd=_py(
            "-m",
            "granian",
            "tools.benchmarks.apps.data_frameworks:create_django_wsgi_app",
            "--factory",
            "--interface",
            "wsgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8152",
            "--workers",
            "4",
        ),
        port=8152,
        env={
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "per_worker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
        },
        description="Regular Django WSGI on Granian data framework baseline",
        category="framework-baseline",
        transport="wsgi",
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    ),
}

VERTEBRA_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "vertebra"]
BASELINE_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "baseline"]
FREETHREADED_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "freethreaded"]
DATA_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "data"]
DATA_BASELINE_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "data-baseline"]
DATA_FREETHREADED_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "data-freethreaded"]
FRAMEWORK_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "framework-baseline"]
GRPC_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "grpc"]
