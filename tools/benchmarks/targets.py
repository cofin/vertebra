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
    """Append ``shm_broker=true`` to PostgreSQL DSNs when SHM broker pooling is enabled."""
    if broker_mode.strip().lower() != "shm_broker" or not dsn:
        return dsn
    lowered = dsn.strip().lower()
    if not lowered.startswith(("postgres://", "postgresql://", "postgresql+")):
        return dsn
    if "shm_broker=" in dsn:
        return dsn
    separator = "&" if "?" in dsn else "?"
    return f"{dsn}{separator}shm_broker=true"


def _shm_broker_env(
    workers: int,
    total_budget: int = 40,
    *,
    extra_env: dict[str, str] | None = None,
) -> Callable[[], dict[str, str]]:
    """Build a lazy environment factory for SHM broker targets with DSN parity."""

    def _factory() -> dict[str, str]:
        env: dict[str, str] = {
            "VERTEBRA_WORKERS": str(workers),
            "VTB_BENCH_WORKERS": str(workers),
            "VTB_BENCH_BROKER_MODE": "shm_broker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": str(total_budget),
        }
        if extra_env:
            env.update(extra_env)
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


_DATA_PER_WORKER_ENV: dict[str, str] = {
    "VTB_BENCH_WORKERS": "4",
    "VTB_BENCH_BROKER_MODE": "per_worker",
    "VTB_BENCH_TOTAL_POOL_BUDGET": "40",
}
_VTB_DATA_4W_ENV: dict[str, str] = {"VERTEBRA_WORKERS": "4", **_DATA_PER_WORKER_ENV}


def _vtb(
    name: str,
    app: str,
    port: int,
    description: str,
    *,
    topology: Topology = Topology.EMBEDDED,
    workers: int = 1,
    extra_args: tuple[str, ...] = (),
    env: dict[str, str] | Callable[[], dict[str, str]] | None = None,
    category: str = "vertebra",
    protocol: str = "http1",
    transport: str = "native",
    adapter: str = "none",
    broker_mode: str = "none",
    pool_size: int | None = None,
    target_workers: int | None = None,
) -> tuple[str, Target]:
    topo_flag = "embedded" if topology is Topology.EMBEDDED else "shm"
    return name, Target(
        name=name,
        topology=topology,
        cmd=_vertebra_run(
            app, *extra_args, "--host", "127.0.0.1", "--port", str(port), topology=topo_flag, workers=workers
        ),
        port=port,
        env=env,
        description=description,
        category=category,
        protocol=protocol,
        transport=transport,
        adapter=adapter,
        broker_mode=broker_mode,
        pool_size=pool_size,
        workers=workers if target_workers is None else target_workers,
    )


def _vtb_data(
    name: str,
    app: str,
    port: int,
    description: str,
    *,
    transport: str = "native",
    topology: Topology = Topology.EMBEDDED,
    broker: bool = False,
    category: str = "data",
    adapter: str = "none",
    extra_env: dict[str, str] | None = None,
    extra_args: tuple[str, ...] = (),
) -> tuple[str, Target]:
    flags = extra_args or (
        ("--asgi",) if transport == "asgi" else (("--interface", transport) if transport in ("rsgi", "wsgi") else ())
    )
    env: dict[str, str] | Callable[[], dict[str, str]]
    if broker:
        env = _shm_broker_env(workers=4, total_budget=40, extra_env=extra_env)
    else:
        env = {**_VTB_DATA_4W_ENV, **(extra_env or {})}
    return _vtb(
        name,
        app,
        port,
        description,
        topology=topology,
        workers=4,
        extra_args=flags,
        env=env,
        category=category,
        transport=transport,
        adapter=adapter,
        broker_mode="shm_broker" if broker else "per_worker",
        pool_size=40 if broker else 10,
    )


def _granian_data(
    name: str,
    app: str,
    port: int,
    interface: str,
    description: str,
    *,
    factory: bool = True,
    category: str = "framework-baseline",
    adapter: str = "none",
    extra_env: dict[str, str] | None = None,
) -> tuple[str, Target]:
    args: list[str] = ["-m", "granian", app]
    if factory:
        args.append("--factory")
    args.extend(["--interface", interface, "--host", "127.0.0.1", "--port", str(port), "--workers", "4"])
    return name, Target(
        name=name,
        topology=Topology.BASELINE,
        cmd=_py(*args),
        port=port,
        env={**_DATA_PER_WORKER_ENV, **(extra_env or {})},
        description=description,
        category=category,
        transport=interface,
        adapter=adapter,
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    )


def _uvicorn_data(
    name: str,
    app: str,
    port: int,
    description: str,
    *,
    factory: bool = True,
    category: str = "framework-baseline",
    adapter: str = "none",
    extra_env: dict[str, str] | None = None,
) -> tuple[str, Target]:
    args: list[str] = ["-m", "uvicorn", app]
    if factory:
        args.append("--factory")
    args.extend(
        ["--host", "127.0.0.1", "--port", str(port), "--http", "httptools", "--loop", "uvloop", "--workers", "4"]
    )
    return name, Target(
        name=name,
        topology=Topology.BASELINE,
        cmd=_py(*args),
        port=port,
        env={**_DATA_PER_WORKER_ENV, **(extra_env or {})},
        description=description,
        category=category,
        transport="asgi",
        adapter=adapter,
        broker_mode="per_worker",
        pool_size=10,
        workers=4,
    )


def _ft_data(
    name: str,
    app: str,
    port: int,
    threads: int,
    transport: str,
    description: str,
    *,
    interface: str | None = None,
) -> tuple[str, Target]:
    return name, Target(
        name=name,
        topology=Topology.FREE_THREADED,
        cmd=_ft_run,
        port=port,
        env=_ft_data_env(app, port, threads=threads, interface=interface),
        description=description,
        category="data-freethreaded",
        transport=transport,
        pool_size=40,
        workers=1,
    )


_VTB_APP = "tools.benchmarks.apps.vertebra_app"
_DATA_NATIVE = "tools.benchmarks.apps.data_native:native_data_app"
_DATA_RSGI = "tools.benchmarks.apps.data_rsgi:vertebra_rsgi_app"
_DATA_ASGI = "tools.benchmarks.apps.data_asgi:vertebra_asgi_app"
_DATA_WSGI = "tools.benchmarks.apps.data_wsgi:vertebra_wsgi_app"
_FW = "tools.benchmarks.apps.data_frameworks"

ALL_TARGETS: dict[str, Target] = dict(
    [
        _vtb(
            "vtb-embedded-native",
            f"{_VTB_APP}:native_app",
            8000,
            "Vertebra embedded, native handlers, 1 worker",
            target_workers=1,
        ),
        _vtb(
            "vtb-embedded-native-4w",
            f"{_VTB_APP}:native_app",
            8016,
            "Vertebra embedded SO_REUSEPORT, native handlers, 4 workers",
            workers=4,
            env={"VERTEBRA_WORKERS": "4"},
            target_workers=1,
        ),
        _vtb(
            "vtb-embedded-native-8w",
            f"{_VTB_APP}:native_app",
            8017,
            "Vertebra embedded SO_REUSEPORT, native handlers, 8 workers",
            workers=8,
            env={"VERTEBRA_WORKERS": "8"},
            target_workers=1,
        ),
        _vtb(
            "vtb-embedded-asgi",
            f"{_VTB_APP}:asgi_app",
            8001,
            "Vertebra embedded, ASGI adapter, 1 worker",
            extra_args=("--asgi",),
        ),
        _vtb("vtb-embedded-wsgi", f"{_VTB_APP}:wsgi_app", 8002, "Vertebra embedded, WSGI adapter, 1 worker"),
        _vtb(
            "vtb-shm-native-1w",
            f"{_VTB_APP}:native_app",
            8010,
            "Vertebra SHM, native handlers, 1 worker",
            topology=Topology.SHM,
            workers=1,
            env={"VERTEBRA_WORKERS": "1"},
        ),
        _vtb(
            "vtb-shm-native-4w",
            f"{_VTB_APP}:native_app",
            8011,
            "Vertebra SHM, native handlers, 4 workers",
            topology=Topology.SHM,
            workers=4,
            env={"VERTEBRA_WORKERS": "4"},
            target_workers=1,
        ),
        _vtb(
            "vtb-shm-native-8w",
            f"{_VTB_APP}:native_app",
            8014,
            "Vertebra SHM, native handlers, 8 workers",
            topology=Topology.SHM,
            workers=8,
            env={"VERTEBRA_WORKERS": "8"},
            target_workers=1,
        ),
        _vtb(
            "vtb-shm-asgi-1w",
            f"{_VTB_APP}:asgi_app",
            8018,
            "Vertebra SHM, ASGI, 1 worker",
            topology=Topology.SHM,
            workers=1,
            extra_args=("--asgi",),
            env={"VERTEBRA_WORKERS": "1"},
        ),
        _vtb(
            "vtb-shm-asgi-4w",
            f"{_VTB_APP}:asgi_app",
            8012,
            "Vertebra SHM, ASGI, 4 workers",
            topology=Topology.SHM,
            workers=4,
            extra_args=("--asgi",),
            env={"VERTEBRA_WORKERS": "4"},
            target_workers=1,
        ),
        _vtb(
            "vtb-shm-asgi-8w",
            f"{_VTB_APP}:asgi_app",
            8015,
            "Vertebra SHM, ASGI, 8 workers",
            topology=Topology.SHM,
            workers=8,
            extra_args=("--asgi",),
            env={"VERTEBRA_WORKERS": "8"},
            target_workers=1,
        ),
        _vtb(
            "vtb-shm-wsgi-4w",
            f"{_VTB_APP}:wsgi_app",
            8013,
            "Vertebra SHM, WSGI, 4 workers",
            topology=Topology.SHM,
            workers=4,
            env={"VERTEBRA_WORKERS": "4"},
            target_workers=1,
        ),
        (
            "vtb-ft-native-4w",
            Target(
                name="vtb-ft-native-4w",
                topology=Topology.FREE_THREADED,
                cmd=_ft_run,
                port=8022,
                env=lambda: {
                    **_ft_env(),
                    "VERTEBRA_APP": f"{_VTB_APP}:native_app",
                    "VERTEBRA_HOST": "127.0.0.1",
                    "VERTEBRA_PORT": "8022",
                    "VERTEBRA_WORKERS": "0",
                    "VERTEBRA_THREADS": "4",
                    "VERTEBRA_CPU_COUNT": "4",
                },
                description="Vertebra FT, native handlers, 4 threads",
                category="freethreaded",
            ),
        ),
        (
            "granian-asgi",
            Target(
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
        ),
        (
            "granian-rsgi",
            Target(
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
        ),
        (
            "uvicorn-httptools",
            Target(
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
        ),
        (
            "spikard",
            Target(
                name="spikard",
                topology=Topology.BASELINE,
                cmd=_py("-m", "tools.benchmarks.apps.spikard_app"),
                port=8005,
                env={"SPIKARD_HOST": "127.0.0.1", "SPIKARD_PORT": "8005"},
                description="Spikard",
                category="baseline",
            ),
        ),
        _vtb(
            "vtb-embedded-rsgi",
            "tools.benchmarks.targets:rsgi_app",
            8100,
            "Vertebra embedded, RSGI adapter, 1 worker",
            extra_args=("--interface", "rsgi"),
            transport="rsgi",
        ),
        _vtb(
            "vtb-data-embedded-native-1w",
            _DATA_NATIVE,
            8101,
            "Vertebra embedded data, native handlers, 1 worker",
            env={"VERTEBRA_WORKERS": "1", "VTB_BENCH_WORKERS": "1", "VTB_BENCH_TOTAL_POOL_BUDGET": "40"},
            category="data",
            pool_size=40,
        ),
        _vtb_data(
            "vtb-data-embedded-native-4w", _DATA_NATIVE, 8102, "Vertebra embedded data, native handlers, 4 workers"
        ),
        _vtb_data(
            "vtb-data-embedded-rsgi-4w",
            _DATA_RSGI,
            8103,
            "Vertebra embedded data, RSGI adapter, 4 workers",
            transport="rsgi",
        ),
        _vtb_data(
            "vtb-data-embedded-asgi-4w",
            _DATA_ASGI,
            8104,
            "Vertebra embedded data, ASGI adapter, 4 workers",
            transport="asgi",
        ),
        _vtb_data(
            "vtb-data-embedded-wsgi-4w",
            _DATA_WSGI,
            8105,
            "Vertebra embedded data, WSGI adapter, 4 workers",
            transport="wsgi",
        ),
        _vtb_data(
            "vtb-data-shm-native-4w",
            _DATA_NATIVE,
            8110,
            "Vertebra SHM data, native handlers, 4 workers per-worker pool",
            topology=Topology.SHM,
        ),
        _vtb_data(
            "vtb-data-shm-native-4w-broker",
            _DATA_NATIVE,
            8111,
            "Vertebra SHM data, native handlers, 4 workers SHM broker pool",
            topology=Topology.SHM,
            broker=True,
        ),
        _vtb_data(
            "vtb-data-shm-rsgi-4w",
            _DATA_RSGI,
            8112,
            "Vertebra SHM data, RSGI adapter, 4 workers per-worker pool",
            transport="rsgi",
            topology=Topology.SHM,
        ),
        _vtb_data(
            "vtb-data-shm-rsgi-4w-broker",
            _DATA_RSGI,
            8113,
            "Vertebra SHM data, RSGI adapter, 4 workers SHM broker pool",
            transport="rsgi",
            topology=Topology.SHM,
            broker=True,
        ),
        _vtb_data(
            "vtb-data-shm-asgi-4w",
            _DATA_ASGI,
            8114,
            "Vertebra SHM data, ASGI adapter, 4 workers per-worker pool",
            transport="asgi",
            topology=Topology.SHM,
        ),
        _vtb_data(
            "vtb-data-shm-asgi-4w-broker",
            _DATA_ASGI,
            8115,
            "Vertebra SHM data, ASGI adapter, 4 workers SHM broker pool",
            transport="asgi",
            topology=Topology.SHM,
            broker=True,
        ),
        _vtb_data(
            "vtb-data-shm-wsgi-4w",
            _DATA_WSGI,
            8116,
            "Vertebra SHM data, WSGI adapter, 4 workers per-worker pool",
            transport="wsgi",
            topology=Topology.SHM,
        ),
        _vtb(
            "vtb-grpc-asgi",
            "tools.benchmarks.apps.data_grpc:grpc_asgi_app",
            8120,
            "Vertebra gRPC over ASGI with HTTP/2 trailers",
            extra_args=("--asgi",),
            env={"VERTEBRA_H2C": "1", "VTB_BENCH_WORKERS": "1", "VTB_BENCH_TOTAL_POOL_BUDGET": "40"},
            category="grpc",
            protocol="grpc",
            transport="grpc-asgi",
            pool_size=40,
        ),
        _vtb(
            "vtb-grpc-native-h2c",
            "tools.benchmarks.apps.data_grpc:grpc_native_app",
            8121,
            "Vertebra native H2C unary gRPC procedures",
            env={"VERTEBRA_H2C": "1", "VTB_BENCH_WORKERS": "1", "VTB_BENCH_TOTAL_POOL_BUDGET": "40"},
            category="grpc",
            protocol="grpc",
            transport="grpc-native-h2c",
            pool_size=40,
        ),
        _ft_data(
            "vtb-ft-data-native-1t", _DATA_NATIVE, 8125, 1, "native", "Vertebra FT data, native handlers, 1 thread"
        ),
        _ft_data(
            "vtb-ft-data-native-4t", _DATA_NATIVE, 8126, 4, "native", "Vertebra FT data, native handlers, 4 threads"
        ),
        _ft_data(
            "vtb-ft-data-native-8t", _DATA_NATIVE, 8127, 8, "native", "Vertebra FT data, native handlers, 8 threads"
        ),
        _ft_data(
            "vtb-ft-data-rsgi-4t",
            _DATA_RSGI,
            8128,
            4,
            "rsgi",
            "Vertebra FT data, RSGI adapter, 4 threads",
            interface="rsgi",
        ),
        _ft_data(
            "vtb-ft-data-asgi-4t",
            _DATA_ASGI,
            8129,
            4,
            "asgi",
            "Vertebra FT data, ASGI adapter, 4 threads",
            interface="asgi",
        ),
        _granian_data(
            "granian-rsgi-data",
            "tools.benchmarks.apps.data_rsgi:rsgi_app",
            8130,
            "rsgi",
            "Granian RSGI data baseline",
            factory=False,
            category="data-baseline",
        ),
        _granian_data(
            "granian-asgi-data",
            "tools.benchmarks.apps.data_asgi:asgi_app",
            8131,
            "asgi",
            "Granian ASGI data baseline",
            factory=False,
            category="data-baseline",
        ),
        _uvicorn_data(
            "uvicorn-httptools-data",
            "tools.benchmarks.apps.data_asgi:asgi_app",
            8132,
            "Uvicorn httptools data baseline",
            factory=False,
            category="data-baseline",
        ),
        (
            "django-bolt-data",
            Target(
                name="django-bolt-data",
                topology=Topology.BASELINE,
                cmd=_py("-m", "django", "runbolt", "--host", "127.0.0.1", "--port", "8140", "--processes", "4"),
                port=8140,
                env=dict(_DATA_PER_WORKER_ENV),
                description="Django-Bolt data framework baseline",
                category="framework-baseline",
                transport="rust-actix",
                broker_mode="per_worker",
                pool_size=10,
                workers=4,
            ),
        ),
        (
            "spikard-data",
            Target(
                name="spikard-data",
                topology=Topology.BASELINE,
                cmd=_py(
                    "-c",
                    "from tools.benchmarks.apps.data_frameworks import create_spikard_data_app; "
                    "create_spikard_data_app().run(host='127.0.0.1', port=8141)",
                ),
                port=8141,
                env={"SPIKARD_HOST": "127.0.0.1", "SPIKARD_PORT": "8141", **_DATA_PER_WORKER_ENV},
                description="Spikard data framework baseline",
                category="framework-baseline",
                transport="rust-spikard",
                broker_mode="per_worker",
                pool_size=10,
                workers=4,
            ),
        ),
        _vtb_data(
            "litestar-vtb-data",
            f"{_FW}:litestar_app",
            8142,
            "Litestar on Vertebra ASGI data framework baseline",
            transport="asgi",
            category="framework-baseline",
        ),
        _granian_data(
            "litestar-granian-data",
            f"{_FW}:create_litestar_app",
            8143,
            "asgi",
            "Litestar on Granian ASGI data framework baseline",
        ),
        _vtb_data(
            "fastapi-vtb-data",
            f"{_FW}:fastapi_app",
            8144,
            "FastAPI on Vertebra ASGI data framework baseline",
            transport="asgi",
            category="framework-baseline",
        ),
        _granian_data(
            "fastapi-granian-data",
            f"{_FW}:create_fastapi_app",
            8145,
            "asgi",
            "FastAPI on Granian ASGI data framework baseline",
        ),
        _uvicorn_data(
            "fastapi-uvicorn-data", f"{_FW}:create_fastapi_app", 8146, "FastAPI on Uvicorn ASGI data framework baseline"
        ),
        _vtb_data(
            "django-vtb-data",
            f"{_FW}:django_app",
            8147,
            "Django on Vertebra ASGI data framework baseline",
            transport="asgi",
            category="framework-baseline",
        ),
        _vtb_data(
            "flask-vtb-data",
            f"{_FW}:flask_app",
            8148,
            "Flask on Vertebra WSGI data framework baseline",
            transport="wsgi",
            category="framework-baseline",
        ),
        _granian_data(
            "django-granian-data",
            f"{_FW}:create_django_app",
            8149,
            "asgi",
            "Regular Django ASGI on Granian data framework baseline",
        ),
        _uvicorn_data(
            "django-uvicorn-data",
            f"{_FW}:create_django_app",
            8150,
            "Regular Django ASGI on Uvicorn data framework baseline",
        ),
        _vtb_data(
            "django-wsgi-vtb-data",
            f"{_FW}:django_wsgi_app",
            8151,
            "Regular Django WSGI on Vertebra data framework baseline",
            transport="wsgi",
            category="framework-baseline",
        ),
        _granian_data(
            "django-wsgi-granian-data",
            f"{_FW}:create_django_wsgi_app",
            8152,
            "wsgi",
            "Regular Django WSGI on Granian data framework baseline",
        ),
        _vtb_data(
            "blacksheep-vtb-data",
            f"{_FW}:blacksheep_app",
            8153,
            "BlackSheep on Vertebra ASGI data framework baseline",
            transport="asgi",
            category="framework-baseline",
        ),
        _granian_data(
            "blacksheep-granian-data",
            f"{_FW}:create_blacksheep_app",
            8154,
            "asgi",
            "BlackSheep on Granian ASGI data framework baseline",
        ),
        _uvicorn_data(
            "blacksheep-uvicorn-data",
            f"{_FW}:create_blacksheep_app",
            8155,
            "BlackSheep on Uvicorn ASGI data framework baseline",
        ),
        _uvicorn_data(
            "litestar-uvicorn-data",
            f"{_FW}:create_litestar_app",
            8156,
            "Litestar on Uvicorn ASGI data framework baseline",
        ),
        _granian_data(
            "flask-granian-data",
            f"{_FW}:create_flask_app",
            8157,
            "wsgi",
            "Flask on Granian WSGI data framework baseline",
        ),
        _vtb_data(
            "vtb-data-sqlspec-native-4w",
            _DATA_NATIVE,
            8160,
            "Vertebra SHM data, SQLSpec adapter, 4 workers per-worker pool",
            topology=Topology.SHM,
            adapter="sqlspec",
            extra_env={"VTB_BENCH_DB_ADAPTER": "sqlspec", "VTB_BENCH_DB_ADAPTER_FLAVOR": "vertebra"},
        ),
        _vtb_data(
            "vtb-data-sqlspec-shm-4w-broker",
            _DATA_NATIVE,
            8161,
            "Vertebra SHM data, SQLSpec adapter, 4 workers SHM broker pool",
            topology=Topology.SHM,
            broker=True,
            adapter="sqlspec",
            extra_env={"VTB_BENCH_DB_ADAPTER": "sqlspec", "VTB_BENCH_DB_ADAPTER_FLAVOR": "vertebra"},
        ),
        _vtb_data(
            "vtb-data-aa-native-4w",
            _DATA_NATIVE,
            8162,
            "Vertebra SHM data, Advanced Alchemy (+vtb) adapter, 4 workers per-worker pool",
            topology=Topology.SHM,
            adapter="advanced_alchemy",
            extra_env={"VTB_BENCH_DB_ADAPTER": "advanced_alchemy", "VTB_BENCH_DB_ADAPTER_FLAVOR": "vertebra"},
        ),
        _vtb_data(
            "vtb-data-aa-shm-4w-broker",
            _DATA_NATIVE,
            8163,
            "Vertebra SHM data, Advanced Alchemy (+vtb) adapter, 4 workers SHM broker pool",
            topology=Topology.SHM,
            broker=True,
            adapter="advanced_alchemy",
            extra_env={"VTB_BENCH_DB_ADAPTER": "advanced_alchemy", "VTB_BENCH_DB_ADAPTER_FLAVOR": "vertebra"},
        ),
        _granian_data(
            "granian-rsgi-data-stdlib",
            "tools.benchmarks.apps.data_rsgi:rsgi_app",
            8170,
            "rsgi",
            "Granian RSGI data baseline (stdlib drivers)",
            factory=False,
            category="data-baseline",
            adapter="stdlib",
            extra_env={"VTB_BENCH_DB_ADAPTER": "stdlib", "VTB_BENCH_DB_ADAPTER_FLAVOR": "upstream"},
        ),
        _granian_data(
            "granian-asgi-data-stdlib",
            "tools.benchmarks.apps.data_asgi:asgi_app",
            8164,
            "asgi",
            "Granian ASGI data baseline (stdlib drivers)",
            factory=False,
            category="data-baseline",
            adapter="stdlib",
            extra_env={"VTB_BENCH_DB_ADAPTER": "stdlib", "VTB_BENCH_DB_ADAPTER_FLAVOR": "upstream"},
        ),
        _granian_data(
            "granian-asgi-data-sqlspec",
            "tools.benchmarks.apps.data_asgi:asgi_app",
            8165,
            "asgi",
            "Granian ASGI data baseline (upstream SQLSpec)",
            factory=False,
            category="data-baseline",
            adapter="sqlspec-upstream",
            extra_env={"VTB_BENCH_DB_ADAPTER": "sqlspec", "VTB_BENCH_DB_ADAPTER_FLAVOR": "upstream"},
        ),
        _granian_data(
            "granian-asgi-data-aa",
            "tools.benchmarks.apps.data_asgi:asgi_app",
            8166,
            "asgi",
            "Granian ASGI data baseline (vanilla Advanced Alchemy)",
            factory=False,
            category="data-baseline",
            adapter="advanced_alchemy-vanilla",
            extra_env={"VTB_BENCH_DB_ADAPTER": "advanced_alchemy", "VTB_BENCH_DB_ADAPTER_FLAVOR": "upstream"},
        ),
        _uvicorn_data(
            "uvicorn-httptools-data-stdlib",
            "tools.benchmarks.apps.data_asgi:asgi_app",
            8167,
            "Uvicorn httptools data baseline (stdlib drivers)",
            factory=False,
            category="data-baseline",
            adapter="stdlib",
            extra_env={"VTB_BENCH_DB_ADAPTER": "stdlib", "VTB_BENCH_DB_ADAPTER_FLAVOR": "upstream"},
        ),
        _uvicorn_data(
            "uvicorn-httptools-data-sqlspec",
            "tools.benchmarks.apps.data_asgi:asgi_app",
            8168,
            "Uvicorn httptools data baseline (upstream SQLSpec)",
            factory=False,
            category="data-baseline",
            adapter="sqlspec-upstream",
            extra_env={"VTB_BENCH_DB_ADAPTER": "sqlspec", "VTB_BENCH_DB_ADAPTER_FLAVOR": "upstream"},
        ),
        _uvicorn_data(
            "uvicorn-httptools-data-aa",
            "tools.benchmarks.apps.data_asgi:asgi_app",
            8169,
            "Uvicorn httptools data baseline (vanilla Advanced Alchemy)",
            factory=False,
            category="data-baseline",
            adapter="advanced_alchemy-vanilla",
            extra_env={"VTB_BENCH_DB_ADAPTER": "advanced_alchemy", "VTB_BENCH_DB_ADAPTER_FLAVOR": "upstream"},
        ),
    ]
)

VERTEBRA_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "vertebra"]
BASELINE_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "baseline"]
FREETHREADED_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "freethreaded"]
DATA_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "data"]
DATA_BASELINE_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "data-baseline"]
DATA_FREETHREADED_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "data-freethreaded"]
FRAMEWORK_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "framework-baseline"]
GRPC_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "grpc"]
