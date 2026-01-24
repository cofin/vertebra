"""Server configurations for ASGI benchmarks.

Defines all servers, runtimes, and frameworks to benchmark, organized into
three categories for fair comparison:

Category 1 (SERVER): Raw ASGI server performance
Category 2 (RUNTIME): Integrated runtimes with native interfaces
Category 3 (FRAMEWORK): Framework overhead on the same server
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class BenchmarkCategory(Enum):
    """Categories for benchmark grouping."""

    SERVER = "server"  # Raw ASGI server performance
    RUNTIME = "runtime"  # Integrated runtimes (server + framework inseparable)
    FRAMEWORK = "framework"  # Framework overhead comparison


class InterfaceType(Enum):
    """Server interface type."""

    ASGI = "asgi"
    NATIVE = "native"  # Robyn, Spikard have custom interfaces


@dataclass(frozen=True)
class ServerConfig:
    """Configuration for a benchmark target."""

    name: str
    display_name: str
    category: BenchmarkCategory
    command: tuple[str, ...]
    port: int
    interface: InterfaceType = InterfaceType.ASGI
    startup_delay: float = 2.0
    requires_install: tuple[str, ...] = field(default_factory=tuple)
    env: dict[str, str] = field(default_factory=dict)
    notes: str = ""


# =============================================================================
# Category 1: Raw ASGI Server Performance
# =============================================================================
# Same minimal ASGI application on different servers.
# Isolates HTTP parsing, connection handling, and event loop efficiency.

CATEGORY_1_SERVERS: dict[str, ServerConfig] = {
    "uvicorn_h11": ServerConfig(
        name="uvicorn_h11",
        display_name="Uvicorn (h11)",
        category=BenchmarkCategory.SERVER,
        command=(
            "uvicorn",
            "--host",
            "127.0.0.1",
            "--port",
            "8002",
            "--workers",
            "1",
            "--loop",
            "uvloop",
            "--http",
            "h11",
            "--no-access-log",
            "benchmarks.servers.app.asgi:app",
        ),
        port=8002,
        requires_install=("uvicorn[standard]",),
        notes="Pure Python HTTP parsing, baseline",
    ),
    "uvicorn_httptools": ServerConfig(
        name="uvicorn_httptools",
        display_name="Uvicorn (httptools)",
        category=BenchmarkCategory.SERVER,
        command=(
            "uvicorn",
            "--host",
            "127.0.0.1",
            "--port",
            "8003",
            "--workers",
            "1",
            "--loop",
            "uvloop",
            "--http",
            "httptools",
            "--no-access-log",
            "benchmarks.servers.app.asgi:app",
        ),
        port=8003,
        requires_install=("uvicorn[standard]",),
        notes="~30% faster HTTP parsing via Cython",
    ),
    "granian": ServerConfig(
        name="granian",
        display_name="Granian",
        category=BenchmarkCategory.SERVER,
        command=(
            "granian",
            "--interface",
            "asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8004",
            "--workers",
            "1",
            "--runtime-threads",
            "1",
            "--http",
            "1",
            "--no-ws",
            "benchmarks.servers.app.asgi:app",
        ),
        port=8004,
        requires_install=("granian",),
        notes="Rust runtime with PyO3",
    ),
    "socketify": ServerConfig(
        name="socketify",
        display_name="Socketify",
        category=BenchmarkCategory.SERVER,
        command=(
            "python",
            "-c",
            "from socketify import ASGI; "
            "from benchmarks.servers.app.asgi import app; "
            "ASGI(app).listen(8007, lambda config: print('Socketify started')).run()",
        ),
        port=8007,
        startup_delay=3.0,  # Socketify needs a bit more time
        requires_install=("socketify",),
        notes="C++ uSockets backend, custom event loop",
    ),
}

# =============================================================================
# Category 2: Integrated Runtime Performance
# =============================================================================
# Each runtime runs its native application format.
# Server and framework are inseparable.

CATEGORY_2_RUNTIMES: dict[str, ServerConfig] = {
    "vertebra": ServerConfig(
        name="vertebra",
        display_name="Vertebra",
        category=BenchmarkCategory.RUNTIME,
        command=(
            "vertebra",
            "serve",
            "benchmarks.servers.app.asgi:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8001",
            "--workers",
            "1",
        ),
        port=8001,
        startup_delay=3.0,  # Bun + Rust + Python startup
        notes="Bun HTTP + Rust IPC + Python ASGI (raw)",
    ),
    "vertebra_asgi_litestar": ServerConfig(
        name="vertebra_asgi_litestar",
        display_name="Vertebra ASGI (Litestar)",
        category=BenchmarkCategory.RUNTIME,
        command=(
            "vertebra",
            "serve",
            "benchmarks.servers.app.litestar_app:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8008",
            "--workers",
            "1",
            "--no-native",
        ),
        port=8008,
        startup_delay=3.0,  # Bun + Rust + Python startup
        notes="Litestar as standard ASGI app through Vertebra (--no-native)",
    ),
    "vertebra_native_litestar": ServerConfig(
        name="vertebra_native_litestar",
        display_name="Vertebra Native (Litestar)",
        category=BenchmarkCategory.RUNTIME,
        command=(
            "vertebra",
            "serve",
            "benchmarks.servers.app.litestar_app:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8009",
            "--workers",
            "1",
            "--native",
        ),
        port=8009,
        startup_delay=3.0,  # Bun + Rust + Python startup
        notes="Litestar with Vertebra native Rust router (--native, default)",
    ),
    "vertebra_fastapi": ServerConfig(
        name="vertebra_fastapi",
        display_name="Vertebra (FastAPI)",
        category=BenchmarkCategory.RUNTIME,
        command=(
            "vertebra",
            "serve",
            "benchmarks.servers.app.fastapi_app:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8013",
            "--workers",
            "1",
            "--no-native",
        ),
        port=8013,
        startup_delay=3.0,  # Bun + Rust + Python startup
        notes="FastAPI through Vertebra ASGI bridge (native mode is Litestar-only)",
    ),
    "robyn": ServerConfig(
        name="robyn",
        display_name="Robyn",
        category=BenchmarkCategory.RUNTIME,
        command=(
            "python",
            "-m",
            "robyn",
            "benchmarks.servers.app.robyn_app:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8010",
            "--workers",
            "1",
            "--processes",
            "1",
        ),
        port=8010,
        interface=InterfaceType.NATIVE,
        requires_install=("robyn",),
        notes="Rust Actix + Tokio runtime, native interface",
    ),
    "sanic_standalone": ServerConfig(
        name="sanic_standalone",
        display_name="Sanic (standalone)",
        category=BenchmarkCategory.RUNTIME,
        command=(
            "sanic",
            "benchmarks.servers.app.sanic_app:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8011",
            "--single-process",
        ),
        port=8011,
        interface=InterfaceType.NATIVE,
        requires_install=("sanic",),
        notes="Built-in uvloop-optimized server",
    ),
    "spikard": ServerConfig(
        name="spikard",
        display_name="Spikard",
        category=BenchmarkCategory.RUNTIME,
        command=("python", "benchmarks/servers/app/spikard_app.py"),
        port=8012,
        interface=InterfaceType.NATIVE,
        requires_install=("spikard",),
        notes="Rust Tower-HTTP runtime with zero-copy PyO3",
    ),
}

# =============================================================================
# Category 3: Framework Overhead Comparison
# =============================================================================
# Same server (Uvicorn httptools), different frameworks.
# Isolates framework routing, middleware, and serialization overhead.

CATEGORY_3_FRAMEWORKS: dict[str, ServerConfig] = {
    "raw_asgi": ServerConfig(
        name="raw_asgi",
        display_name="Raw ASGI",
        category=BenchmarkCategory.FRAMEWORK,
        command=(
            "uvicorn",
            "--host",
            "127.0.0.1",
            "--port",
            "8020",
            "--workers",
            "1",
            "--loop",
            "uvloop",
            "--http",
            "httptools",
            "--no-access-log",
            "benchmarks.servers.app.asgi:app",
        ),
        port=8020,
        requires_install=("uvicorn[standard]",),
        notes="Pure ASGI callable, no framework overhead",
    ),
    "starlette": ServerConfig(
        name="starlette",
        display_name="Starlette",
        category=BenchmarkCategory.FRAMEWORK,
        command=(
            "uvicorn",
            "--host",
            "127.0.0.1",
            "--port",
            "8021",
            "--workers",
            "1",
            "--loop",
            "uvloop",
            "--http",
            "httptools",
            "--no-access-log",
            "benchmarks.servers.app.starlette_app:app",
        ),
        port=8021,
        requires_install=("uvicorn[standard]", "starlette"),
        notes="Minimal routing and middleware",
    ),
    "fastapi_uvicorn": ServerConfig(
        name="fastapi_uvicorn",
        display_name="FastAPI (Uvicorn)",
        category=BenchmarkCategory.FRAMEWORK,
        command=(
            "uvicorn",
            "--host",
            "127.0.0.1",
            "--port",
            "8022",
            "--workers",
            "1",
            "--loop",
            "uvloop",
            "--http",
            "httptools",
            "--no-access-log",
            "benchmarks.servers.app.fastapi_app:app",
        ),
        port=8022,
        requires_install=("uvicorn[standard]", "fastapi"),
        notes="FastAPI on Uvicorn httptools, Pydantic validation",
    ),
    "fastapi_granian": ServerConfig(
        name="fastapi_granian",
        display_name="FastAPI (Granian)",
        category=BenchmarkCategory.FRAMEWORK,
        command=(
            "granian",
            "--interface",
            "asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8027",
            "--workers",
            "1",
            "--runtime-threads",
            "1",
            "--http",
            "1",
            "--no-ws",
            "benchmarks.servers.app.fastapi_app:app",
        ),
        port=8027,
        requires_install=("granian", "fastapi"),
        notes="FastAPI on Granian Rust runtime",
    ),
    "blacksheep": ServerConfig(
        name="blacksheep",
        display_name="BlackSheep",
        category=BenchmarkCategory.FRAMEWORK,
        command=(
            "uvicorn",
            "--host",
            "127.0.0.1",
            "--port",
            "8023",
            "--workers",
            "1",
            "--loop",
            "uvloop",
            "--http",
            "httptools",
            "--no-access-log",
            "benchmarks.servers.app.blacksheep_app:app",
        ),
        port=8023,
        requires_install=("uvicorn[standard]", "blacksheep"),
        notes="Cython-optimized routing",
    ),
    "litestar_uvicorn": ServerConfig(
        name="litestar_uvicorn",
        display_name="Litestar (Uvicorn)",
        category=BenchmarkCategory.FRAMEWORK,
        command=(
            "uvicorn",
            "--host",
            "127.0.0.1",
            "--port",
            "8024",
            "--workers",
            "1",
            "--loop",
            "uvloop",
            "--http",
            "httptools",
            "--no-access-log",
            "benchmarks.servers.app.litestar_app:app",
        ),
        port=8024,
        requires_install=("uvicorn[standard]", "litestar"),
        notes="Litestar on Uvicorn httptools, baseline for framework comparison",
    ),
    "litestar_granian": ServerConfig(
        name="litestar_granian",
        display_name="Litestar (Granian)",
        category=BenchmarkCategory.FRAMEWORK,
        command=(
            "granian",
            "--interface",
            "asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8026",
            "--workers",
            "1",
            "--runtime-threads",
            "1",
            "--http",
            "1",
            "--no-ws",
            "benchmarks.servers.app.litestar_app:app",
        ),
        port=8026,
        requires_install=("granian", "litestar"),
        notes="Litestar on Granian Rust runtime",
    ),
    "sanic_asgi": ServerConfig(
        name="sanic_asgi",
        display_name="Sanic (ASGI)",
        category=BenchmarkCategory.FRAMEWORK,
        command=(
            "uvicorn",
            "--host",
            "127.0.0.1",
            "--port",
            "8025",
            "--workers",
            "1",
            "--loop",
            "uvloop",
            "--http",
            "httptools",
            "--no-access-log",
            "benchmarks.servers.app.sanic_app:app",
        ),
        port=8025,
        requires_install=("uvicorn[standard]", "sanic"),
        notes="Sanic running as ASGI app (vs standalone mode)",
    ),
}

# =============================================================================
# All Servers Combined
# =============================================================================

ALL_SERVERS: dict[str, ServerConfig] = {**CATEGORY_1_SERVERS, **CATEGORY_2_RUNTIMES, **CATEGORY_3_FRAMEWORKS}


def get_servers_by_category(category: BenchmarkCategory) -> dict[str, ServerConfig]:
    """Get all servers in a specific category."""
    return {name: config for name, config in ALL_SERVERS.items() if config.category == category}


def get_available_servers(names: list[str] | None = None) -> dict[str, ServerConfig]:
    """Get servers filtered by name, or all if no filter."""
    if names is None:
        return ALL_SERVERS
    return {name: ALL_SERVERS[name] for name in names if name in ALL_SERVERS}
