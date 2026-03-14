# Chapter 3: Project Buildout
## Vertebra 0.1.0 Ghost Framework Rewrite

**Beads ID:** vertebra-w4i.3
**Flow ID:** project-buildout
**Depends on:** Ch2 (scaffold-base)
**Consolidates:** cli-entrypoints, app-interface, project-buildout, supervisor-lifecycle

---

## Goal

Build the Python application shell on top of the Ch2 scaffold. This chapter delivers three interlocking layers: a user-facing CLI powered by `rich-click`, a `VertebraApp` routing API with stable uint64 handler IDs, and a supervisor/worker shell that bridges the Python entry point to the Rust server binary.

When this chapter is complete a developer can:

1. Run `vertebra --help` and see the formatted command group.
2. Define an app with `@app.get("/path")` decorators.
3. Run `vertebra run app:app` and have the supervisor locate and spawn the Rust binary.
4. Observe the startup banner in a TTY and a structured JSON log in a non-TTY pipeline.

---

## Context and Design Decisions

### Why rich-click?

`rich-click` wraps Click with Rich rendering. It is already a dependency of the scaffold (`pyproject.toml`) and the existing CLI wiring in `src/py/vertebra/cli/config.py` already configures `rich_click.*` style attributes. The new files must import `rich_click as click` (not plain `click`) to inherit these styles.

### Handler ID Generation

Handler IDs are stable uint64 values derived from `method + path`. Stability means the same method/path pair always produces the same ID across Python interpreter restarts. The implementation uses `hashlib.sha256` on the UTF-8 bytes of `f"{method}:{path}"` and keeps the low 8 bytes as a big-endian uint64. SHA-256 is deterministic, portable, and produces well-distributed values with no dependency on Python's `hash()` (which is session-randomized since Python 3.3).

```python
import hashlib, struct

def _make_handler_id(method: str, path: str) -> int:
    digest = hashlib.sha256(f"{method.upper()}:{path}".encode()).digest()
    return struct.unpack_from(">Q", digest)[0]
```

### VertebraApp Storage Model

Routes are stored in two parallel structures:

- `_routes: list[RouteDef]` — ordered list, compatible with the existing `App` class in `vertebra.core.lifecycle`.
- `_handler_index: dict[int, HandlerInfo]` — uint64 handler_id → metadata, for O(1) lookup by the native router.

`HandlerInfo` is a `msgspec.Struct` (frozen, array_like) so it can be serialized for FFI transmission in later chapters.

### Supervisor Design

The supervisor uses `subprocess.Popen` synchronously (not `asyncio.subprocess`). Rationale: the worker execution loop (`runner.py`) is synchronous batch-first. The GIL is released by the native `NativeSupervisor` FFI call (`vertebra._vertebra`). For Ch3 the native extension is not yet built; the supervisor falls back to a pure-Python `subprocess.Popen` implementation gated by a `_NATIVE_AVAILABLE` flag.

### Runner Loop

The runner is a stub synchronous loop:

```python
while True:
    batch = ffi.get_batch()   # STUB — returns None until Ch4
    if batch is None:
        time.sleep(0.001)
        continue
    _dispatch(batch)
```

This structure is intentional. Ch4 (PyO3 Request Proxy) will replace `ffi.get_batch()` with a real FFI call without changing the loop shape.

### Worker Signal Handling

The worker registers `SIGTERM` and `SIGINT` handlers that set a shared `threading.Event`. The runner loop checks this event before each iteration. On Windows, `SIGTERM` is not available; the code guards with `hasattr(signal, "SIGTERM")`.

---

## File Map

```
src/py/vertebra/
  cli/
    __init__.py          (MODIFY — import run/build/init, register commands)
    run.py               (MODIFY — add --asgi flag, wire supervisor)
    config.py            (EXISTS — rich-click style config, import for side effects)
    banner/
      __init__.py        (EXISTS — re-exports render_banner, banner_section)
      renderer.py        (EXISTS — TTYRenderer / BasicRenderer)
      logos.py           (EXISTS — LOGO_FULL / LOGO_COMPACT / LOGO_MINIMAL)
  web/
    __init__.py          (MODIFY — export VertebraApp)
    app.py               (CREATE — VertebraApp class)
    routing.py           (MODIFY — add handler_id() helper; RouteDef already in routing/)
    types.py             (CREATE — Request / Response stubs)
  core/
    __init__.py          (MODIFY — export supervisor helpers)
    supervisor.py        (EXISTS — Supervisor class; add pure-Python fallback)
    runner.py            (CREATE — synchronous batch loop stub)
    worker.py            (EXISTS — ASGI worker; add signal handler helpers)

src/py/tests/
  test_cli.py            (CREATE — CLI --help and run command parsing)
  test_app.py            (CREATE — VertebraApp route registration, handler IDs)
  test_routing.py        (CREATE — decorator tests, path param extraction)
```

> Files marked EXISTS have been read and understood. MODIFY means targeted additions only; no wholesale rewrites of existing logic.

---

## Phase 1: CLI and Configuration

### 1.1 — `src/py/vertebra/cli/__init__.py` (MODIFY)

The existing file already wires `run`, `build`, and `init`. The Ch3 modification adds the `vtb` alias group and ensures the import chain is correct. The file below is the complete target state.

**Exact file content:**

```python
"""Command line interface for Vertebra."""

import click

# Import config module for side effects (rich-click configuration)
from vertebra.cli import config as _config  # noqa: F401

from .build import build
from .init import init
from .run import run

__all__ = ["cli", "main"]


@click.group()
@click.version_option(package_name="vertebra-core")  # type: ignore[call-arg]
def cli() -> None:
    """Vertebra: High-performance polyglot application backbone.

    Python-first architecture with Rust IPC and Bun server layer.
    """


cli.add_command(run)
cli.add_command(build)
cli.add_command(init)

# Short alias
vtb = cli

# Backwards-compatible alias
main = cli

if __name__ == "__main__":
    cli()
```

**Pass criteria:**

- `python -m vertebra --help` exits 0 and shows "run", "build", "init" under their groups.
- `python -m vertebra --version` exits 0 and prints a version string.
- No import error when `rich_click` is not installed (it is a required dep; this check is for CI clarity).

**Fail criteria:**

- Missing `vtb` symbol causes import error in downstream tests.
- Side-effect import of `config` is absent, losing rich-click styling.

---

### 1.2 — `src/py/vertebra/cli/run.py` (MODIFY)

The existing `run.py` is complete. Ch3 validates it handles the `--asgi` flag and the binary-not-found path gracefully. No new logic is added; the verification gate is a test (see Phase 4).

**Exact file content (complete target state — no change from existing):**

```python
"""Run command."""

import logging
import os
import sys
import tomllib
from pathlib import Path
from typing import Any

import click

from vertebra.cli.banner import render_banner
from vertebra.cli.errors import EXIT_CONFIG_ERROR, CLIError
from vertebra.core.serializers import decode_json
from vertebra.core.supervisor import build_server_command, run_supervisor


@click.command()
@click.argument("app")
@click.option("--host", default="127.0.0.1", help="Host to bind to")
@click.option("--port", type=int, default=8000, help="Port to bind to")
@click.option("--workers", type=int, default=1, help="Number of worker processes")
@click.option("--config", "config_path", type=click.Path(exists=True), help="Path to application config")
@click.option("--log-level", default="info", help="Log level")
@click.option("--drain-timeout", type=int, default=5000, help="Drain timeout in milliseconds")
@click.option("--shutdown-timeout", type=int, default=30000, help="Shutdown timeout in milliseconds")
@click.option("--asgi", is_flag=True, default=False, help="Run in ASGI mode (disable native pipeline)")
@click.option("--no-banner", is_flag=True, help="Disable startup banner")
def run(
    app: str,
    host: str,
    port: int,
    workers: int,
    config_path: str | None,
    log_level: str,
    drain_timeout: int,
    shutdown_timeout: int,
    asgi: bool,
    no_banner: bool,
) -> None:
    """Run the Vertebra server."""
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    env = os.environ.copy()
    env["VERTEBRA_PYTHON_APP"] = app
    env["VERTEBRA_HOST"] = host
    env["VERTEBRA_PORT"] = str(port)
    env["VERTEBRA_WORKERS"] = str(workers)
    env["VERTEBRA_LOG_LEVEL"] = log_level
    env["VERTEBRA_NATIVE"] = "0" if asgi else "1"
    config_data: dict[str, Any] | None = None
    if config_path:
        env["VERTEBRA_CONFIG"] = config_path
        config_data = load_config(Path(config_path))

    try:
        server_cmd = build_server_command()
        server_cmd.extend(["--python", sys.executable])
    except SystemExit as e:
        raise CLIError(
            str(e), hint="Ensure vertebra-server binary is available/built.", exit_code=EXIT_CONFIG_ERROR
        ) from e

    if not no_banner:
        render_banner(mode="serve", host=host, port=port, workers=workers, log_level=log_level)

    exit_code = run_supervisor(
        server_cmd=server_cmd,
        env=env,
        workers=0,
        drain_timeout_ms=drain_timeout,
        shutdown_timeout_ms=shutdown_timeout,
        config=config_data,
    )
    sys.exit(exit_code)


def load_config(path: Path) -> dict[str, Any]:
    """Load a Vertebra config file into a dict for supervisor use."""
    if not path.exists():
        msg = f"Config file not found: {path}"
        raise FileNotFoundError(msg)
    if path.suffix in {".toml", ".tml"}:
        with path.open("rb") as f:
            return tomllib.load(f)
    if path.suffix == ".json":
        return decode_json(path.read_bytes(), type_hint=dict[str, Any])
    msg = f"Unsupported config format: {path.suffix}"
    raise ValueError(msg)
```

**Pass criteria:**

- `vertebra run --help` shows all options with rich-click styling.
- `VERTEBRA_NATIVE` env var is set to `"0"` when `--asgi` is passed.
- `CLIError` is raised (not `SystemExit` directly) when binary is missing.

---

### 1.3 — `src/py/vertebra/cli/config.py` (EXISTS — no change)

Already contains `rich_click` style application. No modifications needed. Referenced here so the implementer knows it must be imported for side effects in `__init__.py`.

---

### 1.4 — `src/py/vertebra/cli/banner/__init__.py` (EXISTS — no change)

Exports `render_banner` and `banner_section`. Referenced as the public entry point from `run.py`.

---

## Phase 2: Application Interface

### 2.1 — `src/py/vertebra/web/app.py` (CREATE)

`VertebraApp` is the primary user-facing class. It wraps the lower-level `App` (from `vertebra.core.lifecycle`) and adds:

1. Decorator-style route registration (`@app.get`, `@app.post`, etc.).
2. A `dict[int, HandlerInfo]` index keyed by stable uint64 handler IDs.
3. A `.mount(prefix, sub_app)` method for WSGI/ASGI sub-app delegation (stub — Ch5 fills the body).

**Design invariants:**

- Calling `@app.get("/path")` twice with the same path registers two entries with the **same handler ID**. The second registration overwrites the first in `_handler_index` but both `RouteDef` entries remain in `_routes` (so the native router sees both and can pick the last one via a deterministic conflict rule).
- Handler IDs are computed at decoration time, not at serve time. This means `VertebraApp.__init__` does not need to know anything about workers.
- `VertebraApp` is **not** a subclass of `App`. It holds an `App` instance via composition (`self._app`). This avoids multiple-inheritance complexity with future mixins.

**Exact file content:**

```python
"""VertebraApp: primary user-facing application container."""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Callable
from typing import Any

import msgspec

from vertebra.core.lifecycle import App
from vertebra.web.routing import RouteDef, Router, _normalize_path  # noqa: PLC2701


class HandlerInfo(msgspec.Struct, frozen=True, array_like=True):
    """Metadata stored per registered handler.

    Serializable via msgspec for future FFI transmission.
    """

    handler_id: int          # stable uint64 derived from method + path
    method: str              # HTTP method, upper-case
    path: str                # normalized path string
    handler_name: str        # __qualname__ of the handler callable


def _make_handler_id(method: str, path: str) -> int:
    """Derive a stable uint64 handler ID from method and path.

    Uses SHA-256 so the result is deterministic across interpreter restarts
    and not subject to Python's randomized hash seed (PYTHONHASHSEED).

    Args:
        method: HTTP method (case-insensitive).
        path: URL path, normalized to start with '/'.

    Returns:
        A uint64 value (Python int in range [0, 2**64)).
    """
    key = f"{method.upper()}:{_normalize_path(path)}".encode()
    digest = hashlib.sha256(key).digest()
    return struct.unpack_from(">Q", digest)[0]


class VertebraApp:
    """High-performance application container with decorator-style routing.

    Usage::

        app = VertebraApp()

        @app.get("/hello")
        def hello() -> str:
            return "world"

        @app.post("/items")
        def create_item() -> dict:
            return {"id": 1}

    The app is passed to the CLI as ``vertebra run mymodule:app``.
    """

    __slots__ = ("_app", "_handler_index", "_mounts")

    def __init__(self) -> None:
        self._app: App = App()
        self._handler_index: dict[int, HandlerInfo] = {}
        self._mounts: list[tuple[str, Any]] = []

    # ------------------------------------------------------------------
    # Route decorators
    # ------------------------------------------------------------------

    def get(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a GET handler."""
        return self._route("GET", path)

    def post(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a POST handler."""
        return self._route("POST", path)

    def put(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a PUT handler."""
        return self._route("PUT", path)

    def delete(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a DELETE handler."""
        return self._route("DELETE", path)

    def patch(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a PATCH handler."""
        return self._route("PATCH", path)

    def _route(self, method: str, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        from vertebra.web.routing import _compile_path_template, _normalize_path  # noqa: PLC2701

        normalized = _normalize_path(path)
        handler_id = _make_handler_id(method, normalized)

        def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
            segments = _compile_path_template(normalized)
            route = RouteDef(method=method.upper(), path=normalized, handler=handler, segments=segments)

            # Store in underlying App for compatibility
            self._app._routes.append(route)  # noqa: SLF001

            # Update handler index (last registration wins for duplicate method+path)
            info = HandlerInfo(
                handler_id=handler_id,
                method=method.upper(),
                path=normalized,
                handler_name=getattr(handler, "__qualname__", repr(handler)),
            )
            self._handler_index[handler_id] = info

            return handler

        return decorator

    # ------------------------------------------------------------------
    # Sub-app mounting
    # ------------------------------------------------------------------

    def mount(self, prefix: str, sub_app: Any) -> None:
        """Mount a WSGI or ASGI sub-application under a path prefix.

        Ch3 stub: records the mount for later use by Ch5 (WSGI/ASGI adapters).

        Args:
            prefix: URL prefix, e.g. "/static".
            sub_app: Any callable or ASGI/WSGI application.
        """
        from vertebra.web.routing import _normalize_prefix  # noqa: PLC2701

        self._mounts.append((_normalize_prefix(prefix), sub_app))

    # ------------------------------------------------------------------
    # Read-only views
    # ------------------------------------------------------------------

    @property
    def routes(self) -> tuple[RouteDef, ...]:
        """All registered routes, in registration order."""
        return tuple(self._app._routes)  # noqa: SLF001

    @property
    def handler_index(self) -> dict[int, HandlerInfo]:
        """Mapping from stable uint64 handler_id to HandlerInfo."""
        return dict(self._handler_index)

    @property
    def mounts(self) -> tuple[tuple[str, Any], ...]:
        """All mounted sub-applications."""
        return tuple(self._mounts)

    # ------------------------------------------------------------------
    # Lifecycle delegation
    # ------------------------------------------------------------------

    def on_startup(self, fn: Any = None) -> Any:
        """Register a startup lifecycle hook (delegated to inner App)."""
        return self._app.on_startup(fn)

    def on_shutdown(self, fn: Any = None) -> Any:
        """Register a shutdown lifecycle hook (delegated to inner App)."""
        return self._app.on_shutdown(fn)

    async def startup(self) -> None:
        """Run all registered startup hooks."""
        await self._app.startup()

    async def shutdown(self) -> None:
        """Run all registered shutdown hooks."""
        await self._app.shutdown()


__all__ = ("HandlerInfo", "VertebraApp", "_make_handler_id")
```

**Pass criteria:**

- `app = VertebraApp(); @app.get("/ping") def ping(): ...` — `len(app.routes) == 1`.
- `app.handler_index` contains one entry with key equal to `_make_handler_id("GET", "/ping")`.
- `_make_handler_id("GET", "/ping") == _make_handler_id("GET", "/ping")` — idempotent.
- `_make_handler_id("GET", "/ping") != _make_handler_id("POST", "/ping")` — method differentiation.
- `_make_handler_id("GET", "/Ping") != _make_handler_id("GET", "/ping")` — case-sensitive path.
- `0 <= _make_handler_id("GET", "/x") < 2**64` — fits uint64.
- `app.mount("/static", object())` — adds to `app.mounts` without error.

**Fail criteria:**

- `HandlerInfo` is not a `msgspec.Struct` (breaks future FFI serialization).
- Handler ID uses Python `hash()` (non-deterministic across processes).
- `VertebraApp` subclasses `App` (breaks composition model).

---

### 2.2 — `src/py/vertebra/web/routing.py` (MODIFY)

The existing routing module is at `src/py/vertebra/web/routing/__init__.py`. The `VertebraApp` implementation above imports internal helpers (`_normalize_path`, `_normalize_prefix`, `_compile_path_template`). These are already present in the existing module.

Ch3 adds a single public helper `handler_id()` that wraps `_make_handler_id` for external callers (e.g., the Rust native router needs to verify a Python-computed ID).

**Addition to `src/py/vertebra/web/routing/__init__.py`:**

```python
def handler_id(method: str, path: str) -> int:
    """Compute the stable uint64 handler ID for a method+path pair.

    This is the canonical public function. Use this rather than importing
    ``_make_handler_id`` from ``web.app`` directly.

    Args:
        method: HTTP method, e.g. "GET". Case-insensitive.
        path: URL path. Will be normalized to start with '/'.

    Returns:
        Stable uint64 integer.

    Example::

        from vertebra.web.routing import handler_id
        hid = handler_id("GET", "/ping")   # e.g. 14227497373699494912
    """
    import hashlib
    import struct

    key = f"{method.upper()}:{_normalize_path(path)}".encode()
    digest = hashlib.sha256(key).digest()
    return struct.unpack_from(">Q", digest)[0]
```

And add `"handler_id"` to `__all__`.

**Pass criteria:**

- `from vertebra.web.routing import handler_id` succeeds.
- `handler_id("GET", "/ping")` returns same value as `_make_handler_id("GET", "/ping")` from `web.app`.

---

### 2.3 — `src/py/vertebra/web/types.py` (CREATE)

Minimal request/response stubs. Full implementation deferred to Ch14/15 (WSGI/ASGI adapters). The types here are forward-referenced by `HandlerInfo` and the runner stub.

**Exact file content:**

```python
"""Request and Response type stubs for Ch3.

These are placeholder dataclasses. Full implementations with header
parsing, body streaming, and WSGI/ASGI bridging live in Ch14-15.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Headers:
    """Immutable view of HTTP headers."""

    _data: list[tuple[str, str]] = field(default_factory=list)

    def get(self, name: str, default: str | None = None) -> str | None:
        """Return first header value for name (case-insensitive)."""
        name_lower = name.lower()
        for k, v in self._data:
            if k.lower() == name_lower:
                return v
        return default

    def items(self) -> list[tuple[str, str]]:
        """Return all header pairs."""
        return list(self._data)


@dataclass
class Request:
    """Minimal HTTP request stub.

    Populated by the native batch runner in Ch4.
    Fields match the ASGI HTTP connection scope / message format.
    """

    method: str = "GET"
    path: str = "/"
    query_string: bytes = b""
    headers: Headers = field(default_factory=Headers)
    body: bytes = b""
    handler_id: int = 0        # uint64, set by native router


@dataclass
class Response:
    """Minimal HTTP response stub.

    Returned by handlers and serialized by the native runner in Ch4.
    """

    status: int = 200
    headers: Headers = field(default_factory=Headers)
    body: bytes = b""


__all__ = ("Headers", "Request", "Response")
```

**Pass criteria:**

- `from vertebra.web.types import Request, Response, Headers` succeeds.
- `Request().method == "GET"` and `Response().status == 200`.
- `Headers(_data=[("Content-Type", "application/json")]).get("content-type") == "application/json"`.

---

### 2.4 — `src/py/vertebra/web/__init__.py` (MODIFY)

Add `VertebraApp` to the public exports.

**Target state:**

```python
"""Web module for Vertebra.

Provides:
- VertebraApp: primary application class
- middleware: ASGI middleware components
- guards: Route guards and access control
- controllers: Controller base classes
- routing: Route decorators and path handling
- types: Request / Response stubs
"""

from vertebra.web import controllers, guards, middleware, routing
from vertebra.web.app import VertebraApp
from vertebra.web.types import Request, Response

__all__ = ["VertebraApp", "Request", "Response", "controllers", "guards", "middleware", "routing"]
```

**Pass criteria:**

- `from vertebra.web import VertebraApp` succeeds.
- `from vertebra.web import Request, Response` succeeds.

---

## Phase 3: Supervisor and Worker Shell

### 3.1 — `src/py/vertebra/core/supervisor.py` (MODIFY)

The existing `supervisor.py` uses `NativeSupervisor` from `vertebra._vertebra`. In Ch3 the native extension is not yet compiled (it is produced in Ch6). A pure-Python fallback using `subprocess.Popen` is added, gated by `_NATIVE_AVAILABLE`.

**Additions to existing file (append after `build_server_command`):**

```python
import subprocess
import threading

_NATIVE_AVAILABLE: bool
try:
    from vertebra._vertebra import NativeSupervisor as _NativeSupervisor  # noqa: F401
    _NATIVE_AVAILABLE = True
except ImportError:
    _NATIVE_AVAILABLE = False


class PurePythonSupervisor:
    """Fallback supervisor using subprocess.Popen.

    Used when the native Rust extension (vertebra._vertebra) is not available.
    Provides the same interface as Supervisor so callers are unaffected.

    Limitations vs. native supervisor:
    - No GIL-free wait (uses a daemon thread).
    - No structured shutdown sequencing; SIGTERM is sent to the process group.
    - Auto-restart is not implemented (Ch3 stub).
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self._config = config or {}
        self._proc: subprocess.Popen[bytes] | None = None
        self._exit_code: int = 0
        self._done = threading.Event()

    def start(self, engine_path: str, engine_args: list[str], workers: int = 1) -> None:
        """Spawn the server binary."""
        cmd = [engine_path, *engine_args]
        logger.info("PurePythonSupervisor: spawning %s", cmd)
        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            close_fds=True,
        )
        watcher = threading.Thread(target=self._watch, daemon=True)
        watcher.start()

    def _watch(self) -> None:
        if self._proc is not None:
            self._exit_code = self._proc.wait()
        self._done.set()

    def wait(self) -> int:
        """Block until the server exits."""
        self._done.wait()
        return self._exit_code

    def stop(self) -> None:
        """Terminate the server process."""
        if self._proc is not None and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()


def make_supervisor(config: dict[str, Any] | None = None) -> Supervisor | PurePythonSupervisor:
    """Factory that returns the best available supervisor implementation.

    Returns ``Supervisor`` (native) if ``vertebra._vertebra`` is importable,
    otherwise returns ``PurePythonSupervisor``.
    """
    if _NATIVE_AVAILABLE:
        return Supervisor(config)
    return PurePythonSupervisor(config)
```

The existing `run_supervisor` function is updated to call `make_supervisor` instead of `Supervisor` directly:

```python
# Replace the line:
#   supervisor = Supervisor(config)
# with:
    supervisor = make_supervisor(config)
```

**Pass criteria:**

- `from vertebra.core.supervisor import make_supervisor, PurePythonSupervisor` succeeds.
- `make_supervisor()` returns `PurePythonSupervisor` when `vertebra._vertebra` is absent.
- `PurePythonSupervisor` has `.start()`, `.wait()`, `.stop()` with the same signatures as `Supervisor`.

**Fail criteria:**

- `run_supervisor` raises `ImportError` when `vertebra._vertebra` is absent.
- `PurePythonSupervisor.start()` uses `asyncio.create_subprocess_exec` (must be sync).

---

### 3.2 — `src/py/vertebra/core/runner.py` (CREATE)

The synchronous batch runner loop. Fully stubbed for Ch3; real FFI integration in Ch4.

**Exact file content:**

```python
"""Synchronous batch runner for Vertebra workers.

This module implements the core execution loop that polls the native FFI layer
for request batches and dispatches them to Python handlers.

Ch3 Status: STUB
    - ``ffi.get_batch()`` always returns ``None`` (FFI not yet wired).
    - ``_dispatch()`` is a no-op placeholder.
    - The loop exits cleanly on SIGTERM / SIGINT via a ``threading.Event``.

Ch4 will replace the stub FFI call with a real ``vertebra._vertebra`` call.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

logger = logging.getLogger("vertebra.runner")


class _StubFFI:
    """Stand-in for the native FFI layer until Ch4."""

    def get_batch(self) -> Any:
        """Return None until native bindings are wired."""
        return None


# Module-level singleton; Ch4 will replace this with a real binding.
ffi = _StubFFI()


def run_loop(
    stop_event: threading.Event,
    poll_interval: float = 0.001,
) -> None:
    """Run the synchronous batch dispatch loop.

    Polls ``ffi.get_batch()`` in a tight loop. When a batch is returned,
    dispatches it to ``_dispatch()``. Sleeps ``poll_interval`` seconds between
    empty polls to avoid busy-waiting.

    Args:
        stop_event: Set this event from a signal handler or supervisor to
            request a graceful exit.
        poll_interval: Sleep duration (seconds) between empty polls.
            Defaults to 1 ms (0.001 s).
    """
    logger.info("Runner loop starting (stub mode)")

    while not stop_event.is_set():
        batch = ffi.get_batch()

        if batch is None:
            # No work available; yield the CPU briefly.
            time.sleep(poll_interval)
            continue

        try:
            _dispatch(batch)
        except Exception:
            logger.exception("Unhandled error in batch dispatch")

    logger.info("Runner loop exiting cleanly")


def _dispatch(batch: Any) -> None:
    """Dispatch a request batch to Python handlers.

    Ch3 stub: no-op. Ch4 replaces this with the full FFI dispatch pipeline.

    Args:
        batch: A batch object from ``ffi.get_batch()``.
    """
    # TODO(Ch4): deserialize batch, look up handler by handler_id, invoke.
    logger.debug("_dispatch called with batch: %r (stub — no-op)", batch)


__all__ = ("_StubFFI", "_dispatch", "ffi", "run_loop")
```

**Pass criteria:**

- `from vertebra.core.runner import run_loop, ffi` succeeds.
- Running `run_loop(stop_event)` with `stop_event.set()` before the call returns immediately.
- Running `run_loop(stop_event)` in a thread and setting `stop_event` from the main thread causes the thread to exit within 10 ms (two poll intervals).
- No `asyncio` import in this module.

**Fail criteria:**

- `run_loop` is `async def` or uses `await`.
- Loop does not check `stop_event` on every iteration.

---

### 3.3 — `src/py/vertebra/core/worker.py` (MODIFY)

The existing `worker.py` is the full ASGI worker harness. Ch3 adds a standalone `init_worker_signals` helper that can be called from `run_loop` contexts (non-ASGI). This is extracted so the runner and the ASGI worker share signal-handling code.

**Addition to the end of `src/py/vertebra/core/worker.py`:**

```python
import signal as _signal


def init_worker_signals(stop_event: "threading.Event") -> None:
    """Register SIGTERM and SIGINT handlers that set ``stop_event``.

    Safe to call in the main thread of a worker process. On Windows,
    SIGTERM is not available; only SIGINT is registered there.

    Args:
        stop_event: The event to set when a shutdown signal is received.
    """
    import threading as _threading

    def _handler(signum: int, frame: object) -> None:
        logger.info("Signal %d received — requesting stop", signum)
        stop_event.set()

    if hasattr(_signal, "SIGTERM"):
        _signal.signal(_signal.SIGTERM, _handler)
    _signal.signal(_signal.SIGINT, _handler)
```

And ensure `threading` is imported at the top of the file (it already is via `asyncio`; add an explicit `import threading` for the helper).

**Pass criteria:**

- `from vertebra.core.worker import init_worker_signals` succeeds.
- Calling `init_worker_signals(event)` registers a SIGINT handler without raising.
- The SIGTERM handler is registered on Linux/macOS but not on Windows (guarded by `hasattr`).

---

### 3.4 — `src/py/vertebra/core/__init__.py` (MODIFY)

Expose `make_supervisor` and `PurePythonSupervisor` in the core public API.

**Target state (additions only):**

```python
from vertebra.core.supervisor import PurePythonSupervisor, make_supervisor

__all__ = [
    # ... existing exports ...
    "PurePythonSupervisor",
    "make_supervisor",
]
```

---

## Phase 4: Tests

### 4.1 — `src/py/tests/test_cli.py` (CREATE)

```python
"""CLI smoke tests for Ch3 project buildout."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from vertebra.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


def test_cli_help_shows_commands(runner: CliRunner) -> None:
    """vertebra --help must list run, build, init."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0, result.output
    assert "run" in result.output
    assert "build" in result.output
    assert "init" in result.output


def test_cli_version(runner: CliRunner) -> None:
    """vertebra --version must print a version string."""
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0, result.output
    # Should contain something that looks like a version (digits and dots)
    assert any(ch.isdigit() for ch in result.output)


def test_run_help_shows_options(runner: CliRunner) -> None:
    """vertebra run --help must show key options."""
    result = runner.invoke(cli, ["run", "--help"])
    assert result.exit_code == 0, result.output
    for option in ["--host", "--port", "--workers", "--asgi", "--no-banner"]:
        assert option in result.output, f"Expected {option!r} in help output"


def test_run_sets_native_env_default(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    """By default VERTEBRA_NATIVE should be '1' (native pipeline enabled)."""
    captured_env: dict[str, str] = {}

    def fake_run_supervisor(**kwargs: object) -> int:
        import os
        captured_env.update(os.environ)
        return 0

    monkeypatch.setenv("VERTEBRA_SERVER_BINARY", "/nonexistent")
    monkeypatch.setattr("vertebra.core.supervisor.run_supervisor", fake_run_supervisor)
    monkeypatch.setattr("vertebra.core.supervisor.build_server_command", lambda: ["/nonexistent"])

    runner.invoke(cli, ["run", "myapp:app", "--no-banner"])
    # VERTEBRA_NATIVE is set to "1" (native mode) when --asgi not passed
    assert captured_env.get("VERTEBRA_NATIVE") == "1"


def test_run_asgi_flag_sets_native_zero(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    """--asgi flag must set VERTEBRA_NATIVE to '0'."""
    captured_env: dict[str, str] = {}

    def fake_run_supervisor(**kwargs: object) -> int:
        import os
        captured_env.update(os.environ)
        return 0

    monkeypatch.setenv("VERTEBRA_SERVER_BINARY", "/nonexistent")
    monkeypatch.setattr("vertebra.core.supervisor.run_supervisor", fake_run_supervisor)
    monkeypatch.setattr("vertebra.core.supervisor.build_server_command", lambda: ["/nonexistent"])

    runner.invoke(cli, ["run", "myapp:app", "--asgi", "--no-banner"])
    assert captured_env.get("VERTEBRA_NATIVE") == "0"
```

**Pass criteria:**

- All 5 tests pass with `uv run pytest src/py/tests/test_cli.py -v`.
- No test imports `subprocess` or spawns a real binary.

---

### 4.2 — `src/py/tests/test_app.py` (CREATE)

```python
"""Tests for VertebraApp route registration and handler ID stability."""

from __future__ import annotations

import pytest

from vertebra.web.app import HandlerInfo, VertebraApp, _make_handler_id
from vertebra.web.routing import handler_id


class TestHandlerIdStability:
    def test_same_method_path_same_id(self) -> None:
        assert _make_handler_id("GET", "/ping") == _make_handler_id("GET", "/ping")

    def test_different_method_different_id(self) -> None:
        assert _make_handler_id("GET", "/ping") != _make_handler_id("POST", "/ping")

    def test_different_path_different_id(self) -> None:
        assert _make_handler_id("GET", "/ping") != _make_handler_id("GET", "/pong")

    def test_case_sensitive_path(self) -> None:
        assert _make_handler_id("GET", "/Ping") != _make_handler_id("GET", "/ping")

    def test_method_normalized_to_upper(self) -> None:
        assert _make_handler_id("get", "/ping") == _make_handler_id("GET", "/ping")

    def test_path_normalized_with_leading_slash(self) -> None:
        assert _make_handler_id("GET", "ping") == _make_handler_id("GET", "/ping")

    def test_result_fits_uint64(self) -> None:
        value = _make_handler_id("GET", "/x")
        assert 0 <= value < 2**64

    def test_routing_module_handler_id_matches(self) -> None:
        """handler_id() from routing module must produce identical values."""
        assert handler_id("GET", "/ping") == _make_handler_id("GET", "/ping")


class TestVertebraAppRouteRegistration:
    def test_get_decorator_registers_route(self) -> None:
        app = VertebraApp()

        @app.get("/ping")
        def ping() -> str:
            return "pong"

        assert len(app.routes) == 1
        assert app.routes[0].method == "GET"
        assert app.routes[0].path == "/ping"

    def test_multiple_methods_on_same_path(self) -> None:
        app = VertebraApp()

        @app.get("/items")
        def list_items() -> list:
            return []

        @app.post("/items")
        def create_item() -> dict:
            return {}

        assert len(app.routes) == 2
        methods = {r.method for r in app.routes}
        assert methods == {"GET", "POST"}

    def test_handler_index_populated(self) -> None:
        app = VertebraApp()

        @app.get("/hello")
        def hello() -> str:
            return "world"

        expected_id = _make_handler_id("GET", "/hello")
        assert expected_id in app.handler_index
        info = app.handler_index[expected_id]
        assert isinstance(info, HandlerInfo)
        assert info.method == "GET"
        assert info.path == "/hello"
        assert info.handler_name == "hello"

    def test_handler_index_last_registration_wins(self) -> None:
        """Duplicate method+path: last registration wins in handler_index."""
        app = VertebraApp()

        @app.get("/x")
        def first() -> str:
            return "first"

        @app.get("/x")
        def second() -> str:
            return "second"

        # Both routes are kept in the route list
        assert len(app.routes) == 2
        # But handler_index has only one entry (last wins)
        hid = _make_handler_id("GET", "/x")
        assert app.handler_index[hid].handler_name == "second"

    def test_mount_records_sub_app(self) -> None:
        app = VertebraApp()
        sentinel = object()
        app.mount("/static", sentinel)
        assert len(app.mounts) == 1
        prefix, mounted = app.mounts[0]
        assert prefix == "/static"
        assert mounted is sentinel

    def test_handler_info_is_msgspec_struct(self) -> None:
        import msgspec

        assert issubclass(HandlerInfo, msgspec.Struct)

    @pytest.mark.anyio
    async def test_lifecycle_hooks_delegate_to_inner_app(self) -> None:
        app = VertebraApp()
        calls: list[str] = []

        @app.on_startup
        async def startup() -> None:
            calls.append("startup")

        await app.startup()
        assert calls == ["startup"]
```

**Pass criteria:**

- All tests in `TestHandlerIdStability` and `TestVertebraAppRouteRegistration` pass.
- `pytest.mark.anyio` tests require `anyio[asyncio]` in dev dependencies (already present in scaffold).

---

### 4.3 — `src/py/tests/test_routing.py` (CREATE)

```python
"""Tests for routing decorators and path parameter extraction.

These tests validate the existing routing module (vertebra.web.routing)
in the context of Ch3 patterns. They complement the existing
tests/unit/routing/test_routing.py with Ch3-specific scenarios.
"""

from __future__ import annotations

from uuid import UUID

import pytest

from vertebra.web.routing import (
    ParamSpec,
    RouteDef,
    Router,
    delete,
    extract_path_params,
    get,
    handler_id,
    patch,
    post,
    put,
)


class TestDecorators:
    def test_get_attaches_metadata(self) -> None:
        @get("/items")
        def list_items() -> list:
            return []

        routes = getattr(list_items, "__vertebra_routes__", [])
        assert len(routes) == 1
        assert routes[0].method == "GET"
        assert routes[0].path == "/items"

    def test_post_attaches_metadata(self) -> None:
        @post("/items")
        def create() -> dict:
            return {}

        routes = getattr(create, "__vertebra_routes__", [])
        assert routes[0].method == "POST"

    def test_put_attaches_metadata(self) -> None:
        @put("/items/{item_id:int}")
        def update(item_id: int) -> dict:
            return {}

        routes = getattr(update, "__vertebra_routes__", [])
        assert routes[0].method == "PUT"

    def test_delete_attaches_metadata(self) -> None:
        @delete("/items/{item_id:int}")
        def remove(item_id: int) -> None:
            pass

        routes = getattr(remove, "__vertebra_routes__", [])
        assert routes[0].method == "DELETE"

    def test_patch_attaches_metadata(self) -> None:
        @patch("/items/{item_id:int}")
        def partial_update(item_id: int) -> dict:
            return {}

        routes = getattr(partial_update, "__vertebra_routes__", [])
        assert routes[0].method == "PATCH"

    def test_stacked_decorators_add_multiple_routes(self) -> None:
        @get("/things")
        @post("/things")
        def things() -> list:
            return []

        routes = getattr(things, "__vertebra_routes__", [])
        methods = [r.method for r in routes]
        assert "GET" in methods
        assert "POST" in methods

    def test_handler_returns_original_callable(self) -> None:
        @get("/noop")
        def noop() -> None:
            pass

        assert callable(noop)
        assert noop() is None


class TestRouter:
    def test_router_applies_prefix(self) -> None:
        router = Router("/api/v1")

        @get("/users")
        def list_users() -> list:
            return []

        router.register(list_users)
        assert router.routes[0].path == "/api/v1/users"

    def test_router_strips_trailing_slash_from_prefix(self) -> None:
        router = Router("/api/")

        @get("/users")
        def list_users() -> list:
            return []

        router.register(list_users)
        assert router.routes[0].path == "/api/users"

    def test_empty_prefix_is_identity(self) -> None:
        router = Router("")

        @get("/x")
        def x() -> None:
            pass

        router.register(x)
        assert router.routes[0].path == "/x"


class TestPathParamExtraction:
    def test_integer_param(self) -> None:
        params = extract_path_params("/users/{user_id:int}", "/users/42")
        assert params == {"user_id": 42}

    def test_string_param(self) -> None:
        params = extract_path_params("/users/{username}", "/users/alice")
        assert params == {"username": "alice"}

    def test_uuid_param(self) -> None:
        uuid_str = "550e8400-e29b-41d4-a716-446655440000"
        params = extract_path_params("/items/{item_id:uuid}", f"/items/{uuid_str}")
        assert params == {"item_id": UUID(uuid_str)}

    def test_bool_param_true(self) -> None:
        params = extract_path_params("/flag/{active:bool}", "/flag/true")
        assert params == {"active": True}

    def test_bool_param_false(self) -> None:
        params = extract_path_params("/flag/{active:bool}", "/flag/false")
        assert params == {"active": False}

    def test_float_param(self) -> None:
        params = extract_path_params("/scale/{factor:float}", "/scale/3.14")
        assert params["factor"] == pytest.approx(3.14)

    def test_multiple_params(self) -> None:
        params = extract_path_params(
            "/users/{user_id:int}/posts/{post_id:int}",
            "/users/1/posts/99",
        )
        assert params == {"user_id": 1, "post_id": 99}

    def test_path_mismatch_raises(self) -> None:
        with pytest.raises(ValueError):
            extract_path_params("/items/{item_id:int}", "/items/abc")

    def test_too_many_segments_raises(self) -> None:
        with pytest.raises(ValueError):
            extract_path_params("/items/{item_id:int}", "/items/1/extra")


class TestHandlerIdPublicAPI:
    def test_handler_id_is_int(self) -> None:
        result = handler_id("GET", "/ping")
        assert isinstance(result, int)

    def test_handler_id_is_stable(self) -> None:
        assert handler_id("GET", "/ping") == handler_id("GET", "/ping")

    def test_handler_id_fits_uint64(self) -> None:
        assert 0 <= handler_id("DELETE", "/everything") < 2**64
```

**Pass criteria:**

- All tests pass with `uv run pytest src/py/tests/test_routing.py -v`.
- No test is skipped or xfailed.

---

## Final Gate: `make lint` and `make test`

### Lint checklist

| Check | Tool | Expected |
|-------|------|----------|
| Python formatting | `ruff format` | No diffs |
| Python linting | `ruff check` | Zero errors |
| Type checking | `pyright` | Zero errors on new files |

New files must pass `ruff check` with the project's existing config. Key rules that affect the new code:

- `SLF001` (private member access) — `self._app._routes` in `VertebraApp._route` is intentional; suppress with `# noqa: SLF001` on that line.
- `PLC2701` (private import) — `_normalize_path` etc. imported from routing; suppress with `# noqa: PLC2701`.
- `ANN` (annotations) — all public functions must have return type annotations.

### Test execution

```bash
# All Ch3 tests
uv run pytest src/py/tests/test_cli.py src/py/tests/test_app.py src/py/tests/test_routing.py -v

# Full suite (must not regress)
make test
```

**Gate criteria:**

- Zero test failures across all 3 new test files.
- No regression in any existing test file.
- `make lint` exits 0.

---

## Dependency Verification

All dependencies used in Ch3 are either part of the Python stdlib or already declared in `pyproject.toml`:

| Module | Source | Notes |
|--------|--------|-------|
| `hashlib` | stdlib | SHA-256 for handler IDs |
| `struct` | stdlib | Big-endian uint64 unpacking |
| `subprocess` | stdlib | `PurePythonSupervisor` |
| `threading` | stdlib | `stop_event`, watcher thread |
| `signal` | stdlib | Worker signal handling |
| `time` | stdlib | `run_loop` poll sleep |
| `dataclasses` | stdlib | `Request`, `Response`, `Headers` |
| `msgspec` | declared | `HandlerInfo`, `RouteDef`, `ParamSpec` |
| `rich-click` | declared | CLI rendering |
| `rich` | declared | `console`, `Rule`, `Table` |
| `click` | declared (via rich-click) | Testing via `CliRunner` |
| `anyio` | declared | Async test runner |

No new dependencies are introduced.

---

## Rollback / Recovery

If any phase fails:

1. **Phase 1 failure (CLI)** — The existing `cli/__init__.py` is unchanged from the scaffold; revert any modifications. The only safe addition is `vtb = cli`.
2. **Phase 2 failure (VertebraApp)** — `web/app.py` is a new file. Delete it. Revert `web/__init__.py` to the scaffold state (`from vertebra.web import controllers, guards, middleware, routing`).
3. **Phase 3 failure (Supervisor)** — The fallback code is additive. Remove `PurePythonSupervisor`, `make_supervisor`, and `_NATIVE_AVAILABLE`. Restore `run_supervisor` to call `Supervisor(config)` directly.
4. **Phase 3 failure (Runner)** — `core/runner.py` is a new file. Delete it.
5. **Test failures** — The test files are new. They can be deleted without affecting runtime behavior.

The existing `supervisor.py` and `worker.py` must be backed up before modification.

---

## Session Recovery

To resume a partial implementation:

```bash
# Check what exists
ls src/py/vertebra/web/app.py         # Phase 2
ls src/py/vertebra/core/runner.py     # Phase 3
grep "PurePythonSupervisor" src/py/vertebra/core/supervisor.py  # Phase 3

# Run only failing tests to isolate
uv run pytest src/py/tests/test_app.py -v -x
uv run pytest src/py/tests/test_routing.py -v -x
uv run pytest src/py/tests/test_cli.py -v -x
```

Resume order if interrupted mid-chapter:

1. Complete the phase you were in (check which files are partially written).
2. Run `make lint` on completed files before moving to the next phase.
3. Run the corresponding test file before moving to the next phase.
4. Never run `make test-all` mid-chapter; use targeted `uv run pytest` invocations.
