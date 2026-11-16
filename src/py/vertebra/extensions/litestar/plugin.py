"""First-class Litestar plugin activating Vertebra Rust accelerations and CLI server lifespan hooks."""

import contextlib
import os
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import click

from vertebra.agent.plugin import AgentPlugin
from vertebra.cli._launch import (
    ServerLaunchEnv,
    load_workers_pyproject_config,
    parse_lane_concurrency_map,
)
from vertebra.cli.banner import render_banner
from vertebra.cli.errors import EXIT_CONFIG_ERROR, CLIError
from vertebra.extensions.litestar.config import VertebraConfig
from vertebra.extensions.litestar.fastpath import (
    CompiledRoute,
    FastPath,
    compile_fastpath_routes,
)
from vertebra.extensions.litestar.offload import (
    OffloadPlan,
    _is_rust_offload_active,
    offload_middlewares,
)
from vertebra.extensions.litestar.router import extract_routes
from vertebra.extensions.litestar.store import swap_response_cache_store
from vertebra.server._fastpath_core import FASTPATH_ATTR, get_attached_fastpath, set_attached_fastpath
from vertebra.server._loops import LOOP_BACKEND_CHOICES
from vertebra.supervisor._supervisor import build_server_command, run_supervisor
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import import_optional

__all__ = (
    "VertebraPlugin",
    "enter_litestar_server_lifespan",
)


class VertebraPlugin:
    """Litestar plugin enabling zero-GIL Rust offload, direct handler dispatch, and CLI lifespans."""

    __slots__ = ("_compiled_routes", "_config", "_finalized_app", "_offload_plan", "_router_table")

    def __init__(self, config: VertebraConfig | None = None) -> None:
        self._config = config or VertebraConfig()
        self._offload_plan: OffloadPlan | None = None
        self._compiled_routes: dict[int, CompiledRoute] = {}
        self._router_table: Any = None
        self._finalized_app: Any = None

    @property
    def config(self) -> VertebraConfig:
        """Return the active VertebraConfig."""
        return self._config

    @property
    def offload_plan(self) -> OffloadPlan | None:
        """Return the compiled OffloadPlan after app initialization."""
        return self._offload_plan

    @property
    def compiled_routes(self) -> dict[int, CompiledRoute]:
        """Return pre-compiled direct-dispatch route descriptors indexed by handler_id."""
        return self._compiled_routes

    @property
    def router_table(self) -> Any:
        """Return the extracted router table if fastpath bypass is enabled."""
        return self._router_table

    def on_app_init(self, app_config: Any) -> Any:
        """Intercept CompressionConfig and CORSConfig before Litestar builds its routing trie."""
        app_config.state["vertebra_config"] = self._config
        app_config.state["vertebra_plugin"] = self

        if (
            self._config.offload_compression
            and app_config.compression_config is not None
            and not app_config.before_send
        ):
            backend = str(getattr(app_config.compression_config, "backend", "gzip") or "gzip").strip().lower()
            if backend in {"gzip", "brotli", "br", "zstd"}:
                app_config.state["_vertebra_offloaded_compression_config"] = app_config.compression_config
                if _is_rust_offload_active():
                    app_config.compression_config = None

        if self._config.offload_cors and app_config.cors_config is not None:
            app_config.state["_vertebra_offloaded_cors_config"] = app_config.cors_config

        if self._config.enable_health_endpoint:
            health_path = self._config.health_path

            @_litestar_get(health_path, include_in_schema=False)
            async def _vertebra_health_probe() -> dict[str, str]:
                return {"status": "ok"}

            _vertebra_health_probe.opt["exclude_from_auth"] = True
            app_config.route_handlers.append(_vertebra_health_probe)

        if self._config.mcp_config is not None:
            mcp_mod = import_optional("litestar_mcp")
            litestar_mcp_cls = getattr(mcp_mod, "LitestarMCP", None) if mcp_mod is not None else None
            if litestar_mcp_cls is None:
                raise MissingDependencyError("litestar-mcp", extra="mcp")
            mcp_plugin = litestar_mcp_cls(self._config.mcp_config)
            plugins = getattr(app_config, "plugins", None)
            if isinstance(plugins, list):
                plugins.append(mcp_plugin)
            app_config.state["litestar_mcp_plugin"] = mcp_plugin

        if self._config.a2a_config is not None:
            mcp_mod = import_optional("litestar_mcp")
            litestar_a2a_cls = getattr(mcp_mod, "LitestarA2A", None) if mcp_mod is not None else None
            if litestar_a2a_cls is None:
                raise MissingDependencyError("litestar-mcp", extra="mcp")
            a2a_plugin = litestar_a2a_cls(self._config.a2a_config)
            plugins = getattr(app_config, "plugins", None)
            if isinstance(plugins, list):
                plugins.append(a2a_plugin)
            app_config.state["litestar_a2a_plugin"] = a2a_plugin

        if self._config.agent_config is not None:
            agent_plugin = (
                self._config.agent_config
                if isinstance(self._config.agent_config, AgentPlugin)
                else AgentPlugin(self._config.agent_config)
            )
            agent_plugin.on_app_init(app_config)

        configured_plugins = getattr(app_config, "plugins", None) or ()
        for raw_plugin in configured_plugins:
            if type(raw_plugin).__name__ == "AutowirePlugin" and not getattr(raw_plugin, "_autowire_executed", False):
                on_init = getattr(raw_plugin, "on_app_init", None)
                if callable(on_init):
                    raw_plugin._autowire_executed = True
                    original_on_init = on_init
                    raw_plugin.on_app_init = lambda cfg: cfg
                    app_config = original_on_init(app_config)

        if self._config.enable_fastpath_bypass:
            try:
                self._router_table = extract_routes(app_config)
            except (AttributeError, TypeError, ValueError, KeyError):
                self._router_table = None
            app_config.state[FASTPATH_ATTR] = FastPath(self._router_table)

        self._offload_plan = offload_middlewares(
            app_config,
            config=self._config,
            strip_python_middleware=False,
        )
        app_config.state["vertebra_offload_config_json"] = self._offload_plan.to_server_config_json()

        lifespan_hooks = getattr(app_config, "lifespan", None)
        if isinstance(lifespan_hooks, list):
            plugin_ref = self

            @contextlib.asynccontextmanager
            async def _vertebra_finalize_lifespan(app: Any) -> Any:
                plugin_ref.ensure_finalized(app)
                yield

            lifespan_hooks.append(_vertebra_finalize_lifespan)

        return app_config

    def ensure_finalized(self, app: Any) -> None:
        """Idempotently finalize route compilation, fastpath binding, and offload JSON on ``app``."""
        if app is None or self._finalized_app is app:
            return
        self._finalized_app = app
        self._finalize_app_acceleration(app)

    def on_cli_init(self, cli: Any) -> None:
        """Override Litestar's default ``run`` command so ``litestar run`` launches the Vertebra supervisor."""
        if getattr(cli, "_is_vertebra_cli", False) or getattr(cli, "name", None) in {"vtb", "cli"}:
            return
        add_cmd = getattr(cli, "add_command", None)
        if callable(add_cmd):
            add_cmd(_build_litestar_run_command(self), name="run")

    @contextlib.contextmanager
    def server_lifespan(self, app: Any) -> Iterator[None]:
        """Ensure acceleration state is finalized when entering Litestar's server lifespan."""
        self.ensure_finalized(app)
        yield

    def _finalize_app_acceleration(self, app: Any) -> None:
        rust_active = _is_rust_offload_active()
        self._offload_plan = offload_middlewares(
            app,
            config=self._config,
            strip_python_middleware=rust_active,
        )
        offload_json = self._offload_plan.to_server_config_json()
        state = getattr(app, "state", None)
        if state is not None:
            with contextlib.suppress(TypeError, AttributeError):
                state["vertebra_offload_config_json"] = offload_json

        if self._config.swap_memory_response_cache:
            swap_response_cache_store(app)
        if self._config.enable_direct_handler_fastpath:
            self._compiled_routes = compile_fastpath_routes(app)

        if self._config.enable_fastpath_bypass:
            try:
                self._router_table = extract_routes(app)
            except (AttributeError, TypeError, ValueError, KeyError):
                self._router_table = None

            bypass: FastPath | None = None
            existing = get_attached_fastpath(app)
            if isinstance(existing, FastPath):
                bypass = existing

            if bypass is None:
                bypass = FastPath(
                    self._router_table,
                    app=app,
                    compiled_routes=self._compiled_routes or None,
                )
            else:
                bypass.bind_app(
                    app,
                    router_table=self._router_table,
                    compiled_routes=self._compiled_routes or None,
                )

            with contextlib.suppress(TypeError):
                set_attached_fastpath(app, bypass)


@contextlib.contextmanager
def enter_litestar_server_lifespan(app: Any, *, app_path: str | None = None) -> Iterator[None]:
    """Export LITESTAR_APP and enter all registered CLIPlugin.server_lifespan context managers."""
    if app_path:
        os.environ["LITESTAR_APP"] = app_path
        os.environ["VERTEBRA_APP"] = app_path

    plugins = getattr(app, "plugins", None)
    cli_plugins: list[Any] = []
    seen: set[int] = set()
    if plugins is not None:
        raw_cli = getattr(plugins, "cli", None) or ()
        for p in raw_cli:
            if id(p) not in seen and hasattr(p, "server_lifespan"):
                seen.add(id(p))
                cli_plugins.append(p)
        raw_list = getattr(plugins, "plugins", None)
        if raw_list is None and hasattr(plugins, "__iter__"):
            raw_list = list(plugins)
        for p in raw_list or ():
            if id(p) not in seen and hasattr(p, "server_lifespan"):
                seen.add(id(p))
                cli_plugins.append(p)

    state = getattr(app, "state", None)
    if state is not None and hasattr(state, "get"):
        state_plugin = state.get("vertebra_plugin")
        if state_plugin is not None and id(state_plugin) not in seen and hasattr(state_plugin, "server_lifespan"):
            seen.add(id(state_plugin))
            cli_plugins.append(state_plugin)

    with contextlib.ExitStack() as stack:
        for plugin in cli_plugins:
            lifespan_fn = getattr(plugin, "server_lifespan", None)
            if callable(lifespan_fn):
                stack.enter_context(lifespan_fn(app))
        if state is not None and hasattr(state, "get"):
            offload_json = state.get("vertebra_offload_config_json")
            if isinstance(offload_json, str) and offload_json:
                os.environ["VERTEBRA_OFFLOAD_CONFIG"] = offload_json
        yield


def _build_litestar_run_command(plugin: VertebraPlugin) -> click.Command:
    @click.command(name="run", context_settings={"auto_envvar_prefix": "LITESTAR"})
    @click.option("-r", "--reload", help="Reload server on changes", default=False, is_flag=True)
    @click.option("-p", "--port", help="Serve under this port", type=int, default=8000, show_default=True)
    @click.option(
        "-W",
        "--wc",
        "--web-concurrency",
        "--workers",
        "wc",
        help="The number of worker processes",
        type=click.IntRange(min=1),
        default=None,
    )
    @click.option("-H", "--host", help="Server under this host", default="127.0.0.1", show_default=True)
    @click.option(
        "-F",
        "--fd",
        "--file-descriptor",
        "fd",
        help="Bind to a socket from this file descriptor",
        type=int,
        default=None,
    )
    @click.option(
        "-U",
        "--uds",
        "--unix-domain-socket",
        "uds",
        help="Bind to a UNIX domain socket",
        default=None,
    )
    @click.option("-d", "--debug", help="Run app in debug mode", is_flag=True, default=False)
    @click.option("-P", "--pdb", "--use-pdb", "pdb", help="Drop into PDB on an exception", is_flag=True, default=False)
    @click.option("-R", "--reload-dir", help="Directories to watch for file changes", multiple=True, default=())
    @click.option("-I", "--reload-include", help="Glob patterns to include in reload watch", multiple=True, default=())
    @click.option(
        "-E", "--reload-exclude", help="Glob patterns to exclude from reload watch", multiple=True, default=()
    )
    @click.option(
        "--ssl-certfile",
        type=click.Path(file_okay=True, exists=False, dir_okay=False, readable=True),
        help="SSL certificate file path",
        default=None,
    )
    @click.option(
        "--ssl-keyfile",
        type=click.Path(file_okay=True, exists=False, dir_okay=False, readable=True),
        help="SSL key file path",
        default=None,
    )
    @click.option(
        "--create-self-signed-cert",
        help="If certificate and key are not found at specified locations, create a self-signed certificate",
        is_flag=True,
        default=False,
    )
    @click.option(
        "--topology",
        type=click.Choice(["auto", "embedded", "shm"], case_sensitive=False),
        default=None,
        help="Vertebra runtime topology (auto, embedded, or shm)",
    )
    @click.option("--threads", type=int, default=None, help="Number of worker threads per process")
    @click.option("--jit/--no-jit", default=None, help="Enable or disable CPython experimental JIT")
    @click.option(
        "--loop",
        type=click.Choice(LOOP_BACKEND_CHOICES, case_sensitive=False),
        default="auto",
        help="Event loop backend",
    )
    @click.option("--asgi", is_flag=True, default=False, help="Run in pure ASGI mode without native fastpath")
    @click.option("--log-level", default="info", help="Server log level")
    @click.option("--drain-timeout", type=int, default=5000, help="Graceful drain timeout in milliseconds")
    @click.option("--shutdown-timeout", type=int, default=30000, help="Hard shutdown timeout in milliseconds")
    @click.option(
        "--with-workers/--no-workers",
        default=None,
        help="Co-locate background WorkerRunner lanes inside the server process",
    )
    @click.option("--queues", "worker_queues", default=None, help="Comma-separated queue lane names")
    @click.option("--lane-concurrency", "lane_concurrency", default=None, help="Comma-separated lane=limit assignments")
    @click.option("--no-banner", is_flag=True, default=False, help="Disable startup banner")
    def run_command(
        reload: bool,
        port: int,
        wc: int | None,
        host: str,
        fd: int | None,
        uds: str | None,
        debug: bool,
        pdb: bool,
        reload_dir: tuple[str, ...],
        reload_include: tuple[str, ...],
        reload_exclude: tuple[str, ...],
        ssl_certfile: str | None,
        ssl_keyfile: str | None,
        create_self_signed_cert: bool,
        topology: str | None,
        threads: int | None,
        jit: bool | None,
        loop: str,
        asgi: bool,
        log_level: str,
        drain_timeout: int,
        shutdown_timeout: int,
        with_workers: bool | None,
        worker_queues: str | None,
        lane_concurrency: str | None,
        no_banner: bool,
        ctx: click.Context | None = None,
    ) -> None:
        """Run a Litestar app on the supervised Vertebra Rust runtime."""
        active_ctx = ctx or click.get_current_context(silent=True)
        raw_env = active_ctx.obj if active_ctx is not None else None
        if callable(raw_env):
            raw_env = raw_env()
            if active_ctx is not None:
                active_ctx.obj = raw_env

        app = getattr(raw_env, "app", None) or plugin._finalized_app
        app_path = (
            getattr(raw_env, "app_path", None)
            or os.environ.get("LITESTAR_APP")
            or os.environ.get("VERTEBRA_APP")
            or "app:app"
        )

        if (debug or getattr(raw_env, "debug", False)) and app is not None:
            with contextlib.suppress(AttributeError, TypeError):
                app.debug = True
            os.environ["LITESTAR_DEBUG"] = "1"
        if (pdb or getattr(raw_env, "pdb", False)) and app is not None:
            with contextlib.suppress(AttributeError, TypeError):
                app.pdb_on_exception = True
            os.environ["LITESTAR_PDB"] = "1"

        os.environ["LITESTAR_PORT"] = str(port)
        os.environ["LITESTAR_HOST"] = host
        os.environ["LITESTAR_APP"] = str(app_path)
        os.environ["VERTEBRA_APP"] = str(app_path)

        cfg_enabled, cfg_queues, cfg_lanes = load_workers_pyproject_config(Path.cwd())
        effective_with_workers = cfg_enabled if with_workers is None else with_workers
        effective_queues = (
            tuple(q.strip() for q in worker_queues.split(",") if q.strip()) if worker_queues else cfg_queues
        )
        effective_lanes = dict(cfg_lanes)
        if lane_concurrency:
            effective_lanes.update(parse_lane_concurrency_map(lane_concurrency))

        is_dev = bool(reload or debug or getattr(raw_env, "debug", False))
        launch_env = ServerLaunchEnv(
            app_path=str(app_path),
            host=host,
            port=port,
            workers=wc,
            log_level=log_level,
            dev=is_dev,
            native=not asgi,
            topology=topology,
            threads=threads,
            jit=jit,
            loop=loop,
            with_workers=effective_with_workers,
            worker_queues=effective_queues or ("default",),
            worker_lane_concurrency=effective_lanes,
        )
        env = launch_env.to_environ()
        if reload:
            env["VERTEBRA_RELOAD"] = "1"
        if reload_dir:
            env["VERTEBRA_RELOAD_DIRS"] = os.pathsep.join(reload_dir)
        if reload_include:
            env["VERTEBRA_RELOAD_INCLUDES"] = ",".join(reload_include)
        if reload_exclude:
            env["VERTEBRA_RELOAD_EXCLUDES"] = ",".join(reload_exclude)
        if fd is not None:
            env["VERTEBRA_FD"] = str(fd)
        if uds is not None:
            env["VERTEBRA_UDS"] = str(uds)
        if ssl_certfile and ssl_keyfile:
            cert_path = Path(ssl_certfile).resolve()
            key_path = Path(ssl_keyfile).resolve()
            if not create_self_signed_cert:
                if not cert_path.is_file():
                    raise CLIError(
                        f"SSL certificate file does not exist: {cert_path}",
                        exit_code=EXIT_CONFIG_ERROR,
                    )
                if not key_path.is_file():
                    raise CLIError(
                        f"SSL key file does not exist: {key_path}",
                        exit_code=EXIT_CONFIG_ERROR,
                    )
            env["VERTEBRA_TLS_CERT"] = str(cert_path)
            env["VERTEBRA_TLS_KEY"] = str(key_path)
        elif ssl_certfile or ssl_keyfile:
            raise CLIError(
                "Both --ssl-certfile and --ssl-keyfile must be provided together",
                exit_code=EXIT_CONFIG_ERROR,
            )
        if create_self_signed_cert:
            env["VERTEBRA_SELF_SIGNED_CERT"] = "1"

        try:
            server_cmd = build_server_command()
        except SystemExit as exc:
            raise CLIError(
                str(exc),
                hint="Ensure vertebra-server binary is available/built.",
                exit_code=EXIT_CONFIG_ERROR,
            ) from exc

        if not no_banner:
            render_banner(
                mode="dev" if is_dev else "serve",
                host=host,
                port=port,
                workers=wc or 1,
            )

        with enter_litestar_server_lifespan(app, app_path=str(app_path)):
            state = getattr(app, "state", None)
            offload_json = (
                state.get("vertebra_offload_config_json") if state is not None and hasattr(state, "get") else None
            )
            if isinstance(offload_json, str) and offload_json:
                env["VERTEBRA_OFFLOAD_CONFIG"] = offload_json
            exit_code = run_supervisor(
                server_cmd=server_cmd,
                env=env,
                drain_timeout_ms=drain_timeout,
                shutdown_timeout_ms=shutdown_timeout,
            )
        if exit_code != 0:
            sys.exit(exit_code)

    return run_command


def _load_litestar_get() -> Callable[..., Callable[[Callable[..., Any]], Any]]:
    mod = import_optional("litestar")
    plugins_mod = import_optional("litestar.plugins")
    if mod is not None and plugins_mod is not None:
        for proto_name in ("InitPluginProtocol", "CLIPlugin", "CLIPluginProtocol"):
            proto = getattr(plugins_mod, proto_name, None)
            reg = getattr(proto, "register", None)
            if callable(reg):
                with contextlib.suppress(TypeError):
                    reg(VertebraPlugin)
        get_fn: Callable[..., Callable[[Callable[..., Any]], Any]] = mod.get
        return get_fn

    def fallback_get(path: str, **_kwargs: Any) -> Callable[[Callable[..., Any]], Any]:
        def wrap(fn: Callable[..., Any]) -> Any:
            return SimpleNamespace(
                paths={path},
                http_methods={"GET"},
                fn=fn,
                guards=(),
                middleware=(),
                opt={},
            )

        return wrap

    return fallback_get


_litestar_get = _load_litestar_get()
