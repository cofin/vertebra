"""First-class Litestar plugin activating Vertebra Rust accelerations and CLI server lifespan hooks."""

import contextlib
import importlib
import os
from collections.abc import Callable, Iterator
from types import SimpleNamespace
from typing import Any

from vertebra._typing import MissingDependencyError
from vertebra.extensions.litestar.channels import swap_litestar_channels_backend
from vertebra.extensions.litestar.config import LitestarConfig
from vertebra.extensions.litestar.fastpath import (
    CompiledLitestarRoute,
    LitestarFastPathBypass,
    compile_litestar_fastpath_routes,
)
from vertebra.extensions.litestar.offload import (
    LitestarOffloadPlan,
    offload_litestar_middlewares,
)
from vertebra.extensions.litestar.router import extract_litestar_routes
from vertebra.extensions.litestar.store import swap_litestar_response_cache_store
from vertebra.utils.module_loader import import_optional

__all__ = (
    "LitestarPlugin",
    "enter_litestar_server_lifespan",
)


class LitestarPlugin:
    """Litestar plugin enabling zero-GIL Rust offload, direct handler dispatch, and CLI lifespans."""

    __slots__ = ("_compiled_routes", "_config", "_offload_plan", "_router_table")

    def __init__(self, config: LitestarConfig | None = None) -> None:
        self._config = config or LitestarConfig()
        self._offload_plan: LitestarOffloadPlan | None = None
        self._compiled_routes: dict[int, CompiledLitestarRoute] = {}
        self._router_table: Any = None

    @property
    def config(self) -> LitestarConfig:
        """Return the active LitestarConfig."""
        return self._config

    @property
    def offload_plan(self) -> LitestarOffloadPlan | None:
        """Return the compiled LitestarOffloadPlan after app initialization."""
        return self._offload_plan

    @property
    def compiled_routes(self) -> dict[int, CompiledLitestarRoute]:
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

        if self._config.enable_fastpath_bypass:
            try:
                self._router_table = extract_litestar_routes(app_config)
            except (AttributeError, TypeError, ValueError, KeyError):
                self._router_table = None
            app_config.state["vertebra_fastpath_bypass"] = LitestarFastPathBypass(self._router_table)

        if (
            self._config.offload_compression
            and app_config.compression_config is not None
            and not app_config.before_send
        ):
            app_config.state["_vertebra_offloaded_compression_config"] = app_config.compression_config
            app_config.compression_config = None

        if self._config.offload_cors and app_config.cors_config is not None:
            app_config.state["_vertebra_offloaded_cors_config"] = app_config.cors_config

        if self._config.enable_health_endpoint:
            health_path = self._config.health_path

            @_litestar_get(health_path, include_in_schema=False)
            async def _vertebra_health_probe() -> dict[str, str]:
                return {"status": "ok"}

            app_config.route_handlers.append(_vertebra_health_probe)

        if self._config.mcp_config is not None:
            mcp_mod = import_optional("litestar_mcp")
            litestar_mcp_cls = getattr(mcp_mod, "LitestarMCP", None) if mcp_mod is not None else None
            if litestar_mcp_cls is None:
                raise MissingDependencyError("litestar-mcp", install_package="vertebra[mcp]", extra="mcp")
            mcp_plugin = litestar_mcp_cls(self._config.mcp_config)
            plugins = getattr(app_config, "plugins", None)
            if isinstance(plugins, list):
                plugins.append(mcp_plugin)
            app_config.state["litestar_mcp_plugin"] = mcp_plugin

        if self._config.a2a_config is not None:
            mcp_mod = import_optional("litestar_mcp")
            litestar_a2a_cls = getattr(mcp_mod, "LitestarA2A", None) if mcp_mod is not None else None
            if litestar_a2a_cls is None:
                raise MissingDependencyError("litestar-mcp", install_package="vertebra[mcp]", extra="mcp")
            a2a_plugin = litestar_a2a_cls(self._config.a2a_config)
            plugins = getattr(app_config, "plugins", None)
            if isinstance(plugins, list):
                plugins.append(a2a_plugin)
            app_config.state["litestar_a2a_plugin"] = a2a_plugin

        app_config.on_app_init.append(self._finalize_app_acceleration)
        return app_config

    def on_cli_init(self, cli: Any) -> None:
        """Satisfy Litestar CLIPluginProtocol CLI group registration hook."""
        _ = cli

    @contextlib.contextmanager
    def server_lifespan(self, app: Any) -> Iterator[None]:
        """Ensure acceleration state is finalized when entering Litestar's server lifespan."""
        if self._offload_plan is None:
            self._finalize_app_acceleration(app)
        yield

    def _finalize_app_acceleration(self, app: Any) -> None:
        self._offload_plan = offload_litestar_middlewares(app, config=self._config)
        if self._config.swap_memory_channels:
            swap_litestar_channels_backend(app)
        if self._config.swap_memory_response_cache:
            swap_litestar_response_cache_store(app)
        if self._config.enable_direct_handler_fastpath:
            self._compiled_routes = compile_litestar_fastpath_routes(app)


@contextlib.contextmanager
def enter_litestar_server_lifespan(app: Any, *, app_path: str | None = None) -> Iterator[None]:
    """Export LITESTAR_APP and enter all registered CLIPlugin.server_lifespan context managers."""
    if app_path:
        os.environ["LITESTAR_APP"] = app_path

    plugins = getattr(app, "plugins", None)
    cli_plugins = getattr(plugins, "cli", None)
    if cli_plugins is None and plugins is not None:
        raw_list = getattr(plugins, "plugins", plugins)
        cli_plugins = [p for p in raw_list if hasattr(p, "server_lifespan")]

    with contextlib.ExitStack() as stack:
        for plugin in cli_plugins or ():
            lifespan_fn = getattr(plugin, "server_lifespan", None)
            if callable(lifespan_fn):
                stack.enter_context(lifespan_fn(app))
        yield


def _load_litestar_get() -> Callable[..., Callable[[Callable[..., Any]], Any]]:
    try:
        mod = importlib.import_module("litestar")
        plugins_mod = importlib.import_module("litestar.plugins")
        for proto_name in ("InitPluginProtocol", "CLIPlugin", "CLIPluginProtocol"):
            proto = getattr(plugins_mod, proto_name, None)
            reg = getattr(proto, "register", None)
            if callable(reg):
                with contextlib.suppress(TypeError):
                    reg(LitestarPlugin)
        get_fn: Callable[..., Callable[[Callable[..., Any]], Any]] = mod.get
        return get_fn
    except ImportError:

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
