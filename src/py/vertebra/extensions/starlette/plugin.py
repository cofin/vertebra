"""First-class Starlette plugin activating Vertebra Rust accelerations, fastpath dispatch, and lifespans."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

from vertebra.extensions.starlette.config import VertebraConfig
from vertebra.extensions.starlette.fastpath import (
    CompiledRoute,
    FastPath,
    compile_fastpath_routes,
)
from vertebra.extensions.starlette.offload import (
    OffloadPlan,
    _is_rust_offload_active,
    offload_middlewares,
)
from vertebra.extensions.starlette.router import RouterTable, extract_routes
from vertebra.server._fastpath_core import set_attached_fastpath
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = (
    "VertebraPlugin",
    "enter_starlette_server_lifespan",
)


class VertebraPlugin:
    """Starlette plugin enabling Rust middleware offload, matchit route compilation, and C-API fastpath."""

    __slots__ = ("_compiled_routes", "_config", "_finalized_app", "_offload_plan", "_router_table")

    def __init__(
        self,
        app: Any = None,
        *,
        config: VertebraConfig | None = None,
    ) -> None:
        self._config = config or VertebraConfig()
        self._offload_plan: OffloadPlan | None = None
        self._compiled_routes: dict[int, CompiledRoute] = {}
        self._router_table: RouterTable | None = None
        self._finalized_app: Any = None
        if app is not None:
            self.init_app(app)

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
    def router_table(self) -> RouterTable | None:
        """Return the extracted RouterTable."""
        return self._router_table

    def init_app(self, app: Any) -> Any:
        """Attach Vertebra state, optional health probe, and fastpath bypass to a Starlette app."""
        state = _ensure_app_state(app)
        _set_state_item(state, "vertebra_config", self._config)
        _set_state_item(state, "vertebra_plugin", self)

        if self._config.enable_health_endpoint:
            _mount_health_endpoint(app, self._config.health_path)

        if self._config.openapi_config is not None:
            from vertebra.extensions.starlette.openapi import mount_docs_routes

            mount_docs_routes(app, self._config.openapi_config)

        if self._config.webmcp_config is not None:
            from vertebra.extensions.starlette.mcp import mount_mcp_routes

            mount_mcp_routes(app, webmcp_config=self._config.webmcp_config)

        self.ensure_finalized(app)
        return app

    def ensure_finalized(self, app: Any) -> None:
        """Idempotently finalize route compilation, fastpath binding, and offload JSON on ``app``."""
        if app is None:
            return
        self._finalized_app = app
        state = _ensure_app_state(app)
        rust_active = _is_rust_offload_active()
        self._offload_plan = offload_middlewares(
            app,
            config=self._config,
            strip_python_middleware=rust_active,
        )
        _set_state_item(state, "vertebra_offload_config_json", self._offload_plan.to_server_config_json())

        self._router_table = extract_routes(app)
        if self._config.enable_direct_endpoint_fastpath:
            self._compiled_routes = compile_fastpath_routes(app, router_table=self._router_table)

        if self._config.enable_fastpath_bypass:
            bypass = FastPath(
                self._router_table,
                app=app,
                compiled_routes=self._compiled_routes or None,
            )
            with contextlib.suppress(TypeError):
                set_attached_fastpath(app, bypass)

    @contextlib.contextmanager
    def server_lifespan(self, app: Any) -> Iterator[None]:
        """Ensure acceleration state is finalized when entering server lifespan."""
        self.ensure_finalized(app)
        yield


@contextlib.contextmanager
def enter_starlette_server_lifespan(app: Any) -> Iterator[None]:
    """Finalize any registered VertebraPlugin on ``app`` before serving requests."""
    state = getattr(app, "state", None)
    plugin = _get_state_item(state, "vertebra_plugin")
    if isinstance(plugin, VertebraPlugin):
        with plugin.server_lifespan(app):
            yield
        return
    yield


def _ensure_app_state(app: Any) -> Any:
    state = getattr(app, "state", None)
    if state is None:
        from types import SimpleNamespace

        state = SimpleNamespace()
        with contextlib.suppress(AttributeError, TypeError):
            app.state = state
    return state


def _set_state_item(state: Any, key: str, value: Any) -> None:
    if isinstance(state, dict):
        state[key] = value
        return
    try:
        setattr(state, key, value)
    except (AttributeError, TypeError):
        if hasattr(state, "__setitem__"):
            state[key] = value


def _get_state_item(state: Any, key: str) -> Any:
    if state is None:
        return None
    if isinstance(state, dict):
        return state.get(key)
    if hasattr(state, key):
        return getattr(state, key)
    if hasattr(state, "get"):
        return state.get(key)
    return None


def _mount_health_endpoint(app: Any, health_path: str) -> None:
    routes = getattr(app, "routes", None)
    if not isinstance(routes, list):
        return
    for existing in routes:
        if getattr(existing, "path", None) == health_path:
            return

    routing_mod = import_optional("starlette.routing")
    responses_mod = import_optional("starlette.responses")

    async def _vertebra_health_endpoint(_request: Any) -> Any:
        if responses_mod is not None and hasattr(responses_mod, "Response"):
            return responses_mod.Response(
                content=encode_json_bytes({"status": "ok"}),
                media_type="application/json",
            )
        return {"status": "ok"}

    if routing_mod is not None and hasattr(routing_mod, "Route"):
        routes.append(
            routing_mod.Route(
                health_path,
                endpoint=_vertebra_health_endpoint,
                methods=["GET"],
                include_in_schema=False,
            )
        )
    else:
        from types import SimpleNamespace

        routes.append(
            SimpleNamespace(
                path=health_path,
                endpoint=_vertebra_health_endpoint,
                methods=["GET"],
                name="_vertebra_health_endpoint",
            )
        )
