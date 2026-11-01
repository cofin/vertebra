"""First-class FastAPI plugin activating Vertebra Rust accelerations, CompiledDependantPlan, and docs."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

from vertebra.extensions.fastapi.config import FastAPIConfig
from vertebra.extensions.fastapi.fastpath import (
    CompiledRoute,
    FastPath,
    compile_fastpath_routes,
)
from vertebra.extensions.fastapi.offload import (
    OffloadPlan,
    offload_middlewares,
)
from vertebra.extensions.fastapi.responses import MsgspecJSONResponse
from vertebra.extensions.fastapi.router import RouterTable, extract_routes
from vertebra.extensions.litestar.offload import _is_rust_offload_active
from vertebra.extensions.starlette.plugin import (
    _ensure_app_state,
    _get_state_item,
    _mount_health_endpoint,
    _set_state_item,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = (
    "FastAPIPlugin",
    "enter_fastapi_server_lifespan",
)


class FastAPIPlugin:
    """FastAPI plugin enabling Rust offload, CompiledDependantPlan fastpath, and Scalar/Swagger/MCP docs."""

    __slots__ = ("_compiled_routes", "_config", "_finalized_app", "_offload_plan", "_router_table")

    def __init__(
        self,
        app: Any = None,
        *,
        config: FastAPIConfig | None = None,
    ) -> None:
        self._config = config or FastAPIConfig()
        self._offload_plan: OffloadPlan | None = None
        self._compiled_routes: dict[int, CompiledRoute] = {}
        self._router_table: RouterTable | None = None
        self._finalized_app: Any = None
        if app is not None:
            self.init_app(app)

    @property
    def config(self) -> FastAPIConfig:
        """Return the active FastAPIConfig."""
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
        """Attach Vertebra state, MsgspecJSONResponse, docs portals, and fastpath bypass to a FastAPI app."""
        state = _ensure_app_state(app)
        _set_state_item(state, "vertebra_config", self._config)
        _set_state_item(state, "vertebra_plugin", self)

        if self._config.use_msgspec_json_response and hasattr(app, "router"):
            with contextlib.suppress(AttributeError, TypeError):
                app.router.default_response_class = MsgspecJSONResponse

        if self._config.enable_health_endpoint:
            _mount_health_endpoint(app, self._config.health_path)

        if self._config.openapi_config is not None:
            from vertebra.extensions.fastapi.openapi import mount_docs_routes

            mount_docs_routes(app, self._config.openapi_config)

        if self._config.webmcp_config is not None:
            from vertebra.extensions.fastapi.mcp import mount_mcp_routes

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
        if self._config.enable_compiled_dependencies:
            self._compiled_routes = compile_fastpath_routes(app, router_table=self._router_table)

        if self._config.enable_fastpath_bypass:
            bypass = FastPath(
                self._router_table,
                app=app,
                compiled_routes=self._compiled_routes or None,
            )
            _set_state_item(state, "vertebra_fastpath_bypass", bypass)
            with contextlib.suppress(AttributeError, TypeError):
                app.__vertebra_fastpath__ = bypass

    @contextlib.contextmanager
    def server_lifespan(self, app: Any) -> Iterator[None]:
        """Ensure acceleration state is finalized when entering server lifespan."""
        self.ensure_finalized(app)
        yield


@contextlib.contextmanager
def enter_fastapi_server_lifespan(app: Any) -> Iterator[None]:
    """Finalize any registered FastAPIPlugin on ``app`` before serving requests."""
    state = getattr(app, "state", None)
    plugin = _get_state_item(state, "vertebra_plugin")
    if isinstance(plugin, FastAPIPlugin):
        with plugin.server_lifespan(app):
            yield
        return
    yield
