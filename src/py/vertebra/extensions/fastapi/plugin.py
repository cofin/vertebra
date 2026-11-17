"""First-class FastAPI plugin activating Vertebra Rust accelerations, CompiledDependantPlan, and docs."""

from __future__ import annotations

import contextlib
import weakref
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

from vertebra.extensions.fastapi.config import VertebraConfig
from vertebra.extensions.fastapi.fastpath import (
    CompiledRoute,
    FastPath,
    compile_fastpath_routes,
)
from vertebra.extensions.fastapi.mcp import mount_mcp_routes
from vertebra.extensions.fastapi.offload import (
    OffloadPlan,
    evaluate_cors_preflight,
    evaluate_cors_response_headers,
    evaluate_openapi_cache_request,
    is_rust_offload_active,
    offload_middlewares,
)
from vertebra.extensions.fastapi.openapi import mount_docs_routes
from vertebra.extensions.fastapi.responses import MsgspecJSONResponse
from vertebra.extensions.fastapi.router import RouterTable, extract_routes
from vertebra.server._fastpath import resolve_relative_path, send_asgi_direct
from vertebra.server._fastpath_core import set_attached_fastpath
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

__all__ = (
    "FastAPIOffloadMiddleware",
    "VertebraPlugin",
    "enter_fastapi_server_lifespan",
)


def _encode_str_headers(headers: list[tuple[str, str]]) -> list[tuple[bytes, bytes]]:
    return [(key.lower().encode("latin-1"), value.encode("latin-1")) for key, value in headers]


def _append_missing_str_headers(
    base_headers: list[tuple[str, str]],
    extra_headers: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    existing = {key.lower() for key, _ in base_headers}
    merged = list(base_headers)
    for key, value in extra_headers:
        if key.lower() not in existing:
            merged.append((key, value))
    return merged


def _append_missing_byte_headers(
    base_headers: list[tuple[bytes, bytes]],
    extra_headers: list[tuple[str, str]],
) -> list[tuple[bytes, bytes]]:
    existing = {key.lower() for key, _ in base_headers}
    merged = list(base_headers)
    for key, value in extra_headers:
        key_bytes = key.lower().encode("latin-1")
        if key_bytes not in existing:
            merged.append((key_bytes, value.encode("latin-1")))
    return merged


class FastAPIOffloadMiddleware:
    """Outer ASGI middleware executing CORS/OpenAPI offload and FastPath dispatch for FastAPI."""

    __slots__ = ("_app_ref", "_next_app", "_plugin")

    def __init__(
        self,
        app: Any,
        *,
        plugin: VertebraPlugin,
        app_ref: Any = None,
    ) -> None:
        self._next_app = app
        self._plugin = plugin
        self._app_ref = app_ref

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        """Handle HTTP offload and FastPath dispatch before falling back to FastAPI's router."""
        scope_type = scope.get("type") if hasattr(scope, "get") else scope["type"]
        if scope_type != "http":
            await self._next_app(scope, receive, send)
            return

        plan = self._plugin.offload_plan
        config = self._plugin.config
        method = str(scope.get("method", "GET") if hasattr(scope, "get") else scope["method"]).upper()
        path = resolve_relative_path(scope)
        raw_headers = scope.get("headers") if hasattr(scope, "get") else None

        if plan is not None and plan.cors is not None and config.offload_cors and method == "OPTIONS":
            preflight = evaluate_cors_preflight(plan.cors, raw_headers, method=method)
            if preflight is not None:
                status_code, str_headers = preflight
                encoded = _encode_str_headers(str_headers)
                encoded.append((b"content-length", b"0"))
                await send_asgi_direct(send, status_code, encoded, b"")
                return

        if (
            plan is not None
            and plan.openapi is not None
            and config.offload_openapi
            and method in ("GET", "HEAD")
            and path == plan.openapi.schema_path
        ):
            status_code, str_headers, body = evaluate_openapi_cache_request(plan.openapi, raw_headers)
            if plan.cors is not None and config.offload_cors:
                cors_headers = evaluate_cors_response_headers(plan.cors, raw_headers)
                if cors_headers:
                    str_headers = _append_missing_str_headers(str_headers, cors_headers)
            if method == "HEAD":
                body = b""
            encoded = _encode_str_headers(str_headers)
            if status_code != 304:
                encoded.append((b"content-length", str(len(plan.openapi.body)).encode("ascii")))
            await send_asgi_direct(send, status_code, encoded, body)
            return

        fastpath = self._plugin.fastpath
        if fastpath is not None and config.enable_fastpath_bypass:
            result = await fastpath.dispatch(scope, receive, send)
            if result is not None:
                status_code, resp_headers, resp_body = result
                if plan is not None and plan.cors is not None and config.offload_cors:
                    cors_headers = evaluate_cors_response_headers(plan.cors, raw_headers)
                    if cors_headers:
                        resp_headers = _append_missing_byte_headers(resp_headers, cors_headers)
                await send_asgi_direct(send, status_code, resp_headers, resp_body)
                return

        await self._next_app(scope, receive, send)


class VertebraPlugin:
    """FastAPI plugin enabling Rust offload, CompiledDependantPlan fastpath, and Scalar/Swagger/MCP docs."""

    __slots__ = (
        "_compiled_routes",
        "_config",
        "_fastpath",
        "_finalized_app",
        "_offload_plan",
        "_router_table",
    )

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
        self._fastpath: FastPath | None = None
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

    @property
    def fastpath(self) -> FastPath | None:
        """Return the active FastPath dispatcher when bypass is enabled."""
        return self._fastpath

    def init_app(self, app: Any) -> Any:
        """Attach Vertebra state, lifespan wrapper, ASGI offload middleware, and fastpath to a FastAPI app."""
        state = _ensure_app_state(app)
        _set_state_item(state, "vertebra_config", self._config)
        _set_state_item(state, "vertebra_plugin", self)
        already_initialized = bool(_get_state_item(state, "_vertebra_initialized"))

        if self._config.use_msgspec_json_response and hasattr(app, "router"):
            with contextlib.suppress(AttributeError, TypeError):
                app.router.default_response_class = MsgspecJSONResponse

        if self._config.enable_health_endpoint:
            _mount_health_endpoint(app, self._config.health_path)

        if self._config.openapi_config is not None:
            mount_docs_routes(app, self._config.openapi_config)

        if self._config.webmcp_config is not None:
            mount_mcp_routes(app, webmcp_config=self._config.webmcp_config)

        if not already_initialized:
            _wrap_lifespan_context(self, app)
            _install_asgi_middleware(self, app)
            _set_state_item(state, "_vertebra_initialized", True)

        self._offload_plan = offload_middlewares(
            app,
            config=self._config,
            strip_python_middleware=False,
        )
        _set_state_item(state, "vertebra_offload_config_json", self._offload_plan.to_server_config_json())
        self._router_table = extract_routes(app)
        if self._config.enable_compiled_dependencies:
            self._compiled_routes = compile_fastpath_routes(app, router_table=self._router_table)
        else:
            self._compiled_routes = {}

        if self._config.enable_fastpath_bypass:
            bypass = FastPath(
                self._router_table,
                app=app,
                compiled_routes=self._compiled_routes or None,
            )
            self._fastpath = bypass
            with contextlib.suppress(TypeError):
                set_attached_fastpath(app, bypass)
        else:
            self._fastpath = None
        return app

    def ensure_finalized(self, app: Any) -> None:
        """Idempotently finalize route compilation, fastpath binding, and offload JSON on ``app``."""
        if app is None or self._finalized_app is app:
            return
        self._finalized_app = app
        state = _ensure_app_state(app)
        rust_active = is_rust_offload_active()
        self._offload_plan = offload_middlewares(
            app,
            config=self._config,
            strip_python_middleware=rust_active,
        )
        _set_state_item(state, "vertebra_offload_config_json", self._offload_plan.to_server_config_json())

        self._router_table = extract_routes(app)
        if self._config.enable_compiled_dependencies:
            self._compiled_routes = compile_fastpath_routes(app, router_table=self._router_table)
        else:
            self._compiled_routes = {}

        if self._config.enable_fastpath_bypass:
            bypass = FastPath(
                self._router_table,
                app=app,
                compiled_routes=self._compiled_routes or None,
            )
            self._fastpath = bypass
            with contextlib.suppress(TypeError):
                set_attached_fastpath(app, bypass)
        else:
            self._fastpath = None

    @contextlib.contextmanager
    def server_lifespan(self, app: Any) -> Iterator[None]:
        """Ensure acceleration state is finalized when entering server lifespan."""
        self.ensure_finalized(app)
        yield


@contextlib.contextmanager
def enter_fastapi_server_lifespan(app: Any) -> Iterator[None]:
    """Finalize any registered VertebraPlugin on ``app`` before serving requests."""
    state = getattr(app, "state", None)
    plugin = _get_state_item(state, "vertebra_plugin")
    if isinstance(plugin, VertebraPlugin):
        with plugin.server_lifespan(app):
            yield
        return
    yield


_WRAPPED_LIFESPANS: weakref.WeakSet[Any] = weakref.WeakSet()


def _wrap_lifespan_context(plugin: VertebraPlugin, app: Any) -> None:
    router = getattr(app, "router", None)
    if router is None or not hasattr(router, "lifespan_context"):
        return
    previous_lifespan = getattr(router, "lifespan_context", None)
    if previous_lifespan in _WRAPPED_LIFESPANS:
        return

    @contextlib.asynccontextmanager
    async def _vertebra_lifespan(lifespan_app: Any) -> AsyncIterator[Any]:
        plugin.ensure_finalized(lifespan_app)
        if callable(previous_lifespan):
            async with cast(
                "contextlib.AbstractAsyncContextManager[Any]", previous_lifespan(lifespan_app)
            ) as lifespan_state:
                plugin.ensure_finalized(lifespan_app)
                yield lifespan_state
        else:
            yield None

    _WRAPPED_LIFESPANS.add(_vertebra_lifespan)
    router.lifespan_context = _vertebra_lifespan


def _install_asgi_middleware(plugin: VertebraPlugin, app: Any) -> None:
    add_middleware = getattr(app, "add_middleware", None)
    if not callable(add_middleware):
        return
    if getattr(app, "middleware_stack", None) is not None:
        return
    for mw in getattr(app, "user_middleware", None) or ():
        if getattr(mw, "cls", None) is FastAPIOffloadMiddleware:
            return
    add_middleware(FastAPIOffloadMiddleware, plugin=plugin, app_ref=app)


def _ensure_app_state(app: Any) -> Any:
    state = getattr(app, "state", None)
    if state is None:
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
        routes.append(
            SimpleNamespace(
                path=health_path,
                endpoint=_vertebra_health_endpoint,
                methods=["GET"],
                name="_vertebra_health_endpoint",
            )
        )
