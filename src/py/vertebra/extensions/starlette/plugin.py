"""First-class Starlette plugin activating Vertebra Rust accelerations, fastpath dispatch, and lifespans."""

from __future__ import annotations

import contextlib
import gzip
import hashlib
import inspect
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from vertebra.extensions.starlette.config import VertebraConfig
from vertebra.extensions.starlette.fastpath import (
    CompiledRoute,
    FastPath,
    compile_fastpath_routes,
)
from vertebra.extensions.starlette.mcp import mount_mcp_routes
from vertebra.extensions.starlette.offload import (
    OffloadPlan,
    OpenApiOffloadSpec,
    evaluate_cors_preflight,
    evaluate_cors_response_headers,
    evaluate_openapi_cache_request,
    is_rust_offload_active,
    offload_middlewares,
)
from vertebra.extensions.starlette.openapi import mount_docs_routes
from vertebra.extensions.starlette.router import RouterTable, extract_routes
from vertebra.server._fastpath_core import set_attached_fastpath
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator, Sequence

    import starlette.responses as _starlette_responses
    import starlette.routing as _starlette_routing

    _ = (_starlette_responses, _starlette_routing)

__all__ = (
    "StarletteOffloadMiddleware",
    "VertebraPlugin",
    "enter_starlette_server_lifespan",
)


class StarletteOffloadMiddleware:
    """ASGI middleware executing CORS, OpenAPI ETag, compression offload, and fastpath dispatch."""

    __slots__ = ("_plugin", "app")

    def __init__(self, app: Any, *, plugin: VertebraPlugin) -> None:
        self.app = app
        self._plugin = plugin

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[..., Any],
        send: Callable[..., Any],
    ) -> None:
        """Handle HTTP offload fast paths before falling back to the Starlette ASGI stack."""
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        plan = self._plugin.offload_plan
        method = str(scope.get("method", "GET")).upper()
        path = str(scope.get("path", "/"))
        req_headers = scope.get("headers")

        if plan is not None and plan.cors is not None and method == "OPTIONS":
            preflight = evaluate_cors_preflight(plan.cors, headers=req_headers, method=method)
            if preflight is not None:
                status, preflight_headers = preflight
                raw_headers = _encode_asgi_headers(preflight_headers)
                raw_headers.append((b"content-length", b"0"))
                await _send_asgi_response(send, status, raw_headers, b"")
                return

        if method in {"GET", "HEAD"}:
            openapi_spec = self._plugin.resolve_openapi_spec(path, plan)
            if openapi_spec is not None:
                status, cache_headers, body = evaluate_openapi_cache_request(
                    openapi_spec,
                    headers=req_headers,
                )
                raw_headers = _encode_asgi_headers(cache_headers)
                if plan is not None and plan.cors is not None:
                    cors_hdrs = evaluate_cors_response_headers(plan.cors, headers=req_headers)
                    _append_missing_headers(raw_headers, _encode_asgi_headers(cors_hdrs))
                payload = b"" if method == "HEAD" or status == 304 else body
                if status != 304:
                    _set_content_length(raw_headers, len(body) if method == "HEAD" else len(payload))
                await _send_asgi_response(send, status, raw_headers, payload)
                return

        fastpath = self._plugin.fastpath
        if fastpath is not None:
            fast_res = await fastpath.dispatch(scope, receive, send)
            if fast_res is not None:
                status, fast_headers, body_view = fast_res
                out_headers = list(fast_headers)
                if plan is not None and plan.cors is not None:
                    cors_hdrs = evaluate_cors_response_headers(plan.cors, headers=req_headers)
                    _append_missing_headers(out_headers, _encode_asgi_headers(cors_hdrs))
                out_body, out_headers = _maybe_compress_body(
                    body_view,
                    out_headers,
                    plan=plan,
                    req_headers=req_headers,
                    path=path,
                    status=status,
                )
                if method == "HEAD":
                    out_body = b""
                await _send_asgi_response(send, status, out_headers, out_body)
                return

        capture_openapi = method == "GET" and self._plugin.should_cache_openapi(path, plan)
        needs_cors = plan is not None and plan.cors is not None
        needs_compression = plan is not None and plan.compression is not None and _accepts_gzip(req_headers)
        if not capture_openapi and not needs_cors and not needs_compression:
            await self.app(scope, receive, send)
            return

        start_message: dict[str, Any] | None = None
        body_chunks: list[bytes] = []
        buffering = False

        async def _wrapped_send(message: dict[str, Any]) -> None:
            nonlocal buffering, start_message
            msg_type = message.get("type")
            if msg_type == "http.response.start":
                raw_hdrs = [(bytes(k), bytes(v)) for k, v in (message.get("headers") or ())]
                if needs_cors and plan is not None and plan.cors is not None:
                    cors_hdrs = evaluate_cors_response_headers(plan.cors, headers=req_headers)
                    _append_missing_headers(raw_hdrs, _encode_asgi_headers(cors_hdrs))
                status_code = int(message.get("status", 200))
                if status_code == 200 and (capture_openapi or needs_compression):
                    start_message = {**message, "headers": raw_hdrs}
                    buffering = True
                    return
                await send({**message, "headers": raw_hdrs})
                return

            if msg_type == "http.response.body" and buffering:
                chunk = bytes(message.get("body", b""))
                if chunk:
                    body_chunks.append(chunk)
                if message.get("more_body", False):
                    return

                assert start_message is not None
                full_body = b"".join(body_chunks)
                out_headers = list(start_message["headers"])
                status_code = int(start_message.get("status", 200))

                if capture_openapi:
                    ct_bytes = _get_header_value(out_headers, b"content-type")
                    content_type = ct_bytes.decode("latin-1") if ct_bytes is not None else "application/json"
                    if "json" in content_type.lower():
                        existing_etag = _get_header_value(out_headers, b"etag")
                        etag = (
                            existing_etag.decode("latin-1")
                            if existing_etag is not None
                            else f'"{hashlib.sha256(full_body).hexdigest()[:16]}"'
                        )
                        spec = OpenApiOffloadSpec(
                            schema_path=path,
                            body=full_body,
                            etag=etag,
                            content_type=content_type,
                        )
                        self._plugin.cache_openapi_spec(spec)
                        _append_missing_headers(
                            out_headers,
                            [
                                (b"etag", etag.encode("latin-1")),
                                (b"cache-control", b"public, max-age=0, must-revalidate"),
                            ],
                        )

                out_body, out_headers = _maybe_compress_body(
                    full_body,
                    out_headers,
                    plan=plan,
                    req_headers=req_headers,
                    path=path,
                    status=status_code,
                )
                await send({**start_message, "headers": out_headers})
                await send({"type": "http.response.body", "body": out_body, "more_body": False})
                buffering = False
                return

            await send(message)

        await self.app(scope, receive, _wrapped_send)


class VertebraPlugin:
    """Starlette plugin enabling Rust middleware offload, matchit route compilation, and C-API fastpath."""

    __slots__ = (
        "_cached_openapi_specs",
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
        self._cached_openapi_specs: dict[str, OpenApiOffloadSpec] = {}
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

    def resolve_openapi_spec(self, path: str, plan: OffloadPlan | None = None) -> OpenApiOffloadSpec | None:
        """Return pre-rendered or runtime-captured OpenApiOffloadSpec for ``path``."""
        active_plan = plan if plan is not None else self._offload_plan
        if active_plan is not None and active_plan.openapi is not None and active_plan.openapi.schema_path == path:
            return active_plan.openapi
        return self._cached_openapi_specs.get(path)

    def should_cache_openapi(self, path: str, plan: OffloadPlan | None = None) -> bool:
        """Return True if ``path`` matches the configured OpenAPI schema endpoint."""
        if not self._config.offload_openapi:
            return False
        schema_path = self._configured_openapi_path()
        if path != schema_path:
            return False
        return self.resolve_openapi_spec(path, plan) is None

    def cache_openapi_spec(self, spec: OpenApiOffloadSpec) -> None:
        """Store a runtime-captured OpenAPI schema payload and ETag for subsequent 304 responses."""
        self._cached_openapi_specs[spec.schema_path] = spec

    def init_app(self, app: Any) -> Any:
        """Attach Vertebra state, lifespan wrapper, ASGI offload middleware, and fastpath to a Starlette app."""
        ensure_module("starlette", extra="starlette")
        state = _ensure_app_state(app)
        if _get_state_item(state, "_vertebra_initialized"):
            return app

        _set_state_item(state, "vertebra_config", self._config)
        _set_state_item(state, "vertebra_plugin", self)

        if self._config.enable_health_endpoint:
            _mount_health_endpoint(app, self._config.health_path)

        if self._config.openapi_config is not None:
            mount_docs_routes(app, self._config.openapi_config)

        if self._config.webmcp_config is not None:
            mount_mcp_routes(app, webmcp_config=self._config.webmcp_config)

        self._wrap_lifespan(app, state)
        self._attach_middleware(app, state)
        self._offload_plan = offload_middlewares(
            app,
            config=self._config,
            strip_python_middleware=False,
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
            self._fastpath = bypass
            with contextlib.suppress(TypeError):
                set_attached_fastpath(app, bypass)
        _set_state_item(state, "_vertebra_initialized", True)
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
        if self._config.enable_direct_endpoint_fastpath:
            self._compiled_routes = compile_fastpath_routes(app, router_table=self._router_table)

        if self._config.enable_fastpath_bypass:
            bypass = FastPath(
                self._router_table,
                app=app,
                compiled_routes=self._compiled_routes or None,
            )
            self._fastpath = bypass
            with contextlib.suppress(TypeError):
                set_attached_fastpath(app, bypass)

    def release_handles(self, app: Any = None) -> None:
        """Release transient runtime handles when the application lifespan exits."""
        self._cached_openapi_specs.clear()
        if app is not None:
            state = getattr(app, "state", None)
            if state is not None:
                _set_state_item(state, "vertebra_lifespan_active", False)

    @contextlib.contextmanager
    def server_lifespan(self, app: Any) -> Iterator[None]:
        """Ensure acceleration state is finalized when entering server lifespan."""
        self.ensure_finalized(app)
        try:
            yield
        finally:
            self.release_handles(app)

    def _configured_openapi_path(self) -> str:
        openapi_cfg = self._config.openapi_config
        if openapi_cfg is not None:
            path = getattr(openapi_cfg, "schema_path", None)
            if isinstance(path, str) and path:
                return path
        if self._finalized_app is not None:
            app_url = getattr(self._finalized_app, "openapi_url", None)
            if isinstance(app_url, str) and app_url:
                return app_url
        return "/openapi.json"

    def _wrap_lifespan(self, app: Any, state: Any) -> None:
        router = getattr(app, "router", None)
        if router is None or not hasattr(router, "lifespan_context"):
            return
        if _get_state_item(state, "_vertebra_lifespan_wrapped"):
            return

        existing_lifespan = getattr(router, "lifespan_context", None)

        @contextlib.asynccontextmanager
        async def _vertebra_lifespan(target_app: Any) -> AsyncIterator[Any]:
            self.ensure_finalized(target_app)
            target_state = _ensure_app_state(target_app)
            _set_state_item(target_state, "vertebra_lifespan_active", True)
            run_legacy_hooks = existing_lifespan is None or type(existing_lifespan).__name__ == "_DefaultLifespan"
            if run_legacy_hooks:
                await _run_lifecycle_hooks(target_app, "on_startup")
            try:
                if existing_lifespan is not None:
                    async with existing_lifespan(target_app) as maybe_state:
                        yield maybe_state
                else:
                    yield None
            finally:
                try:
                    if run_legacy_hooks:
                        await _run_lifecycle_hooks(target_app, "on_shutdown")
                finally:
                    self.release_handles(target_app)

        router.lifespan_context = _vertebra_lifespan
        _set_state_item(state, "_vertebra_lifespan_wrapped", True)

    def _attach_middleware(self, app: Any, state: Any) -> None:
        if _get_state_item(state, "_vertebra_middleware_attached"):
            return
        add_mw = getattr(app, "add_middleware", None)
        if not callable(add_mw):
            return
        if getattr(app, "middleware_stack", None) is None:
            add_mw(StarletteOffloadMiddleware, plugin=self)
            _set_state_item(state, "_vertebra_middleware_attached", True)
            return
        user_mw = getattr(app, "user_middleware", None)
        mw_mod = ensure_module("starlette.middleware", package="starlette", extra="starlette")
        if isinstance(user_mw, list) and hasattr(mw_mod, "Middleware"):
            user_mw.insert(0, mw_mod.Middleware(StarletteOffloadMiddleware, plugin=self))
            build_stack = getattr(app, "build_middleware_stack", None)
            if callable(build_stack):
                app.middleware_stack = build_stack()
            _set_state_item(state, "_vertebra_middleware_attached", True)


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


async def _run_lifecycle_hooks(app: Any, attr: str) -> None:
    targets = (getattr(app, attr, None), getattr(getattr(app, "router", None), attr, None))
    seen: set[int] = set()
    for hooks in targets:
        if not hooks:
            continue
        for hook in hooks:
            hook_id = id(hook)
            if hook_id in seen or not callable(hook):
                continue
            seen.add(hook_id)
            res = hook()
            if inspect.isawaitable(res):
                await res


def _mount_health_endpoint(app: Any, health_path: str) -> None:
    routes = getattr(app, "routes", None)
    if not isinstance(routes, list):
        return
    for existing in routes:
        if getattr(existing, "path", None) == health_path:
            return

    routing_mod = ensure_module("starlette.routing", package="starlette", extra="starlette")
    responses_mod = ensure_module("starlette.responses", package="starlette", extra="starlette")

    async def _vertebra_health_endpoint(_request: Any) -> Any:
        return responses_mod.Response(
            content=encode_json_bytes({"status": "ok"}),
            media_type="application/json",
        )

    routes.append(
        routing_mod.Route(
            health_path,
            endpoint=_vertebra_health_endpoint,
            methods=["GET"],
            include_in_schema=False,
        )
    )


def _encode_asgi_headers(headers: Sequence[tuple[str, str]]) -> list[tuple[bytes, bytes]]:
    return [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers]


def _append_missing_headers(
    target: list[tuple[bytes, bytes]],
    additions: Sequence[tuple[bytes, bytes]],
) -> None:
    existing = {k.lower() for k, _ in target}
    for key, val in additions:
        if key.lower() not in existing:
            target.append((key, val))
            existing.add(key.lower())


def _get_header_value(headers: Sequence[tuple[bytes, bytes]], name: bytes) -> bytes | None:
    target = name.lower()
    for key, val in headers:
        if key.lower() == target:
            return val
    return None


def _set_content_length(headers: list[tuple[bytes, bytes]], length: int) -> None:
    val = str(length).encode("ascii")
    for idx, (key, _) in enumerate(headers):
        if key.lower() == b"content-length":
            headers[idx] = (b"content-length", val)
            return
    headers.append((b"content-length", val))


def _accepts_gzip(req_headers: Any) -> bool:
    if not req_headers:
        return False
    for raw_k, raw_v in req_headers:
        k = raw_k.decode("latin-1").lower() if isinstance(raw_k, bytes) else str(raw_k).lower()
        if k == "accept-encoding":
            v = raw_v.decode("latin-1").lower() if isinstance(raw_v, bytes) else str(raw_v).lower()
            return "gzip" in v
    return False


def _maybe_compress_body(
    body_view: bytes | memoryview,
    headers: list[tuple[bytes, bytes]],
    *,
    plan: OffloadPlan | None,
    req_headers: Any,
    path: str,
    status: int,
) -> tuple[bytes | memoryview, list[tuple[bytes, bytes]]]:
    if plan is None or plan.compression is None or status in {204, 304}:
        return body_view, headers
    if not _accepts_gzip(req_headers):
        return body_view, headers
    if _get_header_value(headers, b"content-encoding") is not None:
        return body_view, headers
    if path in plan.compression.excluded_paths:
        return body_view, headers
    if len(body_view) < plan.compression.minimum_size:
        return body_view, headers
    raw_bytes = bytes(body_view) if isinstance(body_view, memoryview) else body_view
    compressed = gzip.compress(raw_bytes, compresslevel=max(1, min(9, plan.compression.gzip_compress_level)))
    updated = [(k, v) for k, v in headers if k.lower() != b"content-length"]
    updated.append((b"content-encoding", b"gzip"))
    updated.append((b"content-length", str(len(compressed)).encode("ascii")))
    _append_missing_headers(updated, [(b"vary", b"Accept-Encoding")])
    return compressed, updated


async def _send_asgi_response(
    send: Callable[..., Any],
    status: int,
    headers: list[tuple[bytes, bytes]],
    body: bytes | memoryview,
) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": headers,
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": bytes(body) if isinstance(body, memoryview) else body,
            "more_body": False,
        }
    )
