"""Unit tests for zero-GIL Rust offload of Litestar v2/v3 Compression, CORS, StaticFiles, and OpenAPI."""

import dataclasses
import importlib
from collections.abc import Callable, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from vertebra.server._asgi._litestar_offload import (
    CorsOffloadSpec,
    LitestarOffloadPlan,
    OpenApiOffloadSpec,
    _get_handler_guards,
    _get_handler_middleware,
    _get_handler_opt,
    evaluate_cors_preflight,
    evaluate_cors_response_headers,
    evaluate_openapi_cache_request,
    offload_litestar_middlewares,
)


def _load_litestar_offload_primitives() -> tuple[Any, Any, Any, Any, Any]:
    """Load Litestar offload classes or fallback stubs when optional extra is absent."""
    try:
        litestar_mod = importlib.import_module("litestar")
        comp_mod = importlib.import_module("litestar.config.compression")
        cors_mod = importlib.import_module("litestar.config.cors")
        static_mod = importlib.import_module("litestar.static_files")
        return (
            litestar_mod.Litestar,
            litestar_mod.get,
            comp_mod.CompressionConfig,
            cors_mod.CORSConfig,
            static_mod.create_static_files_router,
        )
    except ImportError:

        @dataclasses.dataclass
        class CompressionConfigStub:
            backend: str = "gzip"
            minimum_size: int = 500
            gzip_compress_level: int = 6
            brotli_quality: int = 4
            zstd_level: int = 3
            exclude: Sequence[str] | str | None = None
            exclude_opt_key: str | None = None

        @dataclasses.dataclass
        class CORSConfigStub:
            allow_origins: Sequence[str] = ("*",)
            allow_methods: Sequence[str] = ("*",)
            allow_headers: Sequence[str] = ("*",)
            allow_credentials: bool = False
            expose_headers: Sequence[str] = ()
            max_age: int = 600
            allow_origin_regex: str | None = None

        def get_stub(
            path: str, *, opt: dict[str, Any] | None = None, **_kwargs: Any
        ) -> Callable[[Callable[..., Any]], Any]:
            def wrap(fn: Callable[..., Any]) -> Any:
                return SimpleNamespace(
                    paths={path},
                    http_methods={"GET"},
                    fn=fn,
                    guards=(),
                    middleware=(),
                    opt=dict(opt or {}),
                )

            return wrap

        def create_static_files_router_stub(
            *,
            path: str,
            directories: Sequence[str | Path],
            html_mode: bool = False,
            send_as_attachment: bool = False,
            headers: dict[str, str] | None = None,
            file_system: Any = None,
        ) -> Any:
            resolved_directories = [Path(d) for d in directories]

            async def _handle() -> bytes:
                _ = (resolved_directories, file_system, html_mode, send_as_attachment, headers)
                return b""

            normalized = "/" + path.strip("/") + "/{file_path:path}"
            return SimpleNamespace(
                paths={normalized},
                http_methods={"GET"},
                fn=_handle,
                guards=(),
                middleware=(),
                opt={},
            )

        class _AsgiRouterStub:
            def __init__(self, app: Any) -> None:
                self._app = app
                self._registered_routes: set[Any] = set()
                self._plain_routes: set[str] = set()
                self._mount_routes: dict[str, Any] = {}
                self._trie_initialized = True
                self.root_route_map_node = SimpleNamespace(asgi_handlers={}, children={}, path_param_node=None)
                self.construct_routing_trie()

            def construct_routing_trie(self) -> None:
                has_comp = getattr(self._app, "compression_config", None) is not None
                handlers: dict[str, Any] = {}
                for route in self._app.routes:
                    label = "CompressionMiddleware(handler)" if has_comp else "RouteHandler(handler)"
                    handlers[route.path] = SimpleNamespace(asgi_handler=label)
                self.root_route_map_node = SimpleNamespace(
                    asgi_handlers=handlers,
                    children={},
                    path_param_node=None,
                )

        class LitestarStub:
            def __init__(
                self,
                route_handlers: Sequence[Any] | None = None,
                *,
                compression_config: Any = None,
                cors_config: Any = None,
                **_kwargs: Any,
            ) -> None:
                self.compression_config = compression_config
                self.cors_config = cors_config
                self.before_send = None
                self.state: dict[str, Any] = {}
                by_path: dict[str, dict[str, Any]] = {}
                for handler in route_handlers or ():
                    for path in getattr(handler, "paths", ("/",)):
                        method_map = by_path.setdefault(path, {})
                        for method in getattr(handler, "http_methods", ("GET",)):
                            method_map[method] = handler
                self.routes = [
                    SimpleNamespace(path=path, route_handler_map=method_map, path_parameters=())
                    for path, method_map in by_path.items()
                ]
                self.openapi_config = SimpleNamespace(path="/schema")
                self.openapi_schema = SimpleNamespace(
                    to_schema=lambda: {
                        "openapi": "3.1.0",
                        "paths": {r.path: {} for r in self.routes},
                    }
                )
                self.asgi_router = _AsgiRouterStub(self)
                self.asgi_handler = self._create_asgi_handler()

            def _create_asgi_handler(self) -> Any:
                return self.asgi_router

        return (
            LitestarStub,
            get_stub,
            CompressionConfigStub,
            CORSConfigStub,
            create_static_files_router_stub,
        )


Litestar, get, CompressionConfig, CORSConfig, create_static_files_router = _load_litestar_offload_primitives()


def _collect_trie_handler_reprs(app: Any) -> list[str]:
    """Collect string representations of all compiled ASGI handlers in the routing trie."""
    seen: list[str] = []
    stack = [app.asgi_router.root_route_map_node]
    while stack:
        node = stack.pop()
        for entry in getattr(node, "asgi_handlers", {}).values():
            asgi_fn = entry[0] if isinstance(entry, tuple) else getattr(entry, "asgi_handler", entry)
            seen.append(repr(asgi_fn))
        children = getattr(node, "children", None) or {}
        stack.extend(children.values())
        child = getattr(node, "path_param_node", None)
        if child is not None:
            stack.append(child)
    return seen


def test_offload_compression_strips_python_middleware_across_v2_and_v3() -> None:
    """Verify offload_litestar_middlewares extracts CompressionConfig and rebuilds the trie without CompressionMiddleware."""

    @get("/payload", sync_to_thread=False)
    def get_payload() -> dict[str, str]:
        return {"message": "x" * 1024}

    @get("/uncompressed", opt={"exclude_from_compression": True}, sync_to_thread=False)
    def get_uncompressed() -> dict[str, str]:
        return {"message": "raw"}

    app = Litestar(
        route_handlers=[get_payload, get_uncompressed],
        compression_config=CompressionConfig(
            backend="gzip",
            minimum_size=256,
            gzip_compress_level=6,
            exclude=["/healthz"],
            exclude_opt_key="exclude_from_compression",
        ),
    )

    plan: LitestarOffloadPlan = offload_litestar_middlewares(app)
    assert plan.compression is not None
    assert plan.compression.backend == "gzip"
    assert plan.compression.minimum_size == 256
    assert plan.compression.gzip_compress_level == 6
    assert "/healthz" in plan.compression.exclude_patterns
    assert "/uncompressed" in plan.compression.excluded_paths
    assert app.compression_config is None

    trie_reprs = _collect_trie_handler_reprs(app)
    assert not any("CompressionMiddleware" in item for item in trie_reprs)


def test_offload_cors_preflight_and_response_headers() -> None:
    """Verify CORSConfig extraction and Rust-equivalent preflight/response header evaluation."""

    @get("/items", sync_to_thread=False)
    def list_items() -> list[str]:
        return ["a", "b"]

    app = Litestar(
        route_handlers=[list_items],
        cors_config=CORSConfig(
            allow_origins=["https://app.example.com"],
            allow_methods=["GET", "POST"],
            allow_headers=["Authorization", "Content-Type"],
            allow_credentials=True,
            expose_headers=["X-Request-Id"],
            max_age=600,
        ),
    )

    plan = offload_litestar_middlewares(app)
    assert isinstance(plan.cors, CorsOffloadSpec)
    assert plan.cors.allow_origins == ("https://app.example.com",)
    assert plan.cors.allow_credentials is True

    status, preflight_headers = evaluate_cors_preflight(
        plan.cors,
        origin="https://app.example.com",
        access_control_request_method="POST",
        access_control_request_headers="Authorization",
    )
    assert status == 204
    header_map = dict(preflight_headers)
    assert header_map["access-control-allow-origin"] == "https://app.example.com"
    assert header_map["access-control-allow-credentials"] == "true"
    assert header_map["access-control-max-age"] == "600"

    resp_headers = dict(evaluate_cors_response_headers(plan.cors, origin="https://app.example.com"))
    assert resp_headers["access-control-allow-origin"] == "https://app.example.com"
    assert resp_headers["access-control-expose-headers"] == "X-Request-Id"

    bad_method_status, bad_method_headers = evaluate_cors_preflight(
        plan.cors,
        origin="https://app.example.com",
        access_control_request_method="DELETE",
    )
    assert bad_method_status == 400
    assert bad_method_headers == []

    regex_spec = CorsOffloadSpec(
        allow_origins=(),
        allow_methods=("GET", "POST"),
        allow_headers=("Authorization",),
        allow_credentials=True,
        expose_headers=(),
        max_age=300,
        allow_origin_regex=r"https://[a-z0-9-]+\.example\.com",
    )
    regex_status, regex_preflight_headers = evaluate_cors_preflight(
        regex_spec,
        origin="https://tenant-1.example.com",
        access_control_request_method="GET",
    )
    assert regex_status == 204
    assert dict(regex_preflight_headers)["access-control-allow-origin"] == "https://tenant-1.example.com"
    assert evaluate_cors_response_headers(regex_spec, origin="https://evil.other.org") == []


def test_offload_static_files_from_create_static_files_router(tmp_path: Path) -> None:
    """Verify static mount extraction from create_static_files_router handler closures."""
    assets_dir = tmp_path / "public"
    assets_dir.mkdir()
    (assets_dir / "app.js").write_text("console.log('ok');", encoding="utf-8")

    static_router = create_static_files_router(
        path="/static",
        directories=[assets_dir],
        html_mode=True,
    )
    app = Litestar(route_handlers=[static_router])

    plan = offload_litestar_middlewares(app)
    assert len(plan.static_mounts) == 1
    mount = plan.static_mounts[0]
    assert mount.path_prefix == "/static"
    assert str(assets_dir.resolve()) in mount.directories
    assert mount.html_mode is True


def test_offload_openapi_schema_etag_and_304() -> None:
    """Verify OpenAPI schema is pre-rendered once at startup with ETag and 304 Not Modified support."""

    @get("/widgets", sync_to_thread=False)
    def get_widgets() -> list[dict[str, Any]]:
        return [{"id": 1}]

    app = Litestar(route_handlers=[get_widgets])
    plan = offload_litestar_middlewares(app)

    assert isinstance(plan.openapi, OpenApiOffloadSpec)
    assert plan.openapi.schema_path == "/schema/openapi.json"
    assert plan.openapi.etag.startswith('"')
    assert b'"/widgets"' in plan.openapi.body

    status_200, headers_200, body_200 = evaluate_openapi_cache_request(plan.openapi, if_none_match=None)
    assert status_200 == 200
    assert body_200 == plan.openapi.body
    assert dict(headers_200)["etag"] == plan.openapi.etag

    status_304, _headers_304, body_304 = evaluate_openapi_cache_request(
        plan.openapi,
        if_none_match=plan.openapi.etag,
    )
    assert status_304 == 304
    assert body_304 == b""


def test_flattened_v3_and_v2_handler_attribute_helpers() -> None:
    """Verify _get_handler_guards, _get_handler_middleware, and _get_handler_opt handle v2 and v3 handlers."""

    @get("/check", opt={"custom_flag": True}, sync_to_thread=False)
    def check_route() -> dict[str, bool]:
        return {"ok": True}

    app = Litestar(route_handlers=[check_route])
    route = next(r for r in app.routes if getattr(r, "path", None) == "/check")
    raw_entry = route.route_handler_map["GET"]
    handler = raw_entry[0] if isinstance(raw_entry, tuple) else raw_entry

    assert _get_handler_guards(handler) == ()
    assert _get_handler_middleware(handler) == ()
    assert _get_handler_opt(handler).get("custom_flag") is True
