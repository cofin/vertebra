"""Unit tests for zero-GIL Rust offload of Litestar v2/v3 Compression, CORS, StaticFiles, and OpenAPI."""

import inspect
import warnings
from collections.abc import Callable, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import msgspec

from vertebra.extensions.litestar.offload import (
    CorsOffloadSpec,
    OffloadPlan,
    OpenApiOffloadSpec,
    _get_handler_guards,
    _get_handler_middleware,
    _get_handler_opt,
    evaluate_cors_preflight,
    evaluate_cors_response_headers,
    evaluate_openapi_cache_request,
    offload_middlewares,
)
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json


def _load_litestar_offload_primitives() -> tuple[Any, Any, Any, Any, Any]:
    """Load Litestar offload classes or fallback stubs when optional extra is absent."""
    litestar_mod = import_optional("litestar")
    comp_mod = import_optional("litestar.config.compression")
    cors_mod = import_optional("litestar.config.cors")
    static_mod = import_optional("litestar.static_files")
    if litestar_mod is not None and comp_mod is not None and cors_mod is not None and static_mod is not None:
        return (
            litestar_mod.Litestar,
            litestar_mod.get,
            comp_mod.CompressionConfig,
            cors_mod.CORSConfig,
            static_mod.create_static_files_router,
        )

    class CompressionConfigStub(msgspec.Struct, frozen=True, gc=False):
        backend: str = "gzip"
        minimum_size: int = 500
        gzip_compress_level: int = 6
        brotli_quality: int = 4
        zstd_compress_level: int = 3
        exclude: Sequence[str] | str | None = None
        exclude_opt_key: str | None = None

    class CORSConfigStub(msgspec.Struct, frozen=True, gc=False):
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

    plan: OffloadPlan = offload_middlewares(app)
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

    plan = offload_middlewares(app)
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

    plan = offload_middlewares(app)
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
    plan = offload_middlewares(app)

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


def test_offload_plan_to_server_config_matches_rust_offload_schema(tmp_path: Path) -> None:
    """Verify LitestarOffloadPlan.to_server_config exports JSON matching vertebra-http OffloadPlanPayload."""
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir()
    (assets_dir / "bundle.js").write_text("export default 1;", encoding="utf-8")

    @get("/items", sync_to_thread=False)
    def get_items() -> list[str]:
        return ["a"]

    @get("/raw", opt={"exclude_from_compression": True}, sync_to_thread=False)
    def get_raw() -> str:
        return "raw"

    static_router = create_static_files_router(
        path="/static",
        directories=[assets_dir],
        html_mode=True,
    )
    comp_kwargs: dict[str, Any] = {
        "backend": "zstd",
        "minimum_size": 256,
        "exclude": ["/healthz"],
        "exclude_opt_key": "exclude_from_compression",
    }
    if "zstd_compress_level" in inspect.signature(CompressionConfig).parameters:
        comp_kwargs["zstd_compress_level"] = 5

    app = Litestar(
        route_handlers=[get_items, get_raw, static_router],
        compression_config=CompressionConfig(**comp_kwargs),
        cors_config=CORSConfig(
            allow_origins=["https://app.example.com"],
            allow_methods=["GET", "POST"],
            allow_headers=["Authorization"],
            allow_credentials=True,
            expose_headers=["X-Trace-Id"],
            max_age=600,
        ),
    )

    plan = offload_middlewares(app)
    cfg = plan.to_server_config()
    raw_json = plan.to_server_config_json()
    assert decode_json(raw_json) == cfg

    assert cfg["compression"]["backend"] == "zstd"
    assert cfg["compression"]["minimum_size"] == 256
    expected_zstd = 5 if "zstd_compress_level" in comp_kwargs else 3
    assert cfg["compression"]["zstd_level"] == expected_zstd
    assert "/healthz" in cfg["compression"]["exclude_patterns"]
    assert "/raw" in cfg["compression"]["excluded_paths"]

    assert cfg["cors"]["allow_origins"] == ["https://app.example.com"]
    assert cfg["cors"]["allow_methods"] == ["GET", "POST"]
    assert cfg["cors"]["allow_credentials"] is True
    assert cfg["cors"]["max_age"] == 600

    assert len(cfg["static_mounts"]) == 1
    assert cfg["static_mounts"][0]["path_prefix"] == "/static"
    assert str(assets_dir.resolve()) in cfg["static_mounts"][0]["directories"]
    assert cfg["static_mounts"][0]["html_mode"] is True

    assert cfg["openapi"]["schema_path"] == "/schema/openapi.json"
    assert isinstance(cfg["openapi"]["body"], str)
    assert '"/items"' in cfg["openapi"]["body"]


def test_v3_flattened_handler_skips_deprecated_resolve_methods_and_filters_compression_backends() -> None:
    """Verify v3 flattened handlers without ownership_layers skip deprecated resolve_* calls and reject unknown codecs."""

    class _FlattenedV3Handler:
        def __init__(self) -> None:
            self.guards = ("g1",)
            self.middleware = ("m1",)
            self.opt = {"fast": True}

        def resolve_guards(self) -> tuple[str, ...]:
            warnings.warn("resolve_guards is deprecated in Litestar v3", DeprecationWarning, stacklevel=2)
            raise AssertionError("v3 flattened handler must not call deprecated resolve_guards")

        def resolve_middleware(self) -> tuple[str, ...]:
            warnings.warn("resolve_middleware is deprecated in Litestar v3", DeprecationWarning, stacklevel=2)
            raise AssertionError("v3 flattened handler must not call deprecated resolve_middleware")

        def resolve_opts(self) -> dict[str, Any]:
            warnings.warn("resolve_opts is deprecated in Litestar v3", DeprecationWarning, stacklevel=2)
            raise AssertionError("v3 flattened handler must not call deprecated resolve_opts")

    v3_handler = _FlattenedV3Handler()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert _get_handler_guards(v3_handler) == ("g1",)
        assert _get_handler_middleware(v3_handler) == ("m1",)
        assert _get_handler_opt(v3_handler) == {"fast": True}
    assert caught == []

    @get("/custom-codec", sync_to_thread=False)
    def custom_codec_route() -> dict[str, str]:
        return {"ok": "custom"}

    unsupported_app = Litestar(
        route_handlers=[custom_codec_route],
        compression_config=CompressionConfig(backend="lz4", minimum_size=128),
    )
    unsupported_plan = offload_middlewares(unsupported_app)
    assert unsupported_plan.compression is None
    assert unsupported_app.compression_config is not None

    br_app = Litestar(
        route_handlers=[custom_codec_route],
        compression_config=CompressionConfig(backend="br", minimum_size=128),
    )
    br_plan = offload_middlewares(br_app)
    assert br_plan.compression is not None
    assert br_plan.compression.backend == "brotli"


def test_v2_inplace_resolve_opts_and_v3_zstd_compress_level_extraction() -> None:
    """Verify _get_handler_opt handles Litestar v2 resolve_opts() -> None and zstd_compress_level extraction."""

    class _V2InPlaceOptsHandler:
        def __init__(self) -> None:
            self.ownership_layers = (SimpleNamespace(opt={"exclude_from_compression": True}),)
            self.opt: dict[str, Any] = {}
            self.guards = ()
            self.middleware = ()

        def resolve_opts(self) -> None:
            merged: dict[str, Any] = {}
            for layer in self.ownership_layers:
                merged.update(getattr(layer, "opt", None) or {})
            self.opt = merged

    v2_handler = _V2InPlaceOptsHandler()
    assert _get_handler_opt(v2_handler) == {"exclude_from_compression": True}

    v2_app = SimpleNamespace(
        compression_config=SimpleNamespace(
            backend="zstd",
            minimum_size=128,
            zstd_compress_level=7,
            exclude=None,
            exclude_opt_key="exclude_from_compression",
        ),
        cors_config=None,
        before_send=None,
        state={},
        routes=[SimpleNamespace(path="/v2-opt-skip", route_handler_map={"GET": v2_handler}, path_parameters=())],
        asgi_router=None,
    )
    v2_plan = offload_middlewares(v2_app)
    assert v2_plan.compression is not None
    assert v2_plan.compression.zstd_level == 7
    assert v2_plan.compression.excluded_paths == ("/v2-opt-skip",)

    legacy_zstd_app = SimpleNamespace(
        compression_config=SimpleNamespace(
            backend="zstd",
            minimum_size=128,
            zstd_level=4,
            exclude=None,
            exclude_opt_key=None,
        ),
        cors_config=None,
        before_send=None,
        state={},
        routes=[],
        asgi_router=None,
    )
    legacy_plan = offload_middlewares(legacy_zstd_app)
    assert legacy_plan.compression is not None
    assert legacy_plan.compression.zstd_level == 4
