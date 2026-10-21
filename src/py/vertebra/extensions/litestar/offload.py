"""Zero-code-change Litestar v2 and v3 startup introspection for Rust middleware and asset offloading."""

import hashlib
import importlib
import re
from pathlib import Path
from typing import Any

import msgspec

from vertebra.extensions.litestar.router import _unpack_route_handler_map_entry
from vertebra.utils.serializers._json import encode_json_bytes

__all__ = (
    "CompressionOffloadSpec",
    "CorsOffloadSpec",
    "LitestarOffloadPlan",
    "MiddlewareOffloadCompiler",
    "OpenApiOffloadSpec",
    "StaticMountOffloadSpec",
    "_get_handler_after_response",
    "_get_handler_exception_handlers",
    "_get_handler_guards",
    "_get_handler_middleware",
    "_get_handler_opt",
    "_get_handler_request_class",
    "_is_default_local_filesystem",
    "_rebuild_litestar_routing_trie",
    "evaluate_cors_preflight",
    "evaluate_cors_response_headers",
    "evaluate_openapi_cache_request",
    "offload_litestar_middlewares",
)


class CompressionOffloadSpec(msgspec.Struct, frozen=True, gc=False):
    """Extracted Litestar CompressionConfig parameters for Rust vertebra-http."""

    backend: str
    minimum_size: int
    gzip_compress_level: int
    brotli_quality: int
    zstd_level: int
    exclude_patterns: tuple[str, ...]
    exclude_opt_key: str | None
    excluded_paths: tuple[str, ...]


class CorsOffloadSpec(msgspec.Struct, frozen=True, gc=False):
    """Extracted Litestar CORSConfig parameters for Rust vertebra-http."""

    allow_origins: tuple[str, ...]
    allow_methods: tuple[str, ...]
    allow_headers: tuple[str, ...]
    allow_credentials: bool
    expose_headers: tuple[str, ...]
    max_age: int
    allow_origin_regex: str | None = None


class StaticMountOffloadSpec(msgspec.Struct, frozen=True, gc=False):
    """Extracted static file mount configuration for zero-GIL Rust asset serving."""

    path_prefix: str
    directories: tuple[str, ...]
    html_mode: bool
    send_as_attachment: bool
    headers: dict[str, str]


class OpenApiOffloadSpec(msgspec.Struct, frozen=True, gc=False):
    """Pre-rendered OpenAPI JSON payload and ETag for zero-GIL Rust serving."""

    schema_path: str
    body: bytes
    etag: str
    content_type: str = "application/vnd.oai.openapi+json"


class LitestarOffloadPlan(msgspec.Struct, frozen=True, gc=False):
    """Complete startup offload plan extracted from an unmodified Litestar v2 or v3 app."""

    compression: CompressionOffloadSpec | None
    cors: CorsOffloadSpec | None
    static_mounts: tuple[StaticMountOffloadSpec, ...]
    openapi: OpenApiOffloadSpec | None


class MiddlewareOffloadCompiler:
    """Compiler extracting CompressionConfig, CORSConfig, StaticFiles, and OpenAPI into a LitestarOffloadPlan."""

    __slots__ = ()

    def compile(self, app: Any, *, config: Any = None) -> LitestarOffloadPlan:
        """Extract and compile the Rust middleware offload plan for the given Litestar app."""
        return offload_litestar_middlewares(app, config=config)


def offload_litestar_middlewares(app: Any, *, config: Any = None) -> LitestarOffloadPlan:
    """Extract CompressionConfig, CORSConfig, StaticFiles, and OpenAPI from a Litestar v2 or v3 app."""
    cfg = config
    if cfg is None:
        state = getattr(app, "state", None)
        if isinstance(state, dict) or (state is not None and hasattr(state, "get")):
            cfg = state.get("vertebra_config")

    enable_compression = True if cfg is None else bool(getattr(cfg, "offload_compression", True))
    enable_cors = True if cfg is None else bool(getattr(cfg, "offload_cors", True))
    enable_static = True if cfg is None else bool(getattr(cfg, "offload_static_files", True))
    enable_openapi = True if cfg is None else bool(getattr(cfg, "offload_openapi", True))

    compression_spec = _extract_compression_config(app) if enable_compression else None
    if (
        enable_compression
        and compression_spec is not None
        and not getattr(app, "before_send", None)
        and getattr(app, "compression_config", None) is not None
    ):
        app.compression_config = None
        _rebuild_litestar_routing_trie(app)

    cors_spec = _extract_cors_config(app) if enable_cors else None
    static_mounts = _extract_static_mounts(app) if enable_static else ()
    openapi_spec = _extract_openapi_schema_payload(app) if enable_openapi else None
    return LitestarOffloadPlan(
        compression=compression_spec,
        cors=cors_spec,
        static_mounts=static_mounts,
        openapi=openapi_spec,
    )


def evaluate_cors_preflight(
    spec: CorsOffloadSpec,
    *,
    origin: str,
    access_control_request_method: str | None = None,
    access_control_request_headers: str | None = None,
) -> tuple[int, list[tuple[str, str]]]:
    """Evaluate an OPTIONS CORS preflight request against CorsOffloadSpec."""
    if not _origin_allowed(spec, origin):
        return 400, []
    if (
        access_control_request_method is not None
        and "*" not in spec.allow_methods
        and access_control_request_method.upper() not in {m.upper() for m in spec.allow_methods}
    ):
        return 400, []
    wildcard = "*" in spec.allow_origins
    allow_origin = "*" if (wildcard and not spec.allow_credentials) else origin
    headers: list[tuple[str, str]] = [("access-control-allow-origin", allow_origin)]
    if spec.allow_credentials:
        headers.append(("access-control-allow-credentials", "true"))
    methods = (
        access_control_request_method or "GET, POST, PUT, PATCH, DELETE, OPTIONS"
        if "*" in spec.allow_methods
        else ", ".join(spec.allow_methods)
    )
    headers.append(("access-control-allow-methods", methods))
    allow_hdrs = access_control_request_headers or "*" if "*" in spec.allow_headers else ", ".join(spec.allow_headers)
    if allow_hdrs:
        headers.append(("access-control-allow-headers", allow_hdrs))
    headers.append(("access-control-max-age", str(spec.max_age)))
    headers.append(("vary", "Origin"))
    return 204, headers


def evaluate_cors_response_headers(
    spec: CorsOffloadSpec,
    *,
    origin: str,
) -> list[tuple[str, str]]:
    """Generate CORS response headers for a non-preflight request."""
    if not _origin_allowed(spec, origin):
        return []
    wildcard = "*" in spec.allow_origins
    allow_origin = "*" if (wildcard and not spec.allow_credentials) else origin
    headers: list[tuple[str, str]] = [("access-control-allow-origin", allow_origin)]
    if spec.allow_credentials:
        headers.append(("access-control-allow-credentials", "true"))
    if spec.expose_headers:
        headers.append(("access-control-expose-headers", ", ".join(spec.expose_headers)))
    if allow_origin != "*":
        headers.append(("vary", "Origin"))
    return headers


def evaluate_openapi_cache_request(
    spec: OpenApiOffloadSpec,
    *,
    if_none_match: str | None = None,
) -> tuple[int, list[tuple[str, str]], bytes]:
    """Return a 304 Not Modified or 200 OK response for the cached OpenAPI JSON payload."""
    headers = [
        ("content-type", spec.content_type),
        ("etag", spec.etag),
        ("cache-control", "public, max-age=0, must-revalidate"),
    ]
    if if_none_match is not None and if_none_match.strip() == spec.etag:
        return 304, headers, b""
    return 200, headers, spec.body


def _origin_allowed(spec: CorsOffloadSpec, origin: str) -> bool:
    if "*" in spec.allow_origins or origin in spec.allow_origins:
        return True
    return spec.allow_origin_regex is not None and re.fullmatch(spec.allow_origin_regex, origin) is not None


def _is_empty_sentinel(value: Any) -> bool:
    if value is None:
        return True
    type_name = getattr(type(value), "__name__", "")
    return type_name == "EmptyType" or value.__class__.__name__ == "Empty"


def _get_handler_guards(handler: Any) -> tuple[Any, ...]:
    resolve_fn = getattr(handler, "resolve_guards", None)
    if callable(resolve_fn):
        return tuple(resolve_fn())
    raw = getattr(handler, "guards", None)
    if raw is None or _is_empty_sentinel(raw):
        return ()
    return tuple(raw)


def _get_handler_middleware(handler: Any) -> tuple[Any, ...]:
    resolve_fn = getattr(handler, "resolve_middleware", None)
    if callable(resolve_fn):
        return tuple(resolve_fn())
    raw = getattr(handler, "middleware", None)
    if raw is None or _is_empty_sentinel(raw):
        return ()
    return tuple(raw)


def _get_handler_opt(handler: Any) -> dict[str, Any]:
    resolve_fn = getattr(handler, "resolve_opts", None)
    if callable(resolve_fn):
        return dict(resolve_fn())
    raw = getattr(handler, "opt", None)
    if _is_empty_sentinel(raw) or not isinstance(raw, dict):
        return {}
    return dict(raw)


def _get_handler_exception_handlers(handler: Any) -> dict[Any, Any]:
    resolve_fn = getattr(handler, "resolve_exception_handlers", None)
    if callable(resolve_fn):
        return dict(resolve_fn())
    raw = getattr(handler, "exception_handlers", None)
    if _is_empty_sentinel(raw) or not isinstance(raw, dict):
        return {}
    return dict(raw)


def _get_handler_request_class(handler: Any, app: Any) -> Any:
    raw = getattr(handler, "request_class", None)
    if not _is_empty_sentinel(raw):
        return raw
    resolve_fn = getattr(handler, "resolve_request_class", None)
    if callable(resolve_fn):
        return resolve_fn()
    return getattr(app, "request_class", None)


def _get_handler_after_response(handler: Any) -> Any:
    raw = getattr(handler, "after_response", None)
    if not _is_empty_sentinel(raw):
        return raw
    resolve_fn = getattr(handler, "resolve_after_response", None)
    if callable(resolve_fn):
        return resolve_fn()
    return None


def _rebuild_litestar_routing_trie(app: Any) -> None:
    asgi_router = getattr(app, "asgi_router", None)
    if asgi_router is None:
        return
    registered = getattr(asgi_router, "_registered_routes", None)
    if isinstance(registered, set):
        registered.clear()
    plain_routes = getattr(asgi_router, "_plain_routes", None)
    if isinstance(plain_routes, set):
        plain_routes.clear()
    mount_routes = getattr(asgi_router, "_mount_routes", None)
    if isinstance(mount_routes, dict):
        mount_routes.clear()

    try:
        trie_types = importlib.import_module("litestar._asgi.routing_trie.types")
        asgi_router.root_route_map_node = trie_types.create_node()
    except ImportError:
        pass

    if hasattr(asgi_router, "_trie_initialized"):
        asgi_router._trie_initialized = True

    construct_fn = getattr(asgi_router, "construct_routing_trie", None)
    if callable(construct_fn):
        construct_fn()
    create_handler = getattr(app, "_create_asgi_handler", None)
    if callable(create_handler):
        app.asgi_handler = create_handler()


def _extract_compression_config(app: Any) -> CompressionOffloadSpec | None:
    cfg = getattr(app, "compression_config", None)
    if cfg is None:
        state = getattr(app, "state", None)
        if isinstance(state, dict) or (state is not None and hasattr(state, "get")):
            cfg = state.get("_vertebra_offloaded_compression_config")
    if cfg is None:
        return None

    backend = str(getattr(cfg, "backend", "gzip") or "gzip").lower()
    minimum_size = int(getattr(cfg, "minimum_size", 500))
    gzip_level = int(getattr(cfg, "gzip_compress_level", 6))
    brotli_quality = int(getattr(cfg, "brotli_quality", 4))
    zstd_level = int(getattr(cfg, "zstd_level", 3))
    raw_exclude = getattr(cfg, "exclude", None)
    exclude_patterns: tuple[str, ...]
    if isinstance(raw_exclude, str):
        exclude_patterns = (raw_exclude,)
    elif isinstance(raw_exclude, (list, tuple, set)):
        exclude_patterns = tuple(str(x) for x in raw_exclude)
    else:
        exclude_patterns = ()
    exclude_opt_key = getattr(cfg, "exclude_opt_key", None)

    excluded_paths: list[str] = []
    if exclude_opt_key:
        for route in getattr(app, "routes", ()):
            route_path = getattr(route, "path", None)
            handler_map = getattr(route, "route_handler_map", None)
            if not isinstance(route_path, str) or not isinstance(handler_map, dict):
                continue
            for raw_entry in handler_map.values():
                handler, _ = _unpack_route_handler_map_entry(raw_entry)
                if _get_handler_opt(handler).get(exclude_opt_key):
                    excluded_paths.append(route_path)
                    break

    return CompressionOffloadSpec(
        backend=backend,
        minimum_size=minimum_size,
        gzip_compress_level=gzip_level,
        brotli_quality=brotli_quality,
        zstd_level=zstd_level,
        exclude_patterns=exclude_patterns,
        exclude_opt_key=exclude_opt_key,
        excluded_paths=tuple(sorted(set(excluded_paths))),
    )


def _extract_cors_config(app: Any) -> CorsOffloadSpec | None:
    cfg = getattr(app, "cors_config", None)
    if cfg is None:
        state = getattr(app, "state", None)
        if isinstance(state, dict) or (state is not None and hasattr(state, "get")):
            cfg = state.get("_vertebra_offloaded_cors_config")
    if cfg is None:
        return None

    return CorsOffloadSpec(
        allow_origins=tuple(str(x) for x in getattr(cfg, "allow_origins", ())),
        allow_methods=tuple(str(x).upper() for x in getattr(cfg, "allow_methods", ())),
        allow_headers=tuple(str(x) for x in getattr(cfg, "allow_headers", ())),
        allow_credentials=bool(getattr(cfg, "allow_credentials", False)),
        expose_headers=tuple(str(x) for x in getattr(cfg, "expose_headers", ())),
        max_age=int(getattr(cfg, "max_age", 600)),
        allow_origin_regex=getattr(cfg, "allow_origin_regex", None),
    )


def _is_default_local_filesystem(fs: Any) -> bool:
    if fs is None:
        return True
    cls_name = type(fs).__name__
    return cls_name in {"BaseLocalFileSystem", "LocalFileSystem"}


def _extract_closure_dict(fn: Any) -> dict[str, Any]:
    code = getattr(fn, "__code__", None)
    closure = getattr(fn, "__closure__", None)
    if code is None or closure is None:
        return {}
    freevars = getattr(code, "co_freevars", ())
    result: dict[str, Any] = {}
    for name, cell in zip(freevars, closure, strict=False):
        try:
            result[name] = cell.cell_contents
        except ValueError:
            continue
    return result


def _extract_static_mounts(app: Any) -> tuple[StaticMountOffloadSpec, ...]:
    mounts: list[StaticMountOffloadSpec] = []
    seen_prefixes: set[str] = set()

    v2_configs = getattr(app, "static_files_config", None) or ()
    if not isinstance(v2_configs, (list, tuple)):
        v2_configs = (v2_configs,)
    for cfg in v2_configs:
        path_prefix = getattr(cfg, "path", None)
        directories = getattr(cfg, "directories", None)
        fs = getattr(cfg, "file_system", None)
        if not path_prefix or not directories or not _is_default_local_filesystem(fs):
            continue
        if getattr(cfg, "guards", None) or getattr(cfg, "middleware", None):
            continue
        norm_prefix = "/" + str(path_prefix).strip("/")
        seen_prefixes.add(norm_prefix)
        mounts.append(
            StaticMountOffloadSpec(
                path_prefix=norm_prefix,
                directories=tuple(str(Path(d).resolve()) for d in directories),
                html_mode=bool(getattr(cfg, "html_mode", False)),
                send_as_attachment=bool(getattr(cfg, "send_as_attachment", False)),
                headers=dict(getattr(cfg, "headers", None) or {}),
            )
        )

    for route in getattr(app, "routes", ()):
        handler_map = getattr(route, "route_handler_map", None)
        if not isinstance(handler_map, dict):
            continue
        for raw_entry in handler_map.values():
            handler, _ = _unpack_route_handler_map_entry(raw_entry)
            if _get_handler_guards(handler) or _get_handler_middleware(handler):
                continue
            raw_fn = getattr(handler, "fn", None)
            fn = getattr(raw_fn, "value", raw_fn)
            qualname = getattr(fn, "__qualname__", "")
            if "create_static_files_router" not in qualname and "_handle" not in qualname:
                continue
            freevars = _extract_closure_dict(fn)
            resolved_dirs = freevars.get("resolved_directories") or freevars.get("directories")
            fs = freevars.get("file_system")
            if not resolved_dirs or not _is_default_local_filesystem(fs):
                continue
            route_path = str(getattr(route, "path", ""))
            prefix = route_path.split("{", 1)[0].rstrip("/") or "/"
            if prefix in seen_prefixes:
                continue
            seen_prefixes.add(prefix)
            mounts.append(
                StaticMountOffloadSpec(
                    path_prefix=prefix,
                    directories=tuple(str(Path(d).resolve()) for d in resolved_dirs),
                    html_mode=bool(freevars.get("html_mode", False)),
                    send_as_attachment=bool(freevars.get("send_as_attachment", False)),
                    headers=dict(freevars.get("headers") or {}),
                )
            )

    return tuple(mounts)


def _extract_openapi_schema_payload(app: Any) -> OpenApiOffloadSpec | None:
    openapi_cfg = getattr(app, "openapi_config", None)
    if openapi_cfg is None:
        return None
    try:
        schema_obj = getattr(app, "openapi_schema", None)
        if schema_obj is None:
            return None
        to_schema = getattr(schema_obj, "to_schema", None)
        raw_dict = to_schema() if callable(to_schema) else schema_obj
        body = encode_json_bytes(raw_dict)
    except Exception:
        return None

    base_path = str(getattr(openapi_cfg, "path", None) or "/schema").rstrip("/")
    schema_path = f"{base_path}/openapi.json"
    digest = hashlib.sha256(body).hexdigest()[:32]
    return OpenApiOffloadSpec(
        schema_path=schema_path,
        body=body,
        etag=f'"{digest}"',
    )
