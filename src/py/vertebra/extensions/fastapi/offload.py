"""FastAPI middleware, static mount, and OpenAPI JSON schema offload for Rust vertebra-http."""

from __future__ import annotations

import hashlib
from typing import Any

from vertebra.server._offload import (
    CompressionOffloadSpec,
    CorsOffloadSpec,
    OffloadPlan,
    OpenApiOffloadSpec,
    StaticMountOffloadSpec,
    evaluate_cors_preflight,
    evaluate_cors_response_headers,
    evaluate_openapi_cache_request,
    is_rust_offload_active,
)
from vertebra.utils.serializers import encode_json_bytes

__all__ = (
    "CompressionOffloadSpec",
    "CorsOffloadSpec",
    "OffloadPlan",
    "OpenApiOffloadSpec",
    "StaticMountOffloadSpec",
    "evaluate_cors_preflight",
    "evaluate_cors_response_headers",
    "evaluate_openapi_cache_request",
    "has_non_offloadable_middleware",
    "is_rust_offload_active",
    "offload_middlewares",
)

_OFFLOADABLE_MIDDLEWARE_NAMES: frozenset[str] = frozenset({"GZipMiddleware", "CORSMiddleware"})
_PLUGIN_MIDDLEWARE_NAMES: frozenset[str] = frozenset({"FastAPIOffloadMiddleware"})


def offload_middlewares(
    app: Any,
    *,
    config: Any = None,
    strip_python_middleware: bool = False,
) -> OffloadPlan:
    """Extract GZip, CORS, StaticFiles, and pre-rendered OpenAPI JSON from a FastAPI application."""
    enable_compression = True if config is None else bool(getattr(config, "offload_compression", True))
    enable_cors = True if config is None else bool(getattr(config, "offload_cors", True))
    enable_static = True if config is None else bool(getattr(config, "offload_static_files", True))
    enable_openapi = True if config is None else bool(getattr(config, "offload_openapi", True))

    user_middleware = list(getattr(app, "user_middleware", None) or ())
    compression_spec: CompressionOffloadSpec | None = None
    cors_spec: CorsOffloadSpec | None = None
    retained_middleware: list[Any] = []
    removed_any = False
    rust_active = is_rust_offload_active()

    for mw in user_middleware:
        cls = getattr(mw, "cls", None)
        cls_name = getattr(cls, "__name__", "")
        kwargs = dict(getattr(mw, "kwargs", None) or {})
        args = tuple(getattr(mw, "args", None) or ())

        if cls_name == "GZipMiddleware" and enable_compression and compression_spec is None:
            min_size = int(kwargs.get("minimum_size", args[0] if args else 500))
            compresslevel = int(kwargs.get("compresslevel", args[1] if len(args) > 1 else 9))
            compression_spec = CompressionOffloadSpec(
                backend="gzip",
                minimum_size=min_size,
                gzip_compress_level=compresslevel,
                brotli_quality=4,
                zstd_level=3,
                exclude_patterns=(),
                exclude_opt_key=None,
                excluded_paths=(),
            )
            if strip_python_middleware and rust_active:
                removed_any = True
                continue
        elif cls_name == "CORSMiddleware" and enable_cors and cors_spec is None:
            allow_origins = tuple(str(o) for o in kwargs.get("allow_origins", ()))
            allow_methods = tuple(str(m).upper() for m in kwargs.get("allow_methods", ("GET",)))
            allow_headers = tuple(str(h) for h in kwargs.get("allow_headers", ()))
            allow_credentials = bool(kwargs.get("allow_credentials", False))
            expose_headers = tuple(str(h) for h in kwargs.get("expose_headers", ()))
            max_age = int(kwargs.get("max_age", 600))
            allow_origin_regex = kwargs.get("allow_origin_regex")
            cors_spec = CorsOffloadSpec(
                allow_origins=allow_origins,
                allow_methods=allow_methods,
                allow_headers=allow_headers,
                allow_credentials=allow_credentials,
                expose_headers=expose_headers,
                max_age=max_age,
                allow_origin_regex=str(allow_origin_regex) if allow_origin_regex is not None else None,
            )
            if strip_python_middleware and rust_active:
                removed_any = True
                continue

        retained_middleware.append(mw)

    if removed_any and hasattr(app, "user_middleware"):
        app.user_middleware = retained_middleware
        build_stack = getattr(app, "build_middleware_stack", None)
        if callable(build_stack):
            app.middleware_stack = build_stack()

    static_mounts = _extract_fastapi_static_mounts(app) if enable_static else ()
    openapi_spec = _extract_fastapi_openapi_spec(app) if enable_openapi else None
    return OffloadPlan(
        compression=compression_spec,
        cors=cors_spec,
        static_mounts=static_mounts,
        openapi=openapi_spec,
    )


def has_non_offloadable_middleware(app: Any) -> bool:
    """Return True if the application registers custom Python middleware beyond offloaded GZip/CORS."""
    user_middleware = getattr(app, "user_middleware", None) or ()
    rust_active = is_rust_offload_active()
    for mw in user_middleware:
        cls = getattr(mw, "cls", None)
        cls_name = getattr(cls, "__name__", "")
        if cls_name in _PLUGIN_MIDDLEWARE_NAMES:
            continue
        if not (rust_active and cls_name in _OFFLOADABLE_MIDDLEWARE_NAMES):
            return True
    return False


def _extract_fastapi_static_mounts(app: Any) -> tuple[StaticMountOffloadSpec, ...]:
    mounts: list[StaticMountOffloadSpec] = []
    routes = getattr(app, "routes", None) or ()
    for route in routes:
        if type(route).__name__ != "Mount":
            continue
        mounted_app = getattr(route, "app", None)
        if mounted_app is None or type(mounted_app).__name__ != "StaticFiles":
            continue
        path_prefix = str(getattr(route, "path", "") or "/")
        directories: list[str] = []
        raw_dir = getattr(mounted_app, "directory", None)
        if raw_dir is not None:
            directories.append(str(raw_dir))
        for extra_dir in getattr(mounted_app, "all_directories", ()) or ():
            dir_str = str(extra_dir)
            if dir_str not in directories:
                directories.append(dir_str)
        if directories:
            mounts.append(
                StaticMountOffloadSpec(
                    path_prefix=path_prefix,
                    directories=tuple(directories),
                    html_mode=bool(getattr(mounted_app, "html", False)),
                    send_as_attachment=False,
                    headers={},
                )
            )
    return tuple(mounts)


def _extract_fastapi_openapi_spec(app: Any) -> OpenApiOffloadSpec | None:
    openapi_url = getattr(app, "openapi_url", None)
    openapi_fn = getattr(app, "openapi", None)
    if not isinstance(openapi_url, str) or not openapi_url or not callable(openapi_fn):
        return None
    try:
        schema_dict = openapi_fn()
    except Exception:
        return None
    if not isinstance(schema_dict, dict):
        return None
    body_bytes = bytes(encode_json_bytes(schema_dict))
    digest = hashlib.sha256(body_bytes).hexdigest()[:16]
    return OpenApiOffloadSpec(
        schema_path=openapi_url,
        body=body_bytes,
        etag=f'"{digest}"',
        content_type="application/json",
    )
