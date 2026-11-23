"""FastAPI middleware, static mount, and OpenAPI JSON schema offload for Rust vertebra-http."""

from __future__ import annotations

from typing import Any

from vertebra.extensions._web.asgi_starlette import (
    has_non_offloadable_asgi_middleware,
    offload_asgi_middlewares,
)
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

_PLUGIN_MIDDLEWARE_NAMES: frozenset[str] = frozenset({"FastAPIOffloadMiddleware"})


def offload_middlewares(
    app: Any,
    *,
    config: Any = None,
    strip_python_middleware: bool = False,
) -> OffloadPlan:
    """Extract GZip, CORS, StaticFiles, and pre-rendered OpenAPI JSON from a FastAPI application."""
    return offload_asgi_middlewares(
        app,
        config=config,
        strip_python_middleware=strip_python_middleware,
        internal_middleware_names=_PLUGIN_MIDDLEWARE_NAMES,
        check_state_openapi=False,
    )


def has_non_offloadable_middleware(app: Any) -> bool:
    """Return True if the application registers custom Python middleware beyond offloaded GZip/CORS."""
    return has_non_offloadable_asgi_middleware(app, internal_middleware_names=_PLUGIN_MIDDLEWARE_NAMES)
