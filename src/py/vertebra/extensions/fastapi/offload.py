"""FastAPI middleware, static mount, and OpenAPI JSON schema offload for Rust vertebra-http."""

from __future__ import annotations

import hashlib
from typing import Any

from vertebra.extensions.litestar.offload import (
    CompressionOffloadSpec,
    CorsOffloadSpec,
    OffloadPlan,
    OpenApiOffloadSpec,
    StaticMountOffloadSpec,
    evaluate_cors_preflight,
    evaluate_cors_response_headers,
    evaluate_openapi_cache_request,
)
from vertebra.extensions.starlette.offload import (
    has_non_offloadable_middleware,
)
from vertebra.extensions.starlette.offload import (
    offload_middlewares as _offload_starlette_middlewares,
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
    "offload_middlewares",
)


def offload_middlewares(
    app: Any,
    *,
    config: Any = None,
    strip_python_middleware: bool = False,
) -> OffloadPlan:
    """Extract GZip, CORS, StaticFiles, and pre-rendered OpenAPI JSON from a FastAPI application."""
    base_plan = _offload_starlette_middlewares(
        app,
        config=config,
        strip_python_middleware=strip_python_middleware,
    )
    enable_openapi = True if config is None else bool(getattr(config, "offload_openapi", True))
    openapi_spec = _extract_fastapi_openapi_spec(app) if enable_openapi else None
    return OffloadPlan(
        compression=base_plan.compression,
        cors=base_plan.cors,
        static_mounts=base_plan.static_mounts,
        openapi=openapi_spec,
    )


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
