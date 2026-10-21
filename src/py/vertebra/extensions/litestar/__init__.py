"""Vertebra Litestar extension package."""

from vertebra.extensions.litestar.channels import (
    VertebraChannelsBackend,
    swap_litestar_channels_backend,
)
from vertebra.extensions.litestar.config import VertebraConfig
from vertebra.extensions.litestar.fastpath import (
    CompiledLitestarRoute,
    compile_litestar_fastpath_routes,
    create_fused_handler,
    dispatch_litestar_handler_direct,
    preseed_litestar_scope_body,
)
from vertebra.extensions.litestar.offload import (
    CompressionOffloadSpec,
    CorsOffloadSpec,
    LitestarOffloadPlan,
    MiddlewareOffloadCompiler,
    OpenApiOffloadSpec,
    StaticMountOffloadSpec,
    compile_middleware_offload,
    evaluate_cors_preflight,
    evaluate_cors_response_headers,
    evaluate_openapi_cache_request,
    offload_litestar_middlewares,
)
from vertebra.extensions.litestar.plugin import (
    VertebraPlugin,
    enter_litestar_server_lifespan,
)
from vertebra.extensions.litestar.router import (
    ExtractedLitestarRoute,
    LitestarRouteCompiler,
    LitestarRouterTable,
    compile_litestar_routes,
    convert_litestar_path_params,
    extract_litestar_route_entries,
    extract_litestar_routes,
    normalize_litestar_path_for_matchit,
)
from vertebra.extensions.litestar.store import (
    VertebraMemoryStore,
    swap_litestar_response_cache_store,
)

__all__ = (
    "CompiledLitestarRoute",
    "CompressionOffloadSpec",
    "CorsOffloadSpec",
    "ExtractedLitestarRoute",
    "LitestarOffloadPlan",
    "LitestarRouteCompiler",
    "LitestarRouterTable",
    "MiddlewareOffloadCompiler",
    "OpenApiOffloadSpec",
    "StaticMountOffloadSpec",
    "VertebraChannelsBackend",
    "VertebraConfig",
    "VertebraMemoryStore",
    "VertebraPlugin",
    "compile_litestar_fastpath_routes",
    "compile_litestar_routes",
    "compile_middleware_offload",
    "convert_litestar_path_params",
    "create_fused_handler",
    "dispatch_litestar_handler_direct",
    "enter_litestar_server_lifespan",
    "evaluate_cors_preflight",
    "evaluate_cors_response_headers",
    "evaluate_openapi_cache_request",
    "extract_litestar_route_entries",
    "extract_litestar_routes",
    "normalize_litestar_path_for_matchit",
    "offload_litestar_middlewares",
    "preseed_litestar_scope_body",
    "swap_litestar_channels_backend",
    "swap_litestar_response_cache_store",
)
