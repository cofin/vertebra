"""Vertebra Litestar extension package."""

from vertebra.extensions.litestar.channels import (
    ChannelsBackend,
    swap_litestar_channels_backend,
)
from vertebra.extensions.litestar.config import (
    LitestarConfig,
)
from vertebra.extensions.litestar.ecosystem import (
    LitestarEcosystemBridge,
    attach_litestar_ecosystem_bridges,
)
from vertebra.extensions.litestar.fastpath import (
    CompiledLitestarRoute,
    compile_litestar_fastpath_routes,
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
    evaluate_cors_preflight,
    evaluate_cors_response_headers,
    evaluate_openapi_cache_request,
    offload_litestar_middlewares,
)
from vertebra.extensions.litestar.plugin import (
    LitestarPlugin,
    enter_litestar_server_lifespan,
)
from vertebra.extensions.litestar.router import (
    ExtractedLitestarRoute,
    LitestarRouteCompiler,
    LitestarRouterTable,
    convert_litestar_path_params,
    extract_litestar_route_entries,
    extract_litestar_routes,
    normalize_litestar_path_for_matchit,
)
from vertebra.extensions.litestar.store import (
    MemoryStore,
    swap_litestar_response_cache_store,
)

__all__ = (
    "ChannelsBackend",
    "CompiledLitestarRoute",
    "CompressionOffloadSpec",
    "CorsOffloadSpec",
    "ExtractedLitestarRoute",
    "LitestarConfig",
    "LitestarEcosystemBridge",
    "LitestarOffloadPlan",
    "LitestarPlugin",
    "LitestarRouteCompiler",
    "LitestarRouterTable",
    "MemoryStore",
    "MiddlewareOffloadCompiler",
    "OpenApiOffloadSpec",
    "StaticMountOffloadSpec",
    "attach_litestar_ecosystem_bridges",
    "compile_litestar_fastpath_routes",
    "convert_litestar_path_params",
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
