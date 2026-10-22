"""Vertebra Litestar extension package."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.extensions.litestar.channels import ChannelBus, swap_litestar_channels_backend
    from vertebra.extensions.litestar.config import LitestarConfig
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
        ResponseCacheStore,
        swap_litestar_response_cache_store,
    )

__all__ = (
    "ChannelBus",
    "CompiledLitestarRoute",
    "CompressionOffloadSpec",
    "CorsOffloadSpec",
    "ExtractedLitestarRoute",
    "LitestarConfig",
    "LitestarOffloadPlan",
    "LitestarPlugin",
    "LitestarRouteCompiler",
    "LitestarRouterTable",
    "OpenApiOffloadSpec",
    "ResponseCacheStore",
    "StaticMountOffloadSpec",
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

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "ChannelBus": ("vertebra.extensions.litestar.channels", "ChannelBus"),
    "ChannelsBackend": ("vertebra.extensions.litestar.channels", "ChannelsBackend"),
    "swap_litestar_channels_backend": ("vertebra.extensions.litestar.channels", "swap_litestar_channels_backend"),
    "LitestarConfig": ("vertebra.extensions.litestar.config", "LitestarConfig"),
    "CompiledLitestarRoute": ("vertebra.extensions.litestar.fastpath", "CompiledLitestarRoute"),
    "compile_litestar_fastpath_routes": ("vertebra.extensions.litestar.fastpath", "compile_litestar_fastpath_routes"),
    "dispatch_litestar_handler_direct": ("vertebra.extensions.litestar.fastpath", "dispatch_litestar_handler_direct"),
    "preseed_litestar_scope_body": ("vertebra.extensions.litestar.fastpath", "preseed_litestar_scope_body"),
    "CompressionOffloadSpec": ("vertebra.extensions.litestar.offload", "CompressionOffloadSpec"),
    "CorsOffloadSpec": ("vertebra.extensions.litestar.offload", "CorsOffloadSpec"),
    "LitestarOffloadPlan": ("vertebra.extensions.litestar.offload", "LitestarOffloadPlan"),
    "OpenApiOffloadSpec": ("vertebra.extensions.litestar.offload", "OpenApiOffloadSpec"),
    "StaticMountOffloadSpec": ("vertebra.extensions.litestar.offload", "StaticMountOffloadSpec"),
    "evaluate_cors_preflight": ("vertebra.extensions.litestar.offload", "evaluate_cors_preflight"),
    "evaluate_cors_response_headers": ("vertebra.extensions.litestar.offload", "evaluate_cors_response_headers"),
    "evaluate_openapi_cache_request": ("vertebra.extensions.litestar.offload", "evaluate_openapi_cache_request"),
    "offload_litestar_middlewares": ("vertebra.extensions.litestar.offload", "offload_litestar_middlewares"),
    "LitestarPlugin": ("vertebra.extensions.litestar.plugin", "LitestarPlugin"),
    "enter_litestar_server_lifespan": ("vertebra.extensions.litestar.plugin", "enter_litestar_server_lifespan"),
    "ExtractedLitestarRoute": ("vertebra.extensions.litestar.router", "ExtractedLitestarRoute"),
    "LitestarRouteCompiler": ("vertebra.extensions.litestar.router", "LitestarRouteCompiler"),
    "LitestarRouterTable": ("vertebra.extensions.litestar.router", "LitestarRouterTable"),
    "convert_litestar_path_params": ("vertebra.extensions.litestar.router", "convert_litestar_path_params"),
    "extract_litestar_route_entries": ("vertebra.extensions.litestar.router", "extract_litestar_route_entries"),
    "extract_litestar_routes": ("vertebra.extensions.litestar.router", "extract_litestar_routes"),
    "normalize_litestar_path_for_matchit": (
        "vertebra.extensions.litestar.router",
        "normalize_litestar_path_for_matchit",
    ),
    "ResponseCacheStore": ("vertebra.extensions.litestar.store", "ResponseCacheStore"),
    "MemoryStore": ("vertebra.extensions.litestar.store", "MemoryStore"),
    "swap_litestar_response_cache_store": ("vertebra.extensions.litestar.store", "swap_litestar_response_cache_store"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
