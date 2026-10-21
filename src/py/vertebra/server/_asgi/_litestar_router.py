"""Re-export shim for vertebra.extensions.litestar.router."""

from vertebra.extensions.litestar.router import (
    ExtractedLitestarRoute,
    LitestarRouteCompiler,
    LitestarRouterTable,
    _unpack_route_handler_map_entry,
    compile_litestar_routes,
    convert_litestar_path_params,
    extract_litestar_route_entries,
    extract_litestar_routes,
    normalize_litestar_path_for_matchit,
)

__all__ = (
    "ExtractedLitestarRoute",
    "LitestarRouteCompiler",
    "LitestarRouterTable",
    "_unpack_route_handler_map_entry",
    "compile_litestar_routes",
    "convert_litestar_path_params",
    "extract_litestar_route_entries",
    "extract_litestar_routes",
    "normalize_litestar_path_for_matchit",
)
