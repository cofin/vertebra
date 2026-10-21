"""Re-export shim for vertebra.extensions.litestar.fastpath."""

from vertebra.extensions.litestar.fastpath import (
    ASGIResponse,
    CompiledLitestarRoute,
    ScopeState,
    compile_litestar_fastpath_routes,
    create_fused_handler,
    dispatch_litestar_handler_direct,
    preseed_litestar_scope_body,
)

__all__ = (
    "ASGIResponse",
    "CompiledLitestarRoute",
    "ScopeState",
    "compile_litestar_fastpath_routes",
    "create_fused_handler",
    "dispatch_litestar_handler_direct",
    "preseed_litestar_scope_body",
)
