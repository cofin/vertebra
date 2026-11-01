"""Configuration for StarlettePlugin integration."""

from __future__ import annotations

from typing import Any

import msgspec

__all__ = ("StarletteConfig",)


class StarletteConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration controlling Starlette Rust accelerations, fastpath dispatch, and ecosystem bridges."""

    offload_compression: bool = True
    offload_cors: bool = True
    offload_static_files: bool = True
    enable_fastpath_bypass: bool = True
    enable_direct_endpoint_fastpath: bool = True
    enable_health_endpoint: bool = False
    health_path: str = "/healthz"
    openapi_config: Any | None = None
    vite_config: Any | None = None
    webmcp_config: Any | None = None
