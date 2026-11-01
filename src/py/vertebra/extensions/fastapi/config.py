"""Configuration for FastAPIPlugin integration."""

from __future__ import annotations

from typing import Any

import msgspec

__all__ = ("FastAPIConfig",)


class FastAPIConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration controlling FastAPI Rust accelerations, dependency compilation, and docs portal."""

    offload_compression: bool = True
    offload_cors: bool = True
    offload_static_files: bool = True
    offload_openapi: bool = True
    enable_fastpath_bypass: bool = True
    enable_compiled_dependencies: bool = True
    use_msgspec_json_response: bool = True
    enable_health_endpoint: bool = False
    health_path: str = "/healthz"
    openapi_config: Any | None = None
    vite_config: Any | None = None
    webmcp_config: Any | None = None
