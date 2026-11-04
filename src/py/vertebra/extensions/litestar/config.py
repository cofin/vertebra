"""Configuration for VertebraPlugin integration in Litestar."""

from typing import Any

import msgspec

__all__ = ("VertebraConfig",)


class VertebraConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration controlling zero-code-change Litestar accelerations and runtime hooks."""

    offload_compression: bool = True
    """Compress responses in the native server instead of Litestar middleware."""
    offload_cors: bool = True
    """Answer CORS in the native server instead of Litestar middleware."""
    offload_static_files: bool = True
    """Serve static file routes from the native server."""
    offload_openapi: bool = True
    """Serve the generated OpenAPI schema from the native server."""
    swap_memory_channels: bool = False
    """Replace Litestar's `MemoryChannelsBackend` with Vertebra's `ChannelsBackend` during finalization. Off by default."""
    swap_memory_response_cache: bool = False
    """Replace the response cache `MemoryStore` with Vertebra's `MemoryStore` during finalization. Off by default."""
    enable_direct_handler_fastpath: bool = True
    """Compile Litestar route handlers into the direct dispatch fastpath."""
    enable_fastpath_bypass: bool = True
    """Route eligible requests around the Litestar ASGI stack."""
    enable_health_endpoint: bool = False
    """Register a native health check route."""
    health_path: str = "/healthz"
    """Path served by the health check route."""
    mcp_config: Any | None = None
    """Optional MCP configuration for the agent integration."""
    a2a_config: Any | None = None
    """Optional A2A configuration for the agent integration."""
    agent_config: Any | None = None
    """Optional agent harness configuration."""
