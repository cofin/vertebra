"""Configuration for LitestarPlugin integration."""

import dataclasses

__all__ = (
    "LitestarConfig",
    "ServerConfig",
    "VertebraConfig",
    "VertebraServerConfig",
)


@dataclasses.dataclass(slots=True, frozen=True)
class LitestarConfig:
    """Configuration controlling zero-code-change Litestar accelerations and runtime hooks."""

    offload_compression: bool = True
    offload_cors: bool = True
    offload_static_files: bool = True
    offload_openapi: bool = True
    swap_memory_channels: bool = True
    swap_memory_response_cache: bool = True
    enable_direct_handler_fastpath: bool = True
    enable_health_endpoint: bool = False
    health_path: str = "/healthz"


ServerConfig = LitestarConfig
VertebraConfig = LitestarConfig
VertebraServerConfig = LitestarConfig
