"""Factory helper creating LitestarMCP plugin configured with Vertebra accelerations."""

from __future__ import annotations

from typing import Any

from vertebra.extensions.litestar.mcp.tasks import create_shm_mcp_task_config
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

try:
    from litestar_mcp import LitestarMCP, MCPConfig
except ImportError as exc:
    raise MissingDependencyError(
        "litestar-mcp",
        install_package="litestar-mcp",
        extra="mcp",
    ) from exc


def create_mcp_plugin(
    config: MCPConfig | None = None,
    *,
    name: str = "Vertebra MCP API",
    enable_tasks: bool = True,
    **kwargs: Any,
) -> LitestarMCP:
    """Create a LitestarMCP plugin instance pre-configured with Vertebra accelerations."""
    ensure_module(
        "litestar_mcp",
        package="litestar-mcp is required for litestar mcp integration",
        extra="mcp",
    )
    if config is not None:
        return LitestarMCP(config)

    tasks_cfg = create_shm_mcp_task_config() if enable_tasks else False
    resolved_config = MCPConfig(
        name=name,
        tasks=tasks_cfg,
        **kwargs,
    )
    return LitestarMCP(resolved_config)
