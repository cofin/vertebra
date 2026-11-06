"""Factory helper creating LitestarMCP plugin configured with Vertebra accelerations."""

from __future__ import annotations

from typing import Any

from litestar import get

from vertebra.agent.mcp.models import MCPConfig as VertebraMCPConfig
from vertebra.agent.mcp.routes import build_well_known_mcp_document
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
    registry: Any | None = None,
    expose_well_known: bool = True,
    well_known_path: str = "/.well-known/mcp.json",
    **kwargs: Any,
) -> LitestarMCP:
    """Create a LitestarMCP plugin instance pre-configured with Vertebra accelerations."""
    ensure_module(
        "litestar_mcp",
        package="litestar-mcp is required for litestar mcp integration",
        extra="mcp",
    )
    if config is not None:
        resolved_config = config
    else:
        tasks_cfg = create_shm_mcp_task_config() if enable_tasks else False
        resolved_config = MCPConfig(
            name=name,
            tasks=tasks_cfg,
            **kwargs,
        )

    plugin = LitestarMCP(resolved_config)
    if expose_well_known:
        server_name = getattr(resolved_config, "name", None) or name
        server_version = getattr(resolved_config, "version", None) or "1.0.0"
        endpoint_path = getattr(resolved_config, "base_path", None) or "/mcp"

        @get(well_known_path, include_in_schema=False, opt={"exclude_from_auth": True})
        async def _vertebra_mcp_well_known() -> dict[str, Any]:
            discovered_names = list(plugin.discovered_tools.keys())
            if registry is not None and hasattr(registry, "list_specs"):
                for spec in registry.list_specs(exposure="mcp"):
                    if spec.name not in discovered_names:
                        discovered_names.append(spec.name)
            return build_well_known_mcp_document(
                VertebraMCPConfig(
                    server_name=server_name,
                    server_version=server_version,
                    path=endpoint_path,
                    well_known_path=well_known_path,
                ),
                tool_names=discovered_names,
            )

        plugin.register_dynamic_handler(_vertebra_mcp_well_known)

    return plugin
