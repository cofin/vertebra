"""Factory helper creating LitestarMCP plugin configured with Vertebra accelerations."""

from __future__ import annotations

import inspect
from typing import Annotated, Any

from litestar import get
from litestar.params import Dependency

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
        install_package="litestar-mcp>=0.20.0",
        extra="mcp",
    ) from exc

__all__ = ["create_mcp_plugin"]


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
    litestar_mcp_mod = ensure_module("litestar_mcp", package="litestar-mcp", extra="mcp")
    supported = getattr(litestar_mcp_mod, "SUPPORTED_PROTOCOL_VERSIONS", ())
    if "2025-11-25" not in supported:
        raise MissingDependencyError(
            "litestar-mcp",
            install_package="litestar-mcp>=0.20.0",
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
        async def _vertebra_mcp_well_known(
            skill_catalog: Annotated[Any, Dependency(default=None, skip_validation=True)] = None,
        ) -> dict[str, Any]:
            discovered_names = list(plugin.discovered_tools.keys())
            if registry is not None and hasattr(registry, "list_specs"):
                for spec in registry.list_specs(exposure="mcp"):
                    if spec.name not in discovered_names:
                        discovered_names.append(spec.name)
            skill_names: list[str] = []
            if skill_catalog is not None and hasattr(skill_catalog, "list_skills"):
                raw_skills = skill_catalog.list_skills()
                if inspect.isawaitable(raw_skills):
                    raw_skills = await raw_skills
                skill_names = [str(getattr(skill, "name", skill)) for skill in raw_skills]
            return build_well_known_mcp_document(
                VertebraMCPConfig(
                    server_name=server_name,
                    server_version=server_version,
                    path=endpoint_path,
                    well_known_path=well_known_path,
                ),
                tool_names=discovered_names,
                skill_names=skill_names,
            )

        plugin.register_dynamic_handler(_vertebra_mcp_well_known)

    return plugin
