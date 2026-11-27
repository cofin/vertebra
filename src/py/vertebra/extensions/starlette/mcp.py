"""Route-annotation/controller-driven MCP and browser WebMCP integration for Starlette and FastAPI."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.agent.mcp.controllers import (
    PromptController,
    SkillController,
    mcp_prompt,
    mcp_resource,
    mcp_tool,
)
from vertebra.agent.mcp.models import MCPConfig
from vertebra.agent.mcp.registry import ToolRegistry
from vertebra.agent.mcp.routes import AsgiMCPApp, render_well_known_mcp_json
from vertebra.agent.mcp.service import MCPCore
from vertebra.agent.mcp.skills import MCPSkillsConfig, SkillCatalog
from vertebra.agent.mcp.webmcp import (
    WebMCPConfig,
    WebMCPManifest,
    WebMCPToolDescriptor,
    build_webmcp_manifest,
    render_webmcp_manifest_json,
)
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    import starlette.responses as _starlette_responses
    import starlette.routing as _starlette_routing

    _ = (_starlette_responses, _starlette_routing)

__all__ = (
    "AsgiMCPApp",
    "MCPConfig",
    "MCPCore",
    "ToolRegistry",
    "PromptController",
    "SkillController",
    "ToolRegistry",
    "WebMCPConfig",
    "WebMCPManifest",
    "WebMCPToolDescriptor",
    "build_webmcp_manifest",
    "mcp_prompt",
    "mcp_resource",
    "mcp_tool",
    "mount_mcp_routes",
    "render_webmcp_manifest_json",
)


def mount_mcp_routes(
    app: Any,
    *,
    registry: ToolRegistry | None = None,
    config: MCPConfig | None = None,
    webmcp_config: WebMCPConfig | None = None,
    skills: MCPSkillsConfig | SkillCatalog | None = None,
) -> ToolRegistry:
    """Discover @mcp_tool/@mcp_resource/@mcp_prompt on ``app`` routes and mount shared AsgiMCPApp."""
    resolved_config = config or MCPConfig(webmcp=webmcp_config)
    resolved_webmcp = webmcp_config or (
        resolved_config.webmcp if isinstance(resolved_config.webmcp, WebMCPConfig) else None
    )
    resolved_registry = registry if isinstance(registry, ToolRegistry) else ToolRegistry()
    resolved_registry.discover_app_routes(app)

    resolved_skills = SkillCatalog.coerce(skills)
    core = MCPCore(config=resolved_config, registry=resolved_registry, skill_catalog=resolved_skills)
    core.validate()
    mcp_app = AsgiMCPApp(core)

    state = getattr(app, "state", None)
    if state is not None:
        if isinstance(state, dict):
            state["mcp_registry"] = resolved_registry
            state["mcp_core"] = core
            state["mcp_app"] = mcp_app
        else:
            state.mcp_registry = resolved_registry
            state.mcp_core = core
            state.mcp_app = mcp_app

    routes = getattr(app, "routes", None)
    if isinstance(routes, list):
        routing_mod = ensure_module("starlette.routing", package="starlette", extra="starlette")
        responses_mod = ensure_module("starlette.responses", package="starlette", extra="starlette")
        existing_paths = {getattr(r, "path", None) for r in routes}

        if resolved_config.path not in existing_paths:
            routes.append(
                routing_mod.Route(
                    resolved_config.path,
                    endpoint=mcp_app.handle_starlette_request,
                    methods=["POST", "GET", "OPTIONS"],
                    include_in_schema=False,
                )
            )

        if resolved_config.expose_well_known and resolved_config.well_known_path not in existing_paths:
            skill_names = [s.name for s in resolved_skills.list_skills()] if resolved_skills is not None else None
            well_known_bytes = render_well_known_mcp_json(
                resolved_config,
                resolved_registry,
                skill_names=skill_names,
            )

            async def _serve_well_known(_request: Any) -> Any:
                return responses_mod.Response(content=well_known_bytes, media_type="application/json")

            routes.append(
                routing_mod.Route(
                    resolved_config.well_known_path,
                    endpoint=_serve_well_known,
                    methods=["GET"],
                    include_in_schema=False,
                )
            )

        if (
            resolved_webmcp is not None
            and resolved_webmcp.enabled
            and resolved_webmcp.manifest_path not in existing_paths
        ):
            manifest_bytes = render_webmcp_manifest_json(resolved_registry, config=resolved_webmcp)

            async def _serve_webmcp(_request: Any) -> Any:
                return responses_mod.Response(content=manifest_bytes, media_type="application/json")

            routes.append(
                routing_mod.Route(
                    resolved_webmcp.manifest_path,
                    endpoint=_serve_webmcp,
                    methods=["GET"],
                    include_in_schema=False,
                )
            )
    return resolved_registry
