"""Route-annotation/controller-driven MCP and browser WebMCP integration for Starlette and FastAPI."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from vertebra.agent.mcp.controllers import (
    PromptController,
    SkillController,
    mcp_prompt,
    mcp_resource,
    mcp_tool,
)
from vertebra.agent.mcp.models import MCPConfig
from vertebra.agent.mcp.registry import MCPRegistry, ToolRegistry
from vertebra.agent.mcp.routes import AsgiMcpApp, render_well_known_mcp_json
from vertebra.agent.mcp.service import MCPCore
from vertebra.agent.mcp.skills import MCPSkillsConfig, SkillCatalog
from vertebra.agent.mcp.webmcp import (
    WebMCPConfig,
    WebMCPManifest,
    WebMCPToolDescriptor,
    build_webmcp_manifest,
    render_webmcp_manifest_json,
)
from vertebra.utils.module_loader import import_optional

__all__ = (
    "AsgiMcpApp",
    "MCPConfig",
    "MCPCore",
    "MCPRegistry",
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
) -> MCPRegistry:
    """Discover @mcp_tool/@mcp_resource/@mcp_prompt on ``app`` routes and mount shared AsgiMcpApp."""
    resolved_config = config or MCPConfig(webmcp=webmcp_config)
    resolved_webmcp = webmcp_config or (
        resolved_config.webmcp if isinstance(resolved_config.webmcp, WebMCPConfig) else None
    )
    resolved_registry = registry if isinstance(registry, MCPRegistry) else MCPRegistry()
    resolved_registry.discover_app_routes(app)

    resolved_skills = SkillCatalog.coerce(skills)
    core = MCPCore(config=resolved_config, registry=resolved_registry, skill_catalog=resolved_skills)
    core.validate()
    mcp_app = AsgiMcpApp(core)

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
        routing_mod = import_optional("starlette.routing")
        responses_mod = import_optional("starlette.responses")
        existing_paths = {getattr(r, "path", None) for r in routes}

        if resolved_config.path not in existing_paths:
            if routing_mod is not None and hasattr(routing_mod, "Route"):
                routes.append(
                    routing_mod.Route(
                        resolved_config.path,
                        endpoint=mcp_app.handle_starlette_request,
                        methods=["POST", "GET", "OPTIONS"],
                        include_in_schema=False,
                    )
                )
            else:
                routes.append(
                    SimpleNamespace(
                        path=resolved_config.path,
                        endpoint=mcp_app.handle_starlette_request,
                        methods=["POST", "GET", "OPTIONS"],
                        name="_vertebra_mcp_endpoint",
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
                if responses_mod is not None and hasattr(responses_mod, "Response"):
                    return responses_mod.Response(content=well_known_bytes, media_type="application/json")
                return well_known_bytes

            if routing_mod is not None and hasattr(routing_mod, "Route"):
                routes.append(
                    routing_mod.Route(
                        resolved_config.well_known_path,
                        endpoint=_serve_well_known,
                        methods=["GET"],
                        include_in_schema=False,
                    )
                )
            else:
                routes.append(
                    SimpleNamespace(
                        path=resolved_config.well_known_path,
                        endpoint=_serve_well_known,
                        methods=["GET"],
                        name="_vertebra_mcp_well_known",
                    )
                )

        if (
            resolved_webmcp is not None
            and resolved_webmcp.enabled
            and resolved_webmcp.manifest_path not in existing_paths
        ):
            manifest_bytes = render_webmcp_manifest_json(resolved_registry, config=resolved_webmcp)

            async def _serve_webmcp(_request: Any) -> Any:
                if responses_mod is not None and hasattr(responses_mod, "Response"):
                    return responses_mod.Response(content=manifest_bytes, media_type="application/json")
                return manifest_bytes

            if routing_mod is not None and hasattr(routing_mod, "Route"):
                routes.append(
                    routing_mod.Route(
                        resolved_webmcp.manifest_path,
                        endpoint=_serve_webmcp,
                        methods=["GET"],
                        include_in_schema=False,
                    )
                )
            else:
                routes.append(
                    SimpleNamespace(
                        path=resolved_webmcp.manifest_path,
                        endpoint=_serve_webmcp,
                        methods=["GET"],
                        name="_vertebra_webmcp_manifest",
                    )
                )
    return resolved_registry
