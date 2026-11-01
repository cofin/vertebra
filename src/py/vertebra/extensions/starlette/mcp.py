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
from vertebra.agent.mcp.registry import MCPRegistry
from vertebra.agent.mcp.webmcp import (
    WebMCPConfig,
    WebMCPManifest,
    WebMCPToolDescriptor,
    build_webmcp_manifest,
    render_webmcp_manifest_json,
)
from vertebra.utils.module_loader import import_optional

__all__ = (
    "MCPRegistry",
    "PromptController",
    "SkillController",
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
    registry: MCPRegistry | None = None,
    webmcp_config: WebMCPConfig | None = None,
) -> MCPRegistry:
    """Discover @mcp_tool/@mcp_resource/@mcp_prompt on ``app`` routes and mount WebMCP manifest endpoint."""
    resolved_registry = registry if registry is not None else MCPRegistry()
    resolved_registry.discover_app_routes(app)

    state = getattr(app, "state", None)
    if state is not None:
        if isinstance(state, dict):
            state["mcp_registry"] = resolved_registry
        else:
            state.mcp_registry = resolved_registry

    if webmcp_config is not None and webmcp_config.enabled:
        manifest_bytes = render_webmcp_manifest_json(resolved_registry, config=webmcp_config)
        routes = getattr(app, "routes", None)
        if isinstance(routes, list):
            routing_mod = import_optional("starlette.routing")
            responses_mod = import_optional("starlette.responses")

            async def _serve_webmcp(_request: Any) -> Any:
                if responses_mod is not None and hasattr(responses_mod, "Response"):
                    return responses_mod.Response(content=manifest_bytes, media_type="application/json")
                return manifest_bytes

            if routing_mod is not None and hasattr(routing_mod, "Route"):
                routes.append(
                    routing_mod.Route(
                        webmcp_config.manifest_path,
                        endpoint=_serve_webmcp,
                        methods=["GET"],
                        include_in_schema=False,
                    )
                )
            else:
                routes.append(
                    SimpleNamespace(
                        path=webmcp_config.manifest_path,
                        endpoint=_serve_webmcp,
                        methods=["GET"],
                        name="_vertebra_webmcp_manifest",
                    )
                )
    return resolved_registry
