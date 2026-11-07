"""Route-annotation/controller-driven MCP and browser WebMCP integration for Django."""

from __future__ import annotations

from typing import Any, cast

from vertebra._vertebra import Request, Response
from vertebra.agent.mcp.controllers import (
    PromptController,
    SkillController,
    mcp_prompt,
    mcp_resource,
    mcp_tool,
)
from vertebra.agent.mcp.models import MCPConfig
from vertebra.agent.mcp.registry import MCPRegistry, ToolRegistry
from vertebra.agent.mcp.routes import WsgiMcpApp, render_well_known_mcp_json
from vertebra.agent.mcp.service import MCPCore
from vertebra.agent.mcp.webmcp import (
    WebMCPConfig,
    WebMCPManifest,
    WebMCPToolDescriptor,
    build_webmcp_manifest,
    render_webmcp_manifest_json,
)
from vertebra.extensions.django.router import RouteCompiler

__all__ = (
    "MCPConfig",
    "MCPCore",
    "MCPRegistry",
    "PromptController",
    "SkillController",
    "ToolRegistry",
    "WebMCPConfig",
    "WebMCPManifest",
    "WebMCPToolDescriptor",
    "WsgiMcpApp",
    "attach_mcp_routes",
    "build_webmcp_manifest",
    "dispatch_mcp_request",
    "mcp_prompt",
    "mcp_resource",
    "mcp_tool",
    "render_webmcp_manifest_json",
)


def dispatch_mcp_request(
    registry: ToolRegistry,
    request: Request | bytes,
    config: MCPConfig | None = None,
) -> Response:
    """Execute an MCP HTTP request via shared ``WsgiMcpApp`` over ``MCPCore``."""
    resolved = config or MCPConfig(server_name="vertebra-django-mcp")
    cached_app = getattr(registry, "_mcp_app", None)
    if not isinstance(cached_app, WsgiMcpApp) or cached_app.config != resolved:
        cached_app = WsgiMcpApp(MCPCore(config=resolved, registry=registry))
        cast("Any", registry)._mcp_app = cached_app
    return cached_app.handle_vertebra_request(request, default_path=resolved.path)


def attach_mcp_routes(
    compiler_or_views: RouteCompiler | list[Any] | None = None,
    config: MCPConfig | None = None,
) -> tuple[MCPRegistry, dict[str, Response]]:
    """Scan Django views for ``@mcp_tool``, ``@mcp_resource``, and ``@mcp_prompt``."""
    resolved = config or MCPConfig(server_name="vertebra-django-mcp")
    registry = MCPRegistry()

    if isinstance(compiler_or_views, RouteCompiler):
        for entry in compiler_or_views.entries:
            registry.register_route_handler(entry.callback)
    elif isinstance(compiler_or_views, list):
        for item in compiler_or_views:
            cb = getattr(item, "callback", item)
            registry.register_route_handler(cb)

    mcp_app = WsgiMcpApp(MCPCore(config=resolved, registry=registry))
    cast("Any", registry)._mcp_app = mcp_app

    static_responses: dict[str, Response] = {}
    if resolved.expose_well_known:
        well_known_bytes = render_well_known_mcp_json(resolved, registry)
        static_responses[resolved.well_known_path] = Response(
            status=200,
            headers=[("content-type", "application/json")],
            body=well_known_bytes,
        )
    if isinstance(resolved.webmcp, WebMCPConfig) and resolved.webmcp.enabled:
        webmcp_bytes = render_webmcp_manifest_json(
            registry,
            config=resolved.webmcp,
        ).encode("utf-8")
        static_responses[resolved.webmcp.manifest_path] = Response(
            status=200,
            headers=[("content-type", "application/json")],
            body=webmcp_bytes,
        )

    return registry, static_responses
