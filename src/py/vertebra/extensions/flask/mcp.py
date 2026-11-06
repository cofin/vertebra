"""Route-annotation/controller-driven MCP and browser WebMCP integration for Flask."""

from __future__ import annotations

from typing import Any, cast

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
    "mcp_prompt",
    "mcp_resource",
    "mcp_tool",
    "render_webmcp_manifest_json",
)


def attach_mcp_routes(
    app: Any,
    config: MCPConfig | None = None,
) -> MCPRegistry:
    """Scan Flask ``view_functions`` for ``@mcp_tool``, ``@mcp_resource``, and ``@mcp_prompt``."""
    resolved = config or MCPConfig(server_name="vertebra-flask-mcp")
    registry = MCPRegistry()

    view_functions = getattr(app, "view_functions", {}) or {}
    for view_fn in list(view_functions.values()):
        registry.register_route_handler(view_fn)

    core = MCPCore(config=resolved, registry=registry)
    mcp_app = WsgiMcpApp(core)
    cast("Any", registry)._mcp_app = mcp_app
    extensions = getattr(app, "extensions", None)
    if isinstance(extensions, dict):
        extensions["vertebra_mcp_app"] = mcp_app

    if hasattr(app, "add_url_rule") and callable(app.add_url_rule):
        existing_endpoints = set(view_functions.keys())

        if resolved.expose_well_known and "vertebra_mcp_well_known" not in existing_endpoints:
            well_known_bytes = render_well_known_mcp_json(resolved, registry)

            def _serve_well_known() -> tuple[bytes, int, dict[str, str]]:
                return well_known_bytes, 200, {"Content-Type": "application/json"}

            cast("Any", _serve_well_known).__vertebra_fastpath__ = True
            app.add_url_rule(
                resolved.well_known_path,
                endpoint="vertebra_mcp_well_known",
                view_func=_serve_well_known,
                methods=["GET"],
            )

        if (
            isinstance(resolved.webmcp, WebMCPConfig)
            and resolved.webmcp.enabled
            and "vertebra_webmcp_manifest" not in existing_endpoints
        ):
            webmcp_bytes = render_webmcp_manifest_json(
                registry,
                config=resolved.webmcp,
            ).encode("utf-8")

            def _serve_webmcp() -> tuple[bytes, int, dict[str, str]]:
                return webmcp_bytes, 200, {"Content-Type": "application/json"}

            cast("Any", _serve_webmcp).__vertebra_fastpath__ = True
            app.add_url_rule(
                resolved.webmcp.manifest_path,
                endpoint="vertebra_webmcp_manifest",
                view_func=_serve_webmcp,
                methods=["GET"],
            )

        if "vertebra_mcp_rpc" not in existing_endpoints:

            def _serve_mcp_rpc(request: Any = None) -> Any:
                return mcp_app.handle_flask_view(request)

            cast("Any", _serve_mcp_rpc).__vertebra_fastpath__ = True
            app.add_url_rule(
                resolved.path,
                endpoint="vertebra_mcp_rpc",
                view_func=_serve_mcp_rpc,
                methods=["POST", "GET", "OPTIONS"],
            )

    return registry
