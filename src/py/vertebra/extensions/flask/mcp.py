"""Route-annotation/controller-driven MCP and browser WebMCP integration for Flask."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import msgspec

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
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = (
    "MCPConfig",
    "MCPRegistry",
    "PromptController",
    "SkillController",
    "WebMCPConfig",
    "WebMCPManifest",
    "WebMCPToolDescriptor",
    "attach_mcp_routes",
    "build_webmcp_manifest",
    "mcp_prompt",
    "mcp_resource",
    "mcp_tool",
    "render_webmcp_manifest_json",
)


class MCPConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for Flask MCP and WebMCP route mounting."""

    server_name: str = "vertebra-flask-mcp"
    version: str = "0.2.0"
    path: str = "/mcp"
    expose_well_known: bool = True
    well_known_path: str = "/.well-known/mcp.json"
    webmcp: WebMCPConfig | None = None


async def _dispatch_jsonrpc(registry: MCPRegistry, raw_body: bytes, config: MCPConfig) -> bytes:
    """Dispatch an MCP JSON-RPC 2.0 request against ``registry``."""
    payload = decode_json(raw_body) if raw_body else {}
    req_id = payload.get("id")
    method = payload.get("method", "")
    params = payload.get("params") or {}

    if method == "initialize":
        result: Any = {
            "protocolVersion": "2024-11-05",
            "serverInfo": {"name": config.server_name, "version": config.version},
            "capabilities": {"tools": {}, "resources": {}, "prompts": {}},
        }
    elif method == "tools/list":
        result = {
            "tools": [
                {
                    "name": t.name,
                    "description": t.description,
                    "inputSchema": t.input_schema,
                }
                for t in registry.list_tools()
            ]
        }
    elif method == "tools/call":
        tool_name = str(params.get("name", ""))
        args = dict(params.get("arguments") or {})
        tool_obj = registry.get_tool(tool_name)
        if tool_obj is None:
            return encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32601, "message": f"Tool not found: {tool_name}"},
                }
            )
        out = await tool_obj.invoke(args)
        result = {
            "content": [{"type": "text", "text": encode_json_bytes(out).decode("utf-8")}],
            "structuredContent": out,
        }
    elif method == "resources/list":
        result = {
            "resources": [
                {
                    "uri": r.uri,
                    "name": r.name,
                    "description": r.description,
                    "mimeType": r.mimeType,
                }
                for r in registry.list_resources()
            ]
        }
    elif method == "prompts/list":
        result = {
            "prompts": [
                {
                    "name": p.name,
                    "description": p.description,
                }
                for p in registry.list_prompts()
            ]
        }
    else:
        return encode_json_bytes(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"Method not found: {method}"},
            }
        )

    return encode_json_bytes({"jsonrpc": "2.0", "id": req_id, "result": result})


def attach_mcp_routes(
    app: Any,
    config: MCPConfig | None = None,
) -> MCPRegistry:
    """Scan Flask ``view_functions`` for ``@mcp_tool``, ``@mcp_resource``, and ``@mcp_prompt``."""
    resolved = config or MCPConfig()
    registry = MCPRegistry()

    view_functions = getattr(app, "view_functions", {}) or {}
    for view_fn in list(view_functions.values()):
        registry.register_route_handler(view_fn)

    if hasattr(app, "add_url_rule") and callable(app.add_url_rule):
        existing_endpoints = set(view_functions.keys())

        if resolved.expose_well_known and "vertebra_mcp_well_known" not in existing_endpoints:
            well_known_bytes = render_webmcp_manifest_json(
                registry,
                config=resolved.webmcp or WebMCPConfig(manifest_path=resolved.well_known_path),
            ).encode("utf-8")

            def _serve_well_known() -> tuple[bytes, int, dict[str, str]]:
                return well_known_bytes, 200, {"Content-Type": "application/json"}

            cast("Any", _serve_well_known).__vertebra_fastpath__ = True
            app.add_url_rule(
                resolved.well_known_path,
                endpoint="vertebra_mcp_well_known",
                view_func=_serve_well_known,
                methods=["GET"],
            )

        if "vertebra_mcp_rpc" not in existing_endpoints:

            def _serve_mcp_rpc(request: Any = None) -> tuple[bytes, int, dict[str, str]]:
                if request is not None:
                    raw = bytes(request.body)
                else:
                    from vertebra.utils.module_loader import import_optional

                    flask_mod = import_optional("flask")
                    raw = flask_mod.request.get_data() if flask_mod is not None else b"{}"
                out = asyncio.run(_dispatch_jsonrpc(registry, raw, resolved))
                return out, 200, {"Content-Type": "application/json"}

            cast("Any", _serve_mcp_rpc).__vertebra_fastpath__ = True
            app.add_url_rule(
                resolved.path,
                endpoint="vertebra_mcp_rpc",
                view_func=_serve_mcp_rpc,
                methods=["POST"],
            )

    return registry
