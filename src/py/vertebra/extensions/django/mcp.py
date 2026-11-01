"""Route-annotation/controller-driven MCP and browser WebMCP integration for Django."""

from __future__ import annotations

import asyncio
from typing import Any

import msgspec

from vertebra._vertebra import Request, Response
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
from vertebra.extensions.django.router import RouteCompiler
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
    "dispatch_mcp_request",
    "mcp_prompt",
    "mcp_resource",
    "mcp_tool",
    "render_webmcp_manifest_json",
)


class MCPConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for Django MCP and WebMCP route mounting."""

    server_name: str = "vertebra-django-mcp"
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


def dispatch_mcp_request(
    registry: MCPRegistry,
    request: Request | bytes,
    config: MCPConfig | None = None,
) -> Response:
    """Execute an MCP JSON-RPC 2.0 request and return a Vertebra ``Response``."""
    resolved = config or MCPConfig()
    raw = bytes(request.body or b"") if isinstance(request, Request) else bytes(request)
    body = asyncio.run(_dispatch_jsonrpc(registry, raw, resolved))
    return Response(
        status=200,
        headers=[("content-type", "application/json")],
        body=body,
    )


def attach_mcp_routes(
    compiler_or_views: RouteCompiler | list[Any] | None = None,
    config: MCPConfig | None = None,
) -> tuple[MCPRegistry, dict[str, Response]]:
    """Scan Django views for ``@mcp_tool``, ``@mcp_resource``, and ``@mcp_prompt``."""
    resolved = config or MCPConfig()
    registry = MCPRegistry()

    if isinstance(compiler_or_views, RouteCompiler):
        for entry in compiler_or_views.entries:
            registry.register_route_handler(entry.callback)
    elif isinstance(compiler_or_views, list):
        for item in compiler_or_views:
            cb = getattr(item, "callback", item)
            registry.register_route_handler(cb)

    static_responses: dict[str, Response] = {}
    if resolved.expose_well_known:
        well_known_bytes = render_webmcp_manifest_json(
            registry,
            config=resolved.webmcp or WebMCPConfig(manifest_path=resolved.well_known_path),
        ).encode("utf-8")
        static_responses[resolved.well_known_path] = Response(
            status=200,
            headers=[("content-type", "application/json")],
            body=well_known_bytes,
        )

    return registry, static_responses
