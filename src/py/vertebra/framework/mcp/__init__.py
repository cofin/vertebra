"""Streamable HTTP and stdio Model Context Protocol (MCP) server, tools, resources, and prompts."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = (
    "McpPrompt",
    "McpResource",
    "McpServer",
    "McpTool",
    "mcp_prompt",
    "mcp_resource",
    "mcp_tool",
)


class McpTool(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """MCP tool descriptor with JSON Schema parameters and handler callable."""

    name: str
    description: str
    fn: Callable[..., Any]
    input_schema: dict[str, Any] = msgspec.field(default_factory=lambda: {"type": "object", "properties": {}})
    requires_confirmation: bool = False


class McpResource(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """MCP resource descriptor exposed under a URI template."""

    uri: str
    name: str
    description: str
    fn: Callable[..., Any]
    mime_type: str = "application/json"


class McpPrompt(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """MCP prompt template descriptor."""

    name: str
    description: str
    fn: Callable[..., Any]


class McpServer:
    """Streamable HTTP (`/mcp`) and JSON-RPC 2.0 Model Context Protocol dispatcher."""

    __slots__ = ("_name", "_prompts", "_resources", "_tools", "_version")

    def __init__(self, *, name: str = "vertebra-mcp", version: str = "0.1.0") -> None:
        self._name = name
        self._version = version
        self._tools: dict[str, McpTool] = {}
        self._resources: dict[str, McpResource] = {}
        self._prompts: dict[str, McpPrompt] = {}

    @property
    def name(self) -> str:
        """Return the MCP server name."""
        return self._name

    @property
    def version(self) -> str:
        """Return the MCP server version."""
        return self._version

    @property
    def tools(self) -> dict[str, McpTool]:
        """Return registered MCP tools."""
        return dict(self._tools)

    @property
    def resources(self) -> dict[str, McpResource]:
        """Return registered MCP resources."""
        return dict(self._resources)

    @property
    def prompts(self) -> dict[str, McpPrompt]:
        """Return registered MCP prompts."""
        return dict(self._prompts)

    def register_tool(self, tool_def: McpTool) -> McpTool:
        """Register an ``McpTool``."""
        self._tools[tool_def.name] = tool_def
        return tool_def

    def register_resource(self, resource_def: McpResource) -> McpResource:
        """Register an ``McpResource``."""
        self._resources[resource_def.uri] = resource_def
        return resource_def

    def register_prompt(self, prompt_def: McpPrompt) -> McpPrompt:
        """Register an ``McpPrompt``."""
        self._prompts[prompt_def.name] = prompt_def
        return prompt_def

    async def handle_jsonrpc(self, request: dict[str, Any]) -> dict[str, Any]:
        """Dispatch a JSON-RPC 2.0 MCP request (`initialize`, `tools/list`, `tools/call`, `resources/list`)."""
        req_id = request.get("id")
        method = request.get("method", "")
        params = request.get("params") or {}

        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": "2025-03-26",
                    "serverInfo": {"name": self._name, "version": self._version},
                    "capabilities": {"tools": {}, "resources": {}, "prompts": {}},
                },
            }
        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "tools": [
                        {
                            "name": t.name,
                            "description": t.description,
                            "inputSchema": t.input_schema,
                        }
                        for t in self._tools.values()
                    ]
                },
            }
        if method == "tools/call":
            tool_name = params.get("name", "")
            arguments = params.get("arguments") or {}
            if tool_name not in self._tools:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"},
                }
            tool = self._tools[tool_name]
            out = tool.fn(**arguments)
            if inspect.isawaitable(out):
                out = await out
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": str(out)}]},
            }
        if method == "resources/list":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "resources": [
                        {
                            "uri": r.uri,
                            "name": r.name,
                            "description": r.description,
                            "mimeType": r.mime_type,
                        }
                        for r in self._resources.values()
                    ]
                },
            }
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"},
        }


def mcp_tool(
    *,
    name: str | None = None,
    description: str = "",
    input_schema: dict[str, Any] | None = None,
    requires_confirmation: bool = False,
) -> Callable[[Callable[..., Any]], McpTool]:
    """Decorator creating an ``McpTool``."""

    def _decorator(fn: Callable[..., Any]) -> McpTool:
        return McpTool(
            name=name or fn.__name__,
            description=description or (inspect.getdoc(fn) or ""),
            fn=fn,
            input_schema=input_schema or {"type": "object", "properties": {}},
            requires_confirmation=requires_confirmation,
        )

    return _decorator


def mcp_resource(
    uri: str,
    *,
    name: str | None = None,
    description: str = "",
    mime_type: str = "application/json",
) -> Callable[[Callable[..., Any]], McpResource]:
    """Decorator creating an ``McpResource``."""

    def _decorator(fn: Callable[..., Any]) -> McpResource:
        return McpResource(
            uri=uri,
            name=name or fn.__name__,
            description=description or (inspect.getdoc(fn) or ""),
            fn=fn,
            mime_type=mime_type,
        )

    return _decorator


def mcp_prompt(
    *,
    name: str | None = None,
    description: str = "",
) -> Callable[[Callable[..., Any]], McpPrompt]:
    """Decorator creating an ``McpPrompt``."""

    def _decorator(fn: Callable[..., Any]) -> McpPrompt:
        return McpPrompt(
            name=name or fn.__name__,
            description=description or (inspect.getdoc(fn) or ""),
            fn=fn,
        )

    return _decorator
