"""Streamable HTTP route handlers for Model Context Protocol (MCP) 2026-07-28.

This module provides MCPHttpHandler delegating HTTP request validation,
Origin checking, ETag 304 Not Modified caching, and JSON-RPC dispatch to MCPCore.
"""

from collections.abc import AsyncIterable, Mapping

from vertebra.agent.core.context import AgentContext
from vertebra.agent.mcp.client import MCP_NAME_FIELDS, decode_mcp_header_value
from vertebra.agent.mcp.models import MCPConfig, McpHttpRequest
from vertebra.agent.mcp.service import CACHEABLE_METHODS, MCPCore

__all__ = [
    "CACHEABLE_METHODS",
    "MCP_NAME_FIELDS",
    "MCPHttpHandler",
    "decode_mcp_header_value",
]


class MCPHttpHandler:
    """Streamable HTTP route handler delegating validation, caching, and dispatch to MCPCore."""

    def __init__(self, service: MCPCore) -> None:
        self.service = service
        self.config: MCPConfig = service.config

    async def handle_request(
        self,
        method: str,
        path: str,
        headers: Mapping[str, str],
        body: bytes,
        context: AgentContext | None = None,
    ) -> tuple[int, dict[str, str], bytes | AsyncIterable[bytes]]:
        """Process an incoming HTTP request through MCPCore.handle."""
        response = await self.service.handle(
            McpHttpRequest(
                method=method,
                path=path,
                headers=headers,
                body=body,
                context=context,
            )
        )
        return response.status, response.headers, response.body
