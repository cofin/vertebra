"""Route-annotation/controller-driven MCP and browser WebMCP integration for FastAPI."""

from __future__ import annotations

from vertebra.extensions.starlette.mcp import (
    MCPRegistry,
    PromptController,
    SkillController,
    WebMCPConfig,
    WebMCPManifest,
    WebMCPToolDescriptor,
    build_webmcp_manifest,
    mcp_prompt,
    mcp_resource,
    mcp_tool,
    mount_mcp_routes,
    render_webmcp_manifest_json,
)

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
