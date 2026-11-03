"""Browser WebMCP (navigator.modelContext) manifest generator and configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import msgspec

from vertebra.agent.core.schema import Tool
from vertebra.agent.mcp.models import MCP_PROTOCOL_VERSION
from vertebra.utils.serializers import format_json

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vertebra.agent.mcp.registry import MCPRegistry

__all__ = (
    "WebMCPConfig",
    "WebMCPManifest",
    "WebMCPToolDescriptor",
    "build_webmcp_manifest",
    "render_webmcp_manifest_json",
)


class WebMCPConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for browser WebMCP (navigator.modelContext) tool advertisement."""

    enabled: bool = True
    manifest_path: str = "/.well-known/webmcp.json"
    mcp_endpoint: str = "/mcp"


class WebMCPToolDescriptor(
    msgspec.Struct,
    frozen=True,
    gc=False,
    kw_only=True,
    rename={"input_schema": "inputSchema", "requires_confirmation": "requiresConfirmation"},
):
    """Single browser WebMCP tool descriptor for navigator.modelContext registration."""

    name: str
    description: str = ""
    input_schema: dict[str, Any] = msgspec.field(default_factory=dict)
    endpoint: str = "/mcp"
    requires_confirmation: bool = False
    annotations: dict[str, Any] = msgspec.field(default_factory=dict)


class WebMCPManifest(
    msgspec.Struct,
    frozen=True,
    gc=False,
    kw_only=True,
    rename={"protocol_version": "protocolVersion", "mcp_endpoint": "mcpEndpoint"},
):
    """Discovery payload served at /.well-known/webmcp.json."""

    protocol_version: str = MCP_PROTOCOL_VERSION
    mcp_endpoint: str = "/mcp"
    tools: tuple[WebMCPToolDescriptor, ...] = ()


def build_webmcp_manifest(
    source: MCPRegistry | Sequence[Tool],
    config: WebMCPConfig | None = None,
) -> WebMCPManifest:
    """Build a WebMCPManifest listing only tools opted in with ``webmcp=True``.

    Tools without ``webmcp=True``, including plain ``Tool`` objects, are never browser-exposed.
    """
    cfg = config or WebMCPConfig()
    tools_list: Sequence[Tool] = cast("Any", source).list_tools() if hasattr(source, "list_tools") else source
    descriptors: list[WebMCPToolDescriptor] = []
    for t in tools_list:
        meta = getattr(t.fn, "_mcp_tool_meta", None) or {}
        if not (bool(getattr(t, "webmcp", False)) or meta.get("webmcp") is True):
            continue
        annotations = dict(getattr(t, "annotations", None) or meta.get("annotations", {}))
        descriptors.append(
            WebMCPToolDescriptor(
                name=t.name,
                description=t.description,
                input_schema=t.input_schema,
                endpoint=cfg.mcp_endpoint,
                requires_confirmation=t.requires_confirmation,
                annotations=annotations,
            )
        )
    return WebMCPManifest(
        protocol_version=MCP_PROTOCOL_VERSION,
        mcp_endpoint=cfg.mcp_endpoint,
        tools=tuple(descriptors),
    )


def render_webmcp_manifest_json(
    source: MCPRegistry | Sequence[Tool],
    config: WebMCPConfig | None = None,
) -> str:
    """Serialize the WebMCP manifest as formatted JSON."""
    manifest = build_webmcp_manifest(source, config=config)
    return format_json(msgspec.to_builtins(manifest), indent=2)
