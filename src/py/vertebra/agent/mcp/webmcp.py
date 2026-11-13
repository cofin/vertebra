"""Browser WebMCP (navigator.modelContext) manifest generator and configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.agent.core.schema import Tool, ToolSpec, is_app_only_tool
from vertebra.agent.mcp.models import MCP_PROTOCOL_VERSION
from vertebra.agent.mcp.registry import ToolRegistry
from vertebra.utils.serializers import format_json

if TYPE_CHECKING:
    from collections.abc import Sequence

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
    """Whether `/.well-known/webmcp.json` discovery is enabled."""
    manifest_path: str = "/.well-known/webmcp.json"
    """HTTP path serving the WebMCP manifest."""
    mcp_endpoint: str = "/mcp"
    """MCP Streamable HTTP endpoint advertised to browser tools."""


class WebMCPToolDescriptor(
    msgspec.Struct,
    frozen=True,
    gc=False,
    kw_only=True,
    rename={
        "input_schema": "inputSchema",
        "output_schema": "outputSchema",
        "requires_confirmation": "requiresConfirmation",
    },
):
    """Single browser WebMCP tool descriptor for navigator.modelContext registration."""

    name: str
    """Unique tool name registered in `document.modelContext` or `navigator.modelContext`."""
    title: str | None = None
    """Optional human-readable tool title."""
    description: str = ""
    """Human-readable tool description."""
    input_schema: dict[str, Any] = msgspec.field(default_factory=dict)
    """JSON Schema describing accepted tool input arguments."""
    output_schema: dict[str, Any] | None = None
    """Optional JSON Schema describing structured tool return output."""
    endpoint: str = "/mcp"
    """MCP Streamable HTTP endpoint used to invoke the tool."""
    requires_confirmation: bool = False
    """Whether human-in-the-loop confirmation is required before execution."""
    annotations: dict[str, Any] = msgspec.field(default_factory=dict)
    """MCP and WebMCP tool hint annotations (`readOnlyHint`, `consequentialHint`, etc.)."""


class WebMCPManifest(
    msgspec.Struct,
    frozen=True,
    gc=False,
    kw_only=True,
    rename={"protocol_version": "protocolVersion", "mcp_endpoint": "mcpEndpoint"},
):
    """Discovery payload served at /.well-known/webmcp.json."""

    protocol_version: str = MCP_PROTOCOL_VERSION
    """MCP protocol version identifier (`2026-07-28`)."""
    mcp_endpoint: str = "/mcp"
    """MCP Streamable HTTP endpoint path."""
    tools: tuple[WebMCPToolDescriptor, ...] = ()
    """Browser-exposed WebMCP tool descriptors."""


def build_webmcp_manifest(
    source: ToolRegistry | Sequence[Tool | ToolSpec],
    config: WebMCPConfig | None = None,
) -> WebMCPManifest:
    """Build a WebMCPManifest projecting strictly from ``ToolSpec`` entries with ``"webmcp" in exposure``.

    Tools whose ``ToolSpec.exposure`` does not contain ``"webmcp"`` or whose UI visibility is
    exclusively ``["app"]`` are excluded.
    """
    cfg = config or WebMCPConfig()
    descriptors: list[WebMCPToolDescriptor] = []

    if isinstance(source, ToolRegistry):
        for spec in source.list_specs(exposure="webmcp"):
            if is_app_only_tool(spec):
                continue
            tool_obj = source.get_tool(spec.name)
            descriptors.append(_descriptor_from_spec(spec, cfg.mcp_endpoint, tool_obj=tool_obj))
    else:
        for item in source:
            spec = item if isinstance(item, ToolSpec) else item.spec
            if "webmcp" not in spec.exposure or is_app_only_tool(spec):
                continue
            tool_obj = item if isinstance(item, Tool) else None
            descriptors.append(_descriptor_from_spec(spec, cfg.mcp_endpoint, tool_obj=tool_obj))

    return WebMCPManifest(
        protocol_version=MCP_PROTOCOL_VERSION,
        mcp_endpoint=cfg.mcp_endpoint,
        tools=tuple(descriptors),
    )


def render_webmcp_manifest_json(
    source: ToolRegistry | Sequence[Tool | ToolSpec],
    config: WebMCPConfig | None = None,
) -> str:
    """Serialize the WebMCP manifest as formatted JSON."""
    manifest = build_webmcp_manifest(source, config=config)
    return format_json(msgspec.to_builtins(manifest), indent=2)


def _descriptor_from_spec(
    spec: ToolSpec,
    endpoint: str,
    *,
    tool_obj: Tool | None = None,
) -> WebMCPToolDescriptor:
    annotations = dict(spec.annotations.to_mcp_dict())
    if tool_obj is not None and tool_obj.annotations:
        annotations.update(tool_obj.annotations)
    if spec.requires_confirmation or spec.annotations.consequential:
        annotations["consequentialHint"] = True
    return WebMCPToolDescriptor(
        name=spec.name,
        title=spec.title,
        description=spec.description,
        input_schema=dict(spec.input_schema),
        output_schema=dict(spec.output_schema) if spec.output_schema is not None else None,
        endpoint=endpoint,
        requires_confirmation=spec.requires_confirmation,
        annotations=annotations,
    )
