"""Model Context Protocol (MCP) 2026-07-28 frozen msgspec data models.

This module provides data models for tools, resources, prompts, content blocks,
and server configurations compliant with the MCP 2026-07-28 specification.
"""

from collections.abc import Callable, Mapping
from typing import Any, Literal

import msgspec

from vertebra.typing import FrozenBaseStruct

MCP_PROTOCOL_VERSION: str = "2026-07-28"
HEADER_MISMATCH: int = -32020
UNSUPPORTED_PROTOCOL_VERSION: int = -32022


class MCPConfig(FrozenBaseStruct, frozen=True, kw_only=True):
    """Server configuration for Model Context Protocol handlers."""

    server_name: str = "vertebra-mcp"
    """Server identifier returned in `server/discover` and `/.well-known/mcp.json` responses."""
    server_version: str = "0.1.0"
    """Semantic version string advertised during MCP capability discovery."""
    instructions: str = ""
    """System-level usage instructions provided to connecting MCP clients."""
    allowed_origins: tuple[str, ...] = ()
    """Allowed HTTP `Origin` header patterns for Streamable HTTP requests; empty enforces same-origin."""
    max_blob_bytes: int = 16_777_216
    """Maximum decoded payload size in bytes for binary resource and tool blobs."""
    tools_enabled: bool = True
    """Advertise and serve `tools/list` and `tools/call` capabilities."""
    resources_enabled: bool = True
    """Advertise and serve `resources/list`, `resources/read`, and `resources/templates/list`."""
    prompts_enabled: bool = True
    """Advertise and serve `prompts/list` and `prompts/get` capabilities."""
    skills_enabled: bool = True
    """Expose progressive `skill://` resource catalogs and discovery tools."""
    tasks_enabled: bool = True
    """Enable `io.modelcontextprotocol/tasks` durable async task execution."""


class McpHttpRequest(FrozenBaseStruct, frozen=True, kw_only=True):
    """Transport-agnostic HTTP request envelope for MCPCore.handle."""

    method: str = "POST"
    """HTTP verb of the incoming request (`"POST"`, `"GET"`, `"OPTIONS"`, etc.)."""
    path: str = "/mcp"
    """Request path targeted by the MCP transport."""
    headers: Mapping[str, str] = msgspec.field(default_factory=dict)
    """Request HTTP headers mapping."""
    body: bytes = b""
    """Raw HTTP request body bytes."""
    context: Any | None = None
    """Optional active `AgentContext` associated with the caller."""


class McpHttpResponse(FrozenBaseStruct, frozen=True, kw_only=True):
    """Transport-agnostic HTTP response envelope returned by MCPCore.handle."""

    status: int = 200
    """HTTP response status code."""
    headers: dict[str, str] = msgspec.field(default_factory=dict)
    """HTTP response headers dictionary."""
    body: bytes = b""
    """Serialized HTTP response body bytes."""


class TextContent(FrozenBaseStruct, frozen=True, kw_only=True, cache_hash=True):
    """Plain text content item in an MCP tool output or prompt message."""

    type: Literal["text"] = "text"
    """Content block discriminator (`"text"`)."""
    text: str = ""
    """UTF-8 text body of the content block."""


class BlobContent(FrozenBaseStruct, frozen=True, kw_only=True, cache_hash=True):
    """Binary base64-encoded media item in an MCP tool output."""

    type: Literal["image", "audio", "blob"] = "blob"
    """Binary content block discriminator (`"image"`, `"audio"`, or `"blob"`)."""
    data: str = ""
    """Base64-encoded binary payload."""
    mimeType: str = "application/octet-stream"
    """IANA media type of the encoded payload."""


class ResourceLinkContent(FrozenBaseStruct, frozen=True, kw_only=True, cache_hash=True):
    """Pointer referencing an active MCP resource URI."""

    type: Literal["resource_link"] = "resource_link"
    """Content block discriminator (`"resource_link"`)."""
    uri: str = ""
    """Target MCP resource URI (`file://`, `skill://`, or custom scheme)."""
    name: str | None = None
    """Optional human-readable label for the linked resource."""
    description: str | None = None
    """Optional summary of the linked resource contents."""


type ContentBlock = TextContent | BlobContent | ResourceLinkContent


class ToolCallOutput(FrozenBaseStruct, frozen=True, kw_only=True):
    """Normalized output envelope returned by an MCP tools/call invocation."""

    content: tuple[ContentBlock, ...] = ()
    """Ordered sequence of text, blob, or resource-link content blocks."""
    structuredContent: dict[str, Any] | None = None
    """Optional machine-readable JSON object matching the tool's output schema."""
    isError: bool = False
    """Whether the tool execution resulted in an application-level error."""


class ResourceEntry(FrozenBaseStruct, frozen=True, kw_only=True):
    """Static resource descriptor advertised via resources/list."""

    uri: str
    """Canonical URI identifying the resource."""
    name: str
    """Human-readable resource title."""
    description: str = ""
    """Summary describing the resource contents and usage."""
    mimeType: str = "text/plain"
    """IANA media type returned when reading the resource."""
    reader: Callable[..., Any] | None = None
    """Sync or async callable invoked to read the resource payload."""


class ResourceTemplateEntry(FrozenBaseStruct, frozen=True, kw_only=True):
    """Dynamic resource URI template advertised via resources/templates/list."""

    uriTemplate: str
    """RFC 6570 URI template with `{param}` placeholders."""
    name: str
    """Human-readable template name."""
    description: str = ""
    """Description of the parameterized resource family."""
    mimeType: str = "text/plain"
    """Default IANA media type of resolved resources."""
    reader: Callable[..., Any] | None = None
    """Sync or async callable receiving extracted URI template parameters."""


class PromptArgument(FrozenBaseStruct, frozen=True, kw_only=True, cache_hash=True):
    """Argument declaration for an MCP prompt template."""

    name: str
    """Parameter name accepted by the prompt getter."""
    description: str = ""
    """Human-readable explanation of the argument."""
    required: bool = False
    """Whether callers must supply this argument when rendering the prompt."""


class PromptMessage(FrozenBaseStruct, frozen=True, kw_only=True, cache_hash=True):
    """Message item in an instantiated prompt conversation."""

    role: Literal["user", "assistant"] = "user"
    """Conversational speaker role (`"user"` or `"assistant"`)."""
    content: TextContent | BlobContent = msgspec.field(default_factory=TextContent)
    """Text or binary content block for the prompt message."""


class PromptEntry(FrozenBaseStruct, frozen=True, kw_only=True):
    """Prompt template descriptor advertised via prompts/list."""

    name: str
    """Unique identifier of the prompt template."""
    description: str = ""
    """Summary of what the prompt template generates."""
    arguments: tuple[PromptArgument, ...] = ()
    """Declared arguments accepted when rendering the prompt via `prompts/get`."""
    getter: Callable[..., Any] | None = None
    """Sync or async callable returning rendered `PromptMessage` items."""


__all__ = [
    "HEADER_MISMATCH",
    "MCP_PROTOCOL_VERSION",
    "UNSUPPORTED_PROTOCOL_VERSION",
    "BlobContent",
    "ContentBlock",
    "MCPConfig",
    "McpHttpRequest",
    "McpHttpResponse",
    "PromptArgument",
    "PromptEntry",
    "PromptMessage",
    "ResourceEntry",
    "ResourceLinkContent",
    "ResourceTemplateEntry",
    "TextContent",
    "ToolCallOutput",
]
