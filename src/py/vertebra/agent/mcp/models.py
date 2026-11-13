"""Model Context Protocol (MCP) JSON-RPC and Streamable HTTP data models.

This module provides data models for tools, resources, prompts, content blocks,
protocol version constants, and server configurations across MCP 2026-07-28 and
legacy Streamable HTTP protocol revisions.
"""

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from vertebra.typing import FrozenBaseStruct

if TYPE_CHECKING:
    from vertebra.agent.core.identity import IdentitySource

__all__ = [
    "HEADER_MISMATCH",
    "LEGACY_ONLY_METHODS",
    "LEGACY_PROTOCOL_VERSIONS",
    "MCP_FALLBACK_SESSION_VERSION",
    "MCP_LOG_LEVELS",
    "MCP_PROTOCOL_VERSION",
    "SESSION_CAPACITY_EXHAUSTED",
    "SUPPORTED_PROTOCOL_VERSIONS",
    "UNSUPPORTED_PROTOCOL_VERSION",
    "BlobContent",
    "ContentBlock",
    "MCPConfig",
    "McpHttpRequest",
    "McpHttpResponse",
    "McpLogLevel",
    "NegotiatedProtocol",
    "PromptArgument",
    "PromptEntry",
    "PromptMessage",
    "ResourceEntry",
    "ResourceLinkContent",
    "ResourceTemplateEntry",
    "TextContent",
    "ToolCallOutput",
]

MCP_PROTOCOL_VERSION: str = "2026-07-28"
LEGACY_PROTOCOL_VERSIONS: tuple[str, ...] = ("2025-11-25", "2025-06-18", "2025-03-26")
SUPPORTED_PROTOCOL_VERSIONS: tuple[str, ...] = (MCP_PROTOCOL_VERSION, *LEGACY_PROTOCOL_VERSIONS)
MCP_FALLBACK_SESSION_VERSION: str = "2025-11-25"
LEGACY_ONLY_METHODS: frozenset[str] = frozenset(
    {
        "initialize",
        "ping",
        "logging/setLevel",
        "resources/subscribe",
        "resources/unsubscribe",
        "tasks/result",
    }
)
SESSION_CAPACITY_EXHAUSTED: int = -32000
HEADER_MISMATCH: int = -32020
UNSUPPORTED_PROTOCOL_VERSION: int = -32022

type McpLogLevel = Literal[
    "debug",
    "info",
    "notice",
    "warning",
    "error",
    "critical",
    "alert",
    "emergency",
]
MCP_LOG_LEVELS: tuple[McpLogLevel, ...] = (
    "debug",
    "info",
    "notice",
    "warning",
    "error",
    "critical",
    "alert",
    "emergency",
)


class NegotiatedProtocol(FrozenBaseStruct, frozen=True, kw_only=True):
    """Negotiated MCP protocol version and stateless vs stateful mode."""

    version: str
    """Negotiated MCP protocol version string."""
    modern: bool
    """True when operating in stateless MCP 2026-07-28 mode."""


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
    path: str = "/mcp"
    """HTTP path where the Streamable HTTP MCP endpoint is mounted."""
    expose_well_known: bool = True
    """Whether to expose the well-known discovery manifest route."""
    well_known_path: str = "/.well-known/mcp.json"
    """HTTP path for the well-known MCP discovery document."""
    webmcp: Any | None = None
    """Optional `WebMCPConfig` controlling browser WebMCP manifest generation."""

    trust_proxy_identity: bool = False
    """Trust proxy identity only with an explicitly configured identity source."""
    identity_source: "IdentitySource | None" = None
    """Trusted IAP, mTLS, or application identity source from agent.core.identity."""
    session_ttl_seconds: float = 3600.0
    """Time-to-live in seconds for legacy stateful MCP sessions."""
    max_sessions: int = 10_000
    """Maximum number of concurrent legacy MCP sessions retained in memory."""
    logging: bool = True
    """Advertise and serve MCP logging capabilities and `logging/setLevel`."""
    default_log_level: McpLogLevel = "info"
    """Default minimum log severity assigned to newly created MCP sessions."""

    def __post_init__(self) -> None:
        if self.trust_proxy_identity and self.identity_source is None:
            raise ValueError(
                "MCPConfig.trust_proxy_identity=True requires identity_source (IapIdentitySource, MtlsIdentitySource, or CallableIdentitySource)"
            )
        if self.identity_source is not None and not self.trust_proxy_identity:
            raise ValueError("MCPConfig.identity_source requires trust_proxy_identity=True")
        if self.session_ttl_seconds <= 0:
            raise ValueError("MCPConfig.session_ttl_seconds must be positive")
        if self.max_sessions < 0:
            raise ValueError("MCPConfig.max_sessions must be non-negative")
        if self.default_log_level not in MCP_LOG_LEVELS:
            raise ValueError(f"MCPConfig.default_log_level must be one of {MCP_LOG_LEVELS}")


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
