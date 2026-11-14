"""Model Context Protocol (MCP) JSON-RPC and Streamable HTTP data models.

This module provides data models for tools, resources, prompts, content blocks,
protocol version constants, and server configurations across MCP 2026-07-28 and
legacy Streamable HTTP protocol revisions.
"""

from collections.abc import AsyncIterator, Callable, Mapping, Sequence
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
    "SKILLS_EXTENSION",
    "SUPPORTED_PROTOCOL_VERSIONS",
    "UI_EXTENSION",
    "UI_MIME_TYPE",
    "UNSUPPORTED_PROTOCOL_VERSION",
    "BlobContent",
    "ContentBlock",
    "MCPConfig",
    "McpHttpRequest",
    "McpHttpResponse",
    "McpIcon",
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
    "coerce_mcp_icons",
]

MCP_PROTOCOL_VERSION: str = "2026-07-28"
LEGACY_PROTOCOL_VERSIONS: tuple[str, ...] = ("2025-11-25", "2025-06-18", "2025-03-26")
SUPPORTED_PROTOCOL_VERSIONS: tuple[str, ...] = (MCP_PROTOCOL_VERSION, *LEGACY_PROTOCOL_VERSIONS)
MCP_FALLBACK_SESSION_VERSION: str = "2025-11-25"
SKILLS_EXTENSION: str = "io.modelcontextprotocol/skills"
UI_EXTENSION: str = "io.modelcontextprotocol/ui"
UI_MIME_TYPE: str = "text/html;profile=mcp-app"
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
    enable_get_sse: bool = True
    """Allow legacy `GET /mcp` Server-Sent Events notification streams."""
    sse_heartbeat_seconds: float = 15.0
    """Interval in seconds between SSE keepalive comments on idle streams."""
    max_streams_per_owner: int = 8
    """Maximum concurrent SSE streams allowed per authenticated owner key."""
    resource_subscriptions: bool = True
    """Advertise and serve `resources/subscribe` and `resources/unsubscribe`."""
    max_subscriptions_per_session: int = 256
    """Maximum number of active resource URI subscriptions permitted per session."""
    subscription_authorizer: Callable[..., Any] | None = None
    """Optional sync or async callable authorizing resource subscription requests."""
    ui_mime_types: tuple[str, ...] = (UI_MIME_TYPE,)
    """Allowed MIME types for SEP-1865 ``ui://`` app resources; empty disables the UI extension."""
    hide_app_only_tools: bool = True
    """Hide app-only tools from ``tools/list`` when caller capabilities lack the UI extension."""

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
        if self.sse_heartbeat_seconds <= 0:
            raise ValueError("MCPConfig.sse_heartbeat_seconds must be positive")
        if self.max_streams_per_owner < 0:
            raise ValueError("MCPConfig.max_streams_per_owner must be non-negative")
        if self.max_subscriptions_per_session < 0:
            raise ValueError("MCPConfig.max_subscriptions_per_session must be non-negative")
        if self.subscription_authorizer is not None and not callable(self.subscription_authorizer):
            raise TypeError("MCPConfig.subscription_authorizer must be callable")
        for mime in self.ui_mime_types:
            if not isinstance(mime, str) or not mime.strip():
                raise ValueError("MCPConfig.ui_mime_types entries must be non-empty strings")


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
    stream: AsyncIterator[bytes] | None = None
    """Optional incremental byte stream for Server-Sent Events responses."""


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


class McpIcon(FrozenBaseStruct, frozen=True, cache_hash=True):
    """SEP-1865 / MCP icon metadata descriptor for resources, templates, and skills."""

    src: str
    """Icon source URL or data URI."""
    mimeType: str | None = None
    """Optional IANA media type of the icon asset."""
    sizes: tuple[str, ...] = ()
    """Optional size specifications such as ``("32x32", "any")``."""

    def to_mcp_dict(self) -> dict[str, Any]:
        """Serialize this icon into its wire MCP dictionary representation."""
        out: dict[str, Any] = {"src": self.src}
        if self.mimeType is not None:
            out["mimeType"] = self.mimeType
        if self.sizes:
            out["sizes"] = list(self.sizes)
        return out


def coerce_mcp_icons(icons: Sequence[McpIcon | Mapping[str, Any]] | None) -> tuple[McpIcon, ...]:
    """Normalize a sequence of ``McpIcon`` objects or mappings into a canonical tuple."""
    if not icons:
        return ()
    resolved: list[McpIcon] = []
    for item in icons:
        if isinstance(item, McpIcon):
            resolved.append(item)
        elif isinstance(item, Mapping):
            src = item.get("src")
            if not isinstance(src, str) or not src:
                raise ValueError("McpIcon mapping requires a non-empty 'src' string")
            raw_mime = item.get("mimeType")
            mime = str(raw_mime) if raw_mime is not None else None
            raw_sizes = item.get("sizes") or ()
            sizes = (raw_sizes,) if isinstance(raw_sizes, str) else tuple(str(sz) for sz in raw_sizes)
            resolved.append(McpIcon(src=src, mimeType=mime, sizes=sizes))
        else:
            raise TypeError(f"Expected McpIcon or Mapping, got {type(item).__name__}")
    return tuple(resolved)


class ResourceEntry(FrozenBaseStruct, frozen=True, kw_only=True):
    """Static resource descriptor advertised via resources/list."""

    uri: str
    """Canonical URI identifying the resource."""
    name: str
    """Human-readable resource title."""
    title: str | None = None
    """Optional display title for SEP-1865 UI and client presentation."""
    description: str = ""
    """Summary describing the resource contents and usage."""
    mimeType: str = "text/plain"
    """IANA media type returned when reading the resource."""
    icons: tuple[McpIcon, ...] = ()
    """Optional icons advertised alongside this resource."""
    meta: dict[str, Any] | None = None
    """Optional ``_meta`` extension payload (for example SEP-1865 UI settings)."""
    reader: Callable[..., Any] | None = None
    """Sync or async callable invoked to read the resource payload."""


class ResourceTemplateEntry(FrozenBaseStruct, frozen=True, kw_only=True):
    """Dynamic resource URI template advertised via resources/templates/list."""

    uriTemplate: str
    """RFC 6570 URI template with `{param}` placeholders."""
    name: str
    """Human-readable template name."""
    title: str | None = None
    """Optional display title for SEP-1865 UI and client presentation."""
    description: str = ""
    """Description of the parameterized resource family."""
    mimeType: str = "text/plain"
    """Default IANA media type of resolved resources."""
    icons: tuple[McpIcon, ...] = ()
    """Optional icons advertised alongside this resource template."""
    meta: dict[str, Any] | None = None
    """Optional ``_meta`` extension payload."""
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
