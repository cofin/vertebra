"""Model Context Protocol (MCP) 2026-07-28 frozen msgspec data models.

This module provides data models for tools, resources, prompts, content blocks,
and server configurations compliant with the MCP 2026-07-28 specification.
"""

from collections.abc import Callable
from typing import Any, Literal

import msgspec

MCP_PROTOCOL_VERSION: str = "2026-07-28"
HEADER_MISMATCH: int = -32020
UNSUPPORTED_PROTOCOL_VERSION: int = -32022


class MCPConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Server configuration for Model Context Protocol handlers."""

    server_name: str = "vertebra-mcp"
    server_version: str = "0.1.0"
    instructions: str = ""
    allowed_origins: tuple[str, ...] = ("*",)
    max_blob_bytes: int = 16_777_216
    tools_enabled: bool = True
    resources_enabled: bool = True
    prompts_enabled: bool = True
    skills_enabled: bool = True
    tasks_enabled: bool = True


class TextContent(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Plain text content item in an MCP tool output or prompt message."""

    type: Literal["text"] = "text"
    text: str = ""


class BlobContent(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Binary base64-encoded media item in an MCP tool output."""

    type: Literal["image", "audio", "blob"] = "blob"
    data: str = ""
    mimeType: str = "application/octet-stream"


class ResourceLinkContent(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Pointer referencing an active MCP resource URI."""

    type: Literal["resource_link"] = "resource_link"
    uri: str = ""
    name: str | None = None
    description: str | None = None


type ContentBlock = TextContent | BlobContent | ResourceLinkContent


class ToolCallOutput(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Normalized output envelope returned by an MCP tools/call invocation."""

    content: tuple[ContentBlock, ...] = ()
    structuredContent: dict[str, Any] | None = None
    isError: bool = False


class ResourceEntry(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Static resource descriptor advertised via resources/list."""

    uri: str
    name: str
    description: str = ""
    mimeType: str = "text/plain"
    reader: Callable[..., Any] | None = None


class ResourceTemplateEntry(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Dynamic resource URI template advertised via resources/templates/list."""

    uriTemplate: str
    name: str
    description: str = ""
    mimeType: str = "text/plain"
    reader: Callable[..., Any] | None = None


class PromptArgument(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Argument declaration for an MCP prompt template."""

    name: str
    description: str = ""
    required: bool = False


class PromptMessage(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Message item in an instantiated prompt conversation."""

    role: Literal["user", "assistant"] = "user"
    content: TextContent | BlobContent = msgspec.field(default_factory=TextContent)


class PromptEntry(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Prompt template descriptor advertised via prompts/list."""

    name: str
    description: str = ""
    arguments: tuple[PromptArgument, ...] = ()
    getter: Callable[..., Any] | None = None


__all__ = [
    "HEADER_MISMATCH",
    "MCP_PROTOCOL_VERSION",
    "UNSUPPORTED_PROTOCOL_VERSION",
    "BlobContent",
    "ContentBlock",
    "MCPConfig",
    "PromptArgument",
    "PromptEntry",
    "PromptMessage",
    "ResourceEntry",
    "ResourceTemplateEntry",
    "ResourceLinkContent",
    "TextContent",
    "ToolCallOutput",
]
