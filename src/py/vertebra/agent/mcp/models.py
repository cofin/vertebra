"""Model Context Protocol (MCP) 2026-07-28 frozen msgspec data models.

This module provides data models for tools, resources, prompts, content blocks,
and server configurations compliant with the MCP 2026-07-28 specification.
"""

from collections.abc import Callable
from typing import Any, Literal

import msgspec

from vertebra.typing import FrozenBaseStruct

MCP_PROTOCOL_VERSION: str = "2026-07-28"
HEADER_MISMATCH: int = -32020
UNSUPPORTED_PROTOCOL_VERSION: int = -32022


class MCPConfig(FrozenBaseStruct, frozen=True, kw_only=True):
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


class TextContent(FrozenBaseStruct, frozen=True, kw_only=True, cache_hash=True):
    """Plain text content item in an MCP tool output or prompt message."""

    type: Literal["text"] = "text"
    text: str = ""


class BlobContent(FrozenBaseStruct, frozen=True, kw_only=True, cache_hash=True):
    """Binary base64-encoded media item in an MCP tool output."""

    type: Literal["image", "audio", "blob"] = "blob"
    data: str = ""
    mimeType: str = "application/octet-stream"


class ResourceLinkContent(FrozenBaseStruct, frozen=True, kw_only=True, cache_hash=True):
    """Pointer referencing an active MCP resource URI."""

    type: Literal["resource_link"] = "resource_link"
    uri: str = ""
    name: str | None = None
    description: str | None = None


type ContentBlock = TextContent | BlobContent | ResourceLinkContent


class ToolCallOutput(FrozenBaseStruct, frozen=True, kw_only=True):
    """Normalized output envelope returned by an MCP tools/call invocation."""

    content: tuple[ContentBlock, ...] = ()
    structuredContent: dict[str, Any] | None = None
    isError: bool = False


class ResourceEntry(FrozenBaseStruct, frozen=True, kw_only=True):
    """Static resource descriptor advertised via resources/list."""

    uri: str
    name: str
    description: str = ""
    mimeType: str = "text/plain"
    reader: Callable[..., Any] | None = None


class ResourceTemplateEntry(FrozenBaseStruct, frozen=True, kw_only=True):
    """Dynamic resource URI template advertised via resources/templates/list."""

    uriTemplate: str
    name: str
    description: str = ""
    mimeType: str = "text/plain"
    reader: Callable[..., Any] | None = None


class PromptArgument(FrozenBaseStruct, frozen=True, kw_only=True, cache_hash=True):
    """Argument declaration for an MCP prompt template."""

    name: str
    description: str = ""
    required: bool = False


class PromptMessage(FrozenBaseStruct, frozen=True, kw_only=True, cache_hash=True):
    """Message item in an instantiated prompt conversation."""

    role: Literal["user", "assistant"] = "user"
    content: TextContent | BlobContent = msgspec.field(default_factory=TextContent)


class PromptEntry(FrozenBaseStruct, frozen=True, kw_only=True):
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
