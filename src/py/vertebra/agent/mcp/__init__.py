"""Model Context Protocol (MCP) 2026-07-28 implementations for Vertebra.

This package provides native registries, handlers, Streamable HTTP/stdio transports,
progressive skills catalog, durable tasks, and controllers.
"""

from vertebra.agent.mcp.bridge import run_stdio_http_bridge
from vertebra.agent.mcp.client import (
    MCPStreamableHttpClient,
    decode_mcp_header_value,
    encode_mcp_header_value,
    mcp_request_headers,
)
from vertebra.agent.mcp.controllers import (
    PromptController,
    SkillController,
    prompt,
    resource,
)
from vertebra.agent.mcp.models import (
    HEADER_MISMATCH,
    MCP_PROTOCOL_VERSION,
    UNSUPPORTED_PROTOCOL_VERSION,
    BlobContent,
    ContentBlock,
    MCPConfig,
    PromptArgument,
    PromptEntry,
    PromptMessage,
    ResourceEntry,
    ResourceLinkContent,
    ResourceTemplateEntry,
    TextContent,
    ToolCallOutput,
)
from vertebra.agent.mcp.registry import MCPRegistry
from vertebra.agent.mcp.routes import MCPHttpHandler
from vertebra.agent.mcp.service import MCPService
from vertebra.agent.mcp.skills import (
    SKILL_FILE_NAME,
    SKILL_URI_PREFIX,
    FilesystemSkillSource,
    MCPSkillsConfig,
    Skill,
    SkillCatalog,
    SkillFile,
    SkillIntegrityError,
    SkillSource,
    parse_skill_frontmatter,
)
from vertebra.agent.mcp.stdio import run_stdio_server
from vertebra.agent.mcp.tasks import (
    MCPTaskConfig,
    MCPTaskStatus,
    MCPTaskStore,
    TaskLookupError,
    TaskRecord,
    TaskStateError,
)

__all__ = [
    "HEADER_MISMATCH",
    "MCP_PROTOCOL_VERSION",
    "UNSUPPORTED_PROTOCOL_VERSION",
    "BlobContent",
    "ContentBlock",
    "FilesystemSkillSource",
    "MCPConfig",
    "MCPHttpHandler",
    "MCPRegistry",
    "MCPService",
    "MCPSkillsConfig",
    "MCPStreamableHttpClient",
    "MCPTaskConfig",
    "MCPTaskStatus",
    "MCPTaskStore",
    "PromptArgument",
    "PromptController",
    "PromptEntry",
    "PromptMessage",
    "ResourceEntry",
    "ResourceLinkContent",
    "ResourceTemplateEntry",
    "SKILL_FILE_NAME",
    "SKILL_URI_PREFIX",
    "Skill",
    "SkillCatalog",
    "SkillController",
    "SkillFile",
    "SkillIntegrityError",
    "SkillSource",
    "TaskLookupError",
    "TaskRecord",
    "TaskStateError",
    "TextContent",
    "ToolCallOutput",
    "decode_mcp_header_value",
    "encode_mcp_header_value",
    "mcp_request_headers",
    "parse_skill_frontmatter",
    "prompt",
    "resource",
    "run_stdio_http_bridge",
    "run_stdio_server",
]
