"""Vertebra Extendable Agentic Harness.

Provides native MCP 2026-07-28, A2A 1.0, multi-turn AgentRuntime,
PlanTree task decomposition, Workflow DAG orchestration, pluggable session stores,
streaming chat controllers, and GenAI telemetry.
"""

from typing import TYPE_CHECKING, Any

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.agent import tools
    from vertebra.agent.knowledge_catalog import (
        AsyncKnowledgeCatalogClient,
        CatalogEntryRecord,
        CatalogSyncError,
        CatalogSyncSummary,
        KnowledgeCatalogConfig,
        SyncKnowledgeCatalogClient,
    )
    from vertebra.agent.okf import (
        OkfBundle,
        OkfDocument,
        OkfSource,
        OkfValidationError,
    )
    from vertebra.agent.tools import build_tools

__all__ = (
    "A2AAgentBridge",
    "A2AConfig",
    "A2AHttpHandler",
    "A2AService",
    "A2ATaskStore",
    "ANONYMOUS_OWNER",
    "Agent",
    "AgentAuditEvent",
    "AgentCard",
    "AgentCardSigner",
    "AgentChatController",
    "AgentContext",
    "AgentGroup",
    "AgentMessage",
    "AgentPlugin",
    "AgentPluginConfig",
    "AgentRuntime",
    "AgentSessionStore",
    "AgentStreamFrame",
    "AgentTelemetryRecorder",
    "AlchemySessionStore",
    "AsyncKnowledgeCatalogClient",
    "BudgetExceededError",
    "CacheSessionStore",
    "CallerIdentity",
    "CancellationToken",
    "CatalogEntryRecord",
    "CatalogSyncError",
    "CatalogSyncSummary",
    "ChatRequest",
    "ConfirmationCapacityError",
    "ConfirmationDecisionRequest",
    "ConfirmationGate",
    "ConfirmationTicket",
    "CursorPage",
    "GoogleGenAIProvider",
    "GuardCallable",
    "GuardChain",
    "GuardProtocol",
    "JSONRPCError",
    "JSONRPCException",
    "JSONRPCRequest",
    "JSONRPCResponse",
    "JSONRPCRouter",
    "KnowledgeCatalogConfig",
    "MCPCore",
    "MCPHttpRequest",
    "MCPHttpResponse",
    "MCPToolBridge",
    "MemorySessionStore",
    "ModelDelta",
    "ModelProvider",
    "ObjectStoreSessionStore",
    "OkfBundle",
    "OkfDocument",
    "OkfSource",
    "OkfValidationError",
    "PermissionDeniedError",
    "PlanNode",
    "PlanNodeStatus",
    "PlanTree",
    "ProgressCallback",
    "ProgressReporter",
    "PushNotificationDispatcher",
    "QueuedTurnTicket",
    "RateLimitExceededError",
    "RequireAuthenticated",
    "RequireRoles",
    "RequireScopes",
    "RetryPolicy",
    "SQLSpecSessionStore",
    "SchemaBuilder",
    "SchemaDigestCache",
    "SessionHistory",
    "SessionNotFoundError",
    "SseFrame",
    "SseReplayBuffer",
    "SseStreamBroker",
    "SyncKnowledgeCatalogClient",
    "TRANSFER_TOOL_NAME",
    "TenantIsolationGuard",
    "TokenBucketGuard",
    "Tool",
    "ToolAnnotations",
    "ToolArgumentError",
    "ToolCall",
    "ToolExposure",
    "ToolRegistry",
    "ToolResult",
    "ToolSpec",
    "TurnBudget",
    "TurnRequest",
    "TurnResponse",
    "TurnResult",
    "Workflow",
    "WorkflowCheckpoint",
    "WorkflowContext",
    "WorkflowEngine",
    "WorkflowLiveState",
    "WorkflowStep",
    "WorkflowStepResult",
    "a2a",
    "a2a_card_to_mcp_tools",
    "authorize",
    "bind_agent_context",
    "bridges",
    "build_tools",
    "current_agent_context",
    "decode_cursor",
    "encode_cursor",
    "execute_tool_calls",
    "mcp",
    "mcp_service_to_a2a",
    "paginate_sequence",
    "tool",
    "tools",
)

_TOOLS_EXPORTS = frozenset({"build_tools"})

_CORE_EXPORTS = frozenset(
    {
        "AgentContext",
        "BudgetExceededError",
        "CallerIdentity",
        "CancellationToken",
        "CursorPage",
        "GuardCallable",
        "GuardChain",
        "GuardProtocol",
        "JSONRPCError",
        "JSONRPCException",
        "JSONRPCRequest",
        "JSONRPCResponse",
        "JSONRPCRouter",
        "PermissionDeniedError",
        "ProgressCallback",
        "ProgressReporter",
        "RateLimitExceededError",
        "RequireAuthenticated",
        "RequireRoles",
        "RequireScopes",
        "SchemaBuilder",
        "SchemaDigestCache",
        "SseFrame",
        "SseReplayBuffer",
        "SseStreamBroker",
        "TenantIsolationGuard",
        "TokenBucketGuard",
        "Tool",
        "ToolAnnotations",
        "ToolArgumentError",
        "ToolCall",
        "ToolExposure",
        "ToolResult",
        "ToolSpec",
        "TurnBudget",
        "authorize",
        "bind_agent_context",
        "current_agent_context",
        "decode_cursor",
        "encode_cursor",
        "execute_tool_calls",
        "paginate_sequence",
        "tool",
    }
)

_TELEMETRY_EXPORTS = frozenset(
    {
        "AgentAuditEvent",
        "AgentTelemetryRecorder",
    }
)

_A2A_EXPORTS = frozenset(
    {
        "A2AConfig",
        "A2AHttpHandler",
        "A2AService",
        "A2ATaskStore",
        "AgentCard",
        "AgentCardSigner",
        "PushNotificationDispatcher",
    }
)

_BRIDGE_EXPORTS = frozenset(
    {
        "A2AAgentBridge",
        "MCPToolBridge",
        "a2a_card_to_mcp_tools",
        "mcp_service_to_a2a",
    }
)

_AGENT_EXPORTS = frozenset(
    {
        "Agent",
        "AgentGroup",
        "AgentMessage",
        "TRANSFER_TOOL_NAME",
    }
)

_PLANNING_EXPORTS = frozenset(
    {
        "PlanNode",
        "PlanNodeStatus",
        "PlanTree",
    }
)

_RUNTIME_EXPORTS = frozenset(
    {
        "AgentRuntime",
        "AgentStreamFrame",
        "ConfirmationCapacityError",
        "ConfirmationGate",
        "ConfirmationTicket",
        "GoogleGenAIProvider",
        "ModelDelta",
        "ModelProvider",
        "RetryPolicy",
        "TurnRequest",
        "TurnResponse",
    }
)

_WORKFLOW_EXPORTS = frozenset(
    {
        "Workflow",
        "WorkflowCheckpoint",
        "WorkflowContext",
        "WorkflowEngine",
        "WorkflowLiveState",
        "WorkflowStep",
        "WorkflowStepResult",
    }
)

_SESSION_EXPORTS = frozenset(
    {
        "ANONYMOUS_OWNER",
        "AgentSessionStore",
        "AlchemySessionStore",
        "CacheSessionStore",
        "MemorySessionStore",
        "ObjectStoreSessionStore",
        "SQLSpecSessionStore",
    }
)

_CONTROLLER_EXPORTS = frozenset(
    {
        "AgentChatController",
        "ChatRequest",
        "ConfirmationDecisionRequest",
        "QueuedTurnTicket",
        "SessionHistory",
        "SessionNotFoundError",
        "TurnResult",
    }
)

_PLUGIN_EXPORTS = frozenset(
    {
        "AgentPlugin",
        "AgentPluginConfig",
    }
)

_OKF_EXPORTS = frozenset(
    {
        "OkfBundle",
        "OkfDocument",
        "OkfSource",
        "OkfValidationError",
    }
)

_KNOWLEDGE_CATALOG_EXPORTS = frozenset(
    {
        "AsyncKnowledgeCatalogClient",
        "CatalogEntryRecord",
        "CatalogSyncError",
        "CatalogSyncSummary",
        "KnowledgeCatalogConfig",
        "SyncKnowledgeCatalogClient",
    }
)


def __getattr__(name: str) -> Any:
    """Lazy-load agent package exports on first attribute access."""
    if name == "tools":
        mod = ensure_module("vertebra.agent.tools")
        globals()["tools"] = mod
        return mod

    if name in _TOOLS_EXPORTS:
        mod = ensure_module("vertebra.agent.tools")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    if name == "a2a":
        mod = ensure_module("vertebra.agent.a2a")
        globals()["a2a"] = mod
        return mod

    if name == "bridges":
        mod = ensure_module("vertebra.agent.bridges")
        globals()["bridges"] = mod
        return mod

    if name == "mcp":
        mod = ensure_module("vertebra.agent.mcp")
        globals()["mcp"] = mod
        return mod

    if name in ("ToolRegistry", "MCPCore", "MCPHttpRequest", "MCPHttpResponse"):
        mod = ensure_module("vertebra.agent.mcp")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    if name in _CORE_EXPORTS:
        mod = ensure_module("vertebra.agent.core")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    if name in _TELEMETRY_EXPORTS:
        mod = ensure_module("vertebra.agent.telemetry")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    if name in _A2A_EXPORTS:
        mod = ensure_module("vertebra.agent.a2a")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    if name in _BRIDGE_EXPORTS:
        mod = ensure_module("vertebra.agent.bridges")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    if name in _AGENT_EXPORTS:
        mod = ensure_module("vertebra.agent.agents")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    if name in _PLANNING_EXPORTS:
        mod = ensure_module("vertebra.agent.planning")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    if name in _RUNTIME_EXPORTS:
        mod = ensure_module("vertebra.agent.runtime")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    if name in _WORKFLOW_EXPORTS:
        mod = ensure_module("vertebra.agent.workflow")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    if name in _SESSION_EXPORTS:
        mod = ensure_module("vertebra.agent.sessions")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    if name in _CONTROLLER_EXPORTS:
        mod = ensure_module("vertebra.agent.controller")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    if name in _PLUGIN_EXPORTS:
        mod = ensure_module("vertebra.agent.plugin")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    if name in _OKF_EXPORTS:
        mod = ensure_module("vertebra.agent.okf")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    if name in _KNOWLEDGE_CATALOG_EXPORTS:
        mod = ensure_module("vertebra.agent.knowledge_catalog")
        val = getattr(mod, name)
        globals()[name] = val
        return val

    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
