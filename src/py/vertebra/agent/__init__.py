"""Vertebra Extendable Agentic Harness.

This package provides native MCP 2026-07-28, A2A 1.0, multi-turn AgentRuntime,
SpecTree task decomposition, DynamicWorkflow DAG orchestration, pluggable session stores,
streaming chat controllers, and GenAI telemetry.
"""

from typing import TYPE_CHECKING, Any

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.agent import tools
    from vertebra.agent.tools import build_tools

__all__ = (
    "A2AConfig",
    "A2AHttpHandler",
    "A2AService",
    "A2ATaskStore",
    "A2aAgentBridge",
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
    "BudgetExceededError",
    "CacheSessionStore",
    "CallerIdentity",
    "CancellationToken",
    "ChatRequest",
    "ConfirmationDecisionRequest",
    "ConfirmationGate",
    "ConfirmationTicket",
    "CursorPage",
    "DynamicWorkflow",
    "GoogleGenAIProvider",
    "GuardCallable",
    "GuardChain",
    "GuardProtocol",
    "JSONRPCError",
    "JSONRPCErrorException",
    "JSONRPCRequest",
    "JSONRPCResponse",
    "JSONRPCRouter",
    "McpToolBridge",
    "MemorySessionStore",
    "ModelDelta",
    "ModelProvider",
    "ObjectStoreSessionStore",
    "PermissionDeniedError",
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
    "SpecNode",
    "SpecNodeStatus",
    "SpecTree",
    "SseFrame",
    "SseReplayBuffer",
    "SseStreamBroker",
    "StepResult",
    "TRANSFER_TOOL_NAME",
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
    "TurnRequest",
    "TurnResponse",
    "TurnResult",
    "WorkflowCheckpoint",
    "WorkflowContext",
    "WorkflowEngine",
    "WorkflowLiveState",
    "WorkflowNode",
    "a2a",
    "a2a_card_to_mcp_tools",
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
        "JSONRPCErrorException",
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
        "A2aAgentBridge",
        "McpToolBridge",
        "a2a_card_to_mcp_tools",
        "mcp_service_to_a2a",
    }
)

_SPEC_EXPORTS = frozenset(
    {
        "Agent",
        "AgentGroup",
        "AgentMessage",
        "SpecNode",
        "SpecNodeStatus",
        "SpecTree",
        "TRANSFER_TOOL_NAME",
    }
)

_RUNTIME_EXPORTS = frozenset(
    {
        "AgentRuntime",
        "AgentStreamFrame",
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
        "DynamicWorkflow",
        "StepResult",
        "WorkflowCheckpoint",
        "WorkflowContext",
        "WorkflowEngine",
        "WorkflowLiveState",
        "WorkflowNode",
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

    if name in _SPEC_EXPORTS:
        mod = ensure_module("vertebra.agent.spec")
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

    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
