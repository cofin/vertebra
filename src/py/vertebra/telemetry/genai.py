"""OpenTelemetry GenAI semantic conventions, pricing models, token usage, and trace context.

This module provides standardized attribute keys, token cost accounting,
histogram snapshots, and W3C trace context extraction/injection across HTTP and JSON-RPC.
"""

from collections.abc import Mapping
from typing import Any

from vertebra.telemetry.otel import (
    CostTable,
    GenAiUsage,
    HistogramBucket,
    HistogramSnapshot,
    ModelPricing,
    TraceContext,
    calculate_percentile,
)

GEN_AI_OPERATION_NAME: str = "gen_ai.operation.name"
GEN_AI_SYSTEM: str = "gen_ai.system"
GEN_AI_REQUEST_MODEL: str = "gen_ai.request.model"
GEN_AI_AGENT_NAME: str = "gen_ai.agent.name"
GEN_AI_TOOL_NAME: str = "gen_ai.tool.name"
GEN_AI_TOOL_CALL_ID: str = "gen_ai.tool.call.id"
GEN_AI_USAGE_INPUT_TOKENS: str = "gen_ai.usage.input_tokens"
GEN_AI_USAGE_OUTPUT_TOKENS: str = "gen_ai.usage.output_tokens"
GEN_AI_USAGE_THOUGHT_TOKENS: str = "gen_ai.usage.thought_tokens"
GEN_AI_USAGE_CACHED_INPUT_TOKENS: str = "gen_ai.usage.cached_input_tokens"
GEN_AI_USAGE_COST_USD: str = "gen_ai.usage.cost_usd"

MCP_METHOD_NAME: str = "mcp.method.name"
MCP_RESOURCE_URI: str = "mcp.resource.uri"
MCP_SKILL_NAME: str = "mcp.skill.name"
A2A_TASK_ID: str = "a2a.task.id"
A2A_TASK_STATE: str = "a2a.task.state"
VERTEBRA_WORKFLOW_NAME: str = "vertebra.workflow.name"
VERTEBRA_WORKFLOW_NODE: str = "vertebra.workflow.node"


def extract_w3c_trace_context(
    headers: Mapping[str, str] | None = None,
    rpc_params: Mapping[str, Any] | None = None,
) -> TraceContext:
    """Extract or create a W3C TraceContext from HTTP headers or JSON-RPC _meta params."""
    if headers:
        for key in ("traceparent", "x-traceparent"):
            if key in headers:
                parsed = TraceContext.parse(headers[key])
                if parsed is not None:
                    return parsed

    if rpc_params and isinstance(rpc_params, Mapping):
        meta = rpc_params.get("_meta")
        if isinstance(meta, Mapping) and "traceparent" in meta:
            parsed = TraceContext.parse(str(meta["traceparent"]))
            if parsed is not None:
                return parsed

    current = TraceContext.current()
    if current is not None:
        return current.new_child()

    return TraceContext.generate()


def inject_w3c_trace_context(
    ctx: TraceContext,
    headers: dict[str, str] | None = None,
    rpc_params: dict[str, Any] | None = None,
) -> None:
    """Inject a W3C traceparent into HTTP headers and JSON-RPC params._meta."""
    header_val = ctx.format()
    if headers is not None:
        headers["traceparent"] = header_val

    if rpc_params is not None:
        meta = rpc_params.setdefault("_meta", {})
        if isinstance(meta, dict):
            meta["traceparent"] = header_val


__all__ = [
    "A2A_TASK_ID",
    "A2A_TASK_STATE",
    "CostTable",
    "GEN_AI_AGENT_NAME",
    "GEN_AI_OPERATION_NAME",
    "GEN_AI_REQUEST_MODEL",
    "GEN_AI_SYSTEM",
    "GEN_AI_TOOL_CALL_ID",
    "GEN_AI_TOOL_NAME",
    "GEN_AI_USAGE_CACHED_INPUT_TOKENS",
    "GEN_AI_USAGE_COST_USD",
    "GEN_AI_USAGE_INPUT_TOKENS",
    "GEN_AI_USAGE_OUTPUT_TOKENS",
    "GEN_AI_USAGE_THOUGHT_TOKENS",
    "GenAiUsage",
    "HistogramBucket",
    "HistogramSnapshot",
    "MCP_METHOD_NAME",
    "MCP_RESOURCE_URI",
    "MCP_SKILL_NAME",
    "ModelPricing",
    "VERTEBRA_WORKFLOW_NAME",
    "VERTEBRA_WORKFLOW_NODE",
    "calculate_percentile",
    "extract_w3c_trace_context",
    "inject_w3c_trace_context",
]
