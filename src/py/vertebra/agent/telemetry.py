"""Universal OpenTelemetry gen_ai.* spans, token metrics, and audit logging.

This module provides the AgentTelemetryRecorder that automatically instruments
MCP tools, resources, prompts, skills, A2A messages/tasks, agent turns, SpecTree
nodes, and DynamicWorkflow steps with OpenTelemetry spans, metrics, and non-blocking audit events.
"""

import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any, Literal

import msgspec

from vertebra.telemetry.genai import (
    A2A_TASK_ID,
    A2A_TASK_STATE,
    GEN_AI_AGENT_NAME,
    GEN_AI_OPERATION_NAME,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_SYSTEM,
    GEN_AI_TOOL_CALL_ID,
    GEN_AI_TOOL_NAME,
    GEN_AI_USAGE_CACHED_INPUT_TOKENS,
    GEN_AI_USAGE_COST_USD,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GEN_AI_USAGE_THOUGHT_TOKENS,
    MCP_METHOD_NAME,
    MCP_RESOURCE_URI,
    MCP_SKILL_NAME,
    VERTEBRA_WORKFLOW_NAME,
    VERTEBRA_WORKFLOW_NODE,
    CostTable,
    GenAiUsage,
    extract_w3c_trace_context,
)
from vertebra.telemetry.logger import LogSink
from vertebra.telemetry.otel import GenAiRecorder, SpanData, SpanExporter, TraceContext
from vertebra.utils.serializers import encode_json_bytes


class AgentAuditEvent(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Structured audit event emitted to LogSink for every completed span."""

    event_type: str
    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    duration_ns: int = 0
    status: str = "ok"
    timestamp: float = msgspec.field(default_factory=time.time)
    attributes: dict[str, Any] = msgspec.field(default_factory=dict)


class AgentTelemetryRecorder:
    """Universal telemetry recorder capturing gen_ai spans, metrics, and audit logs."""

    def __init__(
        self,
        span_exporter: SpanExporter | None = None,
        log_sink: LogSink | None = None,
        genai_recorder: GenAiRecorder | None = None,
        cost_table: CostTable | None = None,
    ) -> None:
        self.span_exporter = span_exporter if span_exporter is not None else SpanExporter()
        self.log_sink = log_sink if log_sink is not None else LogSink()
        self.cost_table = cost_table if cost_table is not None else CostTable()
        self.genai_recorder = (
            genai_recorder if genai_recorder is not None else GenAiRecorder(cost_table=self.cost_table)
        )

    @contextmanager
    def span_rpc(
        self,
        protocol: Literal["mcp", "a2a"],
        method: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> Iterator[TraceContext]:
        """Span context manager for incoming MCP or A2A JSON-RPC requests."""
        parent_ctx = TraceContext.current()
        if headers or params:
            ctx = extract_w3c_trace_context(headers, params)
        elif parent_ctx is not None:
            ctx = parent_ctx.new_child()
        else:
            ctx = TraceContext.generate()

        TraceContext.set_current(ctx)
        start_ns = time.perf_counter_ns()
        attributes: dict[str, Any] = {
            "rpc.system": protocol,
            "rpc.method": method,
            MCP_METHOD_NAME: method,
        }

        try:
            yield ctx
            self._record_lifecycle(f"{protocol}.rpc.{method}", ctx, parent_ctx, start_ns, attributes)
        except BaseException as exc:
            self._record_lifecycle(f"{protocol}.rpc.{method}", ctx, parent_ctx, start_ns, attributes, exc)
            raise
        finally:
            TraceContext.set_current(parent_ctx)

    @contextmanager
    def span_tool(
        self,
        tool_name: str,
        call_id: str,
        *,
        agent_name: str | None = None,
    ) -> Iterator[TraceContext]:
        """Span context manager for tool invocation execution."""
        parent_ctx = TraceContext.current()
        ctx = parent_ctx.new_child() if parent_ctx is not None else TraceContext.generate()
        TraceContext.set_current(ctx)
        start_ns = time.perf_counter_ns()
        attributes: dict[str, Any] = {
            GEN_AI_OPERATION_NAME: "tool",
            GEN_AI_TOOL_NAME: tool_name,
            GEN_AI_TOOL_CALL_ID: call_id,
        }
        if agent_name:
            attributes[GEN_AI_AGENT_NAME] = agent_name

        try:
            yield ctx
            self._record_lifecycle(f"gen_ai.tool.{tool_name}", ctx, parent_ctx, start_ns, attributes)
        except BaseException as exc:
            self._record_lifecycle(f"gen_ai.tool.{tool_name}", ctx, parent_ctx, start_ns, attributes, exc)
            raise
        finally:
            TraceContext.set_current(parent_ctx)

    @contextmanager
    def span_resource(self, uri: str) -> Iterator[TraceContext]:
        """Span context manager for MCP resource reading."""
        parent_ctx = TraceContext.current()
        ctx = parent_ctx.new_child() if parent_ctx is not None else TraceContext.generate()
        TraceContext.set_current(ctx)
        start_ns = time.perf_counter_ns()
        attributes: dict[str, Any] = {
            MCP_RESOURCE_URI: uri,
        }

        try:
            yield ctx
            self._record_lifecycle("mcp.resource.read", ctx, parent_ctx, start_ns, attributes)
        except BaseException as exc:
            self._record_lifecycle("mcp.resource.read", ctx, parent_ctx, start_ns, attributes, exc)
            raise
        finally:
            TraceContext.set_current(parent_ctx)

    @contextmanager
    def span_prompt(self, prompt_name: str) -> Iterator[TraceContext]:
        """Span context manager for MCP prompt retrieval."""
        parent_ctx = TraceContext.current()
        ctx = parent_ctx.new_child() if parent_ctx is not None else TraceContext.generate()
        TraceContext.set_current(ctx)
        start_ns = time.perf_counter_ns()
        attributes: dict[str, Any] = {
            "mcp.prompt.name": prompt_name,
        }

        try:
            yield ctx
            self._record_lifecycle("mcp.prompt.get", ctx, parent_ctx, start_ns, attributes)
        except BaseException as exc:
            self._record_lifecycle("mcp.prompt.get", ctx, parent_ctx, start_ns, attributes, exc)
            raise
        finally:
            TraceContext.set_current(parent_ctx)

    @contextmanager
    def span_skill(self, skill_name: str, uri: str) -> Iterator[TraceContext]:
        """Span context manager for MCP skill loading and integrity check."""
        parent_ctx = TraceContext.current()
        ctx = parent_ctx.new_child() if parent_ctx is not None else TraceContext.generate()
        TraceContext.set_current(ctx)
        start_ns = time.perf_counter_ns()
        attributes: dict[str, Any] = {
            MCP_SKILL_NAME: skill_name,
            MCP_RESOURCE_URI: uri,
        }

        try:
            yield ctx
            self._record_lifecycle("mcp.skill.load", ctx, parent_ctx, start_ns, attributes)
        except BaseException as exc:
            self._record_lifecycle("mcp.skill.load", ctx, parent_ctx, start_ns, attributes, exc)
            raise
        finally:
            TraceContext.set_current(parent_ctx)

    @contextmanager
    def span_model(
        self,
        model: str,
        *,
        agent_name: str | None = None,
        system: str = "google_genai",
    ) -> Iterator[dict[str, Any]]:
        """Span context manager for an LLM generation call, yielding a mutable usage dict."""
        parent_ctx = TraceContext.current()
        ctx = parent_ctx.new_child() if parent_ctx is not None else TraceContext.generate()
        TraceContext.set_current(ctx)
        start_ns = time.perf_counter_ns()
        time.time()
        attributes: dict[str, Any] = {
            GEN_AI_SYSTEM: system,
            GEN_AI_REQUEST_MODEL: model,
            GEN_AI_OPERATION_NAME: "chat",
        }
        if agent_name:
            attributes[GEN_AI_AGENT_NAME] = agent_name

        usage_box: dict[str, Any] = {"usage": None}

        try:
            yield usage_box
            duration_s = max(0.000001, (time.perf_counter_ns() - start_ns) / 1_000_000_000.0)
            usage: GenAiUsage | None = usage_box.get("usage")
            cost_usd = 0.0

            if usage is not None:
                attributes[GEN_AI_USAGE_INPUT_TOKENS] = usage.input_tokens
                attributes[GEN_AI_USAGE_OUTPUT_TOKENS] = usage.output_tokens
                attributes[GEN_AI_USAGE_THOUGHT_TOKENS] = usage.thought_tokens
                attributes[GEN_AI_USAGE_CACHED_INPUT_TOKENS] = usage.cached_input_tokens
                cost_usd = self.cost_table.compute_cost(model, usage)
                attributes[GEN_AI_USAGE_COST_USD] = cost_usd

                self.genai_recorder.record_generation(
                    model=model,
                    operation="chat",
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                    duration_seconds=duration_s,
                    thought_tokens=usage.thought_tokens,
                    cached_input_tokens=usage.cached_input_tokens,
                    usd_cost=cost_usd,
                    agent_name=agent_name,
                    status="ok",
                )
            else:
                self.genai_recorder.record_generation(
                    model=model,
                    operation="chat",
                    input_tokens=0,
                    output_tokens=0,
                    duration_seconds=duration_s,
                    agent_name=agent_name,
                    status="ok",
                )

            self._record_lifecycle("gen_ai.chat", ctx, parent_ctx, start_ns, attributes)
        except BaseException as exc:
            duration_s = max(0.000001, (time.perf_counter_ns() - start_ns) / 1_000_000_000.0)
            self.genai_recorder.record_generation(
                model=model,
                operation="chat",
                input_tokens=0,
                output_tokens=0,
                duration_seconds=duration_s,
                agent_name=agent_name,
                status="error",
            )
            self._record_lifecycle("gen_ai.chat", ctx, parent_ctx, start_ns, attributes, exc)
            raise
        finally:
            TraceContext.set_current(parent_ctx)

    @contextmanager
    def span_turn(
        self,
        agent_name: str,
        session_id: str,
        turn_id: str,
    ) -> Iterator[TraceContext]:
        """Span context manager for an agent conversation turn."""
        parent_ctx = TraceContext.current()
        ctx = parent_ctx.new_child() if parent_ctx is not None else TraceContext.generate()
        TraceContext.set_current(ctx)
        start_ns = time.perf_counter_ns()
        attributes: dict[str, Any] = {
            GEN_AI_OPERATION_NAME: "agent_turn",
            GEN_AI_AGENT_NAME: agent_name,
            "agent.session_id": session_id,
            "agent.turn_id": turn_id,
        }

        try:
            yield ctx
            self._record_lifecycle("gen_ai.agent.turn", ctx, parent_ctx, start_ns, attributes)
        except BaseException as exc:
            self._record_lifecycle("gen_ai.agent.turn", ctx, parent_ctx, start_ns, attributes, exc)
            raise
        finally:
            TraceContext.set_current(parent_ctx)

    @contextmanager
    def span_spectree_node(
        self,
        tree_id: str,
        node_id: str,
        title: str,
    ) -> Iterator[TraceContext]:
        """Span context manager for SpecTree node execution."""
        parent_ctx = TraceContext.current()
        ctx = parent_ctx.new_child() if parent_ctx is not None else TraceContext.generate()
        TraceContext.set_current(ctx)
        start_ns = time.perf_counter_ns()
        attributes: dict[str, Any] = {
            "spectree.tree_id": tree_id,
            "spectree.node_id": node_id,
            "spectree.title": title,
        }

        try:
            yield ctx
            self._record_lifecycle("gen_ai.spectree.node", ctx, parent_ctx, start_ns, attributes)
        except BaseException as exc:
            self._record_lifecycle("gen_ai.spectree.node", ctx, parent_ctx, start_ns, attributes, exc)
            raise
        finally:
            TraceContext.set_current(parent_ctx)

    @contextmanager
    def span_workflow_step(
        self,
        workflow_name: str,
        run_id: str,
        node_name: str,
    ) -> Iterator[TraceContext]:
        """Span context manager for DynamicWorkflow step execution."""
        parent_ctx = TraceContext.current()
        ctx = parent_ctx.new_child() if parent_ctx is not None else TraceContext.generate()
        TraceContext.set_current(ctx)
        start_ns = time.perf_counter_ns()
        attributes: dict[str, Any] = {
            VERTEBRA_WORKFLOW_NAME: workflow_name,
            "workflow.run_id": run_id,
            VERTEBRA_WORKFLOW_NODE: node_name,
        }

        try:
            yield ctx
            self._record_lifecycle("gen_ai.workflow.step", ctx, parent_ctx, start_ns, attributes)
        except BaseException as exc:
            self._record_lifecycle("gen_ai.workflow.step", ctx, parent_ctx, start_ns, attributes, exc)
            raise
        finally:
            TraceContext.set_current(parent_ctx)

    @contextmanager
    def span_a2a_task(self, task_id: str, state: str) -> Iterator[TraceContext]:
        """Span context manager for A2A task lifecycle transition."""
        parent_ctx = TraceContext.current()
        ctx = parent_ctx.new_child() if parent_ctx is not None else TraceContext.generate()
        TraceContext.set_current(ctx)
        start_ns = time.perf_counter_ns()
        attributes: dict[str, Any] = {
            A2A_TASK_ID: task_id,
            A2A_TASK_STATE: state,
        }

        try:
            yield ctx
            self._record_lifecycle("a2a.task.transition", ctx, parent_ctx, start_ns, attributes)
        except BaseException as exc:
            self._record_lifecycle("a2a.task.transition", ctx, parent_ctx, start_ns, attributes, exc)
            raise
        finally:
            TraceContext.set_current(parent_ctx)

    def _record_lifecycle(
        self,
        name: str,
        ctx: TraceContext,
        parent_ctx: TraceContext | None,
        start_ns: int,
        attributes: dict[str, Any],
        error: BaseException | None = None,
    ) -> None:
        """Finalize and export span and audit event."""
        duration_ns = time.perf_counter_ns() - start_ns
        status_code = 200 if error is None else 500
        status_str = "ok" if error is None else "error"

        if error is not None:
            attributes["error.type"] = type(error).__name__
            attributes["error.message"] = str(error)

        parent_span_id = parent_ctx.span_id if parent_ctx is not None else None

        span = SpanData(
            name=name,
            trace_id=ctx.trace_id,
            span_id=ctx.span_id,
            parent_span_id=parent_span_id,
            start_ns=start_ns,
            duration_ns=duration_ns,
            status_code=status_code,
            attributes=attributes,
        )
        self.span_exporter.record_span(span)

        audit_event = AgentAuditEvent(
            event_type=name,
            trace_id=ctx.trace_id,
            span_id=ctx.span_id,
            parent_span_id=parent_span_id,
            duration_ns=duration_ns,
            status=status_str,
            attributes=attributes,
        )
        self.log_sink.write_bytes(encode_json_bytes(audit_event))


__all__ = [
    "AgentAuditEvent",
    "AgentTelemetryRecorder",
]
