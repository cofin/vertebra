"""Multi-turn AgentRuntime, ConfirmationGate, ModelProvider, and streaming frames.

This module provides the core execution harness for autonomous agents and agent groups,
managing model generation, parallel tool execution, retry backoff, and human-in-the-loop gates.
"""

import dataclasses
import random
import secrets
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Literal, Protocol, cast, runtime_checkable

import anyio
import msgspec

from vertebra.agent.core.context import AgentContext
from vertebra.agent.core.guards import TurnBudget
from vertebra.agent.core.schema import Tool, ToolCall, ToolResult, execute_tool_calls
from vertebra.agent.core.sse import SseFrame
from vertebra.agent.spec import TRANSFER_TOOL_NAME, Agent, AgentGroup, AgentMessage
from vertebra.agent.telemetry import AgentTelemetryRecorder
from vertebra.telemetry.genai import GenAiUsage
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json, encode_json_bytes


class ModelDelta(msgspec.Struct, frozen=True, gc=False, kw_only=True, omit_defaults=True):
    """Incremental chunk produced by a model provider during chat streaming."""

    event_type: Literal["delta", "thought", "tool_call", "usage"]
    """Discriminator identifying text delta, reasoning thought, tool call, or token usage."""
    text: str = ""
    """Incremental text or reasoning content emitted for `delta` and `thought` events."""
    tool_name: str | None = None
    """Target tool identifier when `event_type` is `"tool_call"`."""
    call_id: str | None = None
    """Unique correlation identifier for a `tool_call` invocation."""
    arguments: dict[str, Any] | None = None
    """Decoded JSON argument mapping supplied for a `tool_call` event."""
    usage: GenAiUsage | None = None
    """Token consumption metrics attached to `usage` events."""
    provider_metadata: dict[str, Any] | None = None
    """Provider-specific response metadata."""


@runtime_checkable
class ModelProvider(Protocol):
    """Protocol for pluggable LLM generation providers."""

    def stream_chat(
        self,
        messages: Sequence[AgentMessage],
        tools: Sequence[Tool],
        system_instruction: str,
        *,
        context: AgentContext | None = None,
    ) -> AsyncIterator[ModelDelta]:
        """Stream model responses given conversation transcript and available tools."""
        ...


class GoogleGenAIProvider:
    """Google Gemini GenAI SDK provider with lazy SDK importing."""

    def __init__(
        self,
        model: str = "gemini-2.5-pro",
        api_key: str | None = None,
        client: Any | None = None,
    ) -> None:
        self.model = model
        self.api_key = api_key
        self._client = client

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        genai_module = import_optional("google.genai")
        if genai_module is None:
            msg = "The 'google-genai' package is required to use GoogleGenAIProvider"
            raise RuntimeError(msg)
        self._client = genai_module.Client(api_key=self.api_key)
        return self._client

    async def stream_chat(
        self,
        messages: Sequence[AgentMessage],
        tools: Sequence[Tool],
        system_instruction: str,
        *,
        context: AgentContext | None = None,
    ) -> AsyncIterator[ModelDelta]:
        """Stream model responses via google-genai client."""
        client = self._get_client()
        prompt_text = "\n".join(m.content for m in messages if m.role == "user")
        response = await client.aio.models.generate_content(
            model=self.model,
            contents=prompt_text,
        )
        yield ModelDelta(event_type="delta", text=response.text or "")
        yield ModelDelta(event_type="usage", usage=GenAiUsage(input_tokens=10, output_tokens=10))


class AgentStreamFrame(msgspec.Struct, frozen=True, gc=False, kw_only=True, omit_defaults=True):
    """Wire frame streamed to subscribers during an active agent turn."""

    turn_id: str
    """Unique identifier of the active conversation turn."""
    event_type: Literal[
        "delta",
        "thought",
        "tool_call",
        "tool_result",
        "agent_transfer",
        "confirmation_required",
        "done",
        "error",
    ]
    """Stream event category mapped to the SSE `event:` header."""
    agent_name: str = ""
    """Name of the agent currently executing the turn."""
    text: str = ""
    """Incremental text content or status message."""
    tool_name: str | None = None
    """Tool name associated with `tool_call`, `tool_result`, or `confirmation_required` frames."""
    call_id: str | None = None
    """Correlation identifier for the tool invocation."""
    payload: dict[str, Any] | None = None
    """Structured tool arguments, result envelope, or confirmation ticket dictionary."""
    usage: GenAiUsage | None = None
    """Cumulative token usage metrics for the turn."""

    def to_sse_frame(self, event_id: str) -> SseFrame:
        """Encode this stream frame into a W3C-compliant SSE frame."""
        return SseFrame(
            id=event_id,
            event=self.event_type,
            data=encode_json(self),
        )


class ConfirmationTicket(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Pending authorization ticket issued for a mutating tool call."""

    token: str
    """URL-safe cryptographic token identifying the confirmation request."""
    tool_name: str
    """Name of the mutating tool awaiting human approval."""
    call_id: str
    """Tool call correlation identifier within the paused turn."""
    arguments: dict[str, Any]
    """Validated arguments bound to the pending tool invocation."""
    owner_id: str | None = None
    """Caller ownership key restricting who may approve or reject the ticket."""
    reason: str = ""
    """Human-readable explanation for the confirmation prompt or decision."""
    status: Literal["pending", "approved", "rejected"] = "pending"
    """Current authorization state of the ticket."""
    created_at_iso: str = ""
    """ISO-8601 UTC timestamp when the ticket was issued."""

    def to_dict(self) -> dict[str, Any]:
        """Convert ticket to standard dictionary."""
        return cast(dict[str, Any], decode_json(encode_json_bytes(self)))


class ConfirmationGate:
    """Human-in-the-loop gate controlling execution of sensitive mutating operations."""

    def __init__(self) -> None:
        self._tickets: dict[str, ConfirmationTicket] = {}
        self._approved_tokens: set[str] = set()

    def request_confirmation(
        self,
        tool_name: str,
        call_id: str,
        arguments: dict[str, Any],
        *,
        owner_id: str | None = None,
        reason: str = "",
    ) -> ConfirmationTicket:
        """Issue a new confirmation ticket for a pending mutating tool invocation."""
        token = secrets.token_urlsafe(16)
        ticket = ConfirmationTicket(
            token=token,
            tool_name=tool_name,
            call_id=call_id,
            arguments=arguments,
            owner_id=owner_id,
            reason=reason,
            status="pending",
            created_at_iso=datetime.now(UTC).isoformat(),
        )
        self._tickets[token] = ticket
        return ticket

    def approve(self, token: str, *, owner_id: str | None = None) -> bool:
        """Approve a pending confirmation ticket.

        Returns:
            True when the ticket exists and ``owner_id`` may decide it; False otherwise, with no
            change to the gate.
        """
        ticket = self._tickets.get(token)
        if ticket is None:
            return False
        if owner_id is not None and ticket.owner_id is not None and ticket.owner_id != owner_id:
            return False
        self._approved_tokens.add(token)
        self._tickets[token] = msgspec.structs.replace(ticket, status="approved")
        return True

    def reject(self, token: str, *, owner_id: str | None = None, reason: str = "") -> bool:
        """Reject a pending confirmation ticket.

        Returns:
            True when the ticket exists and ``owner_id`` may decide it; False otherwise, with no
            change to the gate.
        """
        ticket = self._tickets.get(token)
        if ticket is None:
            return False
        if owner_id is not None and ticket.owner_id is not None and ticket.owner_id != owner_id:
            return False
        self._approved_tokens.discard(token)
        self._tickets[token] = msgspec.structs.replace(ticket, status="rejected", reason=reason or ticket.reason)
        return True

    def is_approved(self, token: str | None, *, owner_id: str | None = None) -> bool:
        """Check whether a token is approved under the specified owner."""
        if not token:
            return False
        if token in self._approved_tokens:
            ticket = self._tickets.get(token)
            if ticket is not None and owner_id is not None and ticket.owner_id is not None:
                return ticket.owner_id == owner_id
            return True
        ticket = self._tickets.get(token)
        if ticket is None or ticket.status != "approved":
            return False
        return not (owner_id is not None and ticket.owner_id is not None and ticket.owner_id != owner_id)

    def is_call_approved(
        self,
        token: str | None,
        *,
        tool_name: str,
        arguments: Mapping[str, Any],
        owner_id: str | None = None,
    ) -> bool:
        """Check whether a token authorizes a specific tool name, exact arguments, and caller owner."""
        if not token or not isinstance(token, str):
            return False
        ticket = self._tickets.get(token)
        if ticket is None or ticket.status != "approved":
            return False
        if ticket.owner_id is not None and ticket.owner_id != owner_id:
            return False
        return ticket.tool_name == tool_name and self.arguments_equal(ticket.arguments, dict(arguments))

    def get_ticket(self, token: str) -> ConfirmationTicket | None:
        """Retrieve ticket details by token."""
        return self._tickets.get(token)

    @staticmethod
    def arguments_equal(left: Any, right: Any) -> bool:
        """Compare approved values recursively without Python cross-type scalar coercion."""
        if type(left) is not type(right):
            return False
        if isinstance(left, Mapping):
            if len(left) != len(right):
                return False
            right_keys = {key: key for key in right}
            return all(
                key in right_keys
                and ConfirmationGate.arguments_equal(key, right_keys[key])
                and ConfirmationGate.arguments_equal(value, right[right_keys[key]])
                for key, value in left.items()
            )
        if isinstance(left, (list, tuple)):
            return len(left) == len(right) and all(
                ConfirmationGate.arguments_equal(a, b) for a, b in zip(left, right, strict=True)
            )
        if isinstance(left, (set, frozenset)):
            right_items = {item: item for item in right}
            return len(left) == len(right) and all(
                item in right_items and ConfirmationGate.arguments_equal(item, right_items[item]) for item in left
            )
        if isinstance(left, msgspec.Struct):
            return all(
                ConfirmationGate.arguments_equal(getattr(left, field.name), getattr(right, field.name))
                for field in msgspec.structs.fields(left)
            )
        if dataclasses.is_dataclass(left) and not isinstance(left, type):
            return all(
                ConfirmationGate.arguments_equal(getattr(left, field.name), getattr(right, field.name))
                for field in dataclasses.fields(left)
            )
        return bool(left == right)


class RetryPolicy(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Exponential backoff with jitter retry configuration."""

    max_attempts: int = 3
    """Maximum number of model generation attempts before failing the turn."""
    base_delay_seconds: float = 0.25
    """Initial backoff delay in seconds before the first retry."""
    max_delay_seconds: float = 10.0
    """Upper bound in seconds for exponential backoff delay."""
    jitter_ratio: float = 0.1
    """Proportional uniform random jitter (`0.0` to `1.0`) applied to each backoff delay."""

    def compute_delay(self, attempt: int) -> float:
        """Compute delay in seconds for the given zero-indexed retry attempt."""
        delay = min(self.max_delay_seconds, self.base_delay_seconds * (2**attempt))
        jitter = delay * self.jitter_ratio
        return float(max(0.0, delay + random.uniform(-jitter, jitter)))


class TurnRequest(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Input payload initiating a conversation turn."""

    prompt: str = ""
    """User prompt text initiating or continuing the conversation turn."""
    message: str | None = None
    """Optional alias for `prompt` accepted from chat payloads."""
    session_id: str = ""
    """Session identifier used to load and persist conversation transcripts."""
    turn_id: str | None = None
    """Explicit turn identifier, or `None` to generate a random turn ID."""
    messages: tuple[AgentMessage, ...] = ()
    """Prior conversation messages supplied by the caller."""
    history: tuple[AgentMessage, ...] = ()
    """Optional alias for `messages` containing prior conversation history."""
    agent_name: str | None = None
    """Target specialist agent name when running against an `AgentGroup`."""
    confirmation_token: str | None = None
    """Approved `ConfirmationTicket` token resuming a gated tool execution."""
    budget: TurnBudget | None = None
    """Per-turn step, token, cost, and wall-clock budget override."""
    metadata: dict[str, Any] = msgspec.field(default_factory=dict)
    """Arbitrary caller metadata attached to the turn request."""

    @property
    def effective_prompt(self) -> str:
        """Return the user message text from prompt or message alias."""
        return self.message if self.message is not None else self.prompt

    @property
    def effective_messages(self) -> tuple[AgentMessage, ...]:
        """Return the transcript history from history or messages."""
        return self.history if self.history else self.messages


class TurnResponse(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Result of completing or pausing an agent conversation turn."""

    turn_id: str
    """Unique identifier of the executed turn."""
    session_id: str
    """Session identifier associated with the conversation transcript."""
    agent_name: str
    """Name of the agent that produced the final turn state."""
    text: str
    """Concatenated assistant response text generated during the turn."""
    messages: tuple[AgentMessage, ...] = ()
    """Complete updated conversation transcript including tool calls and results."""
    tool_calls: tuple[ToolCall, ...] = ()
    """All tool invocations requested by the model during the turn."""
    tool_results: tuple[ToolResult, ...] = ()
    """All tool execution results recorded during the turn."""
    confirmation_ticket: ConfirmationTicket | None = None
    """Pending confirmation ticket when `status` is `"confirmation_required"`."""
    status: Literal["completed", "confirmation_required", "budget_exceeded", "error"] = "completed"
    """Terminal or paused outcome status of the turn."""
    usage: GenAiUsage | None = None
    """Aggregated input, output, thought, and cached token usage across all steps."""
    cost_usd: float = 0.0
    """Estimated monetary cost of the turn in USD."""

    @property
    def response(self) -> str:
        """Alias returning generated response text."""
        return self.text

    @property
    def confirmation_token(self) -> str | None:
        """Return the pending confirmation token if paused on gate."""
        return self.confirmation_ticket.token if self.confirmation_ticket is not None else None


class AgentRuntime:
    """Multi-turn execution runtime with parallel tools, confirmation gates, and retry backoff."""

    def __init__(
        self,
        agent: Agent | AgentGroup | None = None,
        model_provider: ModelProvider | None = None,
        confirmation_gate: ConfirmationGate | None = None,
        retry_policy: RetryPolicy | None = None,
        default_budget: TurnBudget | None = None,
        sessions: Any | None = None,
        telemetry: AgentTelemetryRecorder | None = None,
        compaction_hook: Callable[[list[AgentMessage]], Sequence[AgentMessage]] | None = None,
        *,
        default_agent: Agent | None = None,
        agent_group: AgentGroup | None = None,
    ) -> None:
        target_agent = agent or agent_group or default_agent or Agent(name="default_agent")
        self.agent = target_agent
        self.model_provider = model_provider
        self.confirmation_gate = confirmation_gate if confirmation_gate is not None else ConfirmationGate()
        self.retry_policy = retry_policy if retry_policy is not None else RetryPolicy()
        self.default_budget = default_budget if default_budget is not None else TurnBudget()
        self.sessions = sessions
        self.telemetry = telemetry if telemetry is not None else AgentTelemetryRecorder()
        self.compaction_hook = compaction_hook

    def _resolve_active_agent(self, agent_name: str | None = None) -> Agent:
        if isinstance(self.agent, Agent):
            return self.agent
        if agent_name:
            ag = self.agent.get_agent(agent_name)
            if ag is not None:
                return ag
        default = self.agent.get_agent(self.agent.default_agent)
        if default is not None:
            return default
        return self.agent.list_agents()[0]

    async def run_turn(
        self,
        request: TurnRequest,
        *,
        context: AgentContext | None = None,
    ) -> TurnResponse:
        """Run a conversation turn to completion or until a confirmation is required."""
        turn_id = request.turn_id or f"turn-{secrets.token_hex(6)}"
        session_id = request.session_id or f"sess-{secrets.token_hex(6)}"
        budget = request.budget or self.default_budget

        active_agent = self._resolve_active_agent(request.agent_name)
        messages: list[AgentMessage] = list(request.effective_messages)
        if request.effective_prompt:
            messages.append(AgentMessage(role="user", content=request.effective_prompt))

        accumulated_text: list[str] = []
        all_tool_calls: list[ToolCall] = []
        all_tool_results: list[ToolResult] = []
        total_usage = GenAiUsage()
        step_count = 0
        owner_id = context.identity.owner_key() if context and context.identity else None

        with self.telemetry.span_turn(active_agent.name, session_id, turn_id):
            while True:
                step_count += 1
                if step_count > budget.max_steps:
                    return TurnResponse(
                        turn_id=turn_id,
                        session_id=session_id,
                        agent_name=active_agent.name,
                        text="".join(accumulated_text),
                        messages=tuple(messages),
                        tool_calls=tuple(all_tool_calls),
                        tool_results=tuple(all_tool_results),
                        status="budget_exceeded",
                        usage=total_usage,
                    )

                if self.compaction_hook is not None:
                    messages = list(self.compaction_hook(messages))

                step_tool_calls: list[ToolCall] = []
                step_text: list[str] = []

                if self.model_provider is not None:
                    available_tools = active_agent.list_tools()
                    if isinstance(self.agent, AgentGroup):
                        available_tools.append(self.agent.transfer_tool)

                    attempt = 0
                    while True:
                        try:
                            with self.telemetry.span_model(
                                active_agent.model, agent_name=active_agent.name
                            ) as usage_box:
                                async for delta in self.model_provider.stream_chat(
                                    messages,
                                    available_tools,
                                    active_agent.system_instruction,
                                    context=context,
                                ):
                                    if delta.usage is not None:
                                        total_usage = GenAiUsage(
                                            input_tokens=total_usage.input_tokens + delta.usage.input_tokens,
                                            output_tokens=total_usage.output_tokens + delta.usage.output_tokens,
                                            thought_tokens=total_usage.thought_tokens + delta.usage.thought_tokens,
                                            cached_input_tokens=total_usage.cached_input_tokens
                                            + delta.usage.cached_input_tokens,
                                        )
                                        usage_box["usage"] = delta.usage

                                    if delta.event_type == "delta" and delta.text:
                                        step_text.append(delta.text)
                                    elif delta.event_type == "tool_call" and delta.tool_name and delta.call_id:
                                        step_tool_calls.append(
                                            ToolCall(
                                                id=delta.call_id,
                                                name=delta.tool_name,
                                                arguments=delta.arguments or {},
                                            )
                                        )
                            break
                        except Exception:
                            attempt += 1
                            if attempt >= self.retry_policy.max_attempts:
                                return TurnResponse(
                                    turn_id=turn_id,
                                    session_id=session_id,
                                    agent_name=active_agent.name,
                                    text="".join(accumulated_text),
                                    messages=tuple(messages),
                                    status="error",
                                )
                            delay = self.retry_policy.compute_delay(attempt)
                            await anyio.sleep(delay)

                if step_text:
                    accumulated_text.extend(step_text)
                    messages.append(
                        AgentMessage(
                            role="assistant",
                            content="".join(step_text),
                            tool_calls=tuple(step_tool_calls),
                        )
                    )

                if not step_tool_calls:
                    break

                all_tool_calls.extend(step_tool_calls)

                transfer_calls = [c for c in step_tool_calls if c.name == TRANSFER_TOOL_NAME]
                if transfer_calls and isinstance(self.agent, AgentGroup):
                    t_call = transfer_calls[0]
                    target_name = str(t_call.arguments.get("target_agent", ""))
                    target_agent = self.agent.get_agent(target_name)
                    if target_agent is not None:
                        active_agent = target_agent
                        t_res = ToolResult(
                            call_id=t_call.id,
                            name=TRANSFER_TOOL_NAME,
                            output=f"Transferred control to {target_name}",
                        )
                        all_tool_results.append(t_res)
                        messages.append(
                            AgentMessage(
                                role="tool",
                                content=str(t_res.output),
                                tool_call_id=t_call.id,
                                name=TRANSFER_TOOL_NAME,
                            )
                        )
                        continue

                needs_confirm_ticket: ConfirmationTicket | None = None
                for call in step_tool_calls:
                    target_tool = active_agent.get_tool(call.name)
                    if (
                        target_tool is not None
                        and target_tool.requires_confirmation
                        and not self.confirmation_gate.is_call_approved(
                            request.confirmation_token,
                            tool_name=call.name,
                            arguments=call.arguments,
                            owner_id=owner_id,
                        )
                    ):
                        needs_confirm_ticket = self.confirmation_gate.request_confirmation(
                            call.name,
                            call.id,
                            call.arguments,
                            owner_id=owner_id,
                        )
                        break

                if needs_confirm_ticket is not None:
                    return TurnResponse(
                        turn_id=turn_id,
                        session_id=session_id,
                        agent_name=active_agent.name,
                        text="".join(accumulated_text),
                        messages=tuple(messages),
                        tool_calls=tuple(all_tool_calls),
                        tool_results=tuple(all_tool_results),
                        confirmation_ticket=needs_confirm_ticket,
                        status="confirmation_required",
                        usage=total_usage,
                    )

                tools_map = {t.name: t for t in active_agent.list_tools()}
                if isinstance(self.agent, AgentGroup):
                    tools_map[TRANSFER_TOOL_NAME] = self.agent.transfer_tool

                results = await execute_tool_calls(tools_map, step_tool_calls, context=context)
                all_tool_results.extend(results)

                for r in results:
                    messages.append(
                        AgentMessage(
                            role="tool",
                            content=str(r.output) if r.output is not None else str(r.error),
                            name=r.name,
                            tool_call_id=r.call_id,
                        )
                    )

            if self.sessions is not None and owner_id is not None:
                await self.sessions.save(owner_id, session_id, messages)

            final_text = "".join(accumulated_text)
            return TurnResponse(
                turn_id=turn_id,
                session_id=session_id,
                agent_name=active_agent.name,
                text=final_text,
                messages=tuple(messages),
                tool_calls=tuple(all_tool_calls),
                tool_results=tuple(all_tool_results),
                status="completed",
                usage=total_usage,
            )

    async def stream_turn(
        self,
        request: TurnRequest,
        *,
        context: AgentContext | None = None,
    ) -> AsyncIterator[AgentStreamFrame]:
        """Stream real-time frames during turn execution."""
        turn_id = request.turn_id or f"turn-{secrets.token_hex(6)}"
        session_id = request.session_id or f"sess-{secrets.token_hex(6)}"
        budget = request.budget or self.default_budget

        active_agent = self._resolve_active_agent(request.agent_name)
        messages: list[AgentMessage] = list(request.effective_messages)
        if request.effective_prompt:
            messages.append(AgentMessage(role="user", content=request.effective_prompt))

        step_count = 0
        owner_id = context.identity.owner_key() if context and context.identity else None

        with self.telemetry.span_turn(active_agent.name, session_id, turn_id):
            while True:
                step_count += 1
                if step_count > budget.max_steps:
                    yield AgentStreamFrame(
                        turn_id=turn_id,
                        event_type="error",
                        agent_name=active_agent.name,
                        text="Turn budget exceeded",
                    )
                    return

                step_tool_calls: list[ToolCall] = []
                step_text: list[str] = []

                if self.model_provider is not None:
                    available_tools = active_agent.list_tools()
                    if isinstance(self.agent, AgentGroup):
                        available_tools.append(self.agent.transfer_tool)

                    async for delta in self.model_provider.stream_chat(
                        messages,
                        available_tools,
                        active_agent.system_instruction,
                        context=context,
                    ):
                        if delta.event_type == "delta" and delta.text:
                            step_text.append(delta.text)
                            yield AgentStreamFrame(
                                turn_id=turn_id,
                                event_type="delta",
                                agent_name=active_agent.name,
                                text=delta.text,
                            )
                        elif delta.event_type == "thought" and delta.text:
                            yield AgentStreamFrame(
                                turn_id=turn_id,
                                event_type="thought",
                                agent_name=active_agent.name,
                                text=delta.text,
                            )
                        elif delta.event_type == "tool_call" and delta.tool_name and delta.call_id:
                            tc = ToolCall(
                                id=delta.call_id,
                                name=delta.tool_name,
                                arguments=delta.arguments or {},
                            )
                            step_tool_calls.append(tc)
                            yield AgentStreamFrame(
                                turn_id=turn_id,
                                event_type="tool_call",
                                agent_name=active_agent.name,
                                tool_name=delta.tool_name,
                                call_id=delta.call_id,
                                payload=delta.arguments,
                            )

                if step_text:
                    messages.append(
                        AgentMessage(
                            role="assistant",
                            content="".join(step_text),
                            tool_calls=tuple(step_tool_calls),
                        )
                    )

                if not step_tool_calls:
                    break

                transfer_calls = [c for c in step_tool_calls if c.name == TRANSFER_TOOL_NAME]
                if transfer_calls and isinstance(self.agent, AgentGroup):
                    t_call = transfer_calls[0]
                    target_name = str(t_call.arguments.get("target_agent", ""))
                    target_agent = self.agent.get_agent(target_name)
                    if target_agent is not None:
                        active_agent = target_agent
                        yield AgentStreamFrame(
                            turn_id=turn_id,
                            event_type="agent_transfer",
                            agent_name=target_name,
                            text=f"Transferred to {target_name}",
                        )
                        messages.append(
                            AgentMessage(
                                role="tool",
                                content=f"Transferred to {target_name}",
                                tool_call_id=t_call.id,
                                name=TRANSFER_TOOL_NAME,
                            )
                        )
                        continue

                needs_confirm_ticket: ConfirmationTicket | None = None
                for call in step_tool_calls:
                    target_tool = active_agent.get_tool(call.name)
                    if (
                        target_tool is not None
                        and target_tool.requires_confirmation
                        and not self.confirmation_gate.is_call_approved(
                            request.confirmation_token,
                            tool_name=call.name,
                            arguments=call.arguments,
                            owner_id=owner_id,
                        )
                    ):
                        needs_confirm_ticket = self.confirmation_gate.request_confirmation(
                            call.name,
                            call.id,
                            call.arguments,
                            owner_id=owner_id,
                        )
                        break

                if needs_confirm_ticket is not None:
                    yield AgentStreamFrame(
                        turn_id=turn_id,
                        event_type="confirmation_required",
                        agent_name=active_agent.name,
                        tool_name=needs_confirm_ticket.tool_name,
                        call_id=needs_confirm_ticket.call_id,
                        payload=needs_confirm_ticket.to_dict(),
                    )
                    return

                tools_map = {t.name: t for t in active_agent.list_tools()}
                if isinstance(self.agent, AgentGroup):
                    tools_map[TRANSFER_TOOL_NAME] = self.agent.transfer_tool
                results = await execute_tool_calls(tools_map, step_tool_calls, context=context)

                for r in results:
                    yield AgentStreamFrame(
                        turn_id=turn_id,
                        event_type="tool_result",
                        agent_name=active_agent.name,
                        tool_name=r.name,
                        call_id=r.call_id,
                        payload={"output": r.output, "error": r.error, "is_error": r.is_error},
                    )
                    messages.append(
                        AgentMessage(
                            role="tool",
                            content=str(r.output) if r.output is not None else str(r.error),
                            name=r.name,
                            tool_call_id=r.call_id,
                        )
                    )

            yield AgentStreamFrame(
                turn_id=turn_id,
                event_type="done",
                agent_name=active_agent.name,
            )


__all__ = [
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
]
