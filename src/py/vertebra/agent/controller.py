"""AgentChatController implementing SSE chat, turns, background execution, and session management.

This module provides the HTTP and SSE controller endpoints for multi-turn agent conversations,
supporting streaming chat, synchronous turns, background queued turns with Last-Event-ID replay,
human-in-the-loop confirmation gates, and owner-isolated session histories.
"""

import asyncio
import uuid
from collections.abc import AsyncIterable, AsyncIterator, Mapping
from typing import Any, Literal

import msgspec

from vertebra.agent.core.context import AgentContext
from vertebra.agent.core.schema import ToolCall
from vertebra.agent.core.sse import SseFrame, SseStreamBroker
from vertebra.agent.runtime import AgentRuntime, TurnRequest, TurnResponse
from vertebra.agent.sessions import ANONYMOUS_OWNER, AgentSessionStore, MemorySessionStore
from vertebra.agent.spec import AgentMessage
from vertebra.telemetry.genai import GenAiUsage
from vertebra.utils.serializers import decode_json, encode_json_bytes


class SessionNotFoundError(Exception):
    """Raised when an agent session is not found or owner access is unauthorized."""


class ChatRequest(msgspec.Struct, frozen=True, gc=False, kw_only=True, forbid_unknown_fields=True):
    """Request payload for starting or continuing an agent conversation turn."""

    message: str
    session_id: str | None = None
    agent_name: str | None = None
    confirmation_token: str | None = None
    metadata: dict[str, Any] = msgspec.field(default_factory=dict)


class TurnResult(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Synchronous outcome of an agent turn."""

    session_id: str
    turn_id: str
    agent_name: str
    response: str
    tool_calls: tuple[ToolCall, ...] = ()
    usage: GenAiUsage | None = None
    confirmation_token: str | None = None


class QueuedTurnTicket(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Response payload returned when a turn is enqueued for background processing."""

    turn_id: str
    session_id: str
    stream_url: str
    status: str = "queued"


class ConfirmationDecisionRequest(msgspec.Struct, frozen=True, gc=False, kw_only=True, forbid_unknown_fields=True):
    """Payload to approve or reject a pending human-in-the-loop confirmation ticket."""

    decision: Literal["approve", "reject"] = "approve"
    reason: str | None = None


class SessionHistory(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Conversation history for an owner-scoped agent session."""

    session_id: str
    owner: str
    messages: tuple[AgentMessage, ...] = ()


class AgentChatController:
    """Controller providing unified HTTP, SSE, and session operations for AgentRuntime."""

    def __init__(
        self,
        runtime: AgentRuntime,
        session_store: AgentSessionStore | None = None,
        *,
        base_path: str = "/agent",
        queue_service: Any = None,
        stream_broker: SseStreamBroker | None = None,
    ) -> None:
        self.runtime = runtime
        self.session_store = session_store if session_store is not None else MemorySessionStore()
        self.base_path = base_path.rstrip("/")
        self.queue_service = queue_service
        self.stream_broker = stream_broker if stream_broker is not None else SseStreamBroker()

    def _resolve_owner(self, context: AgentContext | None) -> str:
        ctx = context or AgentContext.current()
        if ctx is not None and ctx.identity is not None:
            owner = ctx.identity.owner_key()
            if owner:
                return owner
        return ANONYMOUS_OWNER

    async def chat(
        self,
        request: ChatRequest,
        *,
        context: AgentContext | None = None,
    ) -> AsyncIterable[bytes]:
        """Stream conversational agent responses as Server-Sent Events."""
        owner = self._resolve_owner(context)
        session_id = request.session_id or f"sess_{uuid.uuid4().hex[:12]}"

        async def _generator() -> AsyncIterator[bytes]:
            async with self.session_store.lock(owner, session_id):
                history = await self.session_store.load(owner, session_id) or []
                turn_req = TurnRequest(
                    session_id=session_id,
                    message=request.message,
                    agent_name=request.agent_name,
                    confirmation_token=request.confirmation_token,
                    history=tuple(history),
                    metadata=request.metadata,
                )

                collected_text: list[str] = []
                tool_calls: list[ToolCall] = []
                frame_idx = 0

                async for frame in self.runtime.stream_turn(turn_req, context=context):
                    frame_idx += 1
                    if frame.event_type == "delta" and frame.text:
                        collected_text.append(frame.text)
                    elif frame.event_type == "tool_call":
                        tool_calls.append(
                            ToolCall(
                                id=frame.call_id or "",
                                name=frame.tool_name or "",
                                arguments=frame.payload or {},
                            )
                        )

                    sse_frame = frame.to_sse_frame(str(frame_idx))
                    yield sse_frame.encode()

                full_text = "".join(collected_text)
                updated_history = list(history)
                updated_history.append(AgentMessage(role="user", content=request.message))
                updated_history.append(
                    AgentMessage(
                        role="assistant",
                        content=full_text,
                        tool_calls=tuple(tool_calls),
                    )
                )
                await self.session_store.save(owner, session_id, updated_history)

        return _generator()

    async def turns(
        self,
        request: ChatRequest,
        *,
        context: AgentContext | None = None,
    ) -> TurnResult:
        """Execute a complete conversational turn synchronously."""
        owner = self._resolve_owner(context)
        session_id = request.session_id or f"sess_{uuid.uuid4().hex[:12]}"

        async with self.session_store.lock(owner, session_id):
            history = await self.session_store.load(owner, session_id) or []
            turn_req = TurnRequest(
                session_id=session_id,
                message=request.message,
                agent_name=request.agent_name,
                confirmation_token=request.confirmation_token,
                history=tuple(history),
                metadata=request.metadata,
            )

            turn_resp: TurnResponse = await self.runtime.run_turn(turn_req, context=context)

            updated_history = list(history)
            updated_history.append(AgentMessage(role="user", content=request.message))
            updated_history.append(
                AgentMessage(
                    role="assistant",
                    content=turn_resp.response,
                    tool_calls=turn_resp.tool_calls,
                )
            )
            await self.session_store.save(owner, session_id, updated_history)

            return TurnResult(
                session_id=session_id,
                turn_id=turn_resp.turn_id,
                agent_name=turn_resp.agent_name,
                response=turn_resp.response,
                tool_calls=turn_resp.tool_calls,
                usage=turn_resp.usage,
                confirmation_token=turn_resp.confirmation_token,
            )

    async def turns_async(
        self,
        request: ChatRequest,
        *,
        context: AgentContext | None = None,
    ) -> QueuedTurnTicket:
        """Enqueue a conversational turn for asynchronous execution and return a stream ticket."""
        owner = self._resolve_owner(context)
        session_id = request.session_id or f"sess_{uuid.uuid4().hex[:12]}"
        turn_id = f"turn_{uuid.uuid4().hex[:12]}"
        stream_url = f"{self.base_path}/turns/{turn_id}/stream"

        async def _background_task() -> None:
            async with self.session_store.lock(owner, session_id):
                history = await self.session_store.load(owner, session_id) or []
                turn_req = TurnRequest(
                    session_id=session_id,
                    turn_id=turn_id,
                    message=request.message,
                    agent_name=request.agent_name,
                    confirmation_token=request.confirmation_token,
                    history=tuple(history),
                    metadata=request.metadata,
                )

                frame_idx = 0
                collected_text: list[str] = []
                tool_calls: list[ToolCall] = []

                async for frame in self.runtime.stream_turn(turn_req, context=context):
                    frame_idx += 1
                    if frame.event_type == "delta" and frame.text:
                        collected_text.append(frame.text)
                    elif frame.event_type == "tool_call":
                        tool_calls.append(
                            ToolCall(
                                id=frame.call_id or "",
                                name=frame.tool_name or "",
                                arguments=frame.payload or {},
                            )
                        )

                    sse_frame = frame.to_sse_frame(str(frame_idx))
                    self.stream_broker.publish(turn_id, sse_frame)

                done_frame = SseFrame(id=str(frame_idx + 1), event="done", data="{}")
                self.stream_broker.publish(turn_id, done_frame)

                full_text = "".join(collected_text)
                updated_history = list(history)
                updated_history.append(AgentMessage(role="user", content=request.message))
                updated_history.append(
                    AgentMessage(
                        role="assistant",
                        content=full_text,
                        tool_calls=tuple(tool_calls),
                    )
                )
                await self.session_store.save(owner, session_id, updated_history)

        asyncio.create_task(_background_task())

        return QueuedTurnTicket(
            turn_id=turn_id,
            session_id=session_id,
            stream_url=stream_url,
            status="queued",
        )

    async def get_turn_stream(
        self,
        turn_id: str,
        last_event_id: str | None = None,
    ) -> AsyncIterable[bytes]:
        """Subscribe to an ongoing or finished turn's SSE event stream with Last-Event-ID replay."""
        recv_stream, replay_buffer = self.stream_broker.subscribe(turn_id, last_event_id=last_event_id)

        async def _generator() -> AsyncIterator[bytes]:
            for replay_frame in replay_buffer.replay_since(last_event_id or ""):
                yield replay_frame.encode()
                if replay_frame.event == "done":
                    return

            async with recv_stream:
                async for live_frame in recv_stream:
                    yield live_frame.encode()
                    if live_frame.event == "done":
                        break

        return _generator()

    async def decide_confirmation(
        self,
        token: str,
        decision: ConfirmationDecisionRequest,
        *,
        context: AgentContext | None = None,
    ) -> dict[str, Any]:
        """Approve or reject a human-in-the-loop tool execution ticket."""
        owner = self._resolve_owner(context)
        if decision.decision == "approve":
            success = self.runtime.confirmation_gate.approve(token, owner_id=owner)
        else:
            success = self.runtime.confirmation_gate.reject(token, reason=decision.reason or "", owner_id=owner)

        if not success:
            raise SessionNotFoundError(f"Confirmation ticket '{token}' not found or owner unauthorized")

        return {"status": "ok", "decision": decision.decision, "token": token}

    async def get_session(
        self,
        session_id: str,
        *,
        context: AgentContext | None = None,
    ) -> SessionHistory:
        """Retrieve conversation history for an owner-scoped session."""
        owner = self._resolve_owner(context)
        messages = await self.session_store.load(owner, session_id)
        if messages is None:
            raise SessionNotFoundError(f"Session '{session_id}' not found for owner '{owner}'")
        return SessionHistory(session_id=session_id, owner=owner, messages=tuple(messages))

    async def delete_session(
        self,
        session_id: str,
        *,
        context: AgentContext | None = None,
    ) -> None:
        """Delete conversation history for an owner-scoped session."""
        owner = self._resolve_owner(context)
        await self.session_store.delete(owner, session_id)

    async def handle_request(
        self,
        method: str,
        path: str,
        headers: Mapping[str, str],
        body: bytes,
        context: AgentContext | None = None,
    ) -> tuple[int, dict[str, str], bytes | AsyncIterable[bytes]]:
        """Dispatch HTTP requests to appropriate chat, turn, or session endpoints."""
        normalized_path = path.rstrip("/")
        prefix = self.base_path

        if method == "POST" and normalized_path == f"{prefix}/chat":
            raw = decode_json(body)
            req = msgspec.convert(raw, ChatRequest)
            stream_iter = await self.chat(req, context=context)
            resp_headers = {
                "content-type": "text/event-stream",
                "cache-control": "no-cache",
                "connection": "keep-alive",
            }
            return 200, resp_headers, stream_iter

        if method == "POST" and normalized_path == f"{prefix}/turns":
            raw = decode_json(body)
            req = msgspec.convert(raw, ChatRequest)
            result = await self.turns(req, context=context)
            resp_headers = {"content-type": "application/json"}
            return 200, resp_headers, encode_json_bytes(result)

        if method == "POST" and normalized_path == f"{prefix}/turns/async":
            raw = decode_json(body)
            req = msgspec.convert(raw, ChatRequest)
            ticket = await self.turns_async(req, context=context)
            resp_headers = {"content-type": "application/json"}
            return 202, resp_headers, encode_json_bytes(ticket)

        if method == "GET" and normalized_path.startswith(f"{prefix}/turns/") and normalized_path.endswith("/stream"):
            parts = normalized_path[len(f"{prefix}/turns/") :].split("/")
            if len(parts) == 2 and parts[1] == "stream":
                turn_id = parts[0]
                last_event_id = headers.get("last-event-id") or headers.get("Last-Event-ID")
                stream_iter = await self.get_turn_stream(turn_id, last_event_id=last_event_id)
                resp_headers = {
                    "content-type": "text/event-stream",
                    "cache-control": "no-cache",
                    "connection": "keep-alive",
                }
                return 200, resp_headers, stream_iter

        if method == "POST" and normalized_path.startswith(f"{prefix}/confirmations/"):
            token = normalized_path[len(f"{prefix}/confirmations/") :]
            raw = decode_json(body) if body else {}
            dec_req = msgspec.convert(raw, ConfirmationDecisionRequest)
            try:
                res = await self.decide_confirmation(token, dec_req, context=context)
                return 200, {"content-type": "application/json"}, encode_json_bytes(res)
            except SessionNotFoundError as err:
                err_payload = {"error": "not_found", "message": str(err)}
                return 404, {"content-type": "application/json"}, encode_json_bytes(err_payload)

        if normalized_path.startswith(f"{prefix}/sessions/"):
            session_id = normalized_path[len(f"{prefix}/sessions/") :]
            if method == "GET":
                try:
                    history = await self.get_session(session_id, context=context)
                    return 200, {"content-type": "application/json"}, encode_json_bytes(history)
                except SessionNotFoundError as err:
                    err_payload = {"error": "not_found", "message": str(err)}
                    return 404, {"content-type": "application/json"}, encode_json_bytes(err_payload)
            if method == "DELETE":
                await self.delete_session(session_id, context=context)
                return 204, {}, b""

        return 404, {"content-type": "application/json"}, b'{"error": "not_found"}'


__all__ = [
    "AgentChatController",
    "ChatRequest",
    "ConfirmationDecisionRequest",
    "QueuedTurnTicket",
    "SessionHistory",
    "SessionNotFoundError",
    "TurnResult",
]
