"""Bridge exposing A2A 1.0 agents and skills as callable MCP tools.

This module provides MCPToolBridge which inspects an A2A AgentCard or A2AService,
synthesizes MCP Tool descriptors for each skill, and propagates W3C trace context.
"""

import secrets
from collections.abc import Awaitable, Callable
from typing import Any, cast

from vertebra.agent.a2a.models import AgentCard, DataPart, Message, Part, TextPart
from vertebra.agent.a2a.service import A2AService, _serialize_struct
from vertebra.agent.core.context import AgentContext
from vertebra.agent.core.schema import Tool
from vertebra.telemetry.genai import inject_w3c_trace_context
from vertebra.telemetry.otel import TraceContext

__all__ = [
    "MCPToolBridge",
    "a2a_card_to_mcp_tools",
]


class MCPToolBridge:
    """Translates A2A 1.0 skills into callable MCP Tool instances."""

    @classmethod
    def from_a2a_service(
        cls,
        service: A2AService,
        *,
        prefix: str | None = None,
    ) -> list[Tool]:
        """Convert skills of an in-process A2AService into callable MCP Tool instances."""
        card = service.config.agent_card
        if card is None or not card.skills:
            return []

        async def _local_transport(rpc_request: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
            context = AgentContext.current()
            params = rpc_request.get("params") or {}
            result = await service.handle_message_send(params, context=context)
            return {"result": result}

        return cls.from_agent_card(card, _local_transport, prefix=prefix)

    @classmethod
    def from_agent_card(
        cls,
        card: AgentCard,
        transport: Callable[[dict[str, Any], dict[str, str]], Awaitable[dict[str, Any]]],
        *,
        prefix: str | None = None,
    ) -> list[Tool]:
        """Synthesize callable MCP Tool instances for all skills declared on an AgentCard."""
        tools: list[Tool] = []

        for skill in card.skills:
            tool_name = f"{prefix}_{skill.id}" if prefix else skill.id
            tool_description = skill.description or skill.name or f"A2A skill {skill.id}"
            skill_id = skill.id

            input_schema: dict[str, Any] = {
                "type": "object",
                "properties": {
                    "prompt": {"type": "string", "description": "Instruction or prompt for the skill"},
                    "context_id": {"type": "string", "description": "Optional conversational context ID"},
                    "data": {"type": "object", "description": "Optional structured parameters"},
                },
                "required": ["prompt"],
            }

            async def _make_invoker(
                s_id: str,
                prompt: str,
                context_id: str | None = None,
                data: dict[str, Any] | None = None,
                context: AgentContext | None = None,
            ) -> dict[str, Any]:
                parts: list[Part] = [TextPart(text=prompt)]
                if data:
                    parts.append(DataPart(data=data))

                msg = Message(
                    message_id=f"msg-bridge-{secrets.token_hex(4)}",
                    role="user",
                    parts=tuple(parts),
                    context_id=context_id,
                    metadata={"skill_id": s_id},
                )

                rpc_params: dict[str, Any] = {
                    "message": _serialize_struct(msg),
                    "contextId": context_id,
                }

                headers: dict[str, str] = {}
                current_trace = TraceContext.current()
                if current_trace is not None:
                    inject_w3c_trace_context(current_trace, headers=headers, rpc_params=rpc_params)

                rpc_request: dict[str, Any] = {
                    "jsonrpc": "2.0",
                    "id": f"bridge-{secrets.token_hex(4)}",
                    "method": "message/send",
                    "params": rpc_params,
                }

                resp = await transport(rpc_request, headers)
                if "error" in resp:
                    return {"error": resp["error"]}
                result_payload = resp.get("result", resp)
                return cast(dict[str, Any], result_payload)

            def _bind_closure(sid: str, t_name: str, t_desc: str) -> Callable[..., Awaitable[dict[str, Any]]]:
                async def _bound(
                    prompt: str,
                    context_id: str | None = None,
                    data: dict[str, Any] | None = None,
                    context: AgentContext | None = None,
                ) -> dict[str, Any]:
                    return await _make_invoker(
                        sid,
                        prompt=prompt,
                        context_id=context_id,
                        data=data,
                        context=context,
                    )

                _bound.__name__ = t_name
                _bound.__doc__ = t_desc
                return _bound

            tool_instance = Tool(
                _bind_closure(skill_id, tool_name, tool_description),
                name=tool_name,
                title=skill.name or tool_name,
                description=tool_description,
                input_schema=input_schema,
            )
            tools.append(tool_instance)

        return tools


def a2a_card_to_mcp_tools(
    card: AgentCard,
    transport: Callable[[dict[str, Any], dict[str, str]], Awaitable[dict[str, Any]]],
    *,
    prefix: str | None = None,
) -> list[Tool]:
    """Helper shortcut converting AgentCard skills to MCP tools."""
    return MCPToolBridge.from_agent_card(card, transport, prefix=prefix)
