"""Bridge projecting MCP tools, prompts, and skills into an A2A 1.0 agent.

This module provides A2aAgentBridge exposing a ToolRegistry or MCPCore
as a full A2A 1.0 AgentCard and executable A2AService.
"""

import secrets
from datetime import UTC, datetime
from typing import Any

from vertebra.agent.a2a.models import (
    AgentCard,
    AgentInterface,
    AgentSkill,
    Artifact,
    DataPart,
    Message,
    TaskStatus,
    TextPart,
)
from vertebra.agent.a2a.service import A2AConfig, A2AService
from vertebra.agent.core.context import AgentContext
from vertebra.agent.mcp.service import MCPCore

__all__ = [
    "A2aAgentBridge",
    "mcp_service_to_a2a",
]


class A2aAgentBridge:
    """Projects an MCP core engine and registry into an A2A 1.0 compliant agent service."""

    @classmethod
    def from_mcp_service(
        cls,
        mcp_service: MCPCore,
        *,
        base_url: str | None = None,
        path: str = "/a2a",
        name: str | None = None,
        description: str = "",
    ) -> A2AService:
        """Create an A2AService wrapping an MCPCore instance."""
        skills: list[AgentSkill] = []

        skill_controllers = getattr(mcp_service.registry, "skill_controllers", {})
        for controller in skill_controllers.values():
            if hasattr(controller, "to_agent_skill"):
                s = controller.to_agent_skill()
                if isinstance(s, dict):
                    skills.append(
                        AgentSkill(
                            id=s["id"],
                            name=s["name"],
                            description=s.get("description", ""),
                            tags=tuple(s.get("tags", ())),
                            examples=tuple(s.get("examples", ())),
                        )
                    )
                elif isinstance(s, AgentSkill):
                    skills.append(s)

        for spec in mcp_service.registry.list_specs(exposure="a2a"):
            skills.append(
                AgentSkill(
                    id=spec.name,
                    name=spec.title or spec.name,
                    description=spec.description,
                )
            )

        supported_interfaces: tuple[AgentInterface, ...] = ()
        if base_url:
            agent_url = f"{base_url.rstrip('/')}{path}"
            supported_interfaces = (
                AgentInterface(
                    url=agent_url,
                    protocol_binding="JSONRPC",
                    protocol_version="1.0",
                ),
            )

        card_name: str = name or str(getattr(mcp_service.config, "server_name", "mcp-agent"))
        card_desc: str = description or str(getattr(mcp_service.config, "instructions", ""))

        card = AgentCard(
            name=card_name,
            description=card_desc,
            version="1.0",
            supported_interfaces=supported_interfaces,
            skills=tuple(skills),
        )
        if supported_interfaces:
            card.validate_for_path(path)

        async def _executor(
            message: Message,
            context_id: str | None,
            task_id: str,
        ) -> tuple[TaskStatus, tuple[Artifact, ...]]:
            target_name: str | None = None
            target_args: dict[str, Any] = {}

            if message.metadata and "skill_id" in message.metadata:
                target_name = str(message.metadata["skill_id"])

            for part in message.parts:
                if isinstance(part, DataPart) and part.data:
                    if "tool" in part.data:
                        target_name = str(part.data["tool"])
                    if "arguments" in part.data and isinstance(part.data["arguments"], dict):
                        target_args = part.data["arguments"]
                    elif "data" in part.data and isinstance(part.data["data"], dict):
                        target_args = part.data["data"]
                elif isinstance(part, TextPart) and not target_args and part.text:
                    target_args = {"prompt": part.text}

            if not target_name and mcp_service.registry.list_tools():
                target_name = mcp_service.registry.list_tools()[0].name

            if not target_name:
                reply = Message(
                    message_id=f"msg-reply-{secrets.token_hex(4)}",
                    role="agent",
                    parts=(TextPart(text="No tool or skill specified"),),
                    task_id=task_id,
                    context_id=context_id,
                )
                status = TaskStatus(
                    state="completed",
                    message=reply,
                    timestamp=datetime.now(UTC).isoformat(),
                )
                return status, ()

            context = AgentContext.current()

            skill_controllers = getattr(mcp_service.registry, "skill_controllers", {})
            if target_name in skill_controllers:
                controller = skill_controllers[target_name]
                skill_content = await controller.read_skill_content(controller.skill_id)
                artifact = Artifact(
                    artifact_id=f"art-{secrets.token_hex(4)}",
                    name=target_name,
                    parts=(TextPart(text=skill_content),),
                )
                reply = Message(
                    message_id=f"msg-reply-{secrets.token_hex(4)}",
                    role="agent",
                    parts=(TextPart(text=f"Skill {target_name} loaded"),),
                    task_id=task_id,
                    context_id=context_id,
                )
                status = TaskStatus(
                    state="completed",
                    message=reply,
                    timestamp=datetime.now(UTC).isoformat(),
                )
                return status, (artifact,)

            if mcp_service.registry.get_tool(target_name) is not None:
                resp = await mcp_service.handle_tool_call(
                    {"name": target_name, "arguments": target_args, "callId": f"call_{task_id}"},
                    context=context,
                )
                artifact = Artifact(
                    artifact_id=f"art-{secrets.token_hex(4)}",
                    name=target_name,
                    parts=(DataPart(data=resp),),
                )
                reply = Message(
                    message_id=f"msg-reply-{secrets.token_hex(4)}",
                    role="agent",
                    parts=(TextPart(text=f"Tool {target_name} completed"),),
                    task_id=task_id,
                    context_id=context_id,
                )
                status = TaskStatus(
                    state="completed",
                    message=reply,
                    timestamp=datetime.now(UTC).isoformat(),
                )
                return status, (artifact,)

            err_reply = Message(
                message_id=f"msg-err-{secrets.token_hex(4)}",
                role="agent",
                parts=(TextPart(text=f"Tool or skill '{target_name}' not found"),),
                task_id=task_id,
                context_id=context_id,
            )
            status = TaskStatus(
                state="failed",
                message=err_reply,
                timestamp=datetime.now(UTC).isoformat(),
            )
            return status, ()

        config = A2AConfig(
            path=path,
            base_url=base_url,
            agent_card=card,
            allow_insecure_webhooks=True,
        )

        return A2AService(
            config=config,
            executor=_executor,
            telemetry=mcp_service.telemetry,
        )


def mcp_service_to_a2a(
    mcp_service: MCPCore,
    *,
    base_url: str | None = None,
    path: str = "/a2a",
    name: str | None = None,
    description: str = "",
) -> A2AService:
    """Project an MCPCore instance into an A2AService."""
    return A2aAgentBridge.from_mcp_service(
        mcp_service,
        base_url=base_url,
        path=path,
        name=name,
        description=description,
    )
