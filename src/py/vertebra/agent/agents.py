"""Agent definitions, conversation messages, and multi-agent transfer groups."""

from collections.abc import Sequence
from typing import Any, Literal

import msgspec

from vertebra.agent.core.schema import Tool, ToolCall

__all__ = (
    "TRANSFER_TOOL_NAME",
    "Agent",
    "AgentGroup",
    "AgentMessage",
)

TRANSFER_TOOL_NAME: str = "transfer_to_agent"


class AgentMessage(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Normalized message representation in an agent conversation transcript."""

    role: Literal["user", "assistant", "system", "tool"]
    """Conversational speaker role (`"user"`, `"assistant"`, `"system"`, or `"tool"`)."""
    content: str = ""
    """Text body of the message."""
    name: str | None = None
    """Optional sender or tool name associated with the message."""
    tool_call_id: str | None = None
    """Correlation identifier linking a `"tool"` message to its originating `ToolCall`."""
    tool_calls: tuple[ToolCall, ...] = ()
    """Tool invocations requested by an `"assistant"` turn."""
    metadata: dict[str, Any] = msgspec.field(default_factory=dict)
    """Arbitrary structured metadata attached to the message."""


class Agent(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration and tool registry for a single autonomous agent."""

    name: str
    """Unique identifier of the agent within an `AgentRuntime` or `AgentGroup`."""
    description: str = ""
    """Summary of the agent's responsibilities used for routing and transfers."""
    system_instruction: str = ""
    """System prompt guiding model behavior during turns."""
    model: str = "gemini-2.5-pro"
    """Default model identifier used by the `ModelProvider`."""
    tools: tuple[Tool, ...] = ()
    """Callable `Tool` descriptors registered on this agent."""
    skills: tuple[Any, ...] = ()
    """Progressive MCP skills attached to this agent."""
    metadata: dict[str, Any] = msgspec.field(default_factory=dict)
    """Custom configuration metadata associated with the agent."""

    def __post_init__(self) -> None:
        names: set[str] = set()
        for t in self.tools:
            if t.name == TRANSFER_TOOL_NAME:
                msg = f"Cannot manually register reserved transfer tool '{TRANSFER_TOOL_NAME}' on agent '{self.name}'"
                raise ValueError(msg)
            if t.name in names:
                msg = f"Duplicate tool name '{t.name}' registered on agent '{self.name}'"
                raise ValueError(msg)
            names.add(t.name)

    def get_tool(self, name: str) -> Tool | None:
        """Retrieve a registered tool by exact name."""
        for t in self.tools:
            if t.name == name:
                return t
        return None

    def list_tools(self) -> list[Tool]:
        """Return all tools registered on this agent."""
        return list(self.tools)


class AgentGroup:
    """Group of collaborating agents with dynamic transfer synthesis."""

    def __init__(
        self,
        name: str,
        agents: Sequence[Agent],
        default_agent: str | None = None,
        *,
        default_agent_name: str | None = None,
    ) -> None:
        if not agents:
            raise ValueError("AgentGroup must contain at least one Agent")

        self.name = name
        self._agents: dict[str, Agent] = {a.name: a for a in agents}
        resolved_default = default_agent or default_agent_name or agents[0].name
        self.default_agent: str = resolved_default
        if self.default_agent not in self._agents:
            raise ValueError(f"Default agent '{self.default_agent}' not in agent group")

        self.transfer_tool: Tool = self._synthesize_transfer_tool()

    def get_agent(self, name: str) -> Agent | None:
        """Retrieve a member agent by name."""
        return self._agents.get(name)

    def get_agent_tools(self, agent_name: str) -> tuple[Tool, ...]:
        """Return member agent tools augmented with the synthesized transfer tool."""
        ag = self._agents.get(agent_name)
        if ag is None:
            raise KeyError(f"Agent '{agent_name}' not in group '{self.name}'")
        return ag.tools + (self.transfer_tool,)

    def list_agents(self) -> list[Agent]:
        """Return all member agents in the group."""
        return list(self._agents.values())

    def _synthesize_transfer_tool(self) -> Tool:
        agent_names = sorted(self._agents.keys())

        def transfer_to_agent(target_agent: str, reason: str = "") -> dict[str, Any]:
            """Transfer conversation control to another specialist agent in the group."""
            if target_agent not in self._agents:
                return {"error": f"Agent '{target_agent}' not found in group '{self.name}'"}
            return {"transferred": True, "target_agent": target_agent, "reason": reason}

        transfer_to_agent.__name__ = TRANSFER_TOOL_NAME

        schema: dict[str, Any] = {
            "type": "object",
            "properties": {
                "target_agent": {
                    "type": "string",
                    "enum": agent_names,
                    "description": "Name of the target specialist agent to transfer to",
                },
                "reason": {
                    "type": "string",
                    "description": "Explanation of why this task is being delegated",
                },
            },
            "required": ["target_agent"],
        }

        return Tool(
            transfer_to_agent,
            name=TRANSFER_TOOL_NAME,
            description="Transfer conversation control to another specialist agent in the group.",
            input_schema=schema,
        )
