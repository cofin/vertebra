"""Hierarchical SpecTree task planning, Agent definitions, and AgentGroup transfers.

This module provides Agent, AgentGroup, AgentMessage, and SpecTree for decomposing
complex objectives into parent-child dependency trees with checkpoint serialization.
"""

from collections.abc import Mapping, Sequence
from typing import Any, Literal

import msgspec

from vertebra.agent.core.schema import Tool, ToolCall
from vertebra.utils.serializers import decode_json, encode_json_bytes

TRANSFER_TOOL_NAME: str = "transfer_to_agent"

SpecNodeStatus = Literal[
    "pending",
    "ready",
    "running",
    "completed",
    "failed",
    "skipped",
]


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


class SpecNode(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Hierarchical goal, subtask, or action item in a SpecTree."""

    id: str
    """Unique node identifier within the `SpecTree`."""
    title: str
    """Concise summary title of the goal or subtask."""
    description: str = ""
    """Detailed specification or acceptance criteria for the node."""
    parent_id: str | None = None
    """Identifier of the parent `SpecNode`, or `None` for top-level goals."""
    depends_on: tuple[str, ...] = ()
    """Prerequisite node IDs that must complete before this node becomes ready."""
    assigned_agent: str | None = None
    """Optional specialist `Agent.name` assigned to execute this node."""
    status: SpecNodeStatus = "pending"
    """Execution lifecycle state (`pending`, `ready`, `running`, `completed`, `failed`, `skipped`)."""
    output: Any = None
    """Result payload or error details recorded upon node completion."""
    children: tuple[str, ...] = ()
    """Ordered child subtask node IDs decomposed under this node."""
    metadata: dict[str, Any] = msgspec.field(default_factory=dict)
    """Custom planning metadata attached to the node."""


class SpecTree:
    """Hierarchical task planner managing parent-child goals and checkpoint snapshots."""

    def __init__(self, root_id: str = "root") -> None:
        self.root_id = root_id
        self._nodes: dict[str, SpecNode] = {}

    def add_node(self, node: SpecNode) -> None:
        """Register a node in the SpecTree, linking to parent if applicable."""
        self._nodes[node.id] = node
        if node.parent_id and node.parent_id in self._nodes:
            parent = self._nodes[node.parent_id]
            if node.id not in parent.children:
                updated_children = parent.children + (node.id,)
                updated_parent = SpecNode(
                    id=parent.id,
                    title=parent.title,
                    description=parent.description,
                    parent_id=parent.parent_id,
                    depends_on=parent.depends_on,
                    assigned_agent=parent.assigned_agent,
                    status=parent.status,
                    output=parent.output,
                    children=updated_children,
                    metadata=parent.metadata,
                )
                self._nodes[parent.id] = updated_parent

    def get_node(self, node_id: str) -> SpecNode | None:
        """Retrieve a node by its identifier."""
        return self._nodes.get(node_id)

    def list_nodes(self) -> list[SpecNode]:
        """Return all nodes in the tree."""
        return list(self._nodes.values())

    def expand_subtasks(self, parent_id: str, subtasks: Sequence[SpecNode]) -> None:
        """Dynamically decompose a parent goal into sequenced or parallel subtasks."""
        parent = self._nodes.get(parent_id)
        if parent is None:
            raise ValueError(f"Parent node '{parent_id}' does not exist in SpecTree")

        new_children = list(parent.children)
        for sub in subtasks:
            sub_with_parent = SpecNode(
                id=sub.id,
                title=sub.title,
                description=sub.description,
                parent_id=parent_id,
                depends_on=sub.depends_on,
                assigned_agent=sub.assigned_agent,
                status=sub.status,
                output=sub.output,
                children=sub.children,
                metadata=sub.metadata,
            )
            self._nodes[sub.id] = sub_with_parent
            if sub.id not in new_children:
                new_children.append(sub.id)

        updated_parent = SpecNode(
            id=parent.id,
            title=parent.title,
            description=parent.description,
            parent_id=parent.parent_id,
            depends_on=parent.depends_on,
            assigned_agent=parent.assigned_agent,
            status="running" if parent.status == "pending" else parent.status,
            output=parent.output,
            children=tuple(new_children),
            metadata=parent.metadata,
        )
        self._nodes[parent_id] = updated_parent

    def ready_nodes(self) -> list[SpecNode]:
        """Return all pending or ready nodes whose dependencies are all completed."""
        ready: list[SpecNode] = []
        for node in self._nodes.values():
            if node.status not in ("pending", "ready"):
                continue
            deps_met = True
            for dep_id in node.depends_on:
                dep_node = self._nodes.get(dep_id)
                if dep_node is None or dep_node.status != "completed":
                    deps_met = False
                    break
            if deps_met:
                ready.append(node)
        return ready

    def _update_status(self, node_id: str, status: SpecNodeStatus, output: Any = None) -> None:
        node = self._nodes.get(node_id)
        if node is None:
            raise ValueError(f"Node '{node_id}' not found")
        updated = SpecNode(
            id=node.id,
            title=node.title,
            description=node.description,
            parent_id=node.parent_id,
            depends_on=node.depends_on,
            assigned_agent=node.assigned_agent,
            status=status,
            output=output if output is not None else node.output,
            children=node.children,
            metadata=node.metadata,
        )
        self._nodes[node_id] = updated

        if status == "completed" and node.parent_id:
            parent = self._nodes.get(node.parent_id)
            if (
                parent is not None
                and parent.children
                and all(
                    self._nodes.get(cid) is not None and self._nodes[cid].status == "completed"
                    for cid in parent.children
                )
            ):
                self._update_status(parent.id, "completed")

    def mark_running(self, node_id: str) -> None:
        """Mark a node as running."""
        self._update_status(node_id, "running")

    def mark_completed(self, node_id: str, output: Any = None) -> None:
        """Mark a node as completed and update parent if all children are done."""
        self._update_status(node_id, "completed", output=output)

    def mark_failed(self, node_id: str, error: Any = None) -> None:
        """Mark a node as failed."""
        self._update_status(node_id, "failed", output={"error": str(error)})

    def mark_skipped(self, node_id: str) -> None:
        """Mark a node as skipped."""
        self._update_status(node_id, "skipped")

    def to_checkpoint_bytes(self) -> bytes:
        """Serialize the entire SpecTree state to canonical JSON bytes."""
        records = [decode_json(encode_json_bytes(n)) for n in self._nodes.values()]
        payload = {"rootId": self.root_id, "nodes": records}
        return encode_json_bytes(payload)

    @classmethod
    def from_checkpoint_bytes(cls, data: bytes) -> "SpecTree":
        """Reconstruct a SpecTree from serialized checkpoint bytes."""
        payload = decode_json(data)
        root_id = str(payload.get("rootId", "root"))
        tree = cls(root_id=root_id)
        for raw_node in payload.get("nodes", []):
            node = msgspec.convert(raw_node, SpecNode)
            tree._nodes[node.id] = node
        return tree

    def to_workflow(self, agents: Mapping[str, Agent] | None = None) -> Any:
        """Compile this SpecTree into an executable DynamicWorkflow DAG."""
        from vertebra.agent.workflow import DynamicWorkflow, WorkflowNode

        workflow = DynamicWorkflow(name=f"workflow_{self.root_id}")
        agent_map = dict(agents or {})

        for node in self._nodes.values():
            if node.children:
                continue

            node_id = node.id
            assigned_agent = agent_map.get(node.assigned_agent) if node.assigned_agent else None

            async def _make_step(target_id: str, agent: Agent | None) -> Any:
                async def _step(ctx: Any) -> Any:
                    if agent is not None:
                        return f"Completed {target_id} using agent {agent.name}"
                    return f"Completed {target_id}"

                return _step

            async def _executor(ctx: Any, tid: str = node_id, ag: Agent | None = assigned_agent) -> Any:
                if ag is not None:
                    return f"Executed {tid} with {ag.name}"
                return f"Executed {tid}"

            wf_node = WorkflowNode(
                name=node.id,
                fn=_executor,
                depends_on=node.depends_on,
                description=node.description or node.title,
            )
            workflow.add_node(wf_node)

        return workflow


__all__ = [
    "TRANSFER_TOOL_NAME",
    "Agent",
    "AgentGroup",
    "AgentMessage",
    "SpecNode",
    "SpecNodeStatus",
    "SpecTree",
]
