"""Hierarchical PlanTree goal decomposition and checkpoint serialization.

Provides ``PlanTree``, ``PlanNode``, and ``PlanNodeStatus`` for decomposing
complex objectives into parent-child dependency trees, persisting checkpoints,
and compiling leaf steps into an executable ``Workflow`` DAG.
"""

from collections.abc import Mapping, Sequence
from typing import Any, Literal

import msgspec

from vertebra.agent.agents import Agent
from vertebra.agent.workflow import Workflow, WorkflowStep
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = (
    "PlanNode",
    "PlanNodeStatus",
    "PlanTree",
)

PlanNodeStatus = Literal[
    "pending",
    "ready",
    "running",
    "completed",
    "failed",
    "skipped",
]


class PlanNode(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Hierarchical goal, subtask, or action item in a PlanTree."""

    id: str
    """Unique node identifier within the `PlanTree`."""
    title: str
    """Concise summary title of the goal or subtask."""
    description: str = ""
    """Detailed specification or acceptance criteria for the node."""
    parent_id: str | None = None
    """Identifier of the parent `PlanNode`, or `None` for top-level goals."""
    depends_on: tuple[str, ...] = ()
    """Prerequisite node IDs that must complete before this node becomes ready."""
    assigned_agent: str | None = None
    """Optional specialist `Agent.name` assigned to execute this node."""
    status: PlanNodeStatus = "pending"
    """Execution lifecycle state (`pending`, `ready`, `running`, `completed`, `failed`, `skipped`)."""
    output: Any = None
    """Result payload or error details recorded upon node completion."""
    children: tuple[str, ...] = ()
    """Ordered child subtask node IDs decomposed under this node."""
    metadata: dict[str, Any] = msgspec.field(default_factory=dict)
    """Custom planning metadata attached to the node."""


class PlanTree:
    """Hierarchical task planner managing parent-child goals and checkpoint snapshots."""

    def __init__(self, root_id: str = "root", *, root_goal: str | None = None) -> None:
        self.root_id = root_id
        self._nodes: dict[str, PlanNode] = {}
        if root_goal is not None:
            self._nodes[root_id] = PlanNode(id=root_id, title=root_goal)

    def add_node(self, node: PlanNode) -> PlanNode:
        """Register a PlanNode in the PlanTree, linking to its parent if present."""
        self._nodes[node.id] = node
        if node.parent_id and node.parent_id in self._nodes:
            parent = self._nodes[node.parent_id]
            if node.id not in parent.children:
                updated_children = parent.children + (node.id,)
                updated_parent = PlanNode(
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
        return node

    def add_step(
        self,
        id: str,
        title: str | None = None,
        *,
        description: str = "",
        parent_id: str | None = None,
        depends_on: Sequence[str] = (),
        assigned_agent: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> PlanNode:
        """Create and register a PlanNode, attaching to the root node when present."""
        resolved_parent = parent_id
        if resolved_parent is None and id != self.root_id and self.root_id in self._nodes:
            resolved_parent = self.root_id
        node = PlanNode(
            id=id,
            title=title or id,
            description=description,
            parent_id=resolved_parent,
            depends_on=tuple(depends_on),
            assigned_agent=assigned_agent,
            metadata=dict(metadata or {}),
        )
        return self.add_node(node)

    def get_node(self, node_id: str) -> PlanNode | None:
        """Retrieve a node by its identifier."""
        return self._nodes.get(node_id)

    def list_nodes(self) -> list[PlanNode]:
        """Return all nodes in the tree."""
        return list(self._nodes.values())

    def expand_subtasks(self, parent_id: str, subtasks: Sequence[PlanNode]) -> None:
        """Dynamically decompose a parent goal into sequenced or parallel subtasks."""
        parent = self._nodes.get(parent_id)
        if parent is None:
            raise ValueError(f"Parent node '{parent_id}' does not exist in PlanTree")

        new_children = list(parent.children)
        for sub in subtasks:
            sub_with_parent = PlanNode(
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

        updated_parent = PlanNode(
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

    def ready_nodes(self) -> list[PlanNode]:
        """Return all pending or ready nodes whose dependencies are all completed."""
        ready: list[PlanNode] = []
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
        """Serialize the entire PlanTree state to canonical JSON bytes."""
        records = [decode_json(encode_json_bytes(n)) for n in self._nodes.values()]
        payload = {"rootId": self.root_id, "nodes": records}
        return encode_json_bytes(payload)

    @classmethod
    def from_checkpoint_bytes(cls, data: bytes) -> "PlanTree":
        """Reconstruct a PlanTree from serialized checkpoint bytes."""
        payload = decode_json(data)
        root_id = str(payload.get("rootId", "root"))
        tree = cls(root_id=root_id)
        for raw_node in payload.get("nodes", []):
            node = msgspec.convert(raw_node, PlanNode)
            tree._nodes[node.id] = node
        return tree

    def to_workflow(self, agents: Mapping[str, Agent] | None = None) -> Workflow:
        """Compile this PlanTree into an executable Workflow DAG."""
        workflow = Workflow(name=f"workflow_{self.root_id}")
        agent_map = dict(agents or {})

        for node in self._nodes.values():
            if node.children:
                continue

            node_id = node.id
            assigned_agent = agent_map.get(node.assigned_agent) if node.assigned_agent else None

            async def _executor(ctx: Any, tid: str = node_id, ag: Agent | None = assigned_agent) -> Any:
                if ag is not None:
                    return f"Executed {tid} with {ag.name}"
                return f"Executed {tid}"

            wf_step = WorkflowStep(
                name=node.id,
                fn=_executor,
                depends_on=node.depends_on,
                description=node.description or node.title,
            )
            workflow.add(wf_step)

        return workflow

    def _update_status(self, node_id: str, status: PlanNodeStatus, output: Any = None) -> None:
        node = self._nodes.get(node_id)
        if node is None:
            raise ValueError(f"Node '{node_id}' not found")
        updated = PlanNode(
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
