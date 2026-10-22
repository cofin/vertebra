"""DynamicWorkflow DAG execution engine with LiveState delta broadcasting.

This module provides declarative DAG workflow specification and execution with Kahn's
topological wave parallelism, conditional edge branching, timeout enforcement,
resumable checkpoints, OpenTelemetry span instrumentation, and real-time
Hybrid Logical Clock (HLC) StateDeltaFrame broadcasting over vertebra.framework.live.
"""

import inspect
import uuid
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal

import anyio
import msgspec

from vertebra.agent.telemetry import AgentTelemetryRecorder
from vertebra.framework.live import LiveState, LiveStateRoom
from vertebra.utils.serializers import decode_json, encode_json_bytes


class StepResult(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Result of an individual workflow node execution step."""

    node_name: str
    output: Any = None
    status: Literal["completed", "failed", "skipped"] = "completed"
    error: str | None = None
    duration_ms: float = 0.0


class WorkflowCheckpoint(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Snapshot of a workflow execution state for pause and resume capabilities."""

    run_id: str
    workflow_name: str
    status: str = "running"
    completed_nodes: tuple[str, ...] = ()
    failed_nodes: tuple[str, ...] = ()
    skipped_nodes: tuple[str, ...] = ()
    node_outputs: dict[str, Any] = msgspec.field(default_factory=dict)
    context_state: dict[str, Any] = msgspec.field(default_factory=dict)

    def to_bytes(self) -> bytes:
        """Serialize checkpoint to JSON bytes."""
        return encode_json_bytes(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> "WorkflowCheckpoint":
        """Deserialize checkpoint from JSON bytes."""
        raw = decode_json(data)
        return msgspec.convert(raw, cls)


class WorkflowContext:
    """Execution context passed through nodes in a dynamic workflow DAG."""

    def __init__(
        self,
        run_id: str,
        workflow_name: str,
        *,
        state: dict[str, Any] | None = None,
        node_outputs: dict[str, Any] | None = None,
        completed_nodes: set[str] | None = None,
        failed_nodes: set[str] | None = None,
        skipped_nodes: set[str] | None = None,
    ) -> None:
        self.run_id = run_id
        self.workflow_name = workflow_name
        self.state: dict[str, Any] = dict(state or {})
        self.node_outputs: dict[str, Any] = dict(node_outputs or {})
        self.completed_nodes: set[str] = set(completed_nodes or ())
        self.failed_nodes: set[str] = set(failed_nodes or ())
        self.skipped_nodes: set[str] = set(skipped_nodes or ())

    def get_output(self, node_name: str) -> Any:
        """Return the output produced by a prior completed node."""
        return self.node_outputs.get(node_name)

    def set(self, key: str, value: Any) -> None:
        """Store a value in workflow context state."""
        self.state[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from workflow context state."""
        return self.state.get(key, default)


class WorkflowNode(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Declarative node within a dynamic workflow DAG."""

    name: str
    fn: Callable[[WorkflowContext], Any]
    depends_on: tuple[str, ...] = ()
    description: str = ""
    timeout_seconds: float | None = None
    condition: Callable[[WorkflowContext], bool] | None = None


class WorkflowLiveState(LiveState):
    """Reactive server-client synchronized state for a workflow run."""

    run_id: str = ""
    workflow_name: str = ""
    status: str = "pending"
    active_nodes: list[str] = []
    completed_nodes: list[str] = []
    node_outputs: dict[str, Any] = {}

    def __init__(
        self,
        *,
        run_id: str = "",
        workflow_name: str = "",
        status: str = "pending",
        active_nodes: list[str] | None = None,
        completed_nodes: list[str] | None = None,
        node_outputs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            run_id=run_id,
            workflow_name=workflow_name,
            status=status,
            active_nodes=list(active_nodes or []),
            completed_nodes=list(completed_nodes or []),
            node_outputs=dict(node_outputs or {}),
            **kwargs,
        )


class DynamicWorkflow:
    """Dynamic directed acyclic graph (DAG) workflow specification."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.nodes: dict[str, WorkflowNode] = {}

    def add_node(self, node: WorkflowNode) -> None:
        """Register a pre-constructed WorkflowNode."""
        self.nodes[node.name] = node

    def add_step(
        self,
        name: str,
        fn: Callable[[WorkflowContext], Any],
        *,
        depends_on: Sequence[str] = (),
        description: str = "",
        timeout_seconds: float | None = None,
        condition: Callable[[WorkflowContext], bool] | None = None,
    ) -> WorkflowNode:
        """Add a step function to the workflow."""
        node = WorkflowNode(
            name=name,
            fn=fn,
            depends_on=tuple(depends_on),
            description=description,
            timeout_seconds=timeout_seconds,
            condition=condition,
        )
        self.add_node(node)
        return node

    def _validate_dependencies(self) -> None:
        """Validate that all referenced dependencies exist and the graph is acyclic."""
        for name, node in self.nodes.items():
            for dep in node.depends_on:
                if dep not in self.nodes:
                    raise ValueError(f"Unknown dependency '{dep}' for node '{name}'")
        self.topological_waves()

    def topological_waves(self) -> list[list[WorkflowNode]]:
        """Compute execution waves via Kahn's topological sort algorithm."""
        for name, node in self.nodes.items():
            for dep in node.depends_on:
                if dep not in self.nodes:
                    raise ValueError(f"Unknown dependency '{dep}' for node '{name}'")

        in_degree: dict[str, int] = {name: len(node.depends_on) for name, node in self.nodes.items()}
        dependents: dict[str, list[str]] = {name: [] for name in self.nodes}
        for name, node in self.nodes.items():
            for dep in node.depends_on:
                dependents[dep].append(name)

        waves: list[list[WorkflowNode]] = []
        current_wave = [name for name, deg in in_degree.items() if deg == 0]
        visited_count = 0

        while current_wave:
            current_wave.sort()
            waves.append([self.nodes[name] for name in current_wave])
            visited_count += len(current_wave)
            next_wave: list[str] = []
            for name in current_wave:
                for dep_of in dependents[name]:
                    in_degree[dep_of] -= 1
                    if in_degree[dep_of] == 0:
                        next_wave.append(dep_of)
            current_wave = next_wave

        if visited_count != len(self.nodes):
            raise RuntimeError(f"Cycle detected in workflow DAG '{self.name}'")
        return waves


class WorkflowEngine:
    """Executes DynamicWorkflow DAGs with parallel waves, telemetry, and live HLC broadcasting."""

    def __init__(
        self,
        *,
        telemetry: AgentTelemetryRecorder | None = None,
        live_room: LiveStateRoom | None = None,
    ) -> None:
        self.telemetry = telemetry
        self.live_room = live_room

    def to_checkpoint(self, ctx: WorkflowContext, status: str = "running") -> WorkflowCheckpoint:
        """Create a resumable checkpoint from the current context state."""
        return WorkflowCheckpoint(
            run_id=ctx.run_id,
            workflow_name=ctx.workflow_name,
            status=status,
            completed_nodes=tuple(sorted(ctx.completed_nodes)),
            failed_nodes=tuple(sorted(ctx.failed_nodes)),
            skipped_nodes=tuple(sorted(ctx.skipped_nodes)),
            node_outputs=dict(ctx.node_outputs),
            context_state=dict(ctx.state),
        )

    async def run(
        self,
        workflow: DynamicWorkflow,
        initial_state: Mapping[str, Any] | None = None,
    ) -> WorkflowContext:
        """Execute all nodes in the workflow respecting dependencies and conditions."""
        waves = workflow.topological_waves()
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        ctx = WorkflowContext(
            run_id=run_id,
            workflow_name=workflow.name,
            state=dict(initial_state or {}),
        )
        return await self._execute_waves(workflow, waves, ctx)

    async def resume_run(
        self,
        workflow: DynamicWorkflow,
        checkpoint: WorkflowCheckpoint,
    ) -> WorkflowContext:
        """Resume execution of a workflow from a previously saved checkpoint."""
        waves = workflow.topological_waves()
        ctx = WorkflowContext(
            run_id=checkpoint.run_id,
            workflow_name=workflow.name,
            state=dict(checkpoint.context_state),
            node_outputs=dict(checkpoint.node_outputs),
            completed_nodes=set(checkpoint.completed_nodes),
            failed_nodes=set(checkpoint.failed_nodes),
            skipped_nodes=set(checkpoint.skipped_nodes),
        )
        return await self._execute_waves(workflow, waves, ctx)

    async def _execute_waves(
        self,
        workflow: DynamicWorkflow,
        waves: list[list[WorkflowNode]],
        ctx: WorkflowContext,
    ) -> WorkflowContext:
        """Execute topological waves with parallel task groups and live delta broadcasts."""
        live_state = WorkflowLiveState(
            run_id=ctx.run_id,
            workflow_name=workflow.name,
            status="running",
            active_nodes=[],
            completed_nodes=sorted(ctx.completed_nodes),
            node_outputs=dict(ctx.node_outputs),
        )

        broadcast_lock = anyio.Lock()

        async def _sync_and_broadcast() -> None:
            if self.live_room is not None:
                async with broadcast_lock:
                    await self.live_room.broadcast_pending()

        if self.live_room is not None:
            self.live_room.state = live_state
            await _sync_and_broadcast()

        active_nodes: set[str] = set()

        try:
            for wave in waves:
                nodes_to_run: list[WorkflowNode] = []
                for node in wave:
                    if node.name in ctx.completed_nodes or node.name in ctx.skipped_nodes:
                        continue

                    if any(dep in ctx.failed_nodes for dep in node.depends_on):
                        ctx.skipped_nodes.add(node.name)
                        continue

                    if node.condition is not None and not node.condition(ctx):
                        ctx.skipped_nodes.add(node.name)
                        continue

                    nodes_to_run.append(node)

                if not nodes_to_run:
                    continue

                async def _run_single_node(target_node: WorkflowNode) -> None:
                    async with broadcast_lock:
                        active_nodes.add(target_node.name)
                        live_state.active_nodes = sorted(active_nodes)
                    await _sync_and_broadcast()

                    outcome: Any = None
                    try:
                        if self.telemetry is not None:
                            with self.telemetry.span_workflow_step(
                                workflow.name,
                                ctx.run_id,
                                target_node.name,
                            ):
                                if target_node.timeout_seconds is not None:
                                    with anyio.fail_after(target_node.timeout_seconds):
                                        res = target_node.fn(ctx)
                                        if inspect.isawaitable(res):
                                            outcome = await res
                                        else:
                                            outcome = res
                                else:
                                    res = target_node.fn(ctx)
                                    if inspect.isawaitable(res):
                                        outcome = await res
                                    else:
                                        outcome = res
                        else:
                            if target_node.timeout_seconds is not None:
                                with anyio.fail_after(target_node.timeout_seconds):
                                    res = target_node.fn(ctx)
                                    if inspect.isawaitable(res):
                                        outcome = await res
                                    else:
                                        outcome = res
                            else:
                                res = target_node.fn(ctx)
                                if inspect.isawaitable(res):
                                    outcome = await res
                                else:
                                    outcome = res

                        ctx.node_outputs[target_node.name] = outcome
                        ctx.completed_nodes.add(target_node.name)

                        async with broadcast_lock:
                            active_nodes.discard(target_node.name)
                            live_state.active_nodes = sorted(active_nodes)
                            live_state.completed_nodes = sorted(ctx.completed_nodes)
                            live_state.node_outputs = dict(ctx.node_outputs)
                        await _sync_and_broadcast()

                    except BaseException as exc:
                        ctx.failed_nodes.add(target_node.name)
                        async with broadcast_lock:
                            active_nodes.discard(target_node.name)
                            live_state.active_nodes = sorted(active_nodes)
                            live_state.status = "failed"
                        await _sync_and_broadcast()
                        raise exc

                async with anyio.create_task_group() as tg:
                    for node in nodes_to_run:
                        tg.start_soon(_run_single_node, node)

            live_state.status = "completed"
            live_state.active_nodes = []
            await _sync_and_broadcast()
            return ctx

        except BaseException as exc:
            live_state.status = "failed"
            live_state.active_nodes = []
            await _sync_and_broadcast()
            raise exc


__all__ = [
    "DynamicWorkflow",
    "StepResult",
    "WorkflowCheckpoint",
    "WorkflowContext",
    "WorkflowEngine",
    "WorkflowLiveState",
    "WorkflowNode",
]
