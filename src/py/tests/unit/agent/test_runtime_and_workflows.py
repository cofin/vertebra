"""Unit and integration tests for AgentRuntime, SpecTree, DynamicWorkflow, and AgentPlugin.

Tests cover:
- Multi-turn tool loops, specialist AgentGroup transfers, and ConfirmationGate pausing.
- SpecTree hierarchical task planning, checkpoint serialization, and workflow compilation.
- DynamicWorkflow DAG execution waves, cycle detection, conditional skips, and LiveState HLC deltas.
- All 5 AgentSessionStore persistence backends (Memory, Cache, SQLSpec, Alchemy, ObjectStore).
- AgentChatController SSE streaming, synchronous turns, queued replay, and owner isolation.
- Dual-host mounting of AgentPlugin on native vertebra App and Litestar.
"""

import asyncio
import dataclasses
import sqlite3
from collections.abc import AsyncIterator, Mapping, Sequence
from types import SimpleNamespace
from typing import Any

import anyio
import msgspec
import pytest

from vertebra.agent import (
    A2AConfig,
    Agent,
    AgentCard,
    AgentChatController,
    AgentContext,
    AgentGroup,
    AgentMessage,
    AgentPlugin,
    AgentPluginConfig,
    AgentRuntime,
    AgentStreamFrame,
    AlchemySessionStore,
    CacheSessionStore,
    CallerIdentity,
    ChatRequest,
    ConfirmationDecisionRequest,
    ConfirmationGate,
    DynamicWorkflow,
    MemorySessionStore,
    ModelDelta,
    ObjectStoreSessionStore,
    PermissionDeniedError,
    SessionNotFoundError,
    SpecNode,
    SpecTree,
    SQLSpecSessionStore,
    Tool,
    TurnRequest,
    WorkflowContext,
    WorkflowEngine,
    WorkflowLiveState,
    build_tools,
)
from vertebra.agent.mcp import MCPConfig
from vertebra.agent.mcp.controllers import SkillController
from vertebra.agent.runtime import ConfirmationCapacityError, ModelProvider
from vertebra.db.driver import DatabaseError
from vertebra.framework._app import App
from vertebra.framework.live import LiveStateRoom
from vertebra.framework.stores import KeyValueStore
from vertebra.telemetry.genai import GenAiUsage
from vertebra.typing import ToolPolicyError, VertebraError
from vertebra.utils.serializers import decode_json, encode_json_bytes


class DummyMockModel(ModelProvider):
    """Deterministic scriptable model provider for testing turns and streaming."""

    def __init__(self, responses: Sequence[ModelDelta | list[ModelDelta]]) -> None:
        self.responses: list[ModelDelta | list[ModelDelta]] = list(responses)
        self.call_count: int = 0

    async def stream_chat(
        self,
        messages: Sequence[AgentMessage],
        tools: Sequence[Tool],
        system_instruction: str = "",
        *,
        context: AgentContext | None = None,
    ) -> AsyncIterator[ModelDelta]:
        """Yield scripted delta responses sequentially."""
        if not self.responses:
            yield ModelDelta(event_type="delta", text="Default response")
            return

        idx = min(self.call_count, len(self.responses) - 1)
        self.call_count += 1
        item = self.responses[idx]

        if isinstance(item, list):
            for delta in item:
                yield delta
        else:
            yield item


@pytest.mark.anyio
async def test_agent_runtime_tool_calling_and_budget() -> None:
    """Verify multi-turn tool calling loop and TurnBudget enforcement."""
    call_log: list[str] = []

    async def calculate_area(length: int, width: int) -> int:
        call_log.append(f"calc:{length}x{width}")
        return length * width

    calc_tool = Tool(
        name="calc_area",
        description="Compute rectangular area",
        fn=calculate_area,
        input_schema={"type": "object", "properties": {"length": {"type": "integer"}, "width": {"type": "integer"}}},
    )

    agent = Agent(
        name="MathAgent",
        description="Handles geometry calculations",
        tools=(calc_tool,),
    )

    deltas_step1 = [
        ModelDelta(
            event_type="tool_call",
            tool_name="calc_area",
            call_id="call_1",
            arguments={"length": 4, "width": 5},
        )
    ]
    deltas_step2 = [
        ModelDelta(
            event_type="delta",
            text="The area is 20 square units.",
            usage=GenAiUsage(input_tokens=15, output_tokens=10),
        )
    ]

    mock_model = DummyMockModel([deltas_step1, deltas_step2])
    runtime = AgentRuntime(model_provider=mock_model, default_agent=agent)

    req = TurnRequest(session_id="test_sess_1", message="What is the area of a 4x5 rectangle?")
    resp = await runtime.run_turn(req)

    assert resp.response == "The area is 20 square units."
    assert call_log == ["calc:4x5"]
    assert resp.agent_name == "MathAgent"
    assert resp.usage is not None and resp.usage.input_tokens > 0


@pytest.mark.anyio
async def test_agent_group_specialist_transfer() -> None:
    """Verify specialist agent delegation via synthesized transfer_to_agent tool."""
    agent_triage = Agent(name="TriageAgent", description="Triages customer requests")
    agent_billing = Agent(name="BillingAgent", description="Handles invoices and refunds")

    group = AgentGroup(
        name="SupportTeam",
        agents=(agent_triage, agent_billing),
        default_agent_name="TriageAgent",
    )

    triage_tools = group.get_agent_tools("TriageAgent")
    assert any(t.name == "transfer_to_agent" for t in triage_tools)

    step1 = [
        ModelDelta(
            event_type="tool_call",
            tool_name="transfer_to_agent",
            call_id="call_transfer",
            arguments={"target_agent": "BillingAgent"},
        )
    ]
    step2 = [ModelDelta(event_type="delta", text="Hello from BillingAgent, I can help with invoices.")]

    mock_model = DummyMockModel([step1, step2])
    runtime = AgentRuntime(model_provider=mock_model, agent_group=group)

    resp = await runtime.run_turn(TurnRequest(session_id="sess_transfer", message="I need a refund"))
    assert resp.agent_name == "BillingAgent"
    assert "BillingAgent" in resp.response

    stream_model = DummyMockModel([step1, step2])
    stream_runtime = AgentRuntime(model_provider=stream_model, agent_group=group)
    frames = [
        frame
        async for frame in stream_runtime.stream_turn(
            TurnRequest(session_id="sess_transfer_stream", message="I need a refund")
        )
    ]
    assert any(frame.event_type == "agent_transfer" and frame.agent_name == "BillingAgent" for frame in frames)
    assert any(frame.event_type == "delta" and "BillingAgent" in frame.text for frame in frames)
    assert frames[-1].event_type == "done"
    assert frames[-1].agent_name == "BillingAgent"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("replay_tool", "replay_args"),
    [
        ("drop_db", {"db_name": "production"}),
        ("purge_audit_log", {"db_name": "staging"}),
    ],
    ids=["mutated-arguments", "different-tool"],
)
async def test_agent_runtime_confirmation_binds_tool_and_arguments(
    replay_tool: str,
    replay_args: dict[str, Any],
) -> None:
    """Verify run_turn and stream_turn reject approved tokens replayed for another tool or mutated arguments."""
    gate = ConfirmationGate()
    executed: list[tuple[str, str]] = []

    async def drop_db(db_name: str) -> str:
        executed.append(("drop_db", db_name))
        return f"dropped:{db_name}"

    async def purge_audit_log(db_name: str) -> str:
        executed.append(("purge_audit_log", db_name))
        return f"purged:{db_name}"

    agent = Agent(
        name="AdminAgent",
        tools=(
            Tool(name="drop_db", description="Drop database", fn=drop_db, requires_confirmation=True),
            Tool(
                name="purge_audit_log",
                description="Purge audit log",
                fn=purge_audit_log,
                requires_confirmation=True,
            ),
        ),
    )
    approved_ticket = gate.request_confirmation("drop_db", "call_initial", {"db_name": "staging"})
    assert gate.approve(approved_ticket.token) is True

    replay_step = [
        ModelDelta(
            event_type="tool_call",
            tool_name=replay_tool,
            call_id="call_replay",
            arguments=replay_args,
        )
    ]
    runtime = AgentRuntime(model_provider=DummyMockModel([replay_step]), default_agent=agent, confirmation_gate=gate)
    resp = await runtime.run_turn(
        TurnRequest(session_id="sess_bind", message="Run operation", confirmation_token=approved_ticket.token)
    )
    assert resp.status == "confirmation_required"
    assert resp.confirmation_token is not None
    assert resp.confirmation_token != approved_ticket.token
    assert executed == []

    stream_runtime = AgentRuntime(
        model_provider=DummyMockModel([replay_step]),
        default_agent=agent,
        confirmation_gate=gate,
    )
    frames = [
        frame
        async for frame in stream_runtime.stream_turn(
            TurnRequest(
                session_id="sess_bind_stream", message="Run operation", confirmation_token=approved_ticket.token
            )
        )
    ]
    assert frames[-1].event_type == "confirmation_required"
    assert frames[-1].tool_name == replay_tool
    assert executed == []


@pytest.mark.anyio
@pytest.mark.parametrize("streaming", [False, True])
async def test_multi_call_step_preserves_approved_ticket_when_sibling_call_blocks(streaming: bool) -> None:
    """Approved ticket is not consumed if a sibling tool call in the same step pauses for confirmation."""
    gate = ConfirmationGate()
    executed: list[str] = []

    async def drop_db(db_name: str) -> str:
        executed.append(f"drop:{db_name}")
        return db_name

    async def purge_audit_log(days: int) -> int:
        executed.append(f"purge:{days}")
        return days

    agent = Agent(
        name="AdminAgent",
        tools=(
            Tool(name="drop_db", description="Drop database", fn=drop_db, requires_confirmation=True),
            Tool(name="purge_audit_log", description="Purge audit log", fn=purge_audit_log, requires_confirmation=True),
        ),
    )
    approved = gate.request_confirmation("drop_db", "call_1", {"db_name": "staging"})
    assert gate.approve(approved.token) is True

    multi_step = [
        ModelDelta(event_type="tool_call", tool_name="drop_db", call_id="call_1", arguments={"db_name": "staging"}),
        ModelDelta(event_type="tool_call", tool_name="purge_audit_log", call_id="call_2", arguments={"days": 30}),
    ]
    runtime = AgentRuntime(model_provider=DummyMockModel([multi_step]), default_agent=agent, confirmation_gate=gate)
    req = TurnRequest(session_id="sess_multi", message="Clean staging", confirmation_token=approved.token)
    if streaming:
        frames = [frame async for frame in runtime.stream_turn(req)]
        assert frames[-1].event_type == "confirmation_required"
        assert frames[-1].tool_name == "purge_audit_log"
    else:
        resp = await runtime.run_turn(req)
        assert resp.status == "confirmation_required"
        assert resp.confirmation_ticket is not None
        assert resp.confirmation_ticket.tool_name == "purge_audit_log"
    still_approved = gate.get_ticket(approved.token)
    assert still_approved is not None
    assert still_approved.status == "approved"
    assert executed == []


@pytest.mark.anyio
async def test_confirmation_gate_pause_and_approval() -> None:
    """Verify tools requiring confirmation pause the turn until approved."""
    gate = ConfirmationGate()
    executed_dangerous_tool: list[str] = []

    async def drop_database() -> str:
        executed_dangerous_tool.append("dropped")
        return "Database deleted"

    danger_tool = Tool(
        name="drop_db",
        description="Drop production database",
        fn=drop_database,
        requires_confirmation=True,
    )

    agent = Agent(name="AdminAgent", tools=(danger_tool,))
    deltas = [
        ModelDelta(
            event_type="tool_call",
            tool_name="drop_db",
            call_id="call_drop",
            arguments={},
        )
    ]

    mock_model = DummyMockModel([deltas])
    runtime = AgentRuntime(model_provider=mock_model, default_agent=agent, confirmation_gate=gate)

    req1 = TurnRequest(session_id="sess_gate", message="Please drop the database")
    resp1 = await runtime.run_turn(req1)

    assert resp1.confirmation_token is not None
    assert executed_dangerous_tool == []
    assert resp1.status == "confirmation_required"

    token = resp1.confirmation_token
    approved = gate.approve(token)
    assert approved is True

    step_after_confirm = [
        ModelDelta(
            event_type="tool_call",
            tool_name="drop_db",
            call_id="call_drop",
            arguments={},
        ),
        ModelDelta(event_type="delta", text="Database successfully dropped."),
    ]
    mock_model.responses = [step_after_confirm[0], step_after_confirm[1]]
    mock_model.call_count = 0

    req2 = TurnRequest(session_id="sess_gate", message="Proceed with drop", confirmation_token=token)
    resp2 = await runtime.run_turn(req2)
    assert executed_dangerous_tool == ["dropped"]
    assert "dropped" in resp2.response.lower()


@pytest.mark.anyio
async def test_spectree_decomposition_and_checkpoint() -> None:
    """Verify hierarchical task decomposition, readiness, and checkpoint round-trip."""
    tree = SpecTree(root_id="goal_project")
    root = SpecNode(id="goal_project", title="Build Web Application", description="Root goal")
    tree.add_node(root)

    subtask_db = SpecNode(id="task_db", title="Setup Database Schema")
    subtask_api = SpecNode(id="task_api", title="Implement REST Endpoints", depends_on=("task_db",))
    subtask_ui = SpecNode(id="task_ui", title="Implement Frontend Views", depends_on=("task_api",))

    tree.expand_subtasks("goal_project", (subtask_db, subtask_api, subtask_ui))

    ready = tree.ready_nodes()
    assert len(ready) == 1
    assert ready[0].id == "task_db"

    tree.mark_running("task_db")
    tree.mark_completed("task_db", output={"tables": ["users", "orders"]})

    ready2 = tree.ready_nodes()
    assert len(ready2) == 1
    assert ready2[0].id == "task_api"

    checkpoint_bytes = tree.to_checkpoint_bytes()
    restored_tree = SpecTree.from_checkpoint_bytes(checkpoint_bytes)
    assert restored_tree.root_id == "goal_project"
    restored_node = restored_tree.get_node("task_db")
    assert restored_node is not None
    assert restored_node.status == "completed"
    assert restored_node.output == {"tables": ["users", "orders"]}

    wf = tree.to_workflow()
    assert len(wf.nodes) >= 3


@pytest.mark.anyio
async def test_dynamic_workflow_waves_and_cycle_refusal() -> None:
    """Verify DynamicWorkflow topological wave execution and cycle refusal."""
    cyclic_wf = DynamicWorkflow(name="bad_cycle")
    cyclic_wf.add_step("stepA", lambda ctx: "A", depends_on=("stepB",))
    cyclic_wf.add_step("stepB", lambda ctx: "B", depends_on=("stepA",))

    with pytest.raises(RuntimeError, match="Cycle detected"):
        cyclic_wf.topological_waves()

    missing_wf = DynamicWorkflow(name="missing_dep")
    missing_wf.add_step("stepX", lambda ctx: "X", depends_on=("nonexistent",))

    with pytest.raises(ValueError, match="Unknown dependency"):
        missing_wf.topological_waves()

    valid_wf = DynamicWorkflow(name="data_pipeline")
    execution_order: list[str] = []

    async def step_fetch(ctx: WorkflowContext) -> int:
        execution_order.append("fetch")
        ctx.set("raw_data", 100)
        return 100

    async def step_clean(ctx: WorkflowContext) -> int:
        execution_order.append("clean")
        val = int(ctx.get("raw_data"))
        cleaned = val * 2
        ctx.set("cleaned_data", cleaned)
        return cleaned

    async def step_validate(ctx: WorkflowContext) -> bool:
        execution_order.append("validate")
        return True

    async def step_export(ctx: WorkflowContext) -> str:
        execution_order.append("export")
        return f"Exported: {ctx.get('cleaned_data')}"

    valid_wf.add_step("fetch", step_fetch)
    valid_wf.add_step("clean", step_clean, depends_on=("fetch",))
    valid_wf.add_step("validate", step_validate, depends_on=("fetch",))
    valid_wf.add_step("export", step_export, depends_on=("clean", "validate"))

    engine = WorkflowEngine()
    ctx = await engine.run(valid_wf)

    assert "fetch" in execution_order
    assert "clean" in execution_order
    assert "validate" in execution_order
    assert "export" in execution_order
    assert execution_order[0] == "fetch"
    assert execution_order[-1] == "export"
    assert ctx.get_output("export") == "Exported: 200"


@pytest.mark.anyio
async def test_workflow_live_state_delta_broadcasting() -> None:
    """Verify real-time Hybrid Logical Clock StateDeltaFrame broadcasting to LiveStateRoom."""
    live_state = WorkflowLiveState()
    room = LiveStateRoom(channel="workflow:stream", state=live_state)
    engine = WorkflowEngine(live_room=room)

    wf = DynamicWorkflow(name="monitored_workflow")

    async def fast_step(ctx: WorkflowContext) -> str:
        return "done_fast"

    wf.add_step("step1", fast_step)
    ctx = await engine.run(wf)

    assert ctx.get_output("step1") == "done_fast"
    assert len(room.published_frames) > 0


@pytest.mark.anyio
async def test_all_five_session_stores() -> None:
    """Verify all 5 AgentSessionStore backends round-trip messages and isolate owners."""
    msgs = [
        AgentMessage(role="user", content="Hello, agent!"),
        AgentMessage(role="assistant", content="Greetings, engineer!"),
    ]

    mem_store = MemorySessionStore(max_sessions=10, ttl_seconds=3600.0)
    await mem_store.save("alice", "s1", msgs)
    loaded_mem = await mem_store.load("alice", "s1")
    assert loaded_mem is not None
    assert loaded_mem[0].content == "Hello, agent!"
    assert await mem_store.load("bob", "s1") is None
    await mem_store.delete("alice", "s1")
    assert await mem_store.load("alice", "s1") is None

    backing_mem = KeyValueStore()
    cache_store = CacheSessionStore(backing_mem, ttl_seconds=60)
    await cache_store.save("charlie", "s2", msgs)
    loaded_cache = await cache_store.load("charlie", "s2")
    assert loaded_cache is not None
    assert loaded_cache[1].content == "Greetings, engineer!"
    assert await cache_store.load("david", "s2") is None
    await cache_store.delete("charlie", "s2")
    assert await cache_store.load("charlie", "s2") is None

    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE vertebra_agent_sessions ("
        "owner VARCHAR, session_id VARCHAR, messages TEXT, updated_at TIMESTAMP, "
        "PRIMARY KEY (owner, session_id))"
    )

    class SQLiteSyncDriver:
        def __init__(self, db: sqlite3.Connection) -> None:
            self.db = db

        def execute(self, query: str, params: dict[str, Any]) -> Any:
            norm_query = (
                query.replace(":owner", "?")
                .replace(":session_id", "?")
                .replace(":messages", "?")
                .replace(":updated_at", "?")
            )
            order = []
            if "owner" in params:
                order.append(params["owner"])
            if "session_id" in params:
                order.append(params["session_id"])
            if "messages" in params:
                order.append(params["messages"])
            if "updated_at" in params:
                order.append(params["updated_at"])
            return self.db.execute(norm_query, order)

        def fetch_one(self, query: str, params: dict[str, Any]) -> Any:
            cur = self.execute(query, params)
            return cur.fetchone()

    sql_store = SQLSpecSessionStore(SQLiteSyncDriver(conn))
    await sql_store.save("elena", "s3", msgs)
    loaded_sql = await sql_store.load("elena", "s3")
    assert loaded_sql is not None
    assert loaded_sql[0].content == "Hello, agent!"
    assert await sql_store.load("frank", "s3") is None
    await sql_store.delete("elena", "s3")
    assert await sql_store.load("elena", "s3") is None

    class MockAsyncSession:
        def __init__(self, storage_dict: dict[str, str]) -> None:
            self.storage = storage_dict

        async def __aenter__(self) -> "MockAsyncSession":
            return self

        async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            return None

        async def execute(self, statement: Any, params: dict[str, Any]) -> Any:
            text_str = str(statement)
            if "SELECT" in text_str:
                key = f"{params['owner']}:{params['session_id']}"
                val = self.storage.get(key)
                return SimpleNamespace(fetchone=lambda: (val,) if val else None)
            if "UPDATE" in text_str or "INSERT" in text_str:
                key = f"{params['owner']}:{params['session_id']}"
                self.storage[key] = params["messages"]
                return SimpleNamespace(rowcount=1)
            if "DELETE" in text_str:
                key = f"{params['owner']}:{params['session_id']}"
                self.storage.pop(key, None)
                return SimpleNamespace(rowcount=1)
            return SimpleNamespace(rowcount=0)

        async def commit(self) -> None:
            pass

    alchemy_storage: dict[str, str] = {}
    alchemy_store = AlchemySessionStore(lambda: MockAsyncSession(alchemy_storage))
    await alchemy_store.save("grace", "s4", msgs)
    loaded_alchemy = await alchemy_store.load("grace", "s4")
    assert loaded_alchemy is not None
    assert loaded_alchemy[0].content == "Hello, agent!"
    assert await alchemy_store.load("heidi", "s4") is None
    await alchemy_store.delete("grace", "s4")
    assert await alchemy_store.load("grace", "s4") is None

    class MockObjectStore:
        def __init__(self) -> None:
            self.objects: dict[str, bytes] = {}

        async def put(self, path: str, data: bytes) -> None:
            self.objects[path] = data

        async def get(self, path: str) -> bytes | None:
            return self.objects.get(path)

        async def delete(self, path: str) -> None:
            self.objects.pop(path, None)

    object_store = ObjectStoreSessionStore(MockObjectStore(), prefix="sessions/")
    await object_store.save("ivan", "s5", msgs)
    loaded_obj = await object_store.load("ivan", "s5")
    assert loaded_obj is not None
    assert loaded_obj[1].content == "Greetings, engineer!"
    assert await object_store.load("judy", "s5") is None
    await object_store.delete("ivan", "s5")
    assert await object_store.load("ivan", "s5") is None


@pytest.mark.anyio
async def test_agent_chat_controller_endpoints() -> None:
    """Verify AgentChatController turns, confirmation, session history, and owner isolation."""
    mock_model = DummyMockModel(
        [
            [
                ModelDelta(
                    event_type="delta",
                    text="Controller response",
                    usage=GenAiUsage(input_tokens=12, output_tokens=8),
                )
            ]
        ]
    )
    runtime = AgentRuntime(model_provider=mock_model)
    store = MemorySessionStore()
    ctrl = AgentChatController(runtime=runtime, session_store=store, base_path="/agent")

    user_ctx = AgentContext(identity=CallerIdentity(user_id="user_123", tenant_id="tenant_a"))

    turn_res = await ctrl.turns(ChatRequest(message="Hello API"), context=user_ctx)
    assert turn_res.response == "Controller response"
    sess_id = turn_res.session_id

    history = await ctrl.get_session(sess_id, context=user_ctx)
    assert len(history.messages) == 2
    assert history.messages[0].content == "Hello API"
    assert history.messages[1].content == "Controller response"

    stranger_ctx = AgentContext(identity=CallerIdentity(user_id="user_456", tenant_id="tenant_b"))
    with pytest.raises(SessionNotFoundError):
        await ctrl.get_session(sess_id, context=stranger_ctx)

    code, headers, body = await ctrl.handle_request(
        "POST",
        "/agent/turns",
        {},
        encode_json_bytes({"message": "Turn via HTTP"}),
        context=user_ctx,
    )
    assert code == 200
    parsed = decode_json(body)
    assert parsed["response"] == "Controller response"
    assert isinstance(parsed["usage"], dict)
    assert parsed["usage"]["input_tokens"] == 12
    assert parsed["usage"]["output_tokens"] == 8

    sse_frame = AgentStreamFrame(
        turn_id="turn_sse_1",
        event_type="delta",
        agent_name="DefaultAgent",
        text="streamed chunk",
        usage=GenAiUsage(input_tokens=5, output_tokens=3),
    ).to_sse_frame("1")
    sse_payload = decode_json(sse_frame.data)
    assert isinstance(sse_payload, dict)
    assert sse_payload["turn_id"] == "turn_sse_1"
    assert sse_payload["text"] == "streamed chunk"
    assert "tool_name" not in sse_payload
    assert isinstance(sse_payload["usage"], dict)
    assert sse_payload["usage"]["input_tokens"] == 5

    assert ChatRequest.__struct_config__.forbid_unknown_fields is True
    assert ConfirmationDecisionRequest.__struct_config__.forbid_unknown_fields is True
    with pytest.raises(msgspec.ValidationError):
        msgspec.convert({"message": "hi", "unexpected_field": 1}, ChatRequest)
    with pytest.raises(msgspec.ValidationError):
        msgspec.convert({"decision": "approve", "unexpected_field": 1}, ConfirmationDecisionRequest)


@pytest.mark.anyio
async def test_agent_plugin_dual_host_mounting() -> None:
    """Verify AgentPlugin mounting on native App and Litestar route discovery."""
    plugin = AgentPlugin(
        AgentPluginConfig(
            mcp=MCPConfig(server_name="test_plugin_mcp"),
            a2a=A2AConfig(agent_card=AgentCard(name="PluginAgent")),
            agent_base_path="/agent",
        )
    )

    native_app = App()
    plugin.mount_native(native_app)

    routes = [r.path for r in native_app.routes]
    assert "/.well-known/mcp.json" in routes
    assert "/mcp" in routes
    assert "/.well-known/agent-card.json" in routes
    assert "/a2a" in routes
    assert "/agent/chat" in routes
    assert "/agent/turns" in routes

    class DummyLitestarAppConfig:
        def __init__(self) -> None:
            self.state: dict[str, Any] = {}
            self.dependencies: dict[str, Any] = {}
            self.route_handlers: list[Any] = []

    class SampleSkill(SkillController):
        """Discovered SkillController registering domain skills."""

        def get_tools(self) -> list[Tool]:
            return [Tool(name="skill_action", description="Action tool", fn=lambda: "done")]

    dummy_config = DummyLitestarAppConfig()
    dummy_config.route_handlers.append(SampleSkill)

    plugin.on_app_init(dummy_config)

    assert dummy_config.state["mcp_service"] is not None
    assert dummy_config.state["a2a_service"] is not None
    assert dummy_config.state["agent_runtime"] is not None
    assert plugin.mcp_registry.get_tool("skill_action") is not None


class _ConfirmedToolProvider:
    """Neutral provider exposing confirmation and exact failure propagation."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.failure: BaseException | None = None

    def operations(self) -> dict[str, Any]:
        return {"query": self.query}

    def confirmation_reason(self, operation_name: str, arguments: Mapping[str, Any]) -> str | None:
        return "Approval required" if arguments["sql"] != "SELECT 1" else None

    async def query(self, sql: str, params: dict[str, Any] | None = None, limit: int = 10) -> dict[str, Any]:
        if self.failure is not None:
            raise self.failure
        result = {"sql": sql, "params": params, "limit": limit}
        self.calls.append(result)
        return result


@pytest.mark.anyio
async def test_provider_confirmation_binds_arguments_defaults_and_trusted_owner() -> None:
    """Approval binds deep-copied canonical arguments and injected caller identity."""
    provider = _ConfirmedToolProvider()
    gate = ConfirmationGate()
    tool = build_tools(provider, confirmation_gate=gate)["query"]
    owner = AgentContext(CallerIdentity(user_id="alice"))
    other = AgentContext(CallerIdentity(user_id="bob"))
    assert (await tool.invoke({"sql": "SELECT 1"}))["limit"] == 10
    params = {"ids": [1]}
    with pytest.raises(PermissionDeniedError) as exc:
        await tool.invoke({"sql": "UPDATE t", "params": params, "context": other}, context=owner)
    token = str(exc.value).split("ticket=", 1)[1].rstrip(")")
    ticket = gate.get_ticket(token)
    assert ticket is not None
    assert ticket.arguments == {"sql": "UPDATE t", "params": {"ids": [1]}, "limit": 10}
    assert ticket.owner_id == "default:alice"
    assert ticket.call_id == "call-query"
    params["ids"].append(2)
    assert ticket.arguments["params"] == {"ids": [1]}
    assert gate.approve(token) is False
    assert gate.approve(token, owner_id="default:alice") is True

    with pytest.raises(PermissionDeniedError):
        await tool.invoke(
            {"sql": "UPDATE t", "params": params, "confirmation_token": token, "context": owner},
            context=owner,
        )
    with pytest.raises(PermissionDeniedError):
        await tool.invoke(
            {"sql": "UPDATE other", "params": {"ids": [1]}, "confirmation_token": token, "context": owner},
            context=owner,
        )
    with pytest.raises(PermissionDeniedError):
        await tool.invoke(
            {"sql": "UPDATE t", "params": {"ids": [1]}, "confirmation_token": token, "context": owner},
            context=other,
        )
    with pytest.raises(PermissionDeniedError):
        await tool.invoke(
            {"sql": "UPDATE t", "params": {"ids": [1]}, "confirmation_token": token, "context": owner},
            context=None,
        )
    assert len(provider.calls) == 1

    result = await tool.invoke(
        {"sql": "UPDATE t", "params": {"ids": [1]}, "limit": 10, "confirmation_token": token},
        context=owner,
    )
    assert result == ticket.arguments
    assert len(provider.calls) == 2

    consumed_ticket = gate.get_ticket(token)
    assert consumed_ticket is not None
    assert consumed_ticket.status == "consumed"

    with pytest.raises(PermissionDeniedError) as replay_exc:
        await tool.invoke(
            {"sql": "UPDATE t", "params": {"ids": [1]}, "limit": 10, "confirmation_token": token},
            context=owner,
        )
    replay_token = str(replay_exc.value).split("ticket=", 1)[1].rstrip(")")
    assert replay_token != token
    assert len(provider.calls) == 2


@pytest.mark.anyio
@pytest.mark.parametrize("token_kind", ["missing", "unknown", "pending", "rejected", "wrong-tool", "wrong-arguments"])
async def test_provider_confirmation_denies_invalid_tickets(token_kind: str) -> None:
    """Only actual approved tickets for the same operation and arguments execute."""
    provider = _ConfirmedToolProvider()
    gate = ConfirmationGate()
    token: str | None = None
    if token_kind == "unknown":
        token = "unknown-token"
    elif token_kind != "missing":
        ticket = gate.request_confirmation(
            "other" if token_kind == "wrong-tool" else "query",
            "call-query",
            {"sql": "UPDATE other" if token_kind == "wrong-arguments" else "UPDATE t", "params": None, "limit": 10},
        )
        token = ticket.token
        if token_kind == "rejected":
            gate.reject(token)
        elif token_kind != "pending":
            gate.approve(token)
    with pytest.raises(PermissionDeniedError):
        await build_tools(provider, confirmation_gate=gate)["query"].invoke(
            {"sql": "UPDATE t", "confirmation_token": token}
        )
    assert provider.calls == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    "failure_factory",
    [
        lambda: DatabaseError("native"),
        lambda: PermissionError("native permission"),
        lambda: RuntimeError("driver"),
        asyncio.CancelledError,
    ],
    ids=["database-error", "permission-error", "runtime-error", "cancelled-error"],
)
async def test_provider_tools_preserve_native_failures(failure_factory: Any) -> None:
    """Native driver and cancellation failures retain exact exception identity."""
    provider = _ConfirmedToolProvider()
    tool = build_tools(provider)["query"]
    failure = failure_factory()
    provider.failure = failure
    with pytest.raises(type(failure)) as exc:
        await tool.invoke({"sql": "SELECT 1"})
    assert exc.value is failure


@pytest.mark.anyio
async def test_provider_tools_translate_explicit_policy_errors() -> None:
    """Explicit ToolPolicyError refusals translate to PermissionDeniedError."""
    provider = _ConfirmedToolProvider()
    tool = build_tools(provider)["query"]
    provider.failure = ToolPolicyError("policy limit")
    with pytest.raises(PermissionDeniedError, match="policy limit"):
        await tool.invoke({"sql": "SELECT 1"})


@pytest.mark.anyio
async def test_provider_execution_uses_isolated_approved_arguments() -> None:
    """Caller mutations after authorization cannot change parameters awaiting execution."""
    entered = asyncio.Event()
    resume = asyncio.Event()

    class PausedProvider(_ConfirmedToolProvider):
        async def query(self, sql: str, params: dict[str, Any] | None = None, limit: int = 10) -> dict[str, Any]:
            entered.set()
            await resume.wait()
            return {"sql": sql, "params": params, "limit": limit}

    gate = ConfirmationGate()
    tool = build_tools(PausedProvider(), confirmation_gate=gate)["query"]
    params = {"ids": [1]}
    with pytest.raises(PermissionDeniedError) as exc:
        await tool.invoke({"sql": "UPDATE t", "params": params})
    token = str(exc.value).split("ticket=", 1)[1].rstrip(")")
    gate.approve(token)
    task = asyncio.create_task(tool.invoke({"sql": "UPDATE t", "params": params, "confirmation_token": token}))
    try:
        async with asyncio.timeout(5):
            await entered.wait()
            params["ids"].append(2)
            resume.set()
            assert (await task)["params"] == {"ids": [1]}
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    ticket = gate.get_ticket(token)
    assert ticket is not None
    assert ticket.arguments["params"] == {"ids": [1]}


@pytest.mark.anyio
@pytest.mark.parametrize("approved,replacement", [(1, True), (True, 1), (1, 1.0), (1.0, 1)])
async def test_provider_confirmation_distinguishes_nested_scalar_types(approved: Any, replacement: Any) -> None:
    """Equal Python scalars with different native bind types require separate approval."""
    provider = _ConfirmedToolProvider()
    gate = ConfirmationGate()
    tool = build_tools(provider, confirmation_gate=gate)["query"]
    original = {"sql": "UPDATE t", "params": {"group": [{"value": approved}], "other": 2}}
    with pytest.raises(PermissionDeniedError) as exc:
        await tool.invoke(original)
    token = str(exc.value).split("ticket=", 1)[1].rstrip(")")
    gate.approve(token)
    with pytest.raises(PermissionDeniedError):
        await tool.invoke(
            {"sql": "UPDATE t", "params": {"other": 2, "group": [{"value": replacement}]}, "confirmation_token": token}
        )
    assert provider.calls == []
    equivalent = await tool.invoke(
        {
            "sql": "UPDATE t",
            "params": {"other": 2, "group": [{"value": approved}]},
            "limit": 10,
            "confirmation_token": token,
        }
    )
    assert type(equivalent["params"]["group"][0]["value"]) is type(approved)
    assert len(provider.calls) == 1


class _ApprovalStruct(msgspec.Struct, frozen=True, gc=False):
    values: dict[str, Any]


@dataclasses.dataclass(frozen=True)
class _ApprovalDataclass:
    values: dict[str, Any]


class _RecordConfirmedProvider(_ConfirmedToolProvider):
    """Provider accepting records converted by the public tool pipeline."""

    def __init__(self, record_type: str) -> None:
        super().__init__()
        self.record_type = record_type

    def operations(self) -> dict[str, Any]:
        return {"query": self.struct_query if self.record_type == "struct" else self.dataclass_query}

    async def struct_query(self, sql: str, record: _ApprovalStruct) -> Any:
        self.calls.append({"sql": sql, "record": record})
        return record

    async def dataclass_query(self, sql: str, record: _ApprovalDataclass) -> Any:
        self.calls.append({"sql": sql, "record": record})
        return record


@pytest.mark.anyio
@pytest.mark.parametrize("record_type", ["struct", "dataclass"])
@pytest.mark.parametrize("approved,replacement", [(1, True), (1, 1.0)])
async def test_provider_confirmation_compares_converted_record_fields(
    record_type: str, approved: Any, replacement: Any
) -> None:
    """Converted records retain recursive field types when binding approval."""
    provider = _RecordConfirmedProvider(record_type)
    gate = ConfirmationGate()
    tool = build_tools(provider, confirmation_gate=gate)["query"]
    original = {"sql": "UPDATE t", "record": {"values": {"items": [{"value": approved}], "other": 2}}}
    with pytest.raises(PermissionDeniedError) as exc:
        await tool.invoke(original)
    token = str(exc.value).split("ticket=", 1)[1].rstrip(")")
    gate.approve(token)
    with pytest.raises(PermissionDeniedError):
        await tool.invoke(
            {
                "sql": "UPDATE t",
                "record": {"values": {"other": 2, "items": [{"value": replacement}]}},
                "confirmation_token": token,
            }
        )
    assert provider.calls == []
    result = await tool.invoke({**original, "confirmation_token": token})
    assert type(result.values["items"][0]["value"]) is type(approved)
    assert len(provider.calls) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("container", [tuple, set, frozenset])
@pytest.mark.parametrize("replacement", [True, 1.0])
async def test_provider_confirmation_compares_container_element_types(container: Any, replacement: Any) -> None:
    """Approval distinguishes scalar types inside ordered and unordered containers."""
    provider = _ConfirmedToolProvider()
    gate = ConfirmationGate()
    tool = build_tools(provider, confirmation_gate=gate)["query"]
    with pytest.raises(PermissionDeniedError) as exc:
        await tool.invoke({"sql": "UPDATE t", "params": {"values": container([1, 2])}})
    token = str(exc.value).split("ticket=", 1)[1].rstrip(")")
    gate.approve(token)
    with pytest.raises(PermissionDeniedError):
        await tool.invoke(
            {"sql": "UPDATE t", "params": {"values": container([replacement, 2])}, "confirmation_token": token}
        )
    assert provider.calls == []
    result = await tool.invoke(
        {"sql": "UPDATE t", "params": {"values": container([1, 2])}, "confirmation_token": token}
    )
    assert result["params"]["values"] == container([1, 2])
    assert len(provider.calls) == 1


def test_confirmation_gate_removes_is_call_approved_and_enforces_exact_owner() -> None:
    """ConfirmationGate removes is_call_approved and rejects owner_id=None on owned tickets."""
    assert not hasattr(ConfirmationGate, "is_call_approved")
    gate = ConfirmationGate()
    ticket = gate.request_confirmation("drop_db", "call_1", {"db": "prod"}, owner_id="acme:alice")

    assert gate.approve(ticket.token, owner_id=None) is False
    assert gate.reject(ticket.token, owner_id=None) is False
    assert gate.approve(ticket.token, owner_id="acme:bob") is False
    assert gate.approve(ticket.token, owner_id="acme:alice") is True
    assert gate.is_approved(ticket.token, owner_id=None) is False
    assert gate.is_approved(ticket.token, owner_id="acme:bob") is False
    assert gate.is_approved(ticket.token, owner_id="acme:alice") is True
    assert (
        gate.consume_call_approval(ticket.token, tool_name="drop_db", arguments={"db": "prod"}, owner_id=None) is False
    )
    assert (
        gate.consume_call_approval(
            ticket.token,
            tool_name="drop_db",
            arguments={"db": "prod"},
            owner_id="acme:alice",
        )
        is True
    )
    assert (
        gate.consume_call_approval(
            ticket.token,
            tool_name="drop_db",
            arguments={"db": "prod"},
            owner_id="acme:alice",
        )
        is False
    )


@pytest.mark.parametrize(
    ("initial_action", "second_action", "expected_status"),
    [
        ("reject", "approve", "rejected"),
        ("reject", "reject", "rejected"),
        ("approve", "reject", "approved"),
        ("approve", "approve", "approved"),
        ("consume", "approve", "consumed"),
        ("consume", "reject", "consumed"),
    ],
    ids=[
        "reject-then-approve",
        "reject-then-reject",
        "approve-then-reject",
        "approve-then-approve",
        "consume-then-approve",
        "consume-then-reject",
    ],
)
def test_confirmation_gate_pending_only_immutability(
    initial_action: str,
    second_action: str,
    expected_status: str,
) -> None:
    """Only pending tickets accept approve or reject transitions."""
    gate = ConfirmationGate()
    ticket = gate.request_confirmation("drop_db", "call_1", {"db": "prod"}, owner_id="acme:alice")
    if initial_action == "reject":
        assert gate.reject(ticket.token, owner_id="acme:alice", reason="denied") is True
    elif initial_action == "approve":
        assert gate.approve(ticket.token, owner_id="acme:alice") is True
    else:
        assert gate.approve(ticket.token, owner_id="acme:alice") is True
        assert (
            gate.consume_call_approval(
                ticket.token,
                tool_name="drop_db",
                arguments={"db": "prod"},
                owner_id="acme:alice",
            )
            is True
        )

    if second_action == "approve":
        assert gate.approve(ticket.token, owner_id="acme:alice") is False
    else:
        assert gate.reject(ticket.token, owner_id="acme:alice") is False

    current = gate.get_ticket(ticket.token)
    assert current is not None
    assert current.status == expected_status


@pytest.mark.parametrize("approve_before_expiry", [False, True], ids=["pending-expiry", "approved-expiry"])
def test_confirmation_gate_ttl_expiry_with_injected_clock(approve_before_expiry: bool) -> None:
    """Injected clock advances past ttl_seconds and transitions tickets to expired."""
    now = [1_700_000_000.0]
    gate = ConfirmationGate(ttl_seconds=30.0, clock=lambda: now[0])
    ticket = gate.request_confirmation("drop_db", "call_1", {"db": "prod"}, owner_id="acme:alice")
    assert ticket.expires_at_unix_ms == 1_700_000_030_000

    if approve_before_expiry:
        assert gate.approve(ticket.token, owner_id="acme:alice") is True

    now[0] += 30.0
    assert gate.approve(ticket.token, owner_id="acme:alice") is False
    assert gate.reject(ticket.token, owner_id="acme:alice") is False
    assert gate.is_approved(ticket.token, owner_id="acme:alice") is False
    assert (
        gate.consume_call_approval(
            ticket.token,
            tool_name="drop_db",
            arguments={"db": "prod"},
            owner_id="acme:alice",
        )
        is False
    )
    expired = gate.get_ticket(ticket.token)
    assert expired is not None
    assert expired.status == "expired"


@pytest.mark.parametrize("prune_kind", ["rejected", "consumed", "expired"])
def test_confirmation_gate_max_tickets_pruning_and_capacity_error(prune_kind: str) -> None:
    """Inactive entries are pruned at capacity before raising ConfirmationCapacityError on live tickets."""
    now = [1_700_000_000.0]
    gate = ConfirmationGate(ttl_seconds=60.0, max_tickets=2, clock=lambda: now[0])
    first = gate.request_confirmation("op", "c1", {"id": 1})
    second = gate.request_confirmation("op", "c2", {"id": 2})

    if prune_kind == "rejected":
        assert gate.reject(first.token) is True
    elif prune_kind == "consumed":
        assert gate.approve(first.token) is True
        assert gate.consume_call_approval(first.token, tool_name="op", arguments={"id": 1}) is True
    else:
        now[0] += 60.0
        second = gate.request_confirmation("op", "c2_fresh", {"id": 2})

    third = gate.request_confirmation("op", "c3", {"id": 3})
    assert gate.get_ticket(first.token) is None
    assert gate.get_ticket(second.token) is not None
    assert gate.get_ticket(third.token) is not None

    with pytest.raises(ConfirmationCapacityError) as cap_exc:
        gate.request_confirmation("op", "c4", {"id": 4})
    assert isinstance(cap_exc.value, VertebraError)
    assert isinstance(cap_exc.value, RuntimeError)


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["run_turn", "stream_turn"])
async def test_agent_runtime_single_use_confirmation_rejects_replay(mode: str) -> None:
    """Approved confirmation tokens execute once in run_turn and stream_turn and reject replay."""
    gate = ConfirmationGate()
    executed: list[str] = []

    async def drop_db(db_name: str) -> str:
        executed.append(db_name)
        return f"dropped:{db_name}"

    agent = Agent(
        name="AdminAgent",
        tools=(Tool(name="drop_db", description="Drop database", fn=drop_db, requires_confirmation=True),),
    )
    ticket = gate.request_confirmation("drop_db", "call_1", {"db_name": "staging"})
    assert gate.approve(ticket.token) is True

    call_step = [
        ModelDelta(event_type="tool_call", tool_name="drop_db", call_id="call_1", arguments={"db_name": "staging"})
    ]
    done_step = [ModelDelta(event_type="delta", text="Completed.")]

    if mode == "run_turn":
        runtime = AgentRuntime(
            model_provider=DummyMockModel([call_step, done_step, call_step]),
            default_agent=agent,
            confirmation_gate=gate,
        )
        first_resp = await runtime.run_turn(
            TurnRequest(session_id="s1", message="Drop staging", confirmation_token=ticket.token)
        )
        assert first_resp.status == "completed"
        assert executed == ["staging"]

        replay_resp = await runtime.run_turn(
            TurnRequest(session_id="s1", message="Drop staging again", confirmation_token=ticket.token)
        )
        assert replay_resp.status == "confirmation_required"
        assert replay_resp.confirmation_token is not None
        assert replay_resp.confirmation_token != ticket.token
        assert executed == ["staging"]
    else:
        runtime = AgentRuntime(
            model_provider=DummyMockModel([call_step, done_step, call_step]),
            default_agent=agent,
            confirmation_gate=gate,
        )
        first_frames = [
            frame
            async for frame in runtime.stream_turn(
                TurnRequest(session_id="s1", message="Drop staging", confirmation_token=ticket.token)
            )
        ]
        assert first_frames[-1].event_type == "done"
        assert executed == ["staging"]

        replay_frames = [
            frame
            async for frame in runtime.stream_turn(
                TurnRequest(session_id="s1", message="Drop staging again", confirmation_token=ticket.token)
            )
        ]
        assert replay_frames[-1].event_type == "confirmation_required"
        assert replay_frames[-1].payload is not None
        assert replay_frames[-1].payload["token"] != ticket.token
        assert executed == ["staging"]


@pytest.mark.parametrize(
    "bad_table",
    ["bad; DROP TABLE x", "sessions--", "123invalid", ""],
    ids=["semicolon-drop", "sql-comment", "leading-digit", "empty"],
)
def test_sqlspec_session_store_rejects_invalid_table_name(bad_table: str) -> None:
    """Verify SQLSpecSessionStore validates table_name at initialization."""
    with pytest.raises(ValueError):
        SQLSpecSessionStore(table_name=bad_table)


@pytest.mark.anyio
async def test_sqlspec_session_store_lock_manager_serializes_and_evicts() -> None:
    """Verify _LockManager serializes concurrent tasks on the same session and evicts idle locks."""
    store = SQLSpecSessionStore(table_name="valid_sessions")
    active = 0
    max_active = 0
    completed = 0

    async def worker(idx: int) -> None:
        nonlocal active, max_active, completed
        async with store.lock("owner-1", "sess-1"):
            active += 1
            max_active = max(max_active, active)
            await anyio.sleep(0.005)
            active -= 1
            completed += 1
        async with store.lock("owner-2", f"sess-{idx}"):
            await anyio.sleep(0)

    async with anyio.create_task_group() as tg:
        for i in range(6):
            tg.start_soon(worker, i)

    assert completed == 6
    assert max_active == 1
    assert store._locks._locks == {}
    assert store._locks._refcounts == {}
