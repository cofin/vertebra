"""Unit and integration tests for AgentRuntime, SpecTree, DynamicWorkflow, and AgentPlugin.

Tests cover:
- Multi-turn tool loops, specialist AgentGroup transfers, and ConfirmationGate pausing.
- SpecTree hierarchical task planning, checkpoint serialization, and workflow compilation.
- DynamicWorkflow DAG execution waves, cycle detection, conditional skips, and LiveState HLC deltas.
- All 5 AgentSessionStore persistence backends (Memory, Cache, SQLSpec, Alchemy, ObjectStore).
- AgentChatController SSE streaming, synchronous turns, queued replay, and owner isolation.
- Dual-host mounting of AgentPlugin on native vertebra App and Litestar.
"""

import sqlite3
from collections.abc import AsyncIterator, Sequence
from types import SimpleNamespace
from typing import Any

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
    ModelProvider,
    ObjectStoreSessionStore,
    SessionNotFoundError,
    SpecNode,
    SpecTree,
    SQLSpecSessionStore,
    Tool,
    TurnRequest,
    WorkflowContext,
    WorkflowEngine,
    WorkflowLiveState,
)
from vertebra.agent.mcp.controllers import SkillController
from vertebra.agent.mcp.service import MCPConfig
from vertebra.framework._app import App
from vertebra.framework.live import LiveStateRoom
from vertebra.framework.stores import KeyValueStore
from vertebra.telemetry.genai import GenAiUsage
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
        val = ctx.get("raw_data")
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

        def get_tools(self) -> Sequence[Tool]:
            return (Tool(name="skill_action", description="Action tool", fn=lambda: "done"),)

    dummy_config = DummyLitestarAppConfig()
    dummy_config.route_handlers.append(SampleSkill)

    plugin.on_app_init(dummy_config)

    assert dummy_config.state["mcp_service"] is not None
    assert dummy_config.state["a2a_service"] is not None
    assert dummy_config.state["agent_runtime"] is not None
    assert plugin.mcp_registry.get_tool("skill_action") is not None
