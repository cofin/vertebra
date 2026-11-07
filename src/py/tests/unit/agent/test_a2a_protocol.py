"""Comprehensive integration and unit tests for A2A 1.0 protocol, SSE streaming, signing, and bridges."""

from collections.abc import AsyncIterable
from typing import Any

import pytest

from vertebra.agent.a2a import (
    A2AConfig,
    A2AHttpHandler,
    A2AService,
    AgentCard,
    AgentCardSigner,
    AgentInterface,
    AgentSkill,
    Artifact,
    DataPart,
    FileContent,
    FilePart,
    Message,
    Task,
    TaskStatus,
    TextPart,
)
from vertebra.agent.bridges import A2aAgentBridge, McpToolBridge
from vertebra.agent.core.context import AgentContext, CallerIdentity
from vertebra.agent.core.schema import tool
from vertebra.agent.mcp import MCPConfig, MCPService, ToolRegistry
from vertebra.agent.plugin import AgentPlugin, AgentPluginConfig
from vertebra.telemetry.otel import TraceContext
from vertebra.typing import FrozenCamelizedStruct
from vertebra.utils.serializers import decode_json, encode_json_bytes


def test_a2a_1_0_models_camel_case_wire_roundtrip_and_card_validation() -> None:
    """Verify A2A 1.0 models serialize to camelCase and validate interface path bindings."""
    text_part = TextPart(text="Hello A2A")
    file_part = FilePart(file=FileContent(uri="file:///tmp/doc.txt", mime_type="text/plain"))
    data_part = DataPart(data={"temperature": 0.7})

    msg = Message(
        message_id="msg-101",
        role="user",
        parts=(text_part, file_part, data_part),
        context_id="ctx-42",
        metadata={"priority": "high"},
    )
    msg_bytes = encode_json_bytes(msg)
    msg_dict = decode_json(msg_bytes)

    assert msg_dict["messageId"] == "msg-101"
    assert msg_dict["contextId"] == "ctx-42"
    assert msg_dict["parts"][0]["kind"] == "text"
    assert msg_dict["parts"][1]["file"]["mimeType"] == "text/plain"
    assert msg_dict["parts"][2]["data"]["temperature"] == 0.7

    artifact = Artifact(
        artifact_id="art-202",
        parts=(text_part,),
        name="Summary",
        description="Summary artifact",
    )
    status = TaskStatus(state="working", message=msg)
    task = Task(task_id="task-303", status=status, artifacts=(artifact,), owner_id="tenant-a:user-1")
    task_dict = decode_json(encode_json_bytes(task))

    assert task_dict["taskId"] == "task-303"
    assert task_dict["status"]["state"] == "working"
    assert task_dict["artifacts"][0]["artifactId"] == "art-202"
    assert isinstance(msg, FrozenCamelizedStruct)
    assert isinstance(task, FrozenCamelizedStruct)
    assert isinstance(text_part, FrozenCamelizedStruct)
    assert "metadata" not in decode_json(encode_json_bytes(text_part))

    valid_card = _sample_card("/a2a")
    valid_card.validate_for_path("/a2a")
    valid_card.validate_for_path("a2a")

    proxy_card = AgentCard(
        name="proxy-agent",
        supported_interfaces=(
            AgentInterface(
                url="https://gateway.example.com/agents/v1/a2a",
                protocol_binding="JSONRPC",
                protocol_version="1.0",
            ),
        ),
    )
    proxy_card.validate_for_path("/a2a")

    invalid_binding_card = AgentCard(
        name="grpc-agent",
        supported_interfaces=(
            AgentInterface(
                url="http://localhost:8000/a2a",
                protocol_binding="GRPC",
                protocol_version="1.0",
            ),
        ),
    )
    with pytest.raises(ValueError, match="no supportedInterfaces with JSONRPC 1.0"):
        invalid_binding_card.validate_for_path("/a2a")

    mismatch_path_card = AgentCard(
        name="other-agent",
        supported_interfaces=(
            AgentInterface(
                url="http://localhost:8000/other-endpoint",
                protocol_binding="JSONRPC",
                protocol_version="1.0",
            ),
        ),
    )
    with pytest.raises(ValueError, match="no supportedInterfaces with JSONRPC 1.0"):
        mismatch_path_card.validate_for_path("/a2a")


def test_agent_card_jws_signing_and_tamper_rejection() -> None:
    """Verify RFC 7515 JWS HS256 signing and constant-time tamper rejection."""
    card = _sample_card()
    secret = "test-secret-signing-key-12345"

    signed_card = AgentCardSigner.sign(card, secret)
    assert len(signed_card.signatures) == 1
    sig = signed_card.signatures[0]
    assert sig.protected
    assert sig.signature

    assert AgentCardSigner.verify(signed_card, secret) is True
    assert AgentCardSigner.verify(signed_card, "wrong-secret-key") is False

    tampered_skill = AgentSkill(
        id="summarize",
        name="Tampered Summarizer",
        description="Tampered description",
    )
    tampered_card = AgentCard(
        name=signed_card.name,
        description=signed_card.description,
        version=signed_card.version,
        supported_interfaces=signed_card.supported_interfaces,
        capabilities=signed_card.capabilities,
        skills=(tampered_skill,),
        signatures=signed_card.signatures,
    )
    assert AgentCardSigner.verify(tampered_card, secret) is False


@pytest.mark.anyio
async def test_a2a_http_handler_agent_card_etag_304_and_rust_discovery() -> None:
    """Verify GET /.well-known/agent-card.json returns ETag, supports 304, and populates discovery cache."""
    card = _sample_card()
    service = A2AService(config=A2AConfig(agent_card=card))
    handler = A2AHttpHandler(service)

    status, headers, body = await handler.handle_request("GET", "/.well-known/agent-card.json", {}, b"")
    assert status == 200
    assert headers["A2A-Version"] == "1.0"
    assert headers["Content-Type"] == "application/json"
    assert "ETag" in headers
    etag = headers["ETag"]
    assert isinstance(body, bytes)
    assert decode_json(body)["name"] == "test-agent"

    status_304, _, body_304 = await handler.handle_request(
        "GET",
        "/.well-known/agent-card.json",
        {"if-none-match": etag},
        b"",
    )
    assert status_304 == 304
    assert body_304 == b""

    weak_etag = f"W/{etag.strip('"')}"
    status_weak, _, _ = await handler.handle_request(
        "GET",
        "/.well-known/agent-card.json",
        {"if-none-match": weak_etag},
        b"",
    )
    assert status_weak == 304

    class MockDiscoveryCache:
        registered_body: bytes | None = None

        def register_agent_card(self, payload: bytes) -> None:
            self.registered_body = payload

    cache = MockDiscoveryCache()
    handler.register_in_discovery_cache(cache)
    assert cache.registered_body == card.to_json_bytes()


@pytest.mark.anyio
async def test_a2a_agent_card_auto_interfaces_and_dynamic_request_url() -> None:
    """Verify AgentCard auto-populates JSONRPC 1.0 interface and resolves URL dynamically from request headers."""
    skill = AgentSkill(
        id="order-lookup",
        name="Order Lookup",
        description="Query order status.",
    )
    card = AgentCard(
        name="OrderSupportAgent",
        description="Resolves order status and shipment inquiries.",
        skills=(skill,),
    )
    assert card.supported_interfaces == ()
    assert card.has_explicit_interfaces() is False

    service = A2AService(A2AConfig(path="/a2a", agent_card=card))
    handler = A2AHttpHandler(service)

    status, headers, body = await handler.handle_request(
        "GET",
        "/.well-known/agent-card.json",
        {
            "Host": "agents.example.com",
            "X-Forwarded-Proto": "https",
        },
        b"",
    )
    assert status == 200
    assert "Vary" in headers
    payload = decode_json(body)
    assert payload["name"] == "OrderSupportAgent"
    assert payload["supportedInterfaces"] == [
        {
            "url": "https://agents.example.com/a2a",
            "protocolBinding": "JSONRPC",
            "protocolVersion": "1.0",
        }
    ]

    status_fwd, _, body_fwd = await handler.handle_request(
        "GET",
        "/.well-known/agent-card.json",
        {"Forwarded": 'for=192.0.2.60;proto=https;host="mesh.internal.example:8443"'},
        b"",
    )
    assert status_fwd == 200
    payload_fwd = decode_json(body_fwd)
    assert payload_fwd["supportedInterfaces"][0]["url"] == "https://mesh.internal.example:8443/a2a"

    class MockDiscoveryCache:
        registered_body: bytes | None = None

        def register_agent_card(self, payload: bytes) -> None:
            self.registered_body = payload

    dynamic_cache = MockDiscoveryCache()
    handler.register_in_discovery_cache(dynamic_cache)
    assert dynamic_cache.registered_body is None

    static_base_service = A2AService(
        A2AConfig(
            path="/a2a",
            base_url="https://static.agents.example.com",
            agent_card=card,
        )
    )
    static_base_handler = A2AHttpHandler(static_base_service)
    static_cache = MockDiscoveryCache()
    static_base_handler.register_in_discovery_cache(static_cache)
    assert static_cache.registered_body is not None
    assert (
        decode_json(static_cache.registered_body)["supportedInterfaces"][0]["url"]
        == "https://static.agents.example.com/a2a"
    )


@pytest.mark.anyio
async def test_a2a_message_send_get_list_and_owner_scoped_cancel() -> None:
    """Verify message/send, tasks/get, tasks/list, and owner isolation on cancellation."""
    card = _sample_card()
    service = A2AService(config=A2AConfig(agent_card=card))

    ctx_alice = AgentContext(identity=CallerIdentity(tenant_id="tenant-a", user_id="alice"))
    ctx_bob = AgentContext(identity=CallerIdentity(tenant_id="tenant-b", user_id="bob"))

    send_payload = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "1",
            "method": "message/send",
            "params": {
                "message": {
                    "messageId": "msg-alice-1",
                    "role": "user",
                    "parts": [{"kind": "text", "text": "Task for Alice"}],
                },
            },
        }
    )

    resp_bytes = await service.handle_payload(send_payload, context=ctx_alice)
    resp = decode_json(resp_bytes)
    assert "result" in resp
    task_id = resp["result"]["taskId"]
    assert resp["result"]["status"]["state"] == "completed"

    bob_get = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "2",
            "method": "tasks/get",
            "params": {"taskId": task_id},
        }
    )
    bob_get_resp = decode_json(await service.handle_payload(bob_get, context=ctx_bob))
    assert "error" in bob_get_resp
    assert bob_get_resp["error"]["code"] == -32001

    bob_cancel = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "3",
            "method": "tasks/cancel",
            "params": {"taskId": task_id},
        }
    )
    bob_cancel_resp = decode_json(await service.handle_payload(bob_cancel, context=ctx_bob))
    assert "error" in bob_cancel_resp
    assert bob_cancel_resp["error"]["code"] == -32001

    alice_get = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "4",
            "method": "tasks/get",
            "params": {"taskId": task_id},
        }
    )
    alice_get_resp = decode_json(await service.handle_payload(alice_get, context=ctx_alice))
    assert alice_get_resp["result"]["taskId"] == task_id

    alice_cancel = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "5",
            "method": "tasks/cancel",
            "params": {"taskId": task_id},
        }
    )
    alice_cancel_resp = decode_json(await service.handle_payload(alice_cancel, context=ctx_alice))
    assert "error" in alice_cancel_resp
    assert alice_cancel_resp["error"]["code"] == -32002

    unknown_cancel = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "6",
            "method": "tasks/cancel",
            "params": {"taskId": "nonexistent-task"},
        }
    )
    unknown_resp = decode_json(await service.handle_payload(unknown_cancel, context=ctx_alice))
    assert unknown_resp["error"]["code"] == -32001


@pytest.mark.anyio
async def test_a2a_message_stream_and_tasks_resubscribe_sse_replay() -> None:
    """Verify SSE streaming for message/stream and Last-Event-ID event replay via tasks/resubscribe."""
    card = _sample_card()

    async def _stepped_executor(
        msg: Message,
        context_id: str | None,
        task_id: str,
    ) -> tuple[TaskStatus, tuple[Artifact, ...]]:
        artifact = Artifact(
            artifact_id="art-stream-1",
            parts=(TextPart(text="Generated report"),),
            name="Report",
        )
        reply = Message(
            message_id="msg-final-1",
            role="agent",
            parts=(TextPart(text="Execution complete"),),
            task_id=task_id,
            context_id=context_id,
        )
        status = TaskStatus(state="completed", message=reply)
        return status, (artifact,)

    service = A2AService(config=A2AConfig(agent_card=card), executor=_stepped_executor)
    handler = A2AHttpHandler(service)
    ctx = AgentContext(identity=CallerIdentity(tenant_id="tenant-a", user_id="alice"))

    stream_req = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "s1",
            "method": "message/stream",
            "params": {
                "taskId": "task-stream-001",
                "message": {
                    "messageId": "msg-stream-in",
                    "role": "user",
                    "parts": [{"kind": "text", "text": "Run stepped task"}],
                },
            },
        }
    )

    status_code, headers, stream_body = await handler.handle_request(
        "POST",
        "/a2a",
        {"content-type": "application/json"},
        stream_req,
        context=ctx,
    )
    assert status_code == 200
    assert headers["Content-Type"] == "text/event-stream"
    assert isinstance(stream_body, AsyncIterable)

    events: list[dict[str, Any]] = []
    first_event_id: str = ""

    async for chunk in stream_body:
        lines = chunk.decode("utf-8").strip().splitlines()
        event_dict: dict[str, str] = {}
        for line in lines:
            if line.startswith("id: "):
                event_dict["id"] = line[4:]
            elif line.startswith("event: "):
                event_dict["event"] = line[7:]
            elif line.startswith("data: "):
                event_dict["data"] = line[6:]
        if "data" in event_dict:
            parsed_data = decode_json(event_dict["data"])
            events.append(parsed_data)
            if not first_event_id and "id" in event_dict:
                first_event_id = event_dict["id"]

    assert len(events) >= 3
    assert events[0]["status"]["state"] == "working"
    assert events[1]["artifact"]["artifactId"] == "art-stream-1"
    assert events[-1]["status"]["state"] == "completed"

    resubscribe_req = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "r1",
            "method": "tasks/resubscribe",
            "params": {
                "taskId": "task-stream-001",
                "lastEventId": first_event_id,
            },
        }
    )

    _, _, replay_body = await handler.handle_request(
        "POST",
        "/a2a",
        {"content-type": "application/json"},
        resubscribe_req,
        context=ctx,
    )
    assert isinstance(replay_body, AsyncIterable)

    replayed_events: list[dict[str, Any]] = []
    async for chunk in replay_body:
        for line in chunk.decode("utf-8").strip().splitlines():
            if line.startswith("data: "):
                replayed_events.append(decode_json(line[6:]))

    assert len(replayed_events) >= 2
    assert replayed_events[-1]["status"]["state"] == "completed"


@pytest.mark.anyio
async def test_a2a_push_notification_crud_delivery_and_ssrf_guard() -> None:
    """Verify push notification configuration CRUD and SSRF rejection."""
    card = _sample_card()
    service = A2AService(config=A2AConfig(agent_card=card, allow_insecure_webhooks=False))
    ctx = AgentContext(identity=CallerIdentity(tenant_id="tenant-a", user_id="alice"))

    create_task_payload = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "1",
            "method": "message/send",
            "params": {
                "taskId": "task-push-01",
                "message": {
                    "messageId": "m1",
                    "role": "user",
                    "parts": [{"kind": "text", "text": "start"}],
                },
            },
        }
    )
    await service.handle_payload(create_task_payload, context=ctx)

    ssrf_payload = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "2",
            "method": "tasks/pushNotificationConfig/set",
            "params": {
                "taskId": "task-push-01",
                "config": {"url": "http://169.254.169.254/latest/meta-data/"},
            },
        }
    )
    ssrf_resp = decode_json(await service.handle_payload(ssrf_payload, context=ctx))
    assert "error" in ssrf_resp
    assert ssrf_resp["error"]["code"] == -32602

    valid_payload = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "3",
            "method": "tasks/pushNotificationConfig/set",
            "params": {
                "taskId": "task-push-01",
                "config": {
                    "url": "https://webhook.example.com/a2a-notify",
                    "token": "secret-bearer-token",
                },
            },
        }
    )
    valid_resp = decode_json(await service.handle_payload(valid_payload, context=ctx))
    assert valid_resp["result"]["status"] == "configured"

    get_payload = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "4",
            "method": "tasks/pushNotificationConfig/get",
            "params": {"taskId": "task-push-01"},
        }
    )
    get_resp = decode_json(await service.handle_payload(get_payload, context=ctx))
    assert get_resp["result"]["url"] == "https://webhook.example.com/a2a-notify"
    assert get_resp["result"]["token"] == "secret-bearer-token"

    list_payload = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "5",
            "method": "tasks/pushNotificationConfig/list",
            "params": {"taskId": "task-push-01"},
        }
    )
    list_resp = decode_json(await service.handle_payload(list_payload, context=ctx))
    assert len(list_resp["result"]["configs"]) == 1

    delete_payload = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "6",
            "method": "tasks/pushNotificationConfig/delete",
            "params": {"taskId": "task-push-01"},
        }
    )
    del_resp = decode_json(await service.handle_payload(delete_payload, context=ctx))
    assert del_resp["result"]["deleted"] is True


@pytest.mark.anyio
async def test_bidirectional_mcp_and_a2a_bridges_preserve_trace_context() -> None:
    """Verify MCP -> A2aAgentBridge -> McpToolBridge -> MCP preserves unified trace context."""
    mcp_service = MCPService(config=MCPConfig(server_name="native-mcp"))

    @tool(name="echo_uppercase", description="Converts input text to uppercase")
    def echo_uppercase(prompt: str) -> str:
        return prompt.upper()

    mcp_service.registry.register_tool(echo_uppercase)

    a2a_service = A2aAgentBridge.from_mcp_service(
        mcp_service,
        base_url="http://localhost:8000",
        path="/a2a",
        name="bridged-agent",
    )
    assert a2a_service.config.agent_card is not None
    assert len(a2a_service.config.agent_card.skills) >= 1

    bridged_tools = McpToolBridge.from_a2a_service(a2a_service, prefix="remote")
    assert len(bridged_tools) == 1
    target_tool = bridged_tools[0]
    assert target_tool.name == "remote_echo_uppercase"

    root_trace = TraceContext.generate()
    TraceContext.set_current(root_trace)

    res = await target_tool.invoke({"prompt": "vertebra rocks"})
    assert res is not None
    assert res.get("status", {}).get("state") == "completed"

    spans = mcp_service.telemetry.span_exporter.get_spans()
    assert len(spans) > 0
    for span in spans:
        assert span.trace_id == root_trace.trace_id


@pytest.mark.anyio
async def test_a2a_skill_card_projects_from_toolspec_exposure() -> None:
    """A2AHttpHandler and AgentPlugin project AgentCard skills strictly from ToolSpec entries with 'a2a' in exposure."""
    registry = ToolRegistry()

    @tool(
        name="a2a_visible_tool",
        title="Visible A2A Skill",
        description="Projected into A2A agent card",
        exposure=frozenset({"mcp", "a2a"}),
    )
    def a2a_visible_tool(query: str) -> str:
        return query

    @tool(
        name="mcp_private_tool",
        description="Excluded from A2A agent card",
        exposure=frozenset({"mcp"}),
    )
    def mcp_private_tool(query: str) -> str:
        return query

    registry.register_tool(a2a_visible_tool)
    registry.register_tool(mcp_private_tool)

    card = _sample_card()
    service = A2AService(config=A2AConfig(agent_card=card))
    handler = A2AHttpHandler(service, registry=registry)

    status, _, body = await handler.handle_request("GET", "/.well-known/agent-card.json", {}, b"")
    assert status == 200
    card_doc = decode_json(body)
    skill_ids = [s["id"] for s in card_doc["skills"]]
    assert "a2a_visible_tool" in skill_ids
    assert "mcp_private_tool" not in skill_ids

    plugin = AgentPlugin(
        AgentPluginConfig(
            mcp=MCPConfig(server_name="dual-agent"),
            a2a=A2AConfig(agent_card=AgentCard(name="DualAgent", description="Dual protocol")),
        )
    )
    plugin.mcp_registry.register_tool(a2a_visible_tool)
    plugin.mcp_registry.register_tool(mcp_private_tool)
    assert plugin.a2a_service is not None
    resolved_plugin_card = plugin.a2a_service.resolve_agent_card()
    assert resolved_plugin_card is not None
    plugin_skill_ids = [s.id for s in resolved_plugin_card.skills]
    assert "a2a_visible_tool" in plugin_skill_ids
    assert "mcp_private_tool" not in plugin_skill_ids


def _sample_card(route_path: str = "/a2a") -> AgentCard:
    interface = AgentInterface(
        url=f"http://localhost:8000{route_path}",
        protocol_binding="JSONRPC",
        protocol_version="1.0",
    )
    skill = AgentSkill(
        id="summarize",
        name="Summarizer",
        description="Summarizes text into bullet points",
        tags=("nlp", "text"),
        examples=("Summarize the latest quarterly report",),
    )
    return AgentCard(
        name="test-agent",
        description="A2A test agent",
        version="1.0",
        supported_interfaces=(interface,),
        skills=(skill,),
    )
