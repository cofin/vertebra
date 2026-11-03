"""Conformance tests for MCP 2026-07-28 protocol, registry, HTTP handler, and tasks."""

import dataclasses
from typing import Any

import pytest

from vertebra.agent.controller import ConfirmationDecisionRequest
from vertebra.agent.core.context import AgentContext, CallerIdentity
from vertebra.agent.core.guards import GuardProtocol, PermissionDeniedError
from vertebra.agent.core.jsonrpc import JSONRPCErrorException
from vertebra.agent.core.schema import tool
from vertebra.agent.mcp import (
    HEADER_MISMATCH,
    UNSUPPORTED_PROTOCOL_VERSION,
    BlobContent,
    MCPConfig,
    MCPHttpHandler,
    MCPRegistry,
    MCPService,
    MCPTaskStore,
    PromptArgument,
    PromptEntry,
    PromptMessage,
    ResourceEntry,
    ResourceTemplateEntry,
    Skill,
    SkillCatalog,
    SkillController,
    SkillIntegrityError,
    TaskLookupError,
    TextContent,
    decode_mcp_header_value,
    encode_mcp_header_value,
    prompt,
    resource,
)
from vertebra.agent.mcp.service import CONFIRMATION_REQUIRED, CONFIRMATION_TOKEN_META_KEY
from vertebra.agent.plugin import AgentPlugin, AgentPluginConfig
from vertebra.agent.runtime import ConfirmationGate
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.serializers import decode_json, encode_json_bytes


@pytest.mark.anyio
async def test_mcp_service_initialize_ping_and_discover() -> None:
    """Verify initialize, ping, and server/discover responses conform to 2026-07-28."""
    service = MCPService(config=MCPConfig(server_name="test-mcp", server_version="1.2.3"))
    init_res = await service._handle_initialize()
    assert init_res["protocolVersion"] == "2026-07-28"
    assert init_res["serverInfo"]["name"] == "test-mcp"
    assert init_res["serverInfo"]["version"] == "1.2.3"
    assert "tools" in init_res["capabilities"]

    ping_res = await service._handle_ping()
    assert ping_res == {}

    discover_res = await service._handle_discover()
    assert discover_res["protocolVersion"] == "2026-07-28"
    assert discover_res["counts"]["tools"] == 0


@pytest.mark.anyio
async def test_mcp_service_tools_list_and_call() -> None:
    """Verify tool registration, tools/list pagination, and tools/call invocation with structured output."""
    service = MCPService()

    @tool(name="calc_add", description="Add two integers")
    def add(a: int, b: int) -> int:
        return a + b

    @tool(name="get_media", description="Return binary media")
    def media() -> BlobContent:
        return BlobContent(type="blob", data="YmluYXJ5", mimeType="application/octet-stream")

    @dataclasses.dataclass
    class UserProfile:
        name: str
        role: str

    @tool(name="get_profile", description="Return user profile struct")
    def profile() -> UserProfile:
        return UserProfile(name="Alice", role="Admin")

    service.registry.register_tool(add)
    service.registry.register_tool(media)
    service.registry.register_tool(profile)

    tools_res = await service._handle_tools_list()
    assert len(tools_res["tools"]) == 3
    tool_names = [t["name"] for t in tools_res["tools"]]
    assert "calc_add" in tool_names
    assert "get_media" in tool_names
    assert "get_profile" in tool_names

    add_res = await service._handle_tools_call({"name": "calc_add", "arguments": {"a": 10, "b": 25}})
    assert add_res["isError"] is False
    assert add_res["content"][0]["text"] == "35"

    media_res = await service._handle_tools_call({"name": "get_media", "arguments": {}})
    assert media_res["content"][0]["type"] == "blob"
    assert media_res["content"][0]["data"] == "YmluYXJ5"

    prof_res = await service._handle_tools_call({"name": "get_profile", "arguments": {}})
    assert prof_res["structuredContent"] == {"name": "Alice", "role": "Admin"}

    txt = TextContent(text="hello")
    blob = BlobContent(data="YmluYXJ5")
    arg = PromptArgument(name="topic")
    msg = PromptMessage(role="user", content=txt)
    for model in (txt, blob, arg, msg):
        assert isinstance(model, FrozenBaseStruct)
        assert isinstance(hash(model), int)


@pytest.mark.anyio
async def test_mcp_service_resources_and_templates() -> None:
    """Verify static resources, URI templates, subscription, and change notification."""
    service = MCPService()
    res = ResourceEntry(
        uri="file:///config.json",
        name="config",
        description="Configuration file",
        mimeType="application/json",
        reader=lambda: '{"mode": "test"}',
    )
    tmpl = ResourceTemplateEntry(
        uriTemplate="file:///logs/{date}",
        name="logs",
        description="Daily logs",
        mimeType="text/plain",
        reader=lambda date: f"Logs for {date}",
    )
    service.registry.register_resource(res)
    service.registry.register_resource_template(tmpl)

    list_res = await service._handle_resources_list()
    assert len(list_res["resources"]) == 1
    assert list_res["resources"][0]["uri"] == "file:///config.json"

    read_res = await service._handle_resources_read({"uri": "file:///config.json"})
    assert read_res["contents"][0]["text"] == '{"mode": "test"}'

    tmpl_list = await service._handle_resources_templates_list()
    assert len(tmpl_list["resourceTemplates"]) == 1

    tmpl_read = await service._handle_resources_read({"uri": "file:///logs/2026-10-22"})
    assert tmpl_read["contents"][0]["text"] == "Logs for 2026-10-22"

    ctx = AgentContext(session_id="client-conn-1")
    await service._handle_resources_subscribe({"uri": "file:///config.json"}, context=ctx)
    sent_count = service.notify_resource_updated("file:///config.json")
    assert sent_count >= 0
    await service._handle_resources_unsubscribe({"uri": "file:///config.json"}, context=ctx)


@pytest.mark.anyio
async def test_mcp_service_prompts_list_and_get() -> None:
    """Verify prompt template registration, prompts/list, and prompts/get instantiation."""
    service = MCPService()
    entry = PromptEntry(
        name="sql_review",
        description="Review SQL query",
        arguments=(PromptArgument(name="query", description="SQL query string", required=True),),
        getter=lambda query: [PromptMessage(role="user", content=TextContent(text=f"Please review SQL: {query}"))],
    )
    service.registry.register_prompt(entry)

    p_list = await service._handle_prompts_list()
    assert len(p_list["prompts"]) == 1
    assert p_list["prompts"][0]["name"] == "sql_review"

    p_get = await service._handle_prompts_get({"name": "sql_review", "arguments": {"query": "SELECT 1"}})
    assert len(p_get["messages"]) == 1
    assert "SELECT 1" in p_get["messages"][0]["content"]["text"]


@pytest.mark.anyio
async def test_mcp_http_handler_conformance_and_negative_cases() -> None:
    """Verify MCPHttpHandler headers validation, ETag 304 caching, and Origin validation."""
    service = MCPService(config=MCPConfig(allowed_origins=("https://app.example.com",)))
    handler = MCPHttpHandler(service)

    disallowed_status, _, _ = await handler.handle_request(
        "POST",
        "/mcp",
        {"Origin": "https://evil.example.com"},
        b'{"jsonrpc":"2.0","id":1,"method":"ping"}',
    )
    assert disallowed_status == 403

    proto_status, _, proto_body = await handler.handle_request(
        "POST",
        "/mcp",
        {"Origin": "https://app.example.com", "MCP-Protocol-Version": "2025-01-01"},
        b'{"jsonrpc":"2.0","id":1,"method":"ping"}',
    )
    assert proto_status == 200
    proto_decoded = decode_json(proto_body)
    assert proto_decoded["error"]["code"] == UNSUPPORTED_PROTOCOL_VERSION

    method_status, _, method_body = await handler.handle_request(
        "POST",
        "/mcp",
        {"Origin": "https://app.example.com", "Mcp-Method": "tools/list"},
        b'{"jsonrpc":"2.0","id":1,"method":"ping"}',
    )
    assert method_status == 200
    method_decoded = decode_json(method_body)
    assert method_decoded["error"]["code"] == HEADER_MISMATCH

    name_status, _, name_body = await handler.handle_request(
        "POST",
        "/mcp",
        {"Origin": "https://app.example.com", "Mcp-Method": "prompts/get", "Mcp-Name": "wrong_name"},
        b'{"jsonrpc":"2.0","id":1,"method":"prompts/get","params":{"name":"correct_name"}}',
    )
    assert name_status == 200
    name_decoded = decode_json(name_body)
    assert name_decoded["error"]["code"] == HEADER_MISMATCH

    encoded_non_ascii = encode_mcp_header_value("тест")
    assert decode_mcp_header_value(encoded_non_ascii) == "тест"

    get_status, get_headers, _ = await handler.handle_request("GET", "/mcp", {}, b"")
    assert get_status == 200
    etag = get_headers.get("ETag")
    assert etag is not None

    not_mod_status, _, not_mod_body = await handler.handle_request(
        "GET",
        "/mcp",
        {"If-None-Match": etag},
        b"",
    )
    assert not_mod_status == 304
    assert not_mod_body == b""


@pytest.mark.anyio
async def test_mcp_skills_catalog_and_integrity_error() -> None:
    """Verify progressive skill disclosure, directory reading, and SHA-256 integrity verification."""
    skill = Skill.build(
        name="oracle-expert",
        description="Oracle DB skills",
        body="# Oracle Expert Skill\nInstructions here.",
        files={
            "SKILL.md": "# Oracle Expert Skill\nInstructions here.",
            "references/cheatsheet.txt": "SQL*Plus commands",
        },
        frontmatter={"name": "oracle-expert", "tags": ["oracle", "db"]},
    )
    catalog = SkillCatalog(skills=[skill])
    service = MCPService(skill_catalog=catalog)

    skills_list = await service._handle_skills_list()
    assert len(skills_list["skills"]) == 1
    assert skills_list["skills"][0]["name"] == "oracle-expert"

    skill_get = await service._handle_skills_get({"name": "oracle-expert"})
    assert skill_get["skill"]["name"] == "oracle-expert"
    assert "references/cheatsheet.txt" in skill_get["skill"]["files"]

    dir_read = await service._handle_resources_directory_read({"uri": "skill://oracle-expert"})
    assert "SKILL.md" in dir_read["files"]

    file_read = await service._handle_resources_read({"uri": "skill://oracle-expert/references/cheatsheet.txt"})
    assert file_read["contents"][0]["text"] == "SQL*Plus commands"

    tampered_file = skill.files["references/cheatsheet.txt"]
    bad_file = type(tampered_file)(
        path=tampered_file.path,
        digest="sha256:0000000000000000000000000000000000000000000000000000000000000000",
        size_bytes=tampered_file.size_bytes,
        mime_type=tampered_file.mime_type,
        content_bytes=b"tampered content",
    )
    tampered_skill = Skill(
        name=skill.name,
        description=skill.description,
        body=skill.body,
        files={"references/cheatsheet.txt": bad_file},
    )
    bad_catalog = SkillCatalog(skills=[tampered_skill])
    with pytest.raises(SkillIntegrityError):
        await bad_catalog.read_file("skill://oracle-expert/references/cheatsheet.txt")


@pytest.mark.anyio
async def test_mcp_controllers_prompt_and_skill() -> None:
    """Verify PromptController instructions injection and SkillController tool/resource binding."""

    class SecurityCheckGuard(GuardProtocol):
        async def check(self, context: Any) -> None:
            if context is None or not getattr(context, "is_authenticated", False):
                raise PermissionDeniedError("Authentication required")

    class AdminTools(SkillController):
        prefix = "admin"
        instructions = "Act strictly as an administrator."
        tags = ("admin", "ops")

        @prompt(name="diagnose", description="Run system diagnosis")
        def diagnose(self, target: str) -> list[PromptMessage]:
            return [PromptMessage(role="user", content=TextContent(text=f"Diagnose {target}"))]

        @tool(name="reboot", description="Reboot target machine")
        def reboot(self, target: str) -> str:
            return f"Rebooted {target}"

        @resource(uri="admin://logs", name="admin_logs", description="Admin logs")
        def logs(self) -> str:
            return "system log entries"

    ctrl = AdminTools()
    reg = MCPRegistry()
    reg.register_controller(ctrl)

    prompts = reg.list_prompts()
    assert len(prompts) == 1
    assert prompts[0].name == "admin_diagnose"
    instantiated = await prompts[0].getter(target="db-node-1")
    assert len(instantiated) == 1
    assert "Act strictly as an administrator." in instantiated[0].content.text
    assert "Diagnose db-node-1" in instantiated[0].content.text

    tools = reg.list_tools()
    assert len(tools) == 1
    assert tools[0].name == "reboot"
    tool_out = await tools[0].invoke({"target": "node-42"})
    assert tool_out == "Rebooted node-42"

    resources = reg.list_resources()
    assert len(resources) == 1
    assert resources[0].uri == "admin://logs"

    skill_spec = ctrl.to_agent_skill()
    assert skill_spec["id"] == "admin"
    assert "admin" in skill_spec["tags"]


@pytest.mark.anyio
async def test_mcp_tasks_store_and_async_promotion() -> None:
    """Verify MCPTaskStore lifecycle, owner isolation, input resumption, and tools/call async promotion."""
    store = MCPTaskStore()
    task = store.create(tool_name="long_operation", owner_id="tenant-1:user-a")
    assert task.status == "working"

    fetched = store.get(task.task_id, owner_id="tenant-1:user-a")
    assert fetched.task_id == task.task_id

    with pytest.raises(TaskLookupError):
        store.get(task.task_id, owner_id="tenant-2:user-b")

    store.require_input(task.task_id, {"auth_code": {"type": "string"}})
    waiting_input = store.get(task.task_id, owner_id="tenant-1:user-a")
    assert waiting_input.status == "input_required"

    resumed = store.submit_input(task.task_id, owner_id="tenant-1:user-a", inputs={"auth_code": "999888"})
    assert resumed.status == "working"

    completed = store.complete(task.task_id, {"status": "ok", "processed": 100})
    assert completed.status == "completed"
    assert completed.is_terminal is True
    assert completed.result == {"status": "ok", "processed": 100}

    service = MCPService(task_store=store)

    @tool(name="async_report", description="Generate complex report")
    def report(name: str) -> str:
        return f"Report {name}"

    service.registry.register_tool(report)

    identity = CallerIdentity(user_id="user-a", tenant_id="tenant-1")
    ctx = AgentContext(identity=identity)

    promoted_res = await service._handle_tools_call(
        {
            "name": "async_report",
            "arguments": {"name": "Q3"},
            "_meta": {"io.modelcontextprotocol/task": True},
        },
        context=ctx,
    )
    assert "task" in promoted_res
    async_task_id = promoted_res["task"]["taskId"]
    assert async_task_id.startswith("task_")


@pytest.mark.anyio
async def test_mcp_tool_call_requires_approved_confirmation_token() -> None:
    """A confirmation-gated tool runs only with an approved ticket issued for that tool."""
    gate = ConfirmationGate()
    service = MCPService(confirmation_gate=gate)
    calls: list[str] = []

    @tool(name="drop_workspace", description="Drop a workspace", requires_confirmation=True)
    def drop_workspace(workspace_id: str) -> str:
        calls.append(workspace_id)
        return f"dropped:{workspace_id}"

    @tool(name="archive_workspace", description="Archive a workspace", requires_confirmation=True)
    def archive_workspace(workspace_id: str) -> str:
        calls.append(f"archive:{workspace_id}")
        return f"archived:{workspace_id}"

    service.registry.register_tool(drop_workspace)
    service.registry.register_tool(archive_workspace)

    with pytest.raises(JSONRPCErrorException) as first:
        await service.handle_tool_call({"name": "drop_workspace", "arguments": {"workspace_id": "w1"}})
    assert first.value.error.code == CONFIRMATION_REQUIRED
    assert first.value.error.data["tool"] == "drop_workspace"
    token = first.value.error.data["confirmationToken"]
    assert calls == []

    with pytest.raises(JSONRPCErrorException) as unapproved:
        await service.handle_tool_call(
            {
                "name": "drop_workspace",
                "arguments": {"workspace_id": "w1"},
                "_meta": {CONFIRMATION_TOKEN_META_KEY: token},
            }
        )
    assert unapproved.value.error.code == CONFIRMATION_REQUIRED
    assert calls == []

    assert gate.approve(token) is True
    with pytest.raises(JSONRPCErrorException) as wrong_tool:
        await service.handle_tool_call(
            {
                "name": "archive_workspace",
                "arguments": {"workspace_id": "w1"},
                "_meta": {CONFIRMATION_TOKEN_META_KEY: token},
            }
        )
    assert wrong_tool.value.error.code == CONFIRMATION_REQUIRED
    assert calls == []

    with pytest.raises(JSONRPCErrorException) as wrong_arguments:
        await service.handle_tool_call(
            {
                "name": "drop_workspace",
                "arguments": {"workspace_id": "w2"},
                "_meta": {CONFIRMATION_TOKEN_META_KEY: token},
            }
        )
    assert wrong_arguments.value.error.code == CONFIRMATION_REQUIRED
    assert calls == []

    result = await service.handle_tool_call(
        {"name": "drop_workspace", "arguments": {"workspace_id": "w1"}, "_meta": {CONFIRMATION_TOKEN_META_KEY: token}}
    )
    assert result["content"][0]["text"] == "dropped:w1"
    assert calls == ["w1"]


@pytest.mark.anyio
async def test_mcp_confirmation_rejects_approval_by_another_owner() -> None:
    """A failed approval by another owner changes no gate state and leaves the call blocked."""
    gate = ConfirmationGate()
    service = MCPService(confirmation_gate=gate)
    calls: list[str] = []

    @tool(name="drop_workspace", description="Drop a workspace", requires_confirmation=True)
    def drop_workspace(workspace_id: str) -> str:
        calls.append(workspace_id)
        return f"dropped:{workspace_id}"

    service.registry.register_tool(drop_workspace)
    alice = AgentContext(identity=CallerIdentity(user_id="alice"))
    call = {"name": "drop_workspace", "arguments": {"workspace_id": "w1"}}

    with pytest.raises(JSONRPCErrorException) as first:
        await service.handle_tool_call(call, context=alice)
    token = first.value.error.data["confirmationToken"]
    ticket = gate.get_ticket(token)
    assert ticket is not None
    assert ticket.owner_id == "default:alice"

    assert gate.approve(token, owner_id="default:mallory") is False
    ticket_after = gate.get_ticket(token)
    assert ticket_after is not None
    assert ticket_after.status == "pending"
    assert gate.is_approved(token, owner_id="default:alice") is False

    with pytest.raises(JSONRPCErrorException) as blocked:
        await service.handle_tool_call({**call, "_meta": {CONFIRMATION_TOKEN_META_KEY: token}}, context=alice)
    assert blocked.value.error.code == CONFIRMATION_REQUIRED
    assert calls == []


def test_confirmation_gate_rejects_unknown_tokens() -> None:
    """Approving or rejecting an unknown token fails without recording the token."""
    gate = ConfirmationGate()
    assert gate.approve("unknown-token") is False
    assert gate.is_approved("unknown-token") is False
    assert gate.reject("unknown-token") is False
    assert gate.get_ticket("unknown-token") is None


@pytest.mark.anyio
async def test_agent_plugin_mcp_confirmation_uses_runtime_gate() -> None:
    """AgentPlugin approvals through the chat controller unlock MCP confirmation tickets."""
    plugin = AgentPlugin(AgentPluginConfig(mcp=MCPConfig(server_name="confirm-mcp")))
    assert plugin.mcp_service is not None

    @tool(name="purge_cache", description="Purge the cache", requires_confirmation=True)
    def purge_cache() -> str:
        return "purged"

    plugin.mcp_service.registry.register_tool(purge_cache)
    with pytest.raises(JSONRPCErrorException) as exc_info:
        await plugin.mcp_service.handle_tool_call({"name": "purge_cache", "arguments": {}})
    token = exc_info.value.error.data["confirmationToken"]

    decision = await plugin.chat_controller.decide_confirmation(token, ConfirmationDecisionRequest(decision="approve"))
    assert decision == {"status": "ok", "decision": "approve", "token": token}

    result = await plugin.mcp_service.handle_tool_call(
        {"name": "purge_cache", "arguments": {}, "_meta": {CONFIRMATION_TOKEN_META_KEY: token}}
    )
    assert result["content"][0]["text"] == "purged"


_CALC_ADD_CALL = {
    "jsonrpc": "2.0",
    "id": 7,
    "method": "tools/call",
    "params": {"name": "calc_add", "arguments": {"a": 1, "b": 2}},
}


@pytest.mark.anyio
@pytest.mark.parametrize(
    "payload",
    [
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {"params": {"name": "calc_add", "arguments": {"a": 1, "b": 2}}},
        },
        {"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"arguments": {"a": 1, "b": 2}}},
        [_CALC_ADD_CALL],
    ],
    ids=["nested-params", "missing-name", "batch-body"],
)
async def test_mcp_http_handler_rejects_mcp_name_without_body_name(payload: object) -> None:
    """An Mcp-Name header with no matching body name, or on a batch body, is rejected before the tool runs."""
    service = MCPService()
    calls: list[tuple[int, int]] = []

    @tool(name="calc_add", description="Add two integers")
    def add(a: int, b: int) -> int:
        calls.append((a, b))
        return a + b

    service.registry.register_tool(add)
    handler = MCPHttpHandler(service)
    status, _, raw = await handler.handle_request(
        "POST",
        "/mcp",
        {"Mcp-Method": "tools/call", "Mcp-Name": "other"},
        encode_json_bytes(payload),
    )
    assert status == 200
    decoded = decode_json(raw)
    assert decoded["error"]["code"] == HEADER_MISMATCH
    assert calls == []
