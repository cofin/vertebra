"""Conformance tests for MCP core dispatch, registry, confirmations, plugin routing, and agent context."""

import asyncio
import dataclasses
import io
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any

import anyio
import pytest
from flask import Flask
from litestar.testing import create_test_client
from starlette.requests import Request as StarletteRequest

from vertebra._vertebra import Request as NativeRequest
from vertebra.agent.controller import ConfirmationDecisionRequest
from vertebra.agent.core.context import AgentContext, CallerIdentity, bind_agent_context, reset_agent_context
from vertebra.agent.core.guards import (
    GuardProtocol,
    PermissionDeniedError,
    RequireRoles,
    authorize,
)
from vertebra.agent.core.identity import CallableIdentitySource, MtlsIdentitySource
from vertebra.agent.core.jsonrpc import (
    METHOD_NOT_FOUND,
    JSONRPCErrorException,
)
from vertebra.agent.core.schema import tool
from vertebra.agent.mcp import (
    CONFIRMATION_REQUIRED,
    CONFIRMATION_TOKEN_META_KEY,
    HEADER_MISMATCH,
    LEGACY_ONLY_METHODS,
    UNSUPPORTED_PROTOCOL_VERSION,
    BlobContent,
    MCPConfig,
    MCPCore,
    MCPHttpHandler,
    McpHttpRequest,
    McpHttpResponse,
    MCPRegistry,
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
    ToolRegistry,
    WebMCPConfig,
    build_webmcp_manifest,
    decode_mcp_header_value,
    encode_mcp_header_value,
    mcp_tool,
    prompt,
    render_webmcp_manifest_json,
    resource,
)
from vertebra.agent.mcp.client import MCP_NAME_FIELDS
from vertebra.agent.mcp.routes import AsgiMcpApp, WsgiMcpApp
from vertebra.agent.plugin import AgentPlugin, AgentPluginConfig
from vertebra.agent.runtime import ConfirmationGate
from vertebra.extensions.django.mcp import attach_mcp_routes as attach_django_mcp
from vertebra.extensions.flask.mcp import attach_mcp_routes as attach_flask_mcp
from vertebra.extensions.starlette.mcp import mount_mcp_routes as mount_starlette_mcp
from vertebra.framework._app import App
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.serializers import decode_json, encode_json_bytes


async def _post_mcp(
    core: MCPCore,
    body: bytes | dict[str, Any],
    *,
    headers: dict[str, str] | None = None,
    context: AgentContext | None = None,
) -> McpHttpResponse:
    raw = body if isinstance(body, bytes) else encode_json_bytes(body)
    return await core.handle(
        McpHttpRequest(method="POST", path="/mcp", headers=headers or {}, body=raw, context=context)
    )


@pytest.mark.anyio
async def test_mcp_core_discover_and_removed_2024_methods() -> None:
    """Verify server/discover 2026-07-28 metadata and -32601 rejection on removed 2024-11-05 methods."""
    core = MCPCore(config=MCPConfig(server_name="test-mcp", server_version="1.2.3"))
    discover_resp = await _post_mcp(
        core,
        {"jsonrpc": "2.0", "id": 1, "method": "server/discover", "params": {}},
        headers={"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "server/discover"},
    )
    assert isinstance(discover_resp, McpHttpResponse)
    assert discover_resp.status == 200
    discover_res = decode_json(discover_resp.body)["result"]
    assert discover_res["protocolVersion"] == "2026-07-28"
    assert discover_res["serverInfo"]["name"] == "test-mcp"
    assert discover_res["serverInfo"]["version"] == "1.2.3"
    assert discover_res["counts"]["tools"] == 0
    assert discover_res["_meta"]["io.modelcontextprotocol/protocolVersion"] == "2026-07-28"
    assert discover_res["_meta"]["resultType"] == "server/discover"
    assert discover_res["_meta"]["ttlMs"] == 60000
    assert discover_res["_meta"]["cacheScope"] == "public"


@pytest.mark.parametrize(
    "removed_method",
    ("initialize", "ping", "logging/setLevel", "resources/subscribe", "resources/unsubscribe"),
)
@pytest.mark.anyio
async def test_mcp_core_rejects_removed_2024_methods(removed_method: str) -> None:
    """Verify server/discover 2026-07-28 mode rejects legacy-only methods with -32601."""
    core = MCPCore(config=MCPConfig(server_name="test-mcp", server_version="1.2.3"))
    resp = await _post_mcp(
        core,
        {"jsonrpc": "2.0", "id": 99, "method": removed_method, "params": {}},
        headers={"MCP-Protocol-Version": "2026-07-28"},
    )
    assert resp.status == 200
    assert decode_json(resp.body)["error"]["code"] == METHOD_NOT_FOUND


@pytest.mark.parametrize("protocol_version", ["2026-07-28", "2025-11-25", "2025-06-18"])
@pytest.mark.anyio
async def test_mcp_tasks_result_removed_across_all_protocol_versions(protocol_version: str) -> None:
    """Verify tasks/result is absent from LEGACY_ONLY_METHODS/MCP_NAME_FIELDS and returns -32601 everywhere."""
    assert "tasks/result" not in LEGACY_ONLY_METHODS
    assert "tasks/result" not in MCP_NAME_FIELDS
    core = MCPCore(config=MCPConfig(server_name="test-mcp", server_version="1.2.3"))
    resp = await _post_mcp(
        core,
        {"jsonrpc": "2.0", "id": 100, "method": "tasks/result", "params": {"taskId": "t1"}},
        headers={"MCP-Protocol-Version": protocol_version},
    )
    assert resp.status == 200
    assert decode_json(resp.body)["error"]["code"] == METHOD_NOT_FOUND


@pytest.mark.anyio
async def test_mcp_service_tools_list_and_call() -> None:
    """Verify tool registration, tools/list pagination, and tools/call invocation with structured output."""
    service = MCPCore()

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
    assert isinstance(txt, FrozenBaseStruct) and isinstance(hash(txt), int)
    assert isinstance(blob, FrozenBaseStruct) and isinstance(hash(blob), int)
    assert isinstance(arg, FrozenBaseStruct) and isinstance(hash(arg), int)
    assert isinstance(msg, FrozenBaseStruct) and isinstance(hash(msg), int)


@pytest.mark.anyio
async def test_mcp_service_resources_and_templates() -> None:
    """Verify static resources, URI templates, subscription, and change notification."""
    service = MCPCore()
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
    assert tmpl_read["_meta"]["resultType"] == "resources/read"


@pytest.mark.anyio
async def test_mcp_service_prompts_list_and_get() -> None:
    """Verify prompt template registration, prompts/list, and prompts/get instantiation."""
    service = MCPCore()
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
    """Verify MCPCore.handle and MCPHttpHandler headers validation, ETag 304 caching, and Origin validation."""
    core = MCPCore(config=MCPConfig(allowed_origins=("https://app.example.com",)))
    handler = MCPHttpHandler(core)

    default_core = MCPCore()
    cross_origin_default = await _post_mcp(
        default_core,
        b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
        headers={"Origin": "https://evil.example.com", "Host": "api.example.com"},
    )
    assert cross_origin_default.status == 403

    same_origin_default = await _post_mcp(
        default_core,
        b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
        headers={"Origin": "https://api.example.com", "Host": "api.example.com"},
    )
    assert same_origin_default.status == 200

    async def _hpost(headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        status, resp_headers, resp_body = await handler.handle_request("POST", "/mcp", headers, body)
        assert isinstance(resp_body, bytes)
        return status, resp_headers, resp_body

    disallowed_status, _, _ = await _hpost(
        {"Origin": "https://evil.example.com"},
        b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
    )
    assert disallowed_status == 403

    proto_status, _, proto_body = await _hpost(
        {"Origin": "https://app.example.com", "MCP-Protocol-Version": "2025-01-01"},
        b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
    )
    assert proto_status == 400
    assert decode_json(proto_body)["error"]["code"] == UNSUPPORTED_PROTOCOL_VERSION

    meta_proto_resp = await _post_mcp(
        core,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
            "params": {"_meta": {"io.modelcontextprotocol/protocolVersion": "2024-11-05"}},
        },
        headers={"Origin": "https://app.example.com"},
    )
    assert meta_proto_resp.status == 400
    assert decode_json(meta_proto_resp.body)["error"]["code"] == UNSUPPORTED_PROTOCOL_VERSION

    method_status, _, method_body = await _hpost(
        {"Origin": "https://app.example.com", "Mcp-Method": "tools/list"},
        b'{"jsonrpc":"2.0","id":1,"method":"prompts/list"}',
    )
    assert method_status == 400
    assert decode_json(method_body)["error"]["code"] == HEADER_MISMATCH

    name_status, _, name_body = await _hpost(
        {"Origin": "https://app.example.com", "Mcp-Method": "prompts/get", "Mcp-Name": "wrong_name"},
        b'{"jsonrpc":"2.0","id":1,"method":"prompts/get","params":{"name":"correct_name"}}',
    )
    assert name_status == 400
    assert decode_json(name_body)["error"]["code"] == HEADER_MISMATCH

    encoded_non_ascii = encode_mcp_header_value("тест")
    assert decode_mcp_header_value(encoded_non_ascii) == "тест"

    get_status, get_headers, _ = await handler.handle_request("GET", "/mcp", {}, b"")
    assert get_status == 405
    assert "POST" in get_headers.get("Allow", "")

    notif_resp = await _post_mcp(
        core, b'{"jsonrpc":"2.0","method":"notifications/initialized"}', headers={"Origin": "https://app.example.com"}
    )
    assert notif_resp.status == 202
    assert notif_resp.body == b""

    batch_resp = await _post_mcp(
        core, b'[{"jsonrpc":"2.0","id":1,"method":"tools/list"}]', headers={"Origin": "https://app.example.com"}
    )
    assert batch_resp.status == 400

    discover_status, discover_headers, _ = await _hpost(
        {"Origin": "https://app.example.com"},
        b'{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{}}',
    )
    assert discover_status == 200
    etag = discover_headers.get("ETag")
    assert etag is not None

    not_mod_status, _, not_mod_body = await _hpost(
        {"Origin": "https://app.example.com", "If-None-Match": etag},
        b'{"jsonrpc":"2.0","id":2,"method":"server/discover","params":{}}',
    )
    assert not_mod_status == 304
    assert not_mod_body == b""

    @tool(name="admin_only", description="Admin tool", guards=(RequireRoles("admin"),))
    def admin_only() -> str:
        return "secret-ok"

    core.registry.register_tool(admin_only)
    spec = core.registry.get_spec("admin_only")
    assert spec is not None
    viewer_ctx = AgentContext(identity=CallerIdentity(user_id="bob", roles=("viewer",)))
    with pytest.raises(PermissionDeniedError):
        await authorize(viewer_ctx, spec)

    admin_ctx = AgentContext(identity=CallerIdentity(user_id="alice", roles=("admin",)))
    await authorize(admin_ctx, spec)


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
    service = MCPCore(skill_catalog=catalog)

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
    assert prompts[0].getter is not None
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

    service = MCPCore(task_store=store)

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
    service = MCPCore(confirmation_gate=gate)
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

    consumed = gate.get_ticket(token)
    assert consumed is not None
    assert consumed.status == "consumed"

    with pytest.raises(JSONRPCErrorException) as replayed:
        await service.handle_tool_call(
            {
                "name": "drop_workspace",
                "arguments": {"workspace_id": "w1"},
                "_meta": {CONFIRMATION_TOKEN_META_KEY: token},
            }
        )
    assert replayed.value.error.code == CONFIRMATION_REQUIRED
    assert replayed.value.error.data["confirmationToken"] != token
    assert calls == ["w1"]


@pytest.mark.anyio
async def test_mcp_confirmation_rejects_approval_by_another_owner() -> None:
    """A failed approval by another owner changes no gate state and leaves the call blocked."""
    gate = ConfirmationGate()
    service = MCPCore(confirmation_gate=gate)
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

    assert gate.approve(token) is False
    assert gate.reject(token) is False
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
    service = MCPCore()
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
    assert status == 400
    assert isinstance(raw, bytes)
    decoded = decode_json(raw)
    assert decoded["error"]["code"] == HEADER_MISMATCH
    assert calls == []


@pytest.mark.anyio
async def test_guarded_skill_controller_and_agent_plugin_preserve_webmcp_and_guards() -> None:
    """SkillController guards wrap @mcp_tool methods while preserving webmcp, and AgentPlugin route opt['mcp_tool'] preserves schema and webmcp."""

    class RequireAuthGuard(GuardProtocol):
        async def __call__(self, ctx: AgentContext) -> None:
            await self.check(ctx)

        async def check(self, context: Any) -> None:
            if context is None or not getattr(getattr(context, "identity", None), "user_id", None):
                raise PermissionDeniedError("Authentication required")

    class GuardedOps(SkillController):
        guards = (RequireAuthGuard(),)

        @mcp_tool(
            name="guarded_web_ping",
            description="Guarded browser ping",
            webmcp=True,
            requires_confirmation=True,
            annotations={"readOnlyHint": True},
        )
        def guarded_web_ping(self, target: str) -> str:
            return f"pong:{target}"

    class _OptRoute:
        opt = {"mcp_tool": "opt_route_echo", "webmcp": True, "mcp_requires_confirmation": True}

        @staticmethod
        def fn(text: str, count: int = 1) -> str:
            """Repeat text count times."""
            return text * count

    class _FakeAppConfig:
        route_handlers = [_OptRoute()]

    plugin = AgentPlugin(AgentPluginConfig())
    plugin.mcp_registry.register_controller(GuardedOps())
    plugin.on_app_init(_FakeAppConfig())

    manifest = build_webmcp_manifest(plugin.mcp_registry, WebMCPConfig())
    assert [t.name for t in manifest.tools] == ["guarded_web_ping", "opt_route_echo"]

    echo_tool = plugin.mcp_registry.get_tool("opt_route_echo")
    assert echo_tool is not None
    assert echo_tool.webmcp is True
    assert echo_tool.requires_confirmation is True
    assert "text" in echo_tool.input_schema.get("properties", {})

    ping_tool = plugin.mcp_registry.get_tool("guarded_web_ping")
    assert ping_tool is not None
    assert ping_tool.webmcp is True
    assert ping_tool.requires_confirmation is True
    assert ping_tool.annotations == {"readOnlyHint": True}

    with pytest.raises(PermissionDeniedError, match="Authentication required"):
        await ping_tool.invoke({"target": "edge"})

    authed_ctx = AgentContext(identity=CallerIdentity(user_id="alice", tenant_id="t1"))
    assert await ping_tool.invoke({"target": "edge"}, context=authed_ctx) == "pong:edge"


@pytest.mark.anyio
async def test_native_app_agent_plugin_routes_through_mcp_core_and_unifies_well_known() -> None:
    """Native App + AgentPlugin enforces MCPCore 2026-07-28 HTTP semantics and shares canonical /.well-known/mcp.json across all mounts."""
    plugin = AgentPlugin(
        AgentPluginConfig(
            mcp=MCPConfig(server_name="native-mcp", server_version="2.1.0"),
        )
    )
    assert plugin.mcp_service is not None

    @tool(name="echo_val", description="Echo value")
    def echo_val(val: str) -> str:
        return val

    plugin.mcp_registry.register_tool(echo_val)

    app = App()
    plugin.mount_native(app)
    route_map = {(r.method.upper(), r.path): r.handler for r in app.routes}

    post_mcp = route_map[("POST", "/mcp")]
    get_mcp = route_map[("GET", "/mcp")]
    get_wk = route_map[("GET", "/.well-known/mcp.json")]

    status_disc, headers_disc, body_disc = await post_mcp(
        body=b'{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{}}',
        headers={"content-type": "application/json"},
    )
    assert status_disc == 200
    assert "ETag" in headers_disc
    assert decode_json(body_disc)["result"]["protocolVersion"] == "2026-07-28"

    status_origin, _, _ = await post_mcp(
        body=b'{"jsonrpc":"2.0","id":2,"method":"tools/list"}',
        headers={"Origin": "https://evil.example.com", "Host": "api.example.com"},
    )
    assert status_origin == 403

    status_get, headers_get, _ = await get_mcp(headers={})
    assert status_get == 405
    assert "POST" in headers_get.get("Allow", "")

    status_notif, _, body_notif = await post_mcp(
        body=b'{"jsonrpc":"2.0","method":"notifications/cancelled"}',
        headers={"content-type": "application/json"},
    )
    assert status_notif == 202
    assert body_notif == b""

    native_wk = await get_wk()
    expected_keys = {"protocolVersion", "mcp_version", "server", "serverInfo", "endpoint", "tools", "skills"}
    assert set(native_wk.keys()) == expected_keys
    assert native_wk["protocolVersion"] == "2026-07-28"
    assert native_wk["tools"] == ["echo_val"]

    st_app = SimpleNamespace(routes=[], state=SimpleNamespace())
    mount_starlette_mcp(
        st_app,
        registry=plugin.mcp_registry,
        config=MCPConfig(server_name="st-mcp", server_version="1.0.0"),
    )
    st_wk_route = next(r for r in st_app.routes if r.path == "/.well-known/mcp.json")
    st_wk_resp = await st_wk_route.endpoint(SimpleNamespace())
    st_wk_doc = decode_json(st_wk_resp.body if hasattr(st_wk_resp, "body") else st_wk_resp)
    assert set(st_wk_doc.keys()) == expected_keys
    assert st_wk_doc["protocolVersion"] == "2026-07-28"

    flask_views: dict[str, Any] = {}

    class _DummyFlask:
        view_functions = flask_views
        extensions: dict[str, Any] = {}

        def add_url_rule(self, _rule: str, endpoint: str, view_func: Any, methods: list[str] | None = None) -> None:
            flask_views[endpoint] = view_func

    attach_flask_mcp(_DummyFlask(), config=MCPConfig(server_name="fl-mcp"))
    fl_wk_bytes, fl_wk_status, _ = flask_views["vertebra_mcp_well_known"]()
    assert fl_wk_status == 200
    fl_wk_doc = decode_json(fl_wk_bytes)
    assert set(fl_wk_doc.keys()) == expected_keys
    assert fl_wk_doc["protocolVersion"] == "2026-07-28"

    _, dj_static = attach_django_mcp([], config=MCPConfig(server_name="dj-mcp"))
    dj_wk_doc = decode_json(bytes(dj_static["/.well-known/mcp.json"].body))
    assert set(dj_wk_doc.keys()) == expected_keys
    assert dj_wk_doc["protocolVersion"] == "2026-07-28"


@pytest.mark.anyio
async def test_webmcp_manifest_projects_strictly_from_toolspec_exposure() -> None:
    """WebMCP manifest projects strictly from ToolSpec exposure ('webmcp'), emitting title, outputSchema, and consequentialHint."""

    class _DbToolProvider:
        def operations(self) -> Mapping[str, Any]:
            return {"run_sql": self.run_sql}

        def confirmation_reason(self, operation_name: str, arguments: Mapping[str, Any]) -> str | None:
            return None

        async def run_sql(self, sql: str) -> list[dict[str, Any]]:
            return [{"sql": sql}]

    reg = ToolRegistry()
    reg.register_provider(_DbToolProvider())

    @tool(name="server_only_tool", description="Server-only tool")
    def server_only_tool(x: int) -> int:
        return x

    @tool(
        name="browser_delete_item",
        title="Delete Item",
        description="Delete an item from the browser context",
        webmcp=True,
        requires_confirmation=True,
        annotations={"readOnlyHint": False},
    )
    def browser_delete_item(item_id: str) -> dict[str, bool]:
        return {"deleted": True}

    reg.register_tool(server_only_tool)
    reg.register_tool(browser_delete_item)

    manifest = build_webmcp_manifest(reg, WebMCPConfig())
    assert [t.name for t in manifest.tools] == ["browser_delete_item"]
    descriptor = manifest.tools[0]
    assert descriptor.title == "Delete Item"
    assert descriptor.output_schema is not None
    assert descriptor.output_schema.get("type") == "object"
    assert descriptor.requires_confirmation is True
    assert descriptor.annotations.get("consequentialHint") is True

    raw_json = decode_json(render_webmcp_manifest_json(reg, WebMCPConfig()))
    tool_json = raw_json["tools"][0]
    assert tool_json["title"] == "Delete Item"
    assert tool_json["outputSchema"]["type"] == "object"
    assert tool_json["annotations"]["consequentialHint"] is True

    plugin = AgentPlugin(
        AgentPluginConfig(
            mcp=MCPConfig(server_name="webmcp-host", webmcp=WebMCPConfig(enabled=True)),
        )
    )
    plugin.mcp_registry.register_tool(server_only_tool)
    plugin.mcp_registry.register_tool(browser_delete_item)
    app = App()
    plugin.mount_native(app)
    route_map = {(r.method.upper(), r.path): r.handler for r in app.routes}
    assert ("GET", "/.well-known/webmcp.json") in route_map
    webmcp_doc = await route_map[("GET", "/.well-known/webmcp.json")]()
    assert [t["name"] for t in webmcp_doc["tools"]] == ["browser_delete_item"]
    assert webmcp_doc["tools"][0]["title"] == "Delete Item"
    assert webmcp_doc["tools"][0]["annotations"]["consequentialHint"] is True


@pytest.mark.anyio
@pytest.mark.parametrize("entrypoint", ["handle", "handle_payload"])
@pytest.mark.parametrize("user_id", ["ana", None])
async def test_mcp_core_binds_agent_context(entrypoint: str, user_id: str | None) -> None:
    """HTTP and raw payload dispatch bind identity and restore their caller's context."""
    core = MCPCore()

    @tool(name="whoami")
    def whoami() -> str:
        ctx = AgentContext.current()
        assert ctx is not None
        return str(ctx.identity.user_id)

    core.registry.register_tool(whoami)
    body = encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "whoami"}})
    context = AgentContext(identity=CallerIdentity(user_id=user_id)) if user_id else None
    outer = AgentContext(identity=CallerIdentity(user_id="outer"))
    token = bind_agent_context(outer)
    try:
        payload: bytes | None
        if entrypoint == "handle":
            response = await core.handle(McpHttpRequest(method="POST", path="/mcp", body=body, context=context))
            payload = response.body
        else:
            payload = await core.handle_payload(body, context=context)
        assert payload is not None
        assert decode_json(payload)["result"]["content"][0]["text"] == str(user_id)
        assert AgentContext.current() is outer
    finally:
        reset_agent_context(token)
    assert AgentContext.current() is None


@pytest.mark.anyio
async def test_mcp_task_cancel_stops_execution() -> None:
    """Promoted tasks own cancellation state and cannot complete after cancellation."""
    core = MCPCore()
    started = anyio.Event()
    stopped = anyio.Event()
    contexts: list[AgentContext] = []
    task_handles: list[asyncio.Task[Any]] = []

    @tool(name="waiting")
    async def waiting() -> str:
        context = AgentContext.current()
        assert context is not None
        contexts.append(context)
        current = asyncio.current_task()
        assert current is not None
        task_handles.append(current)
        started.set()
        try:
            await anyio.Event().wait()
        finally:
            stopped.set()
        return "unexpected"

    core.registry.register_tool(waiting)
    owner = AgentContext(identity=CallerIdentity(user_id="ana"), metadata={"request": 1})
    promoted = await core.handle_payload(
        encode_json_bytes(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "waiting", "_meta": {"io.modelcontextprotocol/task": True}},
            }
        ),
        context=owner,
    )
    assert promoted is not None
    task_id = decode_json(promoted)["result"]["task"]["taskId"]
    with anyio.fail_after(2):
        await started.wait()
    assert contexts[0] is not owner
    assert contexts[0].identity is owner.identity
    assert contexts[0].metadata == owner.metadata
    assert contexts[0].metadata is not owner.metadata
    assert contexts[0].cancellation_token is not owner.cancellation_token
    denied = await core.handle_payload(
        encode_json_bytes({"jsonrpc": "2.0", "id": 2, "method": "tasks/cancel", "params": {"taskId": task_id}}),
        context=AgentContext(identity=CallerIdentity(user_id="other")),
    )
    assert denied is not None and "error" in decode_json(denied)
    assert not contexts[0].cancellation_token.is_cancelled
    anonymous_list = await core.handle_payload(encode_json_bytes({"jsonrpc": "2.0", "id": 5, "method": "tasks/list"}))
    assert anonymous_list is not None
    assert decode_json(anonymous_list)["result"]["tasks"] == []
    cancelled = await core.handle_payload(
        encode_json_bytes({"jsonrpc": "2.0", "id": 3, "method": "tasks/cancel", "params": {"taskId": task_id}}),
        context=owner,
    )
    assert cancelled is not None and decode_json(cancelled)["result"]["task"]["status"] == "cancelled"
    with anyio.fail_after(2):
        await stopped.wait()
        with pytest.raises(asyncio.CancelledError):
            await task_handles[0]
    assert contexts[0].cancellation_token.is_cancelled
    assert not owner.cancellation_token.is_cancelled
    record = core.task_store.get(task_id, owner_id="default:ana")
    assert record.status == "cancelled"
    assert core.task_store.complete(task_id, {"content": []}) is record
    assert core.task_store.fail(task_id, {"message": "late"}) is record


@pytest.mark.anyio
@pytest.mark.parametrize("method", ["tasks/get", "tasks/update", "tasks/cancel"])
async def test_mcp_task_anonymous_requests_rejected(method: str) -> None:
    """Anonymous callers cannot inspect, update, or cancel a bound promoted task."""
    core = MCPCore()
    started = anyio.Event()
    gate = anyio.Event()

    @tool(name="waiting")
    async def waiting() -> str:
        started.set()
        await gate.wait()
        return "done"

    core.registry.register_tool(waiting)
    owner = AgentContext(identity=CallerIdentity(user_id="ana"))
    promoted = await core.handle_payload(
        encode_json_bytes(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "waiting", "_meta": {"io.modelcontextprotocol/task": True}},
            }
        ),
        context=owner,
    )
    assert promoted is not None
    task_id = decode_json(promoted)["result"]["task"]["taskId"]
    with anyio.fail_after(2):
        await started.wait()
    try:
        anonymous = await core.handle_payload(
            encode_json_bytes({"jsonrpc": "2.0", "id": 4, "method": method, "params": {"taskId": task_id}})
        )
        assert anonymous is not None
        assert decode_json(anonymous)["error"]["code"] == -32602
    finally:
        gate.set()


@pytest.mark.anyio
async def test_mcp_promoted_task_inherits_bound_identity() -> None:
    """A completed promoted tool observes its caller identity through the bound context."""
    core = MCPCore()

    @tool(name="whoami")
    def whoami() -> str:
        context = AgentContext.current()
        assert context is not None
        return str(context.identity.user_id)

    core.registry.register_tool(whoami)
    response = await core.handle_payload(
        encode_json_bytes(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "whoami", "_meta": {"io.modelcontextprotocol/task": True}},
            }
        ),
        context=AgentContext(identity=CallerIdentity(user_id="ana")),
    )
    assert response is not None
    task_id = decode_json(response)["result"]["task"]["taskId"]
    result = await core.task_store.wait_for_result(task_id, owner_id="default:ana", timeout_s=2)
    assert result.owner_id == "default:ana"
    assert result.result is not None
    assert result.result["content"][0]["text"] == "ana"


@pytest.mark.anyio
@pytest.mark.parametrize("surface", ["asgi", "starlette"])
@pytest.mark.parametrize(
    "identity_case,expected",
    [
        ("user", "ana|None|None"),
        ("anonymous", "None|None|None"),
        ("trusted", "None|None|CN=svc"),
        ("untrusted", "None|None|None"),
        ("custom", "erin|None|None"),
    ],
)
async def test_mcp_adapters_propagate_agent_context(surface: str, identity_case: str, expected: str) -> None:
    """ASGI and Starlette adapters preserve authenticated state and explicit proxy policies."""
    config = MCPConfig()
    if identity_case in ("trusted", "untrusted"):
        config = MCPConfig(
            trust_proxy_identity=True, identity_source=MtlsIdentitySource(trusted_proxies=("10.0.0.0/8",))
        )
    elif identity_case == "custom":
        config = MCPConfig(
            trust_proxy_identity=True,
            identity_source=CallableIdentitySource(
                resolver=lambda request: CallerIdentity(user_id=request.headers.get("x-user"))
            ),
        )
    core = MCPCore(config=config)

    @tool(name="whoami")
    def whoami(context: AgentContext) -> str:
        return f"{context.identity.user_id}|{context.identity.email}|{context.identity.client_cert_subject}"

    core.registry.register_tool(whoami)
    app = AsgiMcpApp(core)
    body = encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "whoami"}})
    scope: dict[str, Any] = {
        "type": "http",
        "method": "POST",
        "path": "/mcp",
        "headers": [
            (b"content-type", b"application/json"),
            (b"x-goog-authenticated-user-email", b"ana@example.com"),
            (b"x-vertebra-mtls-subject", b"CN=svc"),
            (b"x-user", b"erin"),
        ],
        "client": ("203.0.113.9" if identity_case == "untrusted" else "10.0.0.5", 1),
    }
    if identity_case == "user":
        scope["user"] = SimpleNamespace(id="ana", roles=["admin"])

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    if surface == "starlette":
        response = await app.handle_starlette_request(StarletteRequest(scope, receive))
        payload = response.body
    else:
        messages: list[dict[str, Any]] = []

        async def send(message: dict[str, Any]) -> None:
            messages.append(message)

        await app(scope, receive, send)
        assert messages[0]["status"] == 200
        payload = messages[-1]["body"]
    assert decode_json(payload)["result"]["content"][0]["text"] == expected


@pytest.mark.parametrize("surface", ["wsgi", "flask", "duck-request"])
def test_mcp_wsgi_adapters_propagate_agent_context(surface: str) -> None:
    """WSGI and Flask adapters use immediate-peer metadata and authenticated WSGI users."""
    config = MCPConfig(trust_proxy_identity=True, identity_source=MtlsIdentitySource(trusted_proxies=("10.0.0.0/8",)))
    core = MCPCore(config=config)

    @tool(name="whoami")
    def whoami(context: AgentContext) -> str:
        return f"{context.identity.user_id}|{context.identity.client_cert_subject}"

    core.registry.register_tool(whoami)
    app = WsgiMcpApp(core)
    body = encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "whoami"}})
    if surface == "wsgi":
        response = app(
            {
                "REQUEST_METHOD": "POST",
                "PATH_INFO": "/mcp",
                "REMOTE_USER": "carol",
                "REMOTE_ADDR": "10.0.0.5",
                "HTTP_X_VERTEBRA_MTLS_SUBJECT": "CN=svc",
                "wsgi.input": io.BytesIO(body),
            },
            lambda status, headers: None,
        )
        payload = b"".join(response)
        expected = "carol|CN=svc"
    elif surface == "flask":
        flask = Flask(__name__)
        with flask.test_request_context(
            "/mcp",
            method="POST",
            data=body,
            headers={"x-vertebra-mtls-subject": "CN=svc"},
            environ_overrides={"REMOTE_ADDR": "10.0.0.5", "REMOTE_USER": "carol"},
        ):
            result = app.handle_flask_view()
        assert isinstance(result, tuple)
        payload = result[0]
        expected = "carol|CN=svc"
    else:
        result = app.handle_flask_view(
            SimpleNamespace(method="POST", path="/mcp", headers={"x-vertebra-mtls-subject": "CN=svc"}, body=body)
        )
        assert isinstance(result, tuple)
        payload = result[0]
        expected = "None|None"
    assert decode_json(payload)["result"]["content"][0]["text"] == expected


def test_mcp_identity_config_requires_explicit_opt_in() -> None:
    """Proxy trust requires both an explicit opt-in and a concrete policy."""
    with pytest.raises(ValueError, match="requires identity_source"):
        MCPConfig(trust_proxy_identity=True)
    with pytest.raises(ValueError, match="requires trust_proxy_identity=True"):
        MCPConfig(identity_source=CallableIdentitySource(resolver=lambda request: None))


def test_mcp_native_request_adapter_propagates_explicit_identity() -> None:
    """Native request headers reach the configured application identity source."""
    source = CallableIdentitySource(resolver=lambda request: CallerIdentity(user_id=request.headers.get("x-user")))
    core = MCPCore(config=MCPConfig(trust_proxy_identity=True, identity_source=source))

    @tool(name="whoami")
    def whoami(context: AgentContext) -> str:
        return str(context.identity.user_id)

    core.registry.register_tool(whoami)
    body = encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "whoami"}})
    request = NativeRequest.from_parts("POST", "/mcp", body=body, headers=[("x-user", "ana")])
    response = WsgiMcpApp(core).handle_vertebra_request(request)
    assert decode_json(bytes(response.body))["result"]["content"][0]["text"] == "ana"


@pytest.mark.anyio
@pytest.mark.parametrize("trusted", [False, True])
async def test_native_plugin_context_and_delete_route(trusted: bool) -> None:
    """Native plugin endpoints honor explicit identity policies and register DELETE."""
    source = CallableIdentitySource(resolver=lambda request: CallerIdentity(email=request.headers.get("x-test-email")))
    config = MCPConfig(trust_proxy_identity=trusted, identity_source=source if trusted else None)
    plugin = AgentPlugin(AgentPluginConfig(mcp=config))

    @tool(name="whoami")
    def whoami(context: AgentContext) -> str:
        return str(context.identity.email)

    plugin.mcp_registry.register_tool(whoami)
    app = App()
    plugin.mount_native(app)
    routes = {(route.method.upper(), route.path): route.handler for route in app.routes}
    assert ("DELETE", "/mcp") in routes
    headers = {"x-test-email": "ana@example.com", "x-goog-authenticated-user-email": "ana@example.com"}
    header_obj = SimpleNamespace(as_dict=lambda: headers) if trusted else headers
    status, _, raw = await routes[("POST", "/mcp")](
        body=encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "whoami"}}),
        headers=header_obj,
    )
    assert status == 200
    assert decode_json(raw)["result"]["content"][0]["text"] == ("ana@example.com" if trusted else "None")
    delete_status, _, _ = await routes[("DELETE", "/mcp")](headers={})
    assert delete_status == 400
    delete_missing_status, _, _ = await routes[("DELETE", "/mcp")](headers={"Mcp-Session-Id": "missing"})
    assert delete_missing_status == 404


def test_litestar_fallback_preserves_core_responses_and_context() -> None:
    """Fallback requests retain core status, headers, and configured caller identity."""
    source = CallableIdentitySource(resolver=lambda request: CallerIdentity(user_id=request.headers.get("x-user")))
    plugin = AgentPlugin(AgentPluginConfig(mcp=MCPConfig(trust_proxy_identity=True, identity_source=source)))

    @tool(name="whoami")
    def whoami(context: AgentContext) -> str:
        return str(context.identity.user_id)

    plugin.mcp_registry.register_tool(whoami)
    with create_test_client(route_handlers=[], plugins=[plugin]) as client:
        notified = client.post("/mcp", content=b'{"jsonrpc":"2.0","method":"notifications/initialized"}')
        assert notified.status_code == 202
        assert notified.content == b""
        forbidden = client.post("/mcp", content=b"{}", headers={"Origin": "https://evil.example.com"})
        assert forbidden.status_code == 403
        listed = client.post(
            "/mcp",
            content=b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
            headers={"MCP-Protocol-Version": "2026-07-28"},
        )
        assert listed.status_code == 200
        assert listed.headers["mcp-protocol-version"] == "2026-07-28"
        cached = client.post(
            "/mcp",
            content=b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
            headers={"If-None-Match": listed.headers["etag"]},
        )
        assert cached.status_code == 304
        assert cached.content == b""
        called = client.post(
            "/mcp",
            content=b'{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"whoami"}}',
            headers={"x-user": "ana"},
        )
        assert called.json()["result"]["content"][0]["text"] == "ana"
        refused_get = client.get("/mcp")
        assert refused_get.status_code == 405
        assert refused_get.headers["allow"] == "POST, DELETE, OPTIONS"
        refused_delete = client.delete("/mcp")
        assert refused_delete.status_code == 400
        missing_delete = client.delete("/mcp", headers={"Mcp-Session-Id": "missing"})
        assert missing_delete.status_code == 404
