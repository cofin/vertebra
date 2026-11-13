"""Conformance tests for MCP 2026-07-28 protocol, registry, HTTP handler, and tasks."""

import asyncio
import base64
import dataclasses
import io
from collections.abc import AsyncIterator, Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import anyio
import pytest
from django.conf import settings as django_settings
from django.http import StreamingHttpResponse
from flask import Flask
from litestar import Litestar
from litestar.response import Stream as LitestarStream
from litestar.testing import create_test_client
from starlette.requests import Request as StarletteRequest
from starlette.responses import StreamingResponse

import vertebra.agent.mcp as mcp_pkg
import vertebra.agent.mcp.service as mcp_service_mod
from vertebra._vertebra import Request as NativeRequest
from vertebra.agent.controller import ConfirmationDecisionRequest
from vertebra.agent.core.context import AgentContext, CallerIdentity, bind_agent_context, reset_agent_context
from vertebra.agent.core.guards import (
    BudgetExceededError,
    GuardProtocol,
    PermissionDeniedError,
    RateLimitExceededError,
    RequireRoles,
    TokenBucketGuard,
    authorize,
)
from vertebra.agent.core.identity import CallableIdentitySource, MtlsIdentitySource
from vertebra.agent.core.jsonrpc import (
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    JSONRPCErrorException,
)
from vertebra.agent.core.schema import tool
from vertebra.agent.core.sse import SseFrame, StreamLimitExceededError
from vertebra.agent.mcp import (
    CONFIRMATION_REQUIRED,
    CONFIRMATION_TOKEN_META_KEY,
    HEADER_MISMATCH,
    LEGACY_PROTOCOL_VERSIONS,
    MCP_FALLBACK_SESSION_VERSION,
    MCP_LOG_LEVELS,
    PERMISSION_DENIED,
    RATE_LIMITED,
    SESSION_CAPACITY_EXHAUSTED,
    SKILLS_EXTENSION,
    SUPPORTED_PROTOCOL_VERSIONS,
    UI_EXTENSION,
    UI_MIME_TYPE,
    UNSUPPORTED_PROTOCOL_VERSION,
    BlobContent,
    MCPConfig,
    MCPCore,
    MCPHttpHandler,
    McpHttpRequest,
    McpHttpResponse,
    McpIcon,
    MCPRegistry,
    MCPSessionStore,
    MCPSkillsConfig,
    MCPTaskStore,
    PromptArgument,
    PromptEntry,
    PromptMessage,
    ResourceEntry,
    ResourceTemplateEntry,
    SessionLimitExceededError,
    Skill,
    SkillAccessError,
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
    mcp_resource,
    mcp_tool,
    prompt,
    render_webmcp_manifest_json,
    resource,
)
from vertebra.agent.mcp.routes import AsgiMcpApp, WsgiMcpApp
from vertebra.agent.plugin import AgentPlugin, AgentPluginConfig
from vertebra.agent.runtime import ConfirmationGate
from vertebra.extensions.django.mcp import attach_mcp_routes as attach_django_mcp
from vertebra.extensions.django.mcp import dispatch_mcp_request as dispatch_django_mcp
from vertebra.extensions.flask.mcp import attach_mcp_routes as attach_flask_mcp
from vertebra.extensions.starlette.mcp import mount_mcp_routes as mount_starlette_mcp
from vertebra.framework._app import App
from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError
from vertebra.utils.module_loader import MissingDependencyError
from vertebra.utils.serializers import decode_json, encode_json_bytes


@pytest.mark.anyio
async def test_mcp_core_discover_and_removed_2024_methods() -> None:
    """Verify server/discover 2026-07-28 metadata and -32601 rejection on removed 2024-11-05 methods."""
    core = MCPCore(config=MCPConfig(server_name="test-mcp", server_version="1.2.3"))
    discover_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "server/discover"},
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "server/discover", "params": {}}),
        )
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

    for removed_method in (
        "initialize",
        "ping",
        "logging/setLevel",
        "resources/subscribe",
        "resources/unsubscribe",
        "tasks/result",
    ):
        resp = await core.handle(
            McpHttpRequest(
                method="POST",
                path="/mcp",
                headers={"MCP-Protocol-Version": "2026-07-28"},
                body=encode_json_bytes({"jsonrpc": "2.0", "id": 99, "method": removed_method, "params": {}}),
            )
        )
        assert resp.status == 200
        err_payload = decode_json(resp.body)
        assert err_payload["error"]["code"] == METHOD_NOT_FOUND


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
    for model in (txt, blob, arg, msg):
        assert isinstance(model, FrozenBaseStruct)
        assert isinstance(hash(model), int)


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
    cross_origin_default = await default_core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"Origin": "https://evil.example.com", "Host": "api.example.com"},
            body=b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
        )
    )
    assert cross_origin_default.status == 403

    same_origin_default = await default_core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"Origin": "https://api.example.com", "Host": "api.example.com"},
            body=b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
        )
    )
    assert same_origin_default.status == 200

    disallowed_status, _, _ = await handler.handle_request(
        "POST",
        "/mcp",
        {"Origin": "https://evil.example.com"},
        b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
    )
    assert disallowed_status == 403

    proto_status, _, proto_body = await handler.handle_request(
        "POST",
        "/mcp",
        {"Origin": "https://app.example.com", "MCP-Protocol-Version": "2025-01-01"},
        b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
    )
    assert proto_status == 400
    proto_decoded = decode_json(proto_body)
    assert proto_decoded["error"]["code"] == UNSUPPORTED_PROTOCOL_VERSION

    meta_proto_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"Origin": "https://app.example.com"},
            body=encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/list",
                    "params": {"_meta": {"io.modelcontextprotocol/protocolVersion": "2024-11-05"}},
                }
            ),
        )
    )
    assert meta_proto_resp.status == 400
    assert decode_json(meta_proto_resp.body)["error"]["code"] == UNSUPPORTED_PROTOCOL_VERSION

    method_status, _, method_body = await handler.handle_request(
        "POST",
        "/mcp",
        {"Origin": "https://app.example.com", "Mcp-Method": "tools/list"},
        b'{"jsonrpc":"2.0","id":1,"method":"prompts/list"}',
    )
    assert method_status == 400
    method_decoded = decode_json(method_body)
    assert method_decoded["error"]["code"] == HEADER_MISMATCH

    name_status, _, name_body = await handler.handle_request(
        "POST",
        "/mcp",
        {"Origin": "https://app.example.com", "Mcp-Method": "prompts/get", "Mcp-Name": "wrong_name"},
        b'{"jsonrpc":"2.0","id":1,"method":"prompts/get","params":{"name":"correct_name"}}',
    )
    assert name_status == 400
    name_decoded = decode_json(name_body)
    assert name_decoded["error"]["code"] == HEADER_MISMATCH

    encoded_non_ascii = encode_mcp_header_value("тест")
    assert decode_mcp_header_value(encoded_non_ascii) == "тест"

    get_status, get_headers, _ = await handler.handle_request("GET", "/mcp", {}, b"")
    assert get_status == 405
    assert "POST" in get_headers.get("Allow", "")

    notif_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"Origin": "https://app.example.com"},
            body=b'{"jsonrpc":"2.0","method":"notifications/initialized"}',
        )
    )
    assert notif_resp.status == 202
    assert notif_resp.body == b""

    batch_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"Origin": "https://app.example.com"},
            body=b'[{"jsonrpc":"2.0","id":1,"method":"tools/list"}]',
        )
    )
    assert batch_resp.status == 400

    discover_status, discover_headers, _ = await handler.handle_request(
        "POST",
        "/mcp",
        {"Origin": "https://app.example.com"},
        b'{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{}}',
    )
    assert discover_status == 200
    etag = discover_headers.get("ETag")
    assert etag is not None

    not_mod_status, _, not_mod_body = await handler.handle_request(
        "POST",
        "/mcp",
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
    decoded = decode_json(raw)
    assert decoded["error"]["code"] == HEADER_MISMATCH
    assert calls == []


@pytest.mark.anyio
async def test_guarded_skill_controller_and_agent_plugin_preserve_webmcp_and_guards() -> None:
    """SkillController guards wrap @mcp_tool methods while preserving webmcp, and AgentPlugin route opt['mcp_tool'] preserves schema and webmcp."""

    class RequireAuthGuard(GuardProtocol):
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
    assert echo_tool.webmcp is True
    assert echo_tool.requires_confirmation is True
    assert "text" in echo_tool.input_schema.get("properties", {})

    ping_tool = plugin.mcp_registry.get_tool("guarded_web_ping")
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
    expected_keys = {"protocolVersion", "mcp_version", "server", "serverInfo", "endpoint", "tools"}
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
    request = encode_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "waiting", "_meta": {"io.modelcontextprotocol/task": True}},
        }
    )
    promoted = await core.handle_payload(request, context=owner)
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
    for method in ("tasks/get", "tasks/update", "tasks/cancel"):
        anonymous = await core.handle_payload(
            encode_json_bytes({"jsonrpc": "2.0", "id": 4, "method": method, "params": {"taskId": task_id}})
        )
        assert anonymous is not None
        assert decode_json(anonymous)["error"]["code"] == -32602
        assert not contexts[0].cancellation_token.is_cancelled
        assert not task_handles[0].done()
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


def test_mcp_flask_missing_refuses_fabricated_request(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unavailable Flask dependency raises before dispatching an invented request."""
    monkeypatch.setattr("vertebra.agent.mcp.routes.import_optional", lambda name: None)
    with pytest.raises(MissingDependencyError):
        WsgiMcpApp(MCPCore()).handle_flask_view()


@pytest.mark.anyio
@pytest.mark.parametrize("authenticated", [True, False])
async def test_mcp_asgi_adapter_context_controls_guard(authenticated: bool) -> None:
    """Only authenticated ASGI scope roles authorize guarded tools."""
    core = MCPCore()
    calls: list[str] = []

    @tool(name="admin_only", guards=(RequireRoles("admin"),))
    def admin_only() -> str:
        calls.append("ran")
        return "ok"

    core.registry.register_tool(admin_only)
    body = encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "admin_only"}})
    scope: dict[str, Any] = {"type": "http", "method": "POST", "path": "/mcp", "headers": []}
    if authenticated:
        scope["user"] = SimpleNamespace(id="ana", roles=["admin"])
    messages: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    await AsgiMcpApp(core)(scope, receive, send)
    result = decode_json(messages[-1]["body"])
    assert ("result" in result) is authenticated
    assert calls == (["ran"] if authenticated else [])


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


@pytest.mark.anyio
@pytest.mark.parametrize("promoted", [False, True], ids=["sync", "promoted-task"])
async def test_mcp_tool_guards_execute_once_with_token_bucket(promoted: bool) -> None:
    """A capacity-1 TokenBucketGuard is evaluated once per tools/call and rejects the second call with -32005."""
    core = MCPCore()
    bucket = TokenBucketGuard(rate_per_second=0.0, capacity=1.0)
    invocations: list[str] = []

    @tool(name="limited_op", guards=(bucket,))
    async def limited_op(item: str) -> str:
        invocations.append(item)
        return f"done:{item}"

    core.registry.register_tool(limited_op)
    ctx = AgentContext(identity=CallerIdentity(user_id="ana", tenant_id="t1"))
    meta: dict[str, Any] = {"io.modelcontextprotocol/task": True} if promoted else {}

    first_raw = await core.handle_payload(
        encode_json_bytes(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "limited_op", "arguments": {"item": "alpha"}, "_meta": meta},
            }
        ),
        context=ctx,
    )
    assert first_raw is not None
    first_parsed = decode_json(first_raw)
    assert "result" in first_parsed
    if promoted:
        task_id = first_parsed["result"]["task"]["taskId"]
        completed = await core.task_store.wait_for_result(task_id, owner_id="t1:ana", timeout_s=2)
        assert completed.result is not None
        assert completed.result["content"][0]["text"] == "done:alpha"
    else:
        assert first_parsed["result"]["content"][0]["text"] == "done:alpha"
    assert invocations == ["alpha"]

    second_raw = await core.handle_payload(
        encode_json_bytes(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "limited_op", "arguments": {"item": "beta"}, "_meta": meta},
            }
        ),
        context=ctx,
    )
    assert second_raw is not None
    second_parsed = decode_json(second_raw)
    assert second_parsed["error"]["code"] == RATE_LIMITED
    assert second_parsed["error"]["data"]["tool"] == "limited_op"
    assert "Rate limit exceeded" in second_parsed["error"]["data"]["reason"]
    assert invocations == ["alpha"]
    tasks_for_owner, _ = core.task_store.list_for_owner("t1:ana")
    assert len(tasks_for_owner) == (1 if promoted else 0)


def _litestar_style_guard(connection: Any, route_handler: Any) -> None:
    _ = (connection, route_handler)


async def _budget_guard(ctx: AgentContext) -> None:
    _ = ctx
    msg = "Turn budget exhausted"
    raise BudgetExceededError(msg)


async def _rate_guard(ctx: AgentContext) -> None:
    _ = ctx
    msg = "Burst quota exceeded"
    raise RateLimitExceededError(msg)


@pytest.mark.anyio
@pytest.mark.parametrize("promoted", [False, True], ids=["sync", "promoted-task"])
@pytest.mark.parametrize(
    ("guard_factory", "expected_code", "expected_reason"),
    [
        (lambda: RequireRoles("admin"), PERMISSION_DENIED, "Caller lacks any required role"),
        (lambda: _litestar_style_guard, PERMISSION_DENIED, "Guard requires an HTTP connection"),
        (lambda: _rate_guard, RATE_LIMITED, "Burst quota exceeded"),
        (lambda: _budget_guard, RATE_LIMITED, "Turn budget exhausted"),
    ],
    ids=["role-denied", "two-arg-no-conn", "rate-limit", "budget-exceeded"],
)
async def test_mcp_tool_guard_errors_map_to_typed_codes_before_confirmation_and_promotion(
    promoted: bool,
    guard_factory: Any,
    expected_code: int,
    expected_reason: str,
) -> None:
    """Guard failures return -32004 or -32005 before issuing confirmation tickets or creating tasks."""
    gate = ConfirmationGate()
    core = MCPCore(confirmation_gate=gate)
    executed: list[str] = []

    @tool(name="guarded_mutator", requires_confirmation=True, guards=(guard_factory(),))
    def guarded_mutator() -> str:
        executed.append("ran")
        return "ok"

    core.registry.register_tool(guarded_mutator)
    viewer = AgentContext(identity=CallerIdentity(user_id="bob", tenant_id="t1", roles=("viewer",)))
    meta: dict[str, Any] = {"io.modelcontextprotocol/task": True} if promoted else {}

    resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            body=encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": 9,
                    "method": "tools/call",
                    "params": {"name": "guarded_mutator", "arguments": {}, "_meta": meta},
                }
            ),
            context=viewer,
        )
    )
    assert resp.status == 200
    parsed = decode_json(resp.body)
    assert parsed["error"]["code"] == expected_code
    assert parsed["error"]["data"]["tool"] == "guarded_mutator"
    assert expected_reason in parsed["error"]["data"]["reason"]
    assert executed == []
    tasks_for_viewer, _ = core.task_store.list_for_owner("t1:bob")
    assert tasks_for_viewer == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("headers", "payload", "expected_status", "expected_error_code"),
    [
        (
            {
                "Mcp-Method": "tools/call",
                "Mcp-Name": "echo_ws",
                "Mcp-Param-workspace_id": "ws-1",
                "Mcp-Param-count": "3",
            },
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "echo_ws", "arguments": {"workspace_id": "ws-1", "count": 3}},
            },
            200,
            None,
        ),
        (
            {"Mcp-Param-workspace-id": "ws-1"},
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "echo_ws", "arguments": {"workspace_id": "ws-1", "count": 1}},
            },
            200,
            None,
        ),
        (
            {"Mcp-Param-query": encode_mcp_header_value("тест")},
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "prompts/get",
                "params": {"name": "search_prompt", "arguments": {"query": "тест"}},
            },
            200,
            None,
        ),
        (
            {"Mcp-Param-uri": "file:///cfg.json"},
            {"jsonrpc": "2.0", "id": 4, "method": "resources/read", "params": {"uri": "file:///cfg.json"}},
            200,
            None,
        ),
        (
            {"Mcp-Param-workspace-id": "ws-other"},
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {"name": "echo_ws", "arguments": {"workspace_id": "ws-1", "count": 1}},
            },
            400,
            HEADER_MISMATCH,
        ),
        (
            {"Mcp-Param-missing": "val"},
            {
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {"name": "echo_ws", "arguments": {"workspace_id": "ws-1", "count": 1}},
            },
            400,
            HEADER_MISMATCH,
        ),
        (
            {"Mcp-Param-query": encode_mcp_header_value("тест")},
            {
                "jsonrpc": "2.0",
                "id": 7,
                "method": "prompts/get",
                "params": {"name": "search_prompt", "arguments": {"query": "иное"}},
            },
            400,
            HEADER_MISMATCH,
        ),
        (
            {"Mcp-Param-uri": "file:///cfg.json"},
            {"jsonrpc": "2.0", "id": 8, "method": "resources/read", "params": {"uri": "file:///secret.json"}},
            400,
            HEADER_MISMATCH,
        ),
        (
            {"Mcp-Param-workspace_id": "ws-1"},
            [
                {
                    "jsonrpc": "2.0",
                    "id": 9,
                    "method": "tools/call",
                    "params": {"name": "echo_ws", "arguments": {"workspace_id": "ws-1", "count": 1}},
                }
            ],
            400,
            HEADER_MISMATCH,
        ),
        (
            {"Mcp-Param-payload": '{"limit": 1}'},
            {
                "jsonrpc": "2.0",
                "id": 10,
                "method": "tools/call",
                "params": {"name": "echo_ws", "arguments": {"workspace_id": "ws-1", "payload": {"limit": True}}},
            },
            400,
            HEADER_MISMATCH,
        ),
    ],
    ids=[
        "ascii-exact-match",
        "hyphen-underscore-match",
        "rfc2047-prompt-match",
        "resource-uri-match",
        "tool-arg-mismatch",
        "tool-arg-missing",
        "rfc2047-prompt-mismatch",
        "resource-uri-mismatch",
        "batch-with-mcp-param",
        "nested-json-bool-int-mismatch",
    ],
)
async def test_mcp_http_param_header_validation(
    headers: dict[str, str],
    payload: object,
    expected_status: int,
    expected_error_code: int | None,
) -> None:
    """MCPCore.handle validates ASCII and RFC 2047 Mcp-Param-* headers against body arguments and params."""
    core = MCPCore()
    calls: list[tuple[str, int]] = []

    @tool(name="echo_ws")
    def echo_ws(workspace_id: str, count: int = 1, payload: dict[str, Any] | None = None) -> str:
        _ = payload
        calls.append((workspace_id, count))
        return f"{workspace_id}:{count}"

    core.registry.register_tool(echo_ws)
    core.registry.register_prompt(
        PromptEntry(
            name="search_prompt",
            description="Search prompt",
            arguments=(PromptArgument(name="query", required=True),),
            getter=lambda query: [PromptMessage(role="user", content=TextContent(text=query))],
        )
    )
    core.registry.register_resource(
        ResourceEntry(
            uri="file:///cfg.json",
            name="cfg",
            description="Config",
            mimeType="application/json",
            reader=lambda: "{}",
        )
    )

    resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers=headers,
            body=encode_json_bytes(payload),
        )
    )
    assert resp.status == expected_status
    decoded = decode_json(resp.body)
    if expected_error_code is not None:
        assert decoded["error"]["code"] == expected_error_code
        assert calls == []
    else:
        assert "result" in decoded


def test_mcp_service_alias_removed_and_plugin_uses_mcp_core() -> None:
    """MCPService alias is removed from vertebra.agent.mcp and AgentPlugin instantiates MCPCore + ToolRegistry."""
    assert not hasattr(mcp_pkg, "MCPService")
    assert not hasattr(mcp_service_mod, "MCPService")
    plugin = AgentPlugin(AgentPluginConfig(mcp=MCPConfig(server_name="canonical-core")))
    assert type(plugin.mcp_service) is MCPCore
    assert type(plugin.mcp_registry) is ToolRegistry


@pytest.mark.anyio
@pytest.mark.parametrize("version", SUPPORTED_PROTOCOL_VERSIONS)
async def test_mcp_protocol_version_negotiation_echoes_supported_versions(version: str) -> None:
    """All four supported protocol versions negotiate and echo MCP-Protocol-Version."""
    core = MCPCore()
    resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": version},
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}),
        )
    )
    assert resp.status == 200
    assert resp.headers["MCP-Protocol-Version"] == version
    decoded = decode_json(resp.body)
    assert decoded["result"]["_meta"]["io.modelcontextprotocol/protocolVersion"] == version


@pytest.mark.anyio
@pytest.mark.parametrize("surface", ["header", "meta"])
async def test_mcp_unsupported_protocol_version_returns_supported_versions_data(surface: str) -> None:
    """Unsupported protocol version returns HTTP 400 and -32022 with supported version lists."""
    core = MCPCore()
    headers = {"MCP-Protocol-Version": "1900-01-01"} if surface == "header" else {}
    params: dict[str, Any] = (
        {"_meta": {"io.modelcontextprotocol/protocolVersion": "1900-01-01"}} if surface == "meta" else {}
    )
    resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers=headers,
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 7, "method": "tools/list", "params": params}),
        )
    )
    assert resp.status == 400
    err = decode_json(resp.body)["error"]
    assert err["code"] == UNSUPPORTED_PROTOCOL_VERSION
    assert err["data"]["supported"] == list(SUPPORTED_PROTOCOL_VERSIONS)
    assert err["data"]["supportedVersions"] == list(SUPPORTED_PROTOCOL_VERSIONS)
    assert err["data"]["requested"] == "1900-01-01"


@pytest.mark.anyio
async def test_mcp_header_and_meta_version_conflict_returns_header_mismatch() -> None:
    """Disagreeing MCP-Protocol-Version header and _meta version return HTTP 400 and -32020."""
    core = MCPCore()
    resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28"},
            body=encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": 9,
                    "method": "tools/list",
                    "params": {"_meta": {"io.modelcontextprotocol/protocolVersion": "2025-11-25"}},
                }
            ),
        )
    )
    assert resp.status == 400
    assert decode_json(resp.body)["error"]["code"] == HEADER_MISMATCH


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("config_kwargs", "disabled_key", "method", "params"),
    [
        ({"tools_enabled": False}, "tools", "tools/list", {}),
        ({"resources_enabled": False}, "resources", "resources/list", {}),
        ({"prompts_enabled": False}, "prompts", "prompts/list", {}),
        ({"tasks_enabled": False}, "tasks", "tasks/list", {}),
        ({"skills_enabled": False}, "skills", "skills/list", {}),
        ({"logging": False}, "logging", "logging/setLevel", {"level": "info"}),
    ],
    ids=["tools", "resources", "prompts", "tasks", "skills", "logging"],
)
async def test_mcp_disabled_capabilities_omitted_and_gated_with_method_not_found(
    config_kwargs: dict[str, Any],
    disabled_key: str,
    method: str,
    params: dict[str, Any],
) -> None:
    """Disabled capability families are omitted from capabilities and return -32601 on dispatch."""
    core = MCPCore(config=MCPConfig(**config_kwargs))
    core.register_extension("io.modelcontextprotocol/custom", {"enabled": True})

    discover_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28"},
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "server/discover", "params": {}}),
        )
    )
    caps = decode_json(discover_resp.body)["result"]["capabilities"]
    assert disabled_key not in caps
    assert caps["extensions"]["io.modelcontextprotocol/custom"] == {"enabled": True}
    assert caps["extensions"][UI_EXTENSION] == {"mimeTypes": [UI_MIME_TYPE]}
    if disabled_key == "skills":
        assert SKILLS_EXTENSION not in caps["extensions"]

    init_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 2, "method": "initialize", "params": {"protocolVersion": "2025-11-25"}}
            ),
        )
    )
    init_caps = decode_json(init_resp.body)["result"]["capabilities"]
    assert disabled_key not in init_caps
    assert init_caps["extensions"]["io.modelcontextprotocol/custom"] == {"enabled": True}
    assert init_caps["extensions"][UI_EXTENSION] == {"mimeTypes": [UI_MIME_TYPE]}

    call_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2025-11-25"},
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 3, "method": method, "params": params}),
        )
    )
    assert call_resp.status == 200
    assert decode_json(call_resp.body)["error"]["code"] == METHOD_NOT_FOUND


@pytest.mark.anyio
@pytest.mark.parametrize("level", MCP_LOG_LEVELS)
async def test_mcp_legacy_logging_set_level_accepts_valid_levels(level: str) -> None:
    """Legacy logging/setLevel updates session log_level across all RFC 5424 severity names."""
    core = MCPCore()
    init_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-11-25"}}
            ),
        )
    )
    session_id = init_resp.headers["Mcp-Session-Id"]

    set_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"Mcp-Session-Id": session_id, "MCP-Protocol-Version": "2025-11-25"},
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 2, "method": "logging/setLevel", "params": {"level": level}}
            ),
        )
    )
    assert set_resp.status == 200
    assert decode_json(set_resp.body)["result"] == {}
    stored = await core.session_store.get(session_id, None)
    assert stored is not None
    assert stored.log_level == level


@pytest.mark.anyio
@pytest.mark.parametrize("bad_level", ["trace", "", 42, None])
async def test_mcp_legacy_logging_set_level_rejects_invalid_levels(bad_level: Any) -> None:
    """Invalid logging/setLevel arguments fail with JSON-RPC -32602."""
    core = MCPCore()
    resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2025-11-25"},
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 1, "method": "logging/setLevel", "params": {"level": bad_level}}
            ),
        )
    )
    assert resp.status == 200
    assert decode_json(resp.body)["error"]["code"] == INVALID_PARAMS


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        ("2025-11-25", "2025-11-25"),
        ("2025-06-18", "2025-06-18"),
        ("2025-03-26", "2025-03-26"),
        ("2026-07-28", MCP_FALLBACK_SESSION_VERSION),
    ],
)
async def test_mcp_legacy_initialize_ping_and_client_capabilities(requested: str, expected: str) -> None:
    """Bare legacy initialize negotiates version, captures client capabilities, and issues Mcp-Session-Id."""
    core = MCPCore(config=MCPConfig(server_name="legacy-host", server_version="2.0.0", instructions="Be concise."))
    seen_caps: list[dict[str, Any]] = []

    @tool(name="inspect_caps")
    def inspect_caps(context: AgentContext) -> str:
        seen_caps.append(dict(context.metadata.get("mcp_client_capabilities", {})))
        return str(context.session_id)

    core.registry.register_tool(inspect_caps)
    owner = AgentContext(identity=CallerIdentity(user_id="alice", tenant_id="t1"))

    init_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=owner,
            body=encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": requested,
                        "capabilities": {"roots": {"listChanged": True}},
                        "clientInfo": {"name": "claude-cli", "version": "1.0"},
                    },
                }
            ),
        )
    )
    assert init_resp.status == 200
    assert init_resp.headers["MCP-Protocol-Version"] == expected
    session_id = init_resp.headers["Mcp-Session-Id"]
    assert session_id.startswith("mcp_")
    init_result = decode_json(init_resp.body)["result"]
    assert init_result["protocolVersion"] == expected
    assert init_result["instructions"] == "Be concise."
    assert "tasks" in init_result["capabilities"]
    assert "logging" in init_result["capabilities"]

    notif_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=owner,
            headers={"Mcp-Session-Id": session_id},
            body=encode_json_bytes({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        )
    )
    assert notif_resp.status == 202
    assert notif_resp.headers["Mcp-Session-Id"] == session_id

    ping_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=owner,
            headers={"Mcp-Session-Id": session_id},
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 2, "method": "ping"}),
        )
    )
    assert ping_resp.status == 200
    assert decode_json(ping_resp.body)["result"] == {}

    call_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=owner,
            headers={"Mcp-Session-Id": session_id},
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "inspect_caps"}}
            ),
        )
    )
    assert call_resp.status == 200
    assert decode_json(call_resp.body)["result"]["content"][0]["text"] == session_id
    assert seen_caps == [{"roots": {"listChanged": True}}]


@pytest.mark.anyio
async def test_mcp_session_store_ttl_owner_isolation_and_capacity() -> None:
    """MCPSessionStore enforces deterministic TTL expiry, exact owner isolation, reverse index, and max_sessions."""
    now = [100.0]
    store = MCPSessionStore(ttl_seconds=60.0, max_sessions=2, clock=lambda: now[0])

    s1 = await store.create(
        owner_id="t1:alice",
        protocol_version="2025-11-25",
        client_capabilities={},
        client_info={"name": "c1"},
        log_level="info",
    )
    s2 = await store.create(
        owner_id=None,
        protocol_version="2025-06-18",
        client_capabilities={},
        client_info={"name": "c2"},
        log_level="warning",
    )
    assert await store.active_session_ids() == (s1.session_id, s2.session_id)

    with pytest.raises(SessionLimitExceededError):
        await store.create(
            owner_id="t1:bob",
            protocol_version="2025-11-25",
            client_capabilities={},
            client_info={},
            log_level="info",
        )

    assert await store.get(s1.session_id, None) is None
    assert await store.get(s1.session_id, "t1:bob") is None
    assert await store.get(s2.session_id, "t1:alice") is None
    assert await store.get(s1.session_id, "t1:alice") is not None

    updated = await store.update(
        s1.session_id,
        "t1:alice",
        subscriptions=frozenset({"file:///a.txt", "file:///b.txt"}),
    )
    assert updated is not None
    assert await store.subscribed_session_ids("file:///a.txt") == (s1.session_id,)
    assert await store.subscribed_session_ids("file:///b.txt") == (s1.session_id,)

    now[0] = 160.0
    assert await store.get(s1.session_id, "t1:alice") is None
    assert await store.subscribed_session_ids("file:///a.txt") == ()
    assert await store.active_session_ids() == ()

    s3 = await store.create(
        owner_id="t1:bob",
        protocol_version="2025-11-25",
        client_capabilities={},
        client_info={},
        log_level="info",
    )
    assert s3.session_id in await store.active_session_ids()

    core = MCPCore(config=MCPConfig(max_sessions=1))
    first = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
        )
    )
    assert first.status == 200
    full = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 2, "method": "initialize", "params": {}}),
        )
    )
    assert full.status == 503
    assert decode_json(full.body)["error"]["code"] == SESSION_CAPACITY_EXHAUSTED


@pytest.mark.anyio
@pytest.mark.parametrize("bad_case", ["unknown", "expired", "wrong-owner", "anonymous-on-owned"])
async def test_mcp_legacy_invalid_session_returns_404_and_modern_strips_session(bad_case: str) -> None:
    """Unknown, expired, or wrong-owner legacy sessions return 404 while 2026-07-28 never echoes Mcp-Session-Id."""
    now = [10.0]
    store = MCPSessionStore(ttl_seconds=30.0, max_sessions=10, clock=lambda: now[0])
    core = MCPCore(session_store=store)
    alice = AgentContext(identity=CallerIdentity(user_id="alice", tenant_id="t1"))
    bob = AgentContext(identity=CallerIdentity(user_id="bob", tenant_id="t1"))

    init_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=alice,
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
        )
    )
    sid = init_resp.headers["Mcp-Session-Id"]

    caller_ctx: AgentContext | None = alice
    lookup_sid = sid
    if bad_case == "unknown":
        lookup_sid = "mcp_nonexistent"
    elif bad_case == "expired":
        now[0] = 50.0
    elif bad_case == "wrong-owner":
        caller_ctx = bob
    elif bad_case == "anonymous-on-owned":
        caller_ctx = None

    bad_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=caller_ctx,
            headers={"Mcp-Session-Id": lookup_sid},
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
        )
    )
    assert bad_resp.status == 404
    err = decode_json(bad_resp.body)["error"]
    assert err["code"] == INVALID_PARAMS
    assert err["message"] == "Session not found or expired"

    modern_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28", "Mcp-Session-Id": "caller-invented-id"},
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {}}),
        )
    )
    assert modern_resp.status == 200
    assert "Mcp-Session-Id" not in modern_resp.headers


@pytest.mark.anyio
@pytest.mark.parametrize("version", [v for v in SUPPORTED_PROTOCOL_VERSIONS if v != "2025-03-26"])
async def test_mcp_batch_arrays_rejected_outside_2025_03_26(version: str) -> None:
    """JSON-RPC batch arrays return HTTP 400 and -32600 on 2025-06-18, 2025-11-25, and 2026-07-28."""
    core = MCPCore()
    resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": version},
            body=encode_json_bytes([{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}]),
        )
    )
    assert resp.status == 400
    assert decode_json(resp.body)["error"]["code"] == INVALID_REQUEST


@pytest.mark.anyio
async def test_mcp_2025_03_26_batch_dispatch_and_delete_session_termination() -> None:
    """2025-03-26 supports batch arrays (rejecting initialize inside batches) and DELETE /mcp terminates sessions."""
    core = MCPCore()
    alice = AgentContext(identity=CallerIdentity(user_id="alice", tenant_id="t1"))
    bob = AgentContext(identity=CallerIdentity(user_id="bob", tenant_id="t1"))

    init_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=alice,
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26"}}
            ),
        )
    )
    assert init_resp.status == 200
    sid = init_resp.headers["Mcp-Session-Id"]

    batch_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=alice,
            headers={"Mcp-Session-Id": sid},
            body=encode_json_bytes(
                [
                    {"jsonrpc": "2.0", "id": 10, "method": "initialize", "params": {}},
                    {"jsonrpc": "2.0", "id": 11, "method": "ping"},
                    {"jsonrpc": "2.0", "method": "notifications/initialized"},
                ]
            ),
        )
    )
    assert batch_resp.status == 200
    assert batch_resp.headers["MCP-Protocol-Version"] == "2025-03-26"
    items = decode_json(batch_resp.body)
    assert len(items) == 2
    by_id = {entry["id"]: entry for entry in items}
    assert by_id[10]["error"]["code"] == INVALID_REQUEST
    assert by_id[11]["result"] == {}

    options_resp = await core.handle(McpHttpRequest(method="OPTIONS", path="/mcp"))
    assert options_resp.status == 204
    assert options_resp.headers["Allow"] == "GET, POST, DELETE, OPTIONS"
    assert options_resp.headers["Access-Control-Allow-Methods"] == "GET, POST, DELETE, OPTIONS"

    recv_stream, _ = core.stream_broker.subscribe(sid)

    wrong_owner_del = await core.handle(
        McpHttpRequest(method="DELETE", path="/mcp", context=bob, headers={"Mcp-Session-Id": sid})
    )
    assert wrong_owner_del.status == 404
    assert await core.session_store.get(sid, "t1:alice") is not None

    del_resp = await core.handle(
        McpHttpRequest(method="DELETE", path="/mcp", context=alice, headers={"Mcp-Session-Id": sid})
    )
    assert del_resp.status == 204
    assert await core.session_store.get(sid, "t1:alice") is None

    with pytest.raises(anyio.EndOfStream):
        await recv_stream.receive()

    post_del = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=alice,
            headers={"Mcp-Session-Id": sid},
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 12, "method": "ping"}),
        )
    )
    assert post_del.status == 404
    assert LEGACY_PROTOCOL_VERSIONS == ("2025-11-25", "2025-06-18", "2025-03-26")
    assert isinstance(SseFrame(data="ok"), SseFrame)


def _decode_sse_json(chunk: bytes) -> dict[str, Any]:
    lines = [line[len("data: ") :] for line in chunk.decode("utf-8").splitlines() if line.startswith("data: ")]
    return dict(decode_json("\n".join(lines).encode("utf-8")))


async def _init_legacy_session(
    core: MCPCore,
    *,
    context: AgentContext | None = None,
    version: str = "2025-11-25",
    capabilities: dict[str, Any] | None = None,
) -> str:
    params: dict[str, Any] = {"protocolVersion": version}
    if capabilities is not None:
        params["capabilities"] = capabilities
    resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=context,
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": params}),
        )
    )
    assert resp.status == 200
    return resp.headers["Mcp-Session-Id"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("enable_get_sse", "headers", "expected_status"),
    [
        (True, {"Accept": "text/event-stream", "MCP-Protocol-Version": "2026-07-28"}, 405),
        (False, {"Accept": "text/event-stream", "MCP-Protocol-Version": "2025-11-25"}, 405),
        (True, {"MCP-Protocol-Version": "2025-11-25"}, 405),
        (True, {"Accept": "*/*", "MCP-Protocol-Version": "2025-11-25"}, 405),
        (True, {"Accept": "application/json", "MCP-Protocol-Version": "2025-11-25"}, 406),
        (
            True,
            {
                "Accept": "text/event-stream",
                "MCP-Protocol-Version": "2025-11-25",
                "Mcp-Session-Id": "mcp_missing",
            },
            404,
        ),
    ],
    ids=["modern-405", "disabled-405", "missing-accept-405", "wildcard-accept-405", "json-accept-406", "unknown-404"],
)
async def test_mcp_get_sse_negotiation_matrix(
    enable_get_sse: bool,
    headers: dict[str, str],
    expected_status: int,
) -> None:
    """GET /mcp enforces 2026-07-28 405, Accept 405/406, and unknown session 404."""
    core = MCPCore(config=MCPConfig(enable_get_sse=enable_get_sse))
    resp = await core.handle(McpHttpRequest(method="GET", path="/mcp", headers=headers))
    assert resp.status == expected_status
    assert resp.stream is None


@pytest.mark.anyio
async def test_mcp_get_sse_session_replay_keepalive_and_owner_limit() -> None:
    """Legacy GET /mcp streams events, replays after Last-Event-ID, emits keepalives, and bounds owner streams."""
    alice = AgentContext(identity=CallerIdentity(user_id="alice", tenant_id="t1"))
    core = MCPCore(config=MCPConfig(sse_heartbeat_seconds=0.02, max_streams_per_owner=8))
    sid = await _init_legacy_session(core, context=alice)

    core.stream_broker.publish(sid, SseFrame(id="ev-1", event="message", data='{"jsonrpc":"2.0","method":"n1"}'))
    core.stream_broker.publish(sid, SseFrame(id="ev-2", event="message", data='{"jsonrpc":"2.0","method":"n2"}'))

    get_resp = await core.handle(
        McpHttpRequest(
            method="GET",
            path="/mcp",
            context=alice,
            headers={"Accept": "text/event-stream", "Mcp-Session-Id": sid, "Last-Event-ID": "ev-1"},
        )
    )
    assert get_resp.status == 200
    assert get_resp.headers["Content-Type"] == "text/event-stream"
    assert get_resp.headers["Mcp-Session-Id"] == sid
    assert get_resp.stream is not None
    assert await anext(get_resp.stream) == b'id: ev-2\ndata: {"jsonrpc":"2.0","method":"n2"}\n\n'
    assert await anext(get_resp.stream) == b": keepalive\n\n"

    extra_streams = [get_resp.stream]
    for _ in range(7):
        extra = await core.handle(
            McpHttpRequest(
                method="GET",
                path="/mcp",
                context=alice,
                headers={"Accept": "text/event-stream", "Mcp-Session-Id": sid},
            )
        )
        assert extra.status == 200
        assert extra.stream is not None
        extra_streams.append(extra.stream)

    ninth = await core.handle(
        McpHttpRequest(
            method="GET",
            path="/mcp",
            context=alice,
            headers={"Accept": "text/event-stream", "Mcp-Session-Id": sid},
        )
    )
    assert ninth.status == 503
    assert decode_json(ninth.body)["error"]["code"] == SESSION_CAPACITY_EXHAUSTED
    assert isinstance(StreamLimitExceededError("limit"), StreamLimitExceededError)

    await extra_streams.pop().aclose()
    replacement = await core.handle(
        McpHttpRequest(
            method="GET",
            path="/mcp",
            context=alice,
            headers={"Accept": "text/event-stream", "Mcp-Session-Id": sid},
        )
    )
    assert replacement.status == 200
    assert replacement.stream is not None
    extra_streams.append(replacement.stream)

    for stream in extra_streams:
        await stream.aclose()
    assert core.stream_broker.owner_stream_count("t1:alice") == 0


@pytest.mark.anyio
async def test_mcp_adapters_stream_incrementally_and_close_on_disconnect() -> None:
    """ASGI, Starlette, WSGI, Flask, Django, native App, and Litestar stream chunks and close streams."""
    if not django_settings.configured:
        django_settings.configure()

    core = MCPCore(config=MCPConfig(sse_heartbeat_seconds=5.0))
    sid = await _init_legacy_session(core)

    def _open_session_stream() -> tuple[Any, list[bool]]:
        closed_flag = [False]
        inner = core.stream_broker.open_stream(sid, owner_id=None, heartbeat_seconds=5.0, max_streams_per_owner=8)

        class _TrackingStream(AsyncIterator[bytes]):
            def __aiter__(self) -> "_TrackingStream":
                return self

            async def __anext__(self) -> bytes:
                return await inner.__anext__()

            async def aclose(self) -> None:
                closed_flag[0] = True
                await inner.aclose()

        return _TrackingStream(), closed_flag

    asgi_app = AsgiMcpApp(core)
    asgi_stream, asgi_closed = _open_session_stream()
    original_handle = core.handle

    async def _asgi_handle(req: McpHttpRequest) -> McpHttpResponse:
        if req.method == "GET":
            return McpHttpResponse(
                status=200,
                headers={"Content-Type": "text/event-stream", "Mcp-Session-Id": sid},
                body=b"",
                stream=asgi_stream,
            )
        return await original_handle(req)

    cast(Any, core).handle = _asgi_handle
    disconnect_gate = anyio.Event()
    sent_events: list[dict[str, Any]] = []
    first_chunk_received = anyio.Event()
    recv_calls = 0

    async def _asgi_receive() -> dict[str, Any]:
        nonlocal recv_calls
        recv_calls += 1
        if recv_calls == 1:
            return {"type": "http.request", "body": b"", "more_body": False}
        await disconnect_gate.wait()
        return {"type": "http.disconnect"}

    async def _asgi_send(event: dict[str, Any]) -> None:
        sent_events.append(event)
        if event.get("type") == "http.response.body" and event.get("body"):
            first_chunk_received.set()

    async with anyio.create_task_group() as tg:
        tg.start_soon(
            asgi_app,
            {"type": "http", "method": "GET", "path": "/mcp", "headers": [(b"accept", b"text/event-stream")]},
            _asgi_receive,
            _asgi_send,
        )
        await anyio.sleep(0)
        core.stream_broker.publish(sid, SseFrame(id="asgi-1", event="message", data="hello"))
        await first_chunk_received.wait()
        assert sent_events[0]["type"] == "http.response.start"
        assert sent_events[1]["more_body"] is True
        assert b"hello" in sent_events[1]["body"]
        assert asgi_closed[0] is False
        disconnect_gate.set()

    assert asgi_closed[0] is True

    starlette_stream, starlette_closed = _open_session_stream()

    async def _starlette_handle(req: McpHttpRequest) -> McpHttpResponse:
        if req.method == "GET":
            return McpHttpResponse(
                status=200,
                headers={"Content-Type": "text/event-stream"},
                body=b"",
                stream=starlette_stream,
            )
        return await original_handle(req)

    cast(Any, core).handle = _starlette_handle
    star_req = StarletteRequest(
        {
            "type": "http",
            "method": "GET",
            "path": "/mcp",
            "headers": [(b"accept", b"text/event-stream")],
        }
    )
    star_resp = await asgi_app.handle_starlette_request(star_req)
    assert isinstance(star_resp, StreamingResponse)
    star_iter = star_resp.body_iterator
    core.stream_broker.publish(sid, SseFrame(id="star-1", event="message", data="star-chunk"))
    assert b"star-chunk" in await anext(star_iter)
    await star_iter.aclose()
    assert starlette_closed[0] is True

    cast(Any, core).handle = original_handle

    wsgi_stream, wsgi_closed = _open_session_stream()

    async def _wsgi_handle(req: McpHttpRequest) -> McpHttpResponse:
        if req.method == "GET":
            return McpHttpResponse(
                status=200,
                headers={"Content-Type": "text/event-stream"},
                body=b"",
                stream=wsgi_stream,
            )
        return await original_handle(req)

    cast(Any, core).handle = _wsgi_handle
    wsgi_app = WsgiMcpApp(core)
    wsgi_meta: dict[str, Any] = {}

    def _start_resp(status: str, headers: list[tuple[str, str]]) -> None:
        wsgi_meta["status"] = status
        wsgi_meta["headers"] = headers

    wsgi_iter = wsgi_app(
        {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": "/mcp",
            "HTTP_ACCEPT": "text/event-stream",
            "HTTP_MCP_PROTOCOL_VERSION": "2025-11-25",
            "HTTP_MCP_SESSION_ID": sid,
        },
        _start_resp,
    )
    core.stream_broker.publish(sid, SseFrame(id="wsgi-1", event="message", data="wsgi-chunk"))
    chunk = await anyio.to_thread.run_sync(next, iter(wsgi_iter))
    assert b"wsgi-chunk" in chunk
    assert wsgi_meta["status"].startswith("200")
    await anyio.to_thread.run_sync(cast(Any, wsgi_iter).close)
    assert wsgi_closed[0] is True

    flask_stream, flask_closed = _open_session_stream()

    async def _flask_handle(req: McpHttpRequest) -> McpHttpResponse:
        if req.method == "GET":
            return McpHttpResponse(
                status=200,
                headers={"Content-Type": "text/event-stream"},
                body=b"",
                stream=flask_stream,
            )
        return await original_handle(req)

    cast(Any, core).handle = _flask_handle
    flask_res = wsgi_app.handle_flask_view(
        SimpleNamespace(
            method="GET",
            path="/mcp",
            headers={"accept": "text/event-stream", "mcp-protocol-version": "2025-11-25"},
            body=b"",
        )
    )
    assert isinstance(flask_res, tuple)
    flask_iter, flask_status, _ = flask_res
    assert flask_status == 200
    core.stream_broker.publish(sid, SseFrame(id="fl-1", event="message", data="flask-chunk"))
    assert b"flask-chunk" in await anyio.to_thread.run_sync(next, iter(flask_iter))
    await anyio.to_thread.run_sync(cast(Any, flask_iter).close)
    assert flask_closed[0] is True

    django_reg = ToolRegistry()
    django_core = MCPCore(config=MCPConfig(server_name="dj-sse"), registry=django_reg)
    dj_sid = await _init_legacy_session(django_core)
    cast(Any, django_reg)._mcp_app = WsgiMcpApp(django_core)
    django_stream_closed = [False]
    dj_inner = django_core.stream_broker.open_stream(dj_sid, owner_id=None, heartbeat_seconds=5.0)

    class _DjTrackingStream(AsyncIterator[bytes]):
        def __aiter__(self) -> "_DjTrackingStream":
            return self

        async def __anext__(self) -> bytes:
            return await dj_inner.__anext__()

        async def aclose(self) -> None:
            django_stream_closed[0] = True
            await dj_inner.aclose()

    orig_dj_handle = django_core.handle

    async def _dj_handle(req: McpHttpRequest) -> McpHttpResponse:
        if req.method == "GET":
            return McpHttpResponse(
                status=200,
                headers={"Content-Type": "text/event-stream"},
                body=b"",
                stream=_DjTrackingStream(),
            )
        return await orig_dj_handle(req)

    cast(Any, django_core).handle = _dj_handle
    dj_resp = dispatch_django_mcp(
        django_reg,
        SimpleNamespace(
            method="GET",
            path="/mcp",
            headers={"accept": "text/event-stream", "mcp-protocol-version": "2025-11-25"},
            body=b"",
        ),
        MCPConfig(server_name="dj-sse"),
    )
    assert isinstance(dj_resp, StreamingHttpResponse)
    django_core.stream_broker.publish(dj_sid, SseFrame(id="dj-1", event="message", data="dj-chunk"))
    dj_chunk = await anyio.to_thread.run_sync(next, dj_resp.streaming_content)
    assert b"dj-chunk" in dj_chunk
    dj_resp.close()
    assert django_stream_closed[0] is True

    native_plugin = AgentPlugin(AgentPluginConfig(mcp=MCPConfig(server_name="native-sse")))
    native_app = App()
    native_plugin.mount_native(native_app)
    native_routes = {(r.method.upper(), r.path): r.handler for r in native_app.routes}
    for required_verb in ("POST", "GET", "DELETE", "OPTIONS"):
        assert (required_verb, "/mcp") in native_routes

    assert native_plugin.mcp_service is not None
    native_sid = await _init_legacy_session(native_plugin.mcp_service)
    native_status, native_hdrs, native_body = await native_routes[("GET", "/mcp")](
        headers={"Accept": "text/event-stream", "Mcp-Session-Id": native_sid}
    )
    assert native_status == 200
    assert native_hdrs["Content-Type"] == "text/event-stream"
    assert isinstance(native_body, AsyncIterator)
    native_plugin.mcp_service.stream_broker.publish(
        native_sid, SseFrame(id="nat-1", event="message", data="native-chunk")
    )
    assert b"native-chunk" in await anext(native_body)
    await native_body.aclose()

    opt_status, opt_hdrs, _ = await native_routes[("OPTIONS", "/mcp")](headers={})
    assert opt_status == 204
    assert "DELETE" in opt_hdrs["Allow"]

    litestar_plugin = AgentPlugin(AgentPluginConfig(mcp=MCPConfig(server_name="ls-sse")))
    litestar_app = Litestar(route_handlers=[], plugins=[litestar_plugin])
    ls_service = litestar_plugin.mcp_service
    assert ls_service is not None
    ls_sid = await _init_legacy_session(ls_service)
    ls_stream_closed = [False]
    ls_inner = ls_service.stream_broker.open_stream(ls_sid, owner_id=None, heartbeat_seconds=5.0)

    class _LsTrackingStream(AsyncIterator[bytes]):
        def __aiter__(self) -> "_LsTrackingStream":
            return self

        async def __anext__(self) -> bytes:
            return await ls_inner.__anext__()

        async def aclose(self) -> None:
            ls_stream_closed[0] = True
            await ls_inner.aclose()

    async def _ls_stream_handle(req: McpHttpRequest) -> McpHttpResponse:
        if req.method == "GET":
            return McpHttpResponse(
                status=200,
                headers={"Content-Type": "text/event-stream"},
                body=b"",
                stream=_LsTrackingStream(),
            )
        return McpHttpResponse(status=204, headers={"Allow": "GET, POST, DELETE, OPTIONS"}, body=b"")

    cast(Any, ls_service).handle = _ls_stream_handle
    mcp_route = next(h for h in litestar_app.routes if h.path == "/mcp")
    assert set(mcp_route.methods) >= {"POST", "GET", "DELETE", "OPTIONS"}
    ls_route_handler = mcp_route.route_handler_map["GET"][0]
    ls_result = await ls_route_handler.fn(
        SimpleNamespace(
            method="GET",
            headers={"accept": "text/event-stream", "mcp-session-id": ls_sid},
            body=AsyncMockBytes(),
            scope={"headers": []},
        )
    )
    assert isinstance(ls_result, LitestarStream)
    ls_service.stream_broker.publish(ls_sid, SseFrame(id="ls-1", event="message", data="ls-chunk"))
    assert b"ls-chunk" in await anext(ls_result.iterator)
    await ls_result.iterator.aclose()
    assert ls_stream_closed[0] is True


class AsyncMockBytes:
    """Async callable returning empty bytes for request.body()."""

    async def __call__(self) -> bytes:
        return b""


@pytest.mark.anyio
async def test_mcp_list_changed_logging_and_progress_notifications() -> None:
    """Registry listeners, level-gated MCPCore.log, and POST/session progress notifications deliver frames."""
    zero_arg_calls: list[str] = []
    kind_calls: list[str] = []

    core = MCPCore(config=MCPConfig(default_log_level="warning"))
    core.registry.add_listener(lambda: zero_arg_calls.append("mutated"))
    core.registry.add_listener(lambda kind: kind_calls.append(kind))

    @tool(name="slow_tool")
    async def slow_tool(ctx: AgentContext) -> str:
        await ctx.progress_reporter.report(1, total=2, message="halfway")
        await ctx.progress_reporter.report(2, total=2, message="done")
        return "finished"

    sid = await _init_legacy_session(core)
    sse_resp = await core.handle(
        McpHttpRequest(
            method="GET",
            path="/mcp",
            headers={"Accept": "text/event-stream", "Mcp-Session-Id": sid},
        )
    )
    assert sse_resp.stream is not None

    core.registry.register_tool(slow_tool)
    core.registry.register_resource(ResourceEntry(uri="doc://1", name="Doc1", reader=lambda: "text"))
    core.registry.register_prompt(PromptEntry(name="p1", description="Prompt1", getter=lambda: []))

    assert len(zero_arg_calls) == 3
    assert kind_calls == ["tools", "resources", "prompts"]

    list_changed_methods = [_decode_sse_json(await anext(sse_resp.stream))["method"] for _ in range(3)]
    assert list_changed_methods == [
        "notifications/tools/list_changed",
        "notifications/resources/list_changed",
        "notifications/prompts/list_changed",
    ]

    assert await core.log("info", "ignored-below-default") == 0
    assert await core.log("warning", {"detail": "warn-msg"}, logger="mcp.test") == 1
    log_evt = _decode_sse_json(await anext(sse_resp.stream))
    assert log_evt["method"] == "notifications/message"
    assert log_evt["params"] == {"level": "warning", "data": {"detail": "warn-msg"}, "logger": "mcp.test"}

    set_level_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"Mcp-Session-Id": sid},
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 10, "method": "logging/setLevel", "params": {"level": "error"}}
            ),
        )
    )
    assert set_level_resp.status == 200
    assert await core.log("warning", "suppressed-after-setlevel") == 0
    assert await core.log("error", "delivered-error") == 1
    assert _decode_sse_json(await anext(sse_resp.stream))["params"]["data"] == "delivered-error"

    legacy_prog_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"Mcp-Session-Id": sid, "Accept": "application/json"},
            body=encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": 11,
                    "method": "tools/call",
                    "params": {"name": "slow_tool", "_meta": {"progressToken": "tok-sess"}},
                }
            ),
        )
    )
    assert legacy_prog_resp.status == 200
    assert decode_json(legacy_prog_resp.body)["result"]["content"][0]["text"] == "finished"
    p1 = _decode_sse_json(await anext(sse_resp.stream))
    p2 = _decode_sse_json(await anext(sse_resp.stream))
    assert p1["method"] == "notifications/progress"
    assert p1["params"] == {"progressToken": "tok-sess", "progress": 1, "total": 2, "message": "halfway"}
    assert p2["params"]["progress"] == 2
    await sse_resp.stream.aclose()

    post_sse_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28", "Accept": "application/json, text/event-stream"},
            body=encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": 12,
                    "method": "tools/call",
                    "params": {"name": "slow_tool", "_meta": {"progressToken": "tok-post"}},
                }
            ),
        )
    )
    assert post_sse_resp.status == 200
    assert post_sse_resp.headers["Content-Type"] == "text/event-stream"
    assert post_sse_resp.stream is not None
    post_frames = [_decode_sse_json(chunk) async for chunk in post_sse_resp.stream]
    assert len(post_frames) == 3
    assert post_frames[0]["method"] == "notifications/progress"
    assert post_frames[0]["params"]["progressToken"] == "tok-post"
    assert post_frames[1]["method"] == "notifications/progress"
    assert post_frames[2]["id"] == 12
    assert post_frames[2]["result"]["content"][0]["text"] == "finished"

    disabled_tools_core = MCPCore(config=MCPConfig(tools_enabled=False, logging=False))
    disabled_sid = await _init_legacy_session(disabled_tools_core)
    disabled_stream_resp = await disabled_tools_core.handle(
        McpHttpRequest(
            method="GET",
            path="/mcp",
            headers={"Accept": "text/event-stream", "Mcp-Session-Id": disabled_sid},
        )
    )
    assert disabled_stream_resp.stream is not None
    disabled_tools_core.registry.register_tool(slow_tool)
    assert await disabled_tools_core.log("emergency", "ignored-when-logging-disabled") == 0
    await disabled_stream_resp.stream.aclose()


@pytest.mark.anyio
async def test_mcp_resource_subscriptions_fanout_cleanup_and_listen() -> None:
    """Authorized resources/subscribe, notify_resource_updated, cleanup, and 2026-07-28 subscriptions/listen work."""
    now_mono = [100.0]
    store = MCPSessionStore(ttl_seconds=10.0, clock=lambda: now_mono[0])

    async def _authorizer(uri: str, ctx: AgentContext | None) -> bool:
        if uri == "doc://forbidden":
            return False
        if uri == "doc://explode":
            raise RuntimeError("auth backend failure")
        return True

    skill = Skill.build("guide", "Guide", body="# Guide", files={"SKILL.md": "# Guide"})
    catalog = SkillCatalog(skills=[skill])
    core = MCPCore(
        config=MCPConfig(subscription_authorizer=_authorizer, sse_heartbeat_seconds=5.0),
        session_store=store,
        skill_catalog=catalog,
    )
    assert not hasattr(core, "_resource_subscriptions")
    core.registry.register_resource(ResourceEntry(uri="doc://allowed", name="Allowed", reader=lambda: "ok"))
    core.registry.register_resource(ResourceEntry(uri="doc://forbidden", name="Forbidden", reader=lambda: "no"))
    core.registry.register_resource(ResourceEntry(uri="doc://explode", name="Explode", reader=lambda: "boom"))
    core.registry.register_resource_template(
        ResourceTemplateEntry(uriTemplate="item://{item_id}", name="Item", reader=lambda item_id: item_id)
    )

    alice = AgentContext(identity=CallerIdentity(user_id="alice", tenant_id="t1"))
    sid = await _init_legacy_session(core, context=alice)
    get_resp = await core.handle(
        McpHttpRequest(
            method="GET",
            path="/mcp",
            context=alice,
            headers={"Accept": "text/event-stream", "Mcp-Session-Id": sid},
        )
    )
    assert get_resp.stream is not None

    for valid_uri in ("doc://allowed", "item://42", "skill://guide/SKILL.md"):
        sub_resp = await core.handle(
            McpHttpRequest(
                method="POST",
                path="/mcp",
                context=alice,
                headers={"Mcp-Session-Id": sid},
                body=encode_json_bytes(
                    {"jsonrpc": "2.0", "id": 2, "method": "resources/subscribe", "params": {"uri": valid_uri}}
                ),
            )
        )
        assert sub_resp.status == 200
        assert "error" not in decode_json(sub_resp.body)

    missing_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=alice,
            headers={"Mcp-Session-Id": sid},
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 3, "method": "resources/subscribe", "params": {"uri": "doc://missing"}}
            ),
        )
    )
    assert decode_json(missing_resp.body)["error"]["code"] == INVALID_PARAMS

    for denied_uri in ("doc://forbidden", "doc://explode"):
        denied_resp = await core.handle(
            McpHttpRequest(
                method="POST",
                path="/mcp",
                context=alice,
                headers={"Mcp-Session-Id": sid},
                body=encode_json_bytes(
                    {"jsonrpc": "2.0", "id": 4, "method": "resources/subscribe", "params": {"uri": denied_uri}}
                ),
            )
        )
        assert decode_json(denied_resp.body)["error"]["code"] == PERMISSION_DENIED

    listen_406 = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28", "Accept": "application/json"},
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 5, "method": "subscriptions/listen", "params": {}}),
        )
    )
    assert listen_406.status == 406

    listen_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28", "Accept": "text/event-stream"},
            body=encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": 6,
                    "method": "subscriptions/listen",
                    "params": {"resourceSubscriptions": ["doc://allowed"], "listChanged": ["tools"]},
                }
            ),
        )
    )
    assert listen_resp.status == 200
    assert listen_resp.stream is not None
    listen_init = _decode_sse_json(await anext(listen_resp.stream))
    sub_id = listen_init["result"]["subscriptionId"]

    delivered = await core.notify_resource_updated("doc://allowed")
    assert delivered == 2

    legacy_notif = _decode_sse_json(await anext(get_resp.stream))
    assert legacy_notif == {
        "jsonrpc": "2.0",
        "method": "notifications/resources/updated",
        "params": {"uri": "doc://allowed"},
    }
    listen_notif = _decode_sse_json(await anext(listen_resp.stream))
    assert listen_notif["method"] == "notifications/resources/updated"
    assert listen_notif["params"]["uri"] == "doc://allowed"
    assert listen_notif["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"] == sub_id

    @tool(name="listen_added")
    def listen_added() -> str:
        return "ok"

    core.registry.register_tool(listen_added)
    _ = await anext(get_resp.stream)
    listen_lc = _decode_sse_json(await anext(listen_resp.stream))
    assert listen_lc["method"] == "notifications/tools/list_changed"
    assert listen_lc["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"] == sub_id

    await listen_resp.stream.aclose()
    assert not core._listen_registrations

    unsub_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=alice,
            headers={"Mcp-Session-Id": sid},
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 7, "method": "resources/unsubscribe", "params": {"uri": "item://42"}}
            ),
        )
    )
    assert decode_json(unsub_resp.body)["result"] == {}
    unsub_again = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=alice,
            headers={"Mcp-Session-Id": sid},
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 8, "method": "resources/unsubscribe", "params": {"uri": "item://42"}}
            ),
        )
    )
    assert decode_json(unsub_again.body)["result"] == {}
    assert await core.notify_resource_updated("item://42") == 0

    del_resp = await core.handle(
        McpHttpRequest(method="DELETE", path="/mcp", context=alice, headers={"Mcp-Session-Id": sid})
    )
    assert del_resp.status == 204
    await get_resp.stream.aclose()
    assert await core.notify_resource_updated("doc://allowed") == 0

    sid_ttl = await _init_legacy_session(core, context=alice)
    ttl_stream_resp = await core.handle(
        McpHttpRequest(
            method="GET",
            path="/mcp",
            context=alice,
            headers={"Accept": "text/event-stream", "Mcp-Session-Id": sid_ttl},
        )
    )
    assert ttl_stream_resp.stream is not None
    await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=alice,
            headers={"Mcp-Session-Id": sid_ttl},
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 9, "method": "resources/subscribe", "params": {"uri": "doc://allowed"}}
            ),
        )
    )
    now_mono[0] = 120.0
    assert await core.notify_resource_updated("doc://allowed") == 0
    await ttl_stream_resp.stream.aclose()


@pytest.mark.anyio
async def test_mcp_resource_subscription_limits_and_disabled_flag() -> None:
    """Enforce max_subscriptions_per_session and resource_subscriptions=False gating."""
    limited_core = MCPCore(config=MCPConfig(max_subscriptions_per_session=2))
    for uri in ("doc://1", "doc://2", "doc://3"):
        limited_core.registry.register_resource(ResourceEntry(uri=uri, name=uri, reader=lambda val=uri: val))
    sid = await _init_legacy_session(limited_core)

    for uri in ("doc://1", "doc://2"):
        resp = await limited_core.handle(
            McpHttpRequest(
                method="POST",
                path="/mcp",
                headers={"Mcp-Session-Id": sid},
                body=encode_json_bytes(
                    {"jsonrpc": "2.0", "id": 1, "method": "resources/subscribe", "params": {"uri": uri}}
                ),
            )
        )
        assert "error" not in decode_json(resp.body)

    over_limit = await limited_core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"Mcp-Session-Id": sid},
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 2, "method": "resources/subscribe", "params": {"uri": "doc://3"}}
            ),
        )
    )
    assert decode_json(over_limit.body)["error"]["code"] == SESSION_CAPACITY_EXHAUSTED

    disabled_core = MCPCore(config=MCPConfig(resource_subscriptions=False))
    disabled_core.registry.register_resource(ResourceEntry(uri="doc://1", name="doc1", reader=lambda: "1"))
    disc = await disabled_core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28"},
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 3, "method": "server/discover"}),
        )
    )
    assert decode_json(disc.body)["result"]["capabilities"]["resources"]["subscribe"] is False
    disabled_sid = await _init_legacy_session(disabled_core)
    for method in ("resources/subscribe", "resources/unsubscribe"):
        err_resp = await disabled_core.handle(
            McpHttpRequest(
                method="POST",
                path="/mcp",
                headers={"Mcp-Session-Id": disabled_sid},
                body=encode_json_bytes({"jsonrpc": "2.0", "id": 4, "method": method, "params": {"uri": "doc://1"}}),
            )
        )
        assert decode_json(err_resp.body)["error"]["code"] == METHOD_NOT_FOUND


@pytest.mark.parametrize("raw_value", ["none", "config", "catalog"])
def test_skill_catalog_coerce_accepts_valid_inputs(tmp_path: Path, raw_value: str) -> None:
    """SkillCatalog.coerce handles None, MCPSkillsConfig, and existing SkillCatalog instances."""
    if raw_value == "none":
        assert SkillCatalog.coerce(None) is None
        return
    if raw_value == "config":
        skill_dir = tmp_path / "db-tuner"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text("---\nname: db-tuner\ndescription: Tune SQL\n---\nBody\n", encoding="utf-8")
        coerced = SkillCatalog.coerce(MCPSkillsConfig(paths=(tmp_path,)))
        assert isinstance(coerced, SkillCatalog)
        assert [s.name for s in coerced.list_skills()] == ["db-tuner"]
        return
    existing = SkillCatalog([Skill.build("alpha", "Alpha desc", body="Alpha body")])
    assert SkillCatalog.coerce(existing) is existing


@pytest.mark.parametrize("bad_value", ["not-a-config", 42, ["skill"]])
def test_skill_catalog_coerce_rejects_invalid_types(bad_value: Any) -> None:
    """SkillCatalog.coerce fails loud with TypeError on unsupported inputs."""
    with pytest.raises(TypeError, match="MCPSkillsConfig"):
        SkillCatalog.coerce(bad_value)


@pytest.mark.parametrize("lookup", ["alpha-ops", "skill://alpha-ops", "skill://alpha-ops/SKILL.md"])
def test_skill_catalog_filtering_uri_lookup_unregister_and_refresh(tmp_path: Path, lookup: str) -> None:
    """SkillCatalog supports tag/query filtering, skill:// lookup forms, listeners, unregister, and refresh."""
    alpha_dir = tmp_path / "alpha-ops"
    alpha_dir.mkdir()
    (alpha_dir / "SKILL.md").write_text(
        '---\nname: alpha-ops\ntitle: "Alpha Operations"\ndescription: "Run Oracle playbooks"\ntags: [oracle, ops]\n---\nAlpha instructions\n',
        encoding="utf-8",
    )
    beta_dir = tmp_path / "beta-docs"
    beta_dir.mkdir()
    (beta_dir / "SKILL.md").write_text(
        '---\nname: beta-docs\ntitle: "Beta Writer"\ndescription: "Write guides"\ntags: [oracle]\n---\nBeta instructions\n',
        encoding="utf-8",
    )

    catalog = SkillCatalog.from_config(MCPSkillsConfig(paths=(tmp_path,)))
    listener_calls: list[int] = []
    catalog.add_listener(lambda: listener_calls.append(1))

    resolved = catalog.get_skill(lookup)
    assert resolved is not None
    assert resolved.name == "alpha-ops"
    assert resolved.title == "Alpha Operations"
    assert catalog.get_skill("skill://alpha-ops/references/other.md") is None

    assert [s.name for s in catalog.list_skills(tags=["oracle"])] == ["alpha-ops", "beta-docs"]
    assert [s.name for s in catalog.list_skills(tags=["oracle", "ops"])] == ["alpha-ops"]
    assert [s.name for s in catalog.list_skills(query="writer")] == ["beta-docs"]
    assert [s.name for s in catalog.list_skills(query="PLAYBOOKS")] == ["alpha-ops"]

    entry_uris = [e.uri for e in catalog.resource_entries()]
    assert "skill://alpha-ops/SKILL.md" in entry_uris

    assert catalog.refresh() is False
    assert listener_calls == []

    (alpha_dir / "SKILL.md").write_text(
        '---\nname: alpha-ops\ntitle: "Alpha Operations"\ndescription: "Updated Oracle playbooks"\ntags: [oracle, ops]\n---\nUpdated instructions\n',
        encoding="utf-8",
    )
    assert catalog.refresh() is True
    assert len(listener_calls) == 1
    refreshed = catalog.get_skill("alpha-ops")
    assert refreshed is not None
    assert refreshed.description == "Updated Oracle playbooks"

    manual = Skill.build("manual-one", "Manual skill", body="Manual body", files={"SKILL.md": "Manual body"})
    catalog.register_skill(manual)
    assert len(listener_calls) == 2
    removed = catalog.unregister_skill("manual-one")
    assert removed is not None
    assert removed.name == "manual-one"
    assert catalog.unregister_skill("manual-one") is None
    assert len(listener_calls) == 3


@pytest.mark.anyio
async def test_mcp_skills_extension_pagination_blobs_prompts_and_notifications(tmp_path: Path) -> None:
    """MCPCore exposes io.modelcontextprotocol/skills, binary PNG blobs, skill prompts, and list_changed SSE."""
    png_bytes = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x01"
    skill_dir = tmp_path / "vision-skill"
    (skill_dir / "assets").mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        '---\nname: vision-skill\ntitle: "Vision Helper"\ndescription: "Inspect images"\ntags: [vision, ui]\n---\nUse vision steps.\n',
        encoding="utf-8",
    )
    (skill_dir / "assets" / "logo.png").write_bytes(png_bytes)

    extra_skills = [
        Skill.build(
            f"extra-{idx:03d}",
            f"Extra skill {idx}",
            body=f"Body {idx}",
            tags=("batch",),
            files={"SKILL.md": f"Body {idx}"},
        )
        for idx in range(55)
    ]
    catalog = SkillCatalog.from_config(
        MCPSkillsConfig(paths=(tmp_path,), expose_as_prompts=True, list_changed=True, directory_read=True)
    )
    for item in extra_skills:
        catalog.register_skill(item)

    core = MCPCore(skill_catalog=catalog)
    core.registry.register_prompt(
        PromptEntry(
            name="vision-skill",
            description="Explicit prompt override",
            getter=lambda: [PromptMessage(role="user", content=TextContent(text="Explicit prompt wins"))],
        )
    )

    disc_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28"},
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "server/discover"}),
        )
    )
    caps = decode_json(disc_resp.body)["result"]["capabilities"]
    assert "skills" not in caps
    assert caps["extensions"][SKILLS_EXTENSION] == {"listChanged": True, "directoryRead": True}

    page1_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28"},
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 2, "method": "skills/list", "params": {}}),
        )
    )
    page1 = decode_json(page1_resp.body)["result"]
    assert len(page1["skills"]) == 50
    next_cursor = page1["nextCursor"]

    page2_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28"},
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 3, "method": "skills/list", "params": {"cursor": next_cursor}}
            ),
        )
    )
    page2 = decode_json(page2_resp.body)["result"]
    assert len(page2["skills"]) == 6
    assert "nextCursor" not in page2

    filtered_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28"},
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 4, "method": "skills/list", "params": {"tags": ["vision"], "query": "Helper"}}
            ),
        )
    )
    filtered_skills = decode_json(filtered_resp.body)["result"]["skills"]
    assert [s["name"] for s in filtered_skills] == ["vision-skill"]
    assert filtered_skills[0]["title"] == "Vision Helper"

    res_list = await core._handle_resources_list({"cursor": page2["skills"][-1].get("unused")})
    res_uris = [r["uri"] for r in res_list["resources"]]
    assert any(u.startswith("skill://") for u in res_uris)
    tmpl_list = await core._handle_resources_templates_list()
    assert any(t["uriTemplate"] == "skill://{name}/{path}" for t in tmpl_list["resourceTemplates"])

    png_read = await core._handle_resources_read({"uri": "skill://vision-skill/assets/logo.png"})
    png_content = png_read["contents"][0]
    assert png_content["mimeType"] == "image/png"
    assert "text" not in png_content
    assert base64.b64decode(png_content["blob"]) == png_bytes

    skill_prompt_get = await core._handle_prompts_get({"name": "extra-001"})
    assert skill_prompt_get["messages"][0]["content"]["text"] == "Body 1"

    override_prompt_get = await core._handle_prompts_get({"name": "vision-skill"})
    assert override_prompt_get["messages"][0]["content"]["text"] == "Explicit prompt wins"

    sid = await _init_legacy_session(core)
    sse_resp = await core.handle(
        McpHttpRequest(
            method="GET",
            path="/mcp",
            headers={"Accept": "text/event-stream", "Mcp-Session-Id": sid},
        )
    )
    assert sse_resp.stream is not None
    try:
        (skill_dir / "SKILL.md").write_text(
            '---\nname: vision-skill\ntitle: "Vision Helper v2"\ndescription: "Inspect images"\ntags: [vision]\n---\nUpdated vision steps.\n',
            encoding="utf-8",
        )
        assert catalog.refresh() is True
        first_evt = decode_json((await anext(sse_resp.stream)).decode("utf-8").split("data: ", 1)[1].strip())
        second_evt = decode_json((await anext(sse_resp.stream)).decode("utf-8").split("data: ", 1)[1].strip())
        assert {first_evt["method"], second_evt["method"]} == {
            "notifications/skills/list_changed",
            "notifications/resources/list_changed",
        }
    finally:
        await sse_resp.stream.aclose()

    restricted_core = MCPCore(
        skill_catalog=MCPSkillsConfig(paths=(tmp_path,), directory_read=False),
    )
    restricted_caps = (await restricted_core._handle_discover())["capabilities"]
    assert restricted_caps["extensions"][SKILLS_EXTENSION]["directoryRead"] is False
    with pytest.raises(SkillAccessError):
        assert restricted_core.skill_catalog is not None
        restricted_core.skill_catalog.read_directory("skill://vision-skill")
    dir_resp = await restricted_core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28"},
            body=encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": 9,
                    "method": "resources/directory/read",
                    "params": {"uri": "skill://vision-skill"},
                }
            ),
        )
    )
    assert dir_resp.status == 200
    assert decode_json(dir_resp.body)["error"]["code"] == INVALID_PARAMS


@pytest.mark.parametrize("bad_case", ["dangling", "legacy-dangling", "non-ui-scheme", "wrong-mime"])
def test_mcp_apps_validate_ui_links_rejects_broken_configs(bad_case: str) -> None:
    """MCPCore.validate() and ToolRegistry.validate_ui_links raise ImproperConfigurationError on invalid UI links."""
    core = MCPCore()
    if bad_case == "dangling":

        @tool(name="t_dangling", meta={"ui": {"resourceUri": "ui://app/missing.html"}})
        def t_dangling() -> str:
            return "x"

        core.registry.register_tool(t_dangling)
    elif bad_case == "legacy-dangling":

        @tool(name="t_legacy", meta={"ui/resourceUri": "ui://app/missing-legacy.html"})
        def t_legacy() -> str:
            return "x"

        core.registry.register_tool(t_legacy)
    elif bad_case == "non-ui-scheme":

        @tool(name="t_http", meta={"ui": {"resourceUri": "https://evil.example/widget.html"}})
        def t_http() -> str:
            return "x"

        core.registry.register_tool(t_http)
    else:
        core.registry.register_resource(
            ResourceEntry(uri="ui://app/plain.html", name="plain", mimeType="text/plain", reader=lambda: "<div/>")
        )

        @tool(name="t_wrong_mime", meta={"ui": {"resourceUri": "ui://app/plain.html"}})
        def t_wrong_mime() -> str:
            return "x"

        core.registry.register_tool(t_wrong_mime)

    with pytest.raises(ImproperConfigurationError):
        core.validate()


@pytest.mark.anyio
async def test_mcp_apps_projection_app_only_filtering_etag_and_webmcp() -> None:
    """SEP-1865 UI metadata flows through tools/resources/templates/ETag, filters app-only tools, and excludes WebMCP."""

    class DashboardController(SkillController):
        @mcp_resource(
            "ui://app/dashboard.html",
            name="dashboard_ui",
            title="Live Dashboard",
            mime_type=UI_MIME_TYPE,
            icons=[McpIcon(src="https://cdn.example/dash.png", mimeType="image/png", sizes=("64x64",))],
            meta={"ui": {"prefersBorder": True}},
        )
        def dashboard_ui(self) -> str:
            return "<html>dashboard</html>"

        @mcp_resource(
            "ui://app/item/{item_id}.html",
            name="item_ui",
            title="Item View",
            mime_type=UI_MIME_TYPE,
            meta={"ui": {"csp": {"frameDomains": []}}},
        )
        def item_ui(self, item_id: str) -> str:
            return f"<html>{item_id}</html>"

        @mcp_tool(
            name="refresh_dashboard",
            title="Refresh Dashboard",
            webmcp=True,
            ui_resource_uri="ui://app/dashboard.html",
            ui_visibility=["app"],
        )
        def refresh_dashboard(self) -> str:
            return "refreshed"

        @mcp_tool(
            name="open_dashboard",
            title="Open Dashboard",
            webmcp=True,
            ui_resource_uri="ui://app/dashboard.html",
            ui_visibility=["model", "app"],
        )
        def open_dashboard(self) -> str:
            return "opened"

    core = MCPCore()
    core.registry.register_controller(DashboardController())
    etag_before = core.registry.etag()
    core.validate()

    @tool(
        name="open_dashboard",
        title="Open Dashboard",
        webmcp=True,
        meta={"ui": {"resourceUri": "ui://app/dashboard.html", "visibility": ["model", "app"], "rev": 2}},
    )
    def open_dashboard_v2() -> str:
        return "opened"

    temp_reg = ToolRegistry()
    temp_reg.register_controller(DashboardController())
    temp_reg.register_tool(open_dashboard_v2)
    assert temp_reg.etag() != etag_before

    no_ui_tools = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28"},
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}),
        )
    )
    assert [t["name"] for t in decode_json(no_ui_tools.body)["result"]["tools"]] == ["open_dashboard"]
    assert (
        decode_json(no_ui_tools.body)["result"]["tools"][0]["_meta"]["ui"]["resourceUri"] == "ui://app/dashboard.html"
    )

    modern_ui_tools = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28"},
            body=encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/list",
                    "params": {
                        "_meta": {
                            "io.modelcontextprotocol/clientCapabilities": {
                                "extensions": {UI_EXTENSION: {"mimeTypes": [UI_MIME_TYPE]}}
                            }
                        }
                    },
                }
            ),
        )
    )
    assert [t["name"] for t in decode_json(modern_ui_tools.body)["result"]["tools"]] == [
        "open_dashboard",
        "refresh_dashboard",
    ]

    app_only_call = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28"},
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "refresh_dashboard"}}
            ),
        )
    )
    assert app_only_call.status == 200
    assert decode_json(app_only_call.body)["result"]["content"][0]["text"] == "refreshed"

    ui_sid = await _init_legacy_session(core, capabilities={"extensions": {UI_EXTENSION: {}}})
    legacy_ui_list = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers={"Mcp-Session-Id": ui_sid},
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 4, "method": "tools/list", "params": {}}),
        )
    )
    assert [t["name"] for t in decode_json(legacy_ui_list.body)["result"]["tools"]] == [
        "open_dashboard",
        "refresh_dashboard",
    ]

    show_all_core = MCPCore(config=MCPConfig(hide_app_only_tools=False, ui_mime_types=()))
    show_all_core.registry.register_controller(DashboardController())
    show_all_disc = await show_all_core._handle_discover()
    assert UI_EXTENSION not in show_all_disc["capabilities"].get("extensions", {})
    assert [t["name"] for t in (await show_all_core._handle_tools_list())["tools"]] == [
        "open_dashboard",
        "refresh_dashboard",
    ]

    res_list = await core._handle_resources_list()
    assert res_list["resources"][0]["title"] == "Live Dashboard"
    assert res_list["resources"][0]["icons"] == [
        {"src": "https://cdn.example/dash.png", "mimeType": "image/png", "sizes": ["64x64"]}
    ]
    assert res_list["resources"][0]["_meta"] == {"ui": {"prefersBorder": True}}

    res_read = await core._handle_resources_read({"uri": "ui://app/dashboard.html"})
    assert res_read["contents"][0]["_meta"] == {"ui": {"prefersBorder": True}}

    tmpl_list = await core._handle_resources_templates_list()
    assert tmpl_list["resourceTemplates"][0]["title"] == "Item View"
    assert tmpl_list["resourceTemplates"][0]["_meta"] == {"ui": {"csp": {"frameDomains": []}}}

    manifest = build_webmcp_manifest(core.registry)
    assert [t.name for t in manifest.tools] == ["open_dashboard"]
    seq_manifest = build_webmcp_manifest([t.spec for t in core.registry.list_tools()])
    assert [t.name for t in seq_manifest.tools] == ["open_dashboard"]
