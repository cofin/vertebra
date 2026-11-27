"""Conformance tests for MCP protocol negotiation, guards, sessions, batching, and SSE streaming."""

from collections.abc import AsyncGenerator, AsyncIterator, Iterable
from types import SimpleNamespace
from typing import Any, cast

import anyio
import pytest
from django.conf import settings as django_settings
from django.http import StreamingHttpResponse
from litestar import Litestar
from litestar.response import Stream as LitestarStream
from litestar.routes import HTTPRoute
from starlette.requests import Request as StarletteRequest
from starlette.responses import StreamingResponse

import vertebra.agent.mcp as mcp_pkg
import vertebra.agent.mcp.service as mcp_service_mod
from vertebra.agent.core.context import AgentContext, CallerIdentity
from vertebra.agent.core.guards import (
    BudgetExceededError,
    RateLimitExceededError,
    RequireRoles,
    TokenBucketGuard,
)
from vertebra.agent.core.jsonrpc import (
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
)
from vertebra.agent.core.schema import tool
from vertebra.agent.core.sse import SseFrame, StreamLimitExceededError
from vertebra.agent.mcp import (
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
    MCPConfig,
    MCPCore,
    MCPHttpRequest,
    MCPHttpResponse,
    MCPSessionStore,
    PromptArgument,
    PromptEntry,
    PromptMessage,
    ResourceEntry,
    SessionLimitExceededError,
    TextContent,
    ToolRegistry,
    encode_mcp_header_value,
)
from vertebra.agent.mcp.routes import AsgiMCPApp, WsgiMCPApp
from vertebra.agent.plugin import AgentPlugin, AgentPluginConfig
from vertebra.agent.runtime import ConfirmationGate
from vertebra.extensions.django.mcp import dispatch_mcp_request as dispatch_django_mcp
from vertebra.framework._app import App
from vertebra.utils.module_loader import MissingDependencyError
from vertebra.utils.serializers import decode_json, encode_json_bytes


async def _aclose(stream: Any) -> None:
    """Close an async SSE stream iterator."""
    await stream.aclose()


def test_mcp_flask_missing_refuses_fabricated_request(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unavailable Flask dependency raises before dispatching an invented request."""
    monkeypatch.setattr("vertebra.agent.mcp.routes.import_optional", lambda name: None)
    with pytest.raises(MissingDependencyError):
        WsgiMCPApp(MCPCore()).handle_flask_view()


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

    await AsgiMCPApp(core)(scope, receive, send)
    result = decode_json(messages[-1]["body"])
    assert ("result" in result) is authenticated
    assert calls == (["ran"] if authenticated else [])


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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
            method="POST",
            path="/mcp",
            body=encode_json_bytes(
                {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-11-25"}}
            ),
        )
    )
    session_id = init_resp.headers["Mcp-Session-Id"]

    set_resp = await core.handle(
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
            method="POST",
            path="/mcp",
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
        )
    )
    assert first.status == 200
    full = await core.handle(
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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
        MCPHttpRequest(
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

    options_resp = await core.handle(MCPHttpRequest(method="OPTIONS", path="/mcp"))
    assert options_resp.status == 204
    assert options_resp.headers["Allow"] == "GET, POST, DELETE, OPTIONS"
    assert options_resp.headers["Access-Control-Allow-Methods"] == "GET, POST, DELETE, OPTIONS"

    recv_stream, _ = core.stream_broker.subscribe(sid)

    wrong_owner_del = await core.handle(
        MCPHttpRequest(method="DELETE", path="/mcp", context=bob, headers={"Mcp-Session-Id": sid})
    )
    assert wrong_owner_del.status == 404
    assert await core.session_store.get(sid, "t1:alice") is not None

    del_resp = await core.handle(
        MCPHttpRequest(method="DELETE", path="/mcp", context=alice, headers={"Mcp-Session-Id": sid})
    )
    assert del_resp.status == 204
    assert await core.session_store.get(sid, "t1:alice") is None

    with pytest.raises(anyio.EndOfStream):
        await recv_stream.receive()

    post_del = await core.handle(
        MCPHttpRequest(
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
        MCPHttpRequest(
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
    resp = await core.handle(MCPHttpRequest(method="GET", path="/mcp", headers=headers))
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
        MCPHttpRequest(
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
            MCPHttpRequest(
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
        MCPHttpRequest(
            method="GET",
            path="/mcp",
            context=alice,
            headers={"Accept": "text/event-stream", "Mcp-Session-Id": sid},
        )
    )
    assert ninth.status == 503
    assert decode_json(ninth.body)["error"]["code"] == SESSION_CAPACITY_EXHAUSTED
    assert isinstance(StreamLimitExceededError("limit"), StreamLimitExceededError)

    await _aclose(extra_streams.pop())
    replacement = await core.handle(
        MCPHttpRequest(
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
        await _aclose(stream)
    assert core.stream_broker.owner_stream_count("t1:alice") == 0


@pytest.mark.anyio
async def test_mcp_adapters_stream_incrementally_and_close_on_disconnect() -> None:
    """ASGI, Starlette, WSGI, Flask, Django, native App, and Litestar stream chunks and close streams."""
    if not django_settings.configured:
        django_settings.configure()

    core = MCPCore(config=MCPConfig(sse_heartbeat_seconds=5.0))
    sid = await _init_legacy_session(core)

    def _wrap_tracking_stream(inner: AsyncIterator[bytes]) -> tuple[AsyncIterator[bytes], list[bool]]:
        closed_flag = [False]

        class _TrackingStream(AsyncIterator[bytes]):
            def __aiter__(self) -> "_TrackingStream":
                return self

            async def __anext__(self) -> bytes:
                return await inner.__anext__()

            async def aclose(self) -> None:
                closed_flag[0] = True
                await _aclose(inner)

        return _TrackingStream(), closed_flag

    def _open_session_stream() -> tuple[Any, list[bool]]:
        inner = core.stream_broker.open_stream(sid, owner_id=None, heartbeat_seconds=5.0, max_streams_per_owner=8)
        return _wrap_tracking_stream(inner)

    asgi_app = AsgiMCPApp(core)
    asgi_stream, asgi_closed = _open_session_stream()
    original_handle = core.handle

    async def _asgi_handle(req: MCPHttpRequest) -> MCPHttpResponse:
        if req.method == "GET":
            return MCPHttpResponse(
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

    async def _starlette_handle(req: MCPHttpRequest) -> MCPHttpResponse:
        if req.method == "GET":
            return MCPHttpResponse(
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
    star_iter = cast("AsyncGenerator[bytes, None]", star_resp.body_iterator)
    core.stream_broker.publish(sid, SseFrame(id="star-1", event="message", data="star-chunk"))
    assert b"star-chunk" in await anext(star_iter)
    await star_iter.aclose()
    assert starlette_closed[0] is True

    cast(Any, core).handle = original_handle

    wsgi_stream, wsgi_closed = _open_session_stream()

    async def _wsgi_handle(req: MCPHttpRequest) -> MCPHttpResponse:
        if req.method == "GET":
            return MCPHttpResponse(
                status=200,
                headers={"Content-Type": "text/event-stream"},
                body=b"",
                stream=wsgi_stream,
            )
        return await original_handle(req)

    cast(Any, core).handle = _wsgi_handle
    wsgi_app = WsgiMCPApp(core)
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

    async def _flask_handle(req: MCPHttpRequest) -> MCPHttpResponse:
        if req.method == "GET":
            return MCPHttpResponse(
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
    cast(Any, django_reg)._mcp_app = WsgiMCPApp(django_core)
    dj_inner = django_core.stream_broker.open_stream(dj_sid, owner_id=None, heartbeat_seconds=5.0)
    dj_tracking, django_stream_closed = _wrap_tracking_stream(dj_inner)

    orig_dj_handle = django_core.handle

    async def _dj_handle(req: MCPHttpRequest) -> MCPHttpResponse:
        if req.method == "GET":
            return MCPHttpResponse(
                status=200,
                headers={"Content-Type": "text/event-stream"},
                body=b"",
                stream=dj_tracking,
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
    dj_iter = iter(cast("Iterable[bytes]", dj_resp.streaming_content))
    dj_chunk = await anyio.to_thread.run_sync(lambda: next(dj_iter))
    assert b"dj-chunk" in dj_chunk
    dj_resp.close()
    assert django_stream_closed[0] is True

    native_plugin = AgentPlugin(AgentPluginConfig(mcp=MCPConfig(server_name="native-sse")))
    native_app = App()
    native_plugin.mount_native(native_app)
    native_routes = {(r.method.upper(), r.path): r.handler for r in native_app.routes}
    assert {("POST", "/mcp"), ("GET", "/mcp"), ("DELETE", "/mcp"), ("OPTIONS", "/mcp")}.issubset(native_routes)

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
    await _aclose(native_body)

    opt_status, opt_hdrs, _ = await native_routes[("OPTIONS", "/mcp")](headers={})
    assert opt_status == 204
    assert "DELETE" in opt_hdrs["Allow"]

    litestar_plugin = AgentPlugin(AgentPluginConfig(mcp=MCPConfig(server_name="ls-sse")))
    litestar_app = Litestar(route_handlers=[], plugins=[litestar_plugin])
    ls_service = litestar_plugin.mcp_service
    assert ls_service is not None
    ls_sid = await _init_legacy_session(ls_service)
    ls_inner = ls_service.stream_broker.open_stream(ls_sid, owner_id=None, heartbeat_seconds=5.0)
    ls_tracking, ls_stream_closed = _wrap_tracking_stream(ls_inner)

    async def _ls_stream_handle(req: MCPHttpRequest) -> MCPHttpResponse:
        if req.method == "GET":
            return MCPHttpResponse(
                status=200,
                headers={"Content-Type": "text/event-stream"},
                body=b"",
                stream=ls_tracking,
            )
        return MCPHttpResponse(status=204, headers={"Allow": "GET, POST, DELETE, OPTIONS"}, body=b"")

    cast(Any, ls_service).handle = _ls_stream_handle
    mcp_route = next(h for h in litestar_app.routes if h.path == "/mcp")
    assert isinstance(mcp_route, HTTPRoute)
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
    ls_iter = cast("AsyncGenerator[bytes, None]", ls_result.iterator)
    ls_service.stream_broker.publish(ls_sid, SseFrame(id="ls-1", event="message", data="ls-chunk"))
    assert b"ls-chunk" in await anext(ls_iter)
    await ls_iter.aclose()
    assert ls_stream_closed[0] is True


class AsyncMockBytes:
    """Async callable returning empty bytes for request.body()."""

    async def __call__(self) -> bytes:
        return b""
