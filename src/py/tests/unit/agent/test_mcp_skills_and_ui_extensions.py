"""Conformance tests for MCP notifications, resource subscriptions, skills, UI extensions, completions, and cache."""

import base64
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from flask import Flask
from litestar import Litestar
from litestar.testing import TestClient as LitestarTestClient
from starlette.applications import Starlette
from starlette.testclient import TestClient as StarletteTestClient

from vertebra._vertebra import Request as NativeRequest
from vertebra.agent.core.context import AgentContext, CallerIdentity
from vertebra.agent.core.jsonrpc import (
    INVALID_PARAMS,
    METHOD_NOT_FOUND,
)
from vertebra.agent.core.schema import tool
from vertebra.agent.core.sse import CacheSseStreamBroker, SseFrame
from vertebra.agent.mcp import (
    PERMISSION_DENIED,
    SESSION_CAPACITY_EXHAUSTED,
    SKILLS_EXTENSION,
    UI_EXTENSION,
    UI_MIME_TYPE,
    MCPConfig,
    MCPCore,
    McpHttpRequest,
    McpHttpResponse,
    McpIcon,
    MCPRegistry,
    MCPSessionStore,
    MCPSkillsConfig,
    MCPTaskStore,
    PromptEntry,
    PromptMessage,
    ResourceEntry,
    ResourceTemplateEntry,
    Skill,
    SkillAccessError,
    SkillCatalog,
    SkillController,
    TaskLookupError,
    TextContent,
    ToolRegistry,
    build_webmcp_manifest,
    mcp_resource,
    mcp_tool,
)
from vertebra.agent.mcp.routes import build_well_known_mcp_document
from vertebra.agent.mcp.sessions import CacheMCPSessionStore
from vertebra.agent.mcp.tasks import CacheMCPTaskStore
from vertebra.agent.plugin import AgentPlugin, AgentPluginConfig
from vertebra.cache import AsyncCacheStore, SyncCacheStore
from vertebra.extensions.django.mcp import attach_mcp_routes as attach_django_mcp
from vertebra.extensions.django.mcp import dispatch_mcp_request as dispatch_django_mcp
from vertebra.extensions.fastapi.mcp import mount_mcp_routes as mount_fastapi_mcp
from vertebra.extensions.flask.mcp import attach_mcp_routes as attach_flask_mcp
from vertebra.extensions.starlette.mcp import mount_mcp_routes as mount_starlette_mcp
from vertebra.framework._app import App
from vertebra.framework.queues.base import QueueBackendSettings, QueueService
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json, encode_json_bytes


async def _post_mcp(
    core: MCPCore,
    payload: dict[str, Any],
    *,
    headers: dict[str, str] | None = None,
    context: AgentContext | None = None,
) -> McpHttpResponse:
    return await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            headers=headers or {},
            body=encode_json_bytes(payload),
            context=context,
        )
    )


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

    set_level_resp = await _post_mcp(
        core,
        {"jsonrpc": "2.0", "id": 10, "method": "logging/setLevel", "params": {"level": "error"}},
        headers={"Mcp-Session-Id": sid},
    )
    assert set_level_resp.status == 200
    assert await core.log("warning", "suppressed-after-setlevel") == 0
    assert await core.log("error", "delivered-error") == 1
    assert _decode_sse_json(await anext(sse_resp.stream))["params"]["data"] == "delivered-error"

    legacy_prog_resp = await _post_mcp(
        core,
        {
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {"name": "slow_tool", "_meta": {"progressToken": "tok-sess"}},
        },
        headers={"Mcp-Session-Id": sid, "Accept": "application/json"},
    )
    assert legacy_prog_resp.status == 200
    assert decode_json(legacy_prog_resp.body)["result"]["content"][0]["text"] == "finished"
    p1 = _decode_sse_json(await anext(sse_resp.stream))
    p2 = _decode_sse_json(await anext(sse_resp.stream))
    assert p1["method"] == "notifications/progress"
    assert p1["params"] == {"progressToken": "tok-sess", "progress": 1, "total": 2, "message": "halfway"}
    assert p2["params"]["progress"] == 2
    await _aclose(sse_resp.stream)

    post_sse_resp = await _post_mcp(
        core,
        {
            "jsonrpc": "2.0",
            "id": 12,
            "method": "tools/call",
            "params": {"name": "slow_tool", "_meta": {"progressToken": "tok-post"}},
        },
        headers={"MCP-Protocol-Version": "2026-07-28", "Accept": "application/json, text/event-stream"},
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
    await _aclose(disabled_stream_resp.stream)


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

    async def _post_alice(req_id: int, method: str, params: dict[str, Any], sess_id: str = sid) -> McpHttpResponse:
        return await core.handle(
            McpHttpRequest(
                method="POST",
                path="/mcp",
                context=alice,
                headers={"Mcp-Session-Id": sess_id},
                body=encode_json_bytes({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}),
            )
        )

    for valid_uri in ("doc://allowed", "item://42", "skill://guide/SKILL.md"):
        sub_resp = await _post_alice(2, "resources/subscribe", {"uri": valid_uri})
        assert sub_resp.status == 200
        assert "error" not in decode_json(sub_resp.body)

    missing_resp = await _post_alice(3, "resources/subscribe", {"uri": "doc://missing"})
    assert decode_json(missing_resp.body)["error"]["code"] == INVALID_PARAMS

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

    await _aclose(listen_resp.stream)
    assert not core._listen_registrations

    unsub_resp = await _post_alice(7, "resources/unsubscribe", {"uri": "item://42"})
    assert decode_json(unsub_resp.body)["result"] == {}
    unsub_again = await _post_alice(8, "resources/unsubscribe", {"uri": "item://42"})
    assert decode_json(unsub_again.body)["result"] == {}
    assert await core.notify_resource_updated("item://42") == 0

    del_resp = await core.handle(
        McpHttpRequest(method="DELETE", path="/mcp", context=alice, headers={"Mcp-Session-Id": sid})
    )
    assert del_resp.status == 204
    await _aclose(get_resp.stream)
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
    await _post_alice(9, "resources/subscribe", {"uri": "doc://allowed"}, sess_id=sid_ttl)
    now_mono[0] = 120.0
    assert await core.notify_resource_updated("doc://allowed") == 0
    await _aclose(ttl_stream_resp.stream)


@pytest.mark.anyio
@pytest.mark.parametrize("denied_uri", ["doc://forbidden", "doc://explode"])
async def test_mcp_resource_subscribe_denied_uris(denied_uri: str) -> None:
    """Resource subscription enforces resource authorization and fails closed on authorizer errors."""
    alice = AgentContext(identity=CallerIdentity(user_id="alice"))

    async def _authorizer(uri: str, ctx: AgentContext | None) -> bool:
        _ = ctx
        if uri == "doc://forbidden":
            return False
        if uri == "doc://explode":
            raise RuntimeError("auth backend failure")
        return True

    core = MCPCore(config=MCPConfig(subscription_authorizer=_authorizer))
    core.registry.register_resource(ResourceEntry(uri="doc://forbidden", name="forb", reader=lambda: "secret"))
    core.registry.register_resource(ResourceEntry(uri="doc://explode", name="boom", reader=lambda: "boom"))
    sid = await _init_legacy_session(core, context=alice)
    denied_resp = await _post_mcp(
        core,
        {"jsonrpc": "2.0", "id": 4, "method": "resources/subscribe", "params": {"uri": denied_uri}},
        headers={"Mcp-Session-Id": sid},
        context=alice,
    )
    assert decode_json(denied_resp.body)["error"]["code"] == PERMISSION_DENIED


@pytest.mark.anyio
async def test_mcp_resource_subscription_limits_and_disabled_flag() -> None:
    """Enforce max_subscriptions_per_session and resource_subscriptions=False gating."""
    limited_core = MCPCore(config=MCPConfig(max_subscriptions_per_session=2))
    for uri in ("doc://1", "doc://2", "doc://3"):
        limited_core.registry.register_resource(ResourceEntry(uri=uri, name=uri, reader=lambda val=uri: val))
    sid = await _init_legacy_session(limited_core)

    for uri in ("doc://1", "doc://2"):
        resp = await _post_mcp(
            limited_core,
            {"jsonrpc": "2.0", "id": 1, "method": "resources/subscribe", "params": {"uri": uri}},
            headers={"Mcp-Session-Id": sid},
        )
        assert "error" not in decode_json(resp.body)

    over_limit = await _post_mcp(
        limited_core,
        {"jsonrpc": "2.0", "id": 2, "method": "resources/subscribe", "params": {"uri": "doc://3"}},
        headers={"Mcp-Session-Id": sid},
    )
    assert decode_json(over_limit.body)["error"]["code"] == SESSION_CAPACITY_EXHAUSTED

    disabled_core = MCPCore(config=MCPConfig(resource_subscriptions=False))
    disabled_core.registry.register_resource(ResourceEntry(uri="doc://1", name="doc1", reader=lambda: "1"))
    disc = await _post_mcp(
        disabled_core,
        {"jsonrpc": "2.0", "id": 3, "method": "server/discover"},
        headers={"MCP-Protocol-Version": "2026-07-28"},
    )
    assert decode_json(disc.body)["result"]["capabilities"]["resources"]["subscribe"] is False


@pytest.mark.parametrize("method", ("resources/subscribe", "resources/unsubscribe"))
@pytest.mark.anyio
async def test_mcp_resource_subscriptions_disabled_rejects_methods(method: str) -> None:
    """Reject subscription methods with -32601 when resource_subscriptions=False."""
    disabled_core = MCPCore(config=MCPConfig(resource_subscriptions=False))
    disabled_core.registry.register_resource(ResourceEntry(uri="doc://1", name="doc1", reader=lambda: "1"))
    disabled_sid = await _init_legacy_session(disabled_core)
    err_resp = await _post_mcp(
        disabled_core,
        {"jsonrpc": "2.0", "id": 4, "method": method, "params": {"uri": "doc://1"}},
        headers={"Mcp-Session-Id": disabled_sid},
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
        await _aclose(sse_resp.stream)

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

    no_ui_tools = await _post_mcp(
        core,
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
        headers={"MCP-Protocol-Version": "2026-07-28"},
    )
    assert [t["name"] for t in decode_json(no_ui_tools.body)["result"]["tools"]] == ["open_dashboard"]
    assert (
        decode_json(no_ui_tools.body)["result"]["tools"][0]["_meta"]["ui"]["resourceUri"] == "ui://app/dashboard.html"
    )

    modern_ui_tools = await _post_mcp(
        core,
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
        },
        headers={"MCP-Protocol-Version": "2026-07-28"},
    )
    assert [t["name"] for t in decode_json(modern_ui_tools.body)["result"]["tools"]] == [
        "open_dashboard",
        "refresh_dashboard",
    ]

    app_only_call = await _post_mcp(
        core,
        {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "refresh_dashboard"}},
        headers={"MCP-Protocol-Version": "2026-07-28"},
    )
    assert app_only_call.status == 200
    assert decode_json(app_only_call.body)["result"]["content"][0]["text"] == "refreshed"

    ui_sid = await _init_legacy_session(core, capabilities={"extensions": {UI_EXTENSION: {}}})
    legacy_ui_list = await _post_mcp(
        core,
        {"jsonrpc": "2.0", "id": 4, "method": "tools/list", "params": {}},
        headers={"Mcp-Session-Id": ui_sid},
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


@pytest.mark.parametrize(
    ("skill_names", "expected"),
    [
        (None, []),
        ([], []),
        (["zeta", "alpha", "zeta"], ["alpha", "zeta"]),
    ],
)
def test_build_well_known_mcp_document_emits_sorted_skills(
    skill_names: list[str] | None,
    expected: list[str],
) -> None:
    """Verify build_well_known_mcp_document always includes deduplicated sorted skills."""
    doc = build_well_known_mcp_document(MCPConfig(server_name="wk-test"), skill_names=skill_names)
    assert doc["skills"] == expected


def _build_demo_skills_config(tmp_path: Path) -> MCPSkillsConfig:
    skill_dir = tmp_path / "triage"
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text("---\ntitle: Triage Skill\n---\n# Triage\nRun triage.\n", encoding="utf-8")
    return MCPSkillsConfig(paths=(tmp_path,))


@pytest.mark.anyio
async def test_agent_plugin_native_and_litestar_skills_and_ui_validation(tmp_path: Path) -> None:
    """Verify AgentPlugin wires SkillCatalog, well-known skills, and mount-time ui:// validation."""
    skills_cfg = _build_demo_skills_config(tmp_path)

    native_app = App()

    @mcp_tool(name="ping_native")
    def ping_native() -> str:
        return "pong"

    native_plugin = AgentPlugin(AgentPluginConfig(skills=skills_cfg))
    native_plugin.mcp_registry.register_tool(ping_native)
    native_plugin.mount_native(native_app)
    assert native_plugin.mcp_service is not None
    native_skills = await native_plugin.mcp_service._handle_skills_list()
    assert [s["name"] for s in native_skills["skills"]] == ["triage"]

    broken_native_app = App()

    @mcp_tool(name="broken_native", ui_resource_uri="ui://missing/view.html")
    def broken_native() -> str:
        return "fail"

    broken_native_plugin = AgentPlugin(AgentPluginConfig(skills=skills_cfg))
    broken_native_plugin.mcp_registry.register_tool(broken_native)
    with pytest.raises(ImproperConfigurationError, match="ui://missing/view.html"):
        broken_native_plugin.mount_native(broken_native_app)

    ls_plugin = AgentPlugin(AgentPluginConfig(skills=skills_cfg))
    ls_app = Litestar(route_handlers=[], plugins=[ls_plugin])
    with LitestarTestClient(app=ls_app) as client:
        wk_resp = client.get("/.well-known/mcp.json")
        assert wk_resp.status_code == 200
        assert wk_resp.json()["skills"] == ["triage"]

        skills_resp = client.post(
            "/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "skills/list"},
            json={"jsonrpc": "2.0", "id": 1, "method": "skills/list", "params": {}},
        )
        assert skills_resp.status_code == 200
        assert [s["name"] for s in skills_resp.json()["result"]["skills"]] == ["triage"]

    broken_ls_plugin = AgentPlugin(AgentPluginConfig(skills=skills_cfg))

    @mcp_tool(name="broken_ls", ui_resource_uri="ui://missing/ls.html")
    def broken_ls() -> str:
        return "fail"

    broken_ls_plugin.mcp_registry.register_tool(broken_ls)
    with pytest.raises(ImproperConfigurationError, match="ui://missing/ls.html"):
        Litestar(route_handlers=[], plugins=[broken_ls_plugin])


@pytest.mark.parametrize("use_fastapi", [False, True])
def test_starlette_and_fastapi_mount_skills_well_known_and_ui_validation(tmp_path: Path, use_fastapi: bool) -> None:
    """Verify Starlette and FastAPI mount_mcp_routes wire SkillCatalog, well-known skills, and UI validation."""
    skills_cfg = _build_demo_skills_config(tmp_path)
    app = FastAPI() if use_fastapi else Starlette()
    mount_fn = mount_fastapi_mcp if use_fastapi else mount_starlette_mcp
    mount_fn(app, skills=skills_cfg)

    with StarletteTestClient(app) as client:
        wk_resp = client.get("/.well-known/mcp.json")
        assert wk_resp.status_code == 200
        assert wk_resp.json()["skills"] == ["triage"]

        skills_resp = client.post(
            "/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "skills/list"},
            json={"jsonrpc": "2.0", "id": 1, "method": "skills/list", "params": {}},
        )
        assert skills_resp.status_code == 200
        assert [s["name"] for s in skills_resp.json()["result"]["skills"]] == ["triage"]

    bad_app = FastAPI() if use_fastapi else Starlette()
    bad_registry = MCPRegistry()

    @mcp_tool(name="broken_asgi", ui_resource_uri="ui://missing/asgi.html")
    def broken_asgi() -> str:
        return "fail"

    bad_registry.register_tool(broken_asgi)
    with pytest.raises(ImproperConfigurationError, match="ui://missing/asgi.html"):
        mount_fn(bad_app, registry=bad_registry, skills=skills_cfg)


def test_flask_and_django_mount_skills_well_known_and_ui_validation(tmp_path: Path) -> None:
    """Verify Flask and Django mounts expose skills, emit well-known skill lists, and validate ui:// links."""
    skills_cfg = _build_demo_skills_config(tmp_path)

    flask_app = Flask(__name__)
    attach_flask_mcp(flask_app, skills=skills_cfg)
    with flask_app.test_client() as client:
        wk_resp = client.get("/.well-known/mcp.json")
        assert wk_resp.status_code == 200
        assert decode_json(wk_resp.data)["skills"] == ["triage"]

        skills_resp = client.post(
            "/mcp",
            headers={"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "skills/list"},
            data=encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "skills/list", "params": {}}),
            content_type="application/json",
        )
        assert skills_resp.status_code == 200
        assert [s["name"] for s in decode_json(skills_resp.data)["result"]["skills"]] == ["triage"]

    bad_flask_app = Flask(__name__)

    @mcp_tool(name="broken_flask", ui_resource_uri="ui://missing/flask.html")
    def broken_flask() -> str:
        return "fail"

    bad_flask_app.view_functions["broken_flask"] = broken_flask
    with pytest.raises(ImproperConfigurationError, match="ui://missing/flask.html"):
        attach_flask_mcp(bad_flask_app, skills=skills_cfg)

    dj_registry, dj_static = attach_django_mcp([], skills=skills_cfg)
    assert decode_json(dj_static["/.well-known/mcp.json"].body)["skills"] == ["triage"]
    dj_resp = dispatch_django_mcp(
        dj_registry,
        NativeRequest.from_parts(
            "POST",
            "/mcp",
            headers=[("mcp-protocol-version", "2026-07-28"), ("content-type", "application/json")],
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 1, "method": "skills/list", "params": {}}),
        ),
    )
    assert dj_resp.status == 200
    assert [s["name"] for s in decode_json(bytes(dj_resp.body))["result"]["skills"]] == ["triage"]

    standalone_dj_reg = MCPRegistry()
    direct_resp = dispatch_django_mcp(
        standalone_dj_reg,
        NativeRequest.from_parts(
            "POST",
            "/mcp",
            headers=[("mcp-protocol-version", "2026-07-28"), ("content-type", "application/json")],
            body=encode_json_bytes({"jsonrpc": "2.0", "id": 2, "method": "skills/list", "params": {}}),
        ),
        skills=skills_cfg,
    )
    assert direct_resp.status == 200
    assert [s["name"] for s in decode_json(bytes(direct_resp.body))["result"]["skills"]] == ["triage"]

    @mcp_tool(name="broken_django", ui_resource_uri="ui://missing/django.html")
    def broken_django() -> str:
        return "fail"

    with pytest.raises(ImproperConfigurationError, match="ui://missing/django.html"):
        attach_django_mcp([broken_django], skills=skills_cfg)

    bad_standalone_dj_reg = MCPRegistry()
    bad_standalone_dj_reg.register_tool(broken_django)
    with pytest.raises(ImproperConfigurationError, match="ui://missing/django.html"):
        dispatch_django_mcp(bad_standalone_dj_reg, b"{}", skills=skills_cfg)


async def _aclose(stream: Any) -> None:
    """Close an async SSE stream iterator."""
    await stream.aclose()


@pytest.mark.anyio
async def test_mcp_completions_capability_sync_async_pagination_and_unknown_ref() -> None:
    """Verify completions capability discovery, sync/async providers, 100-item truncation, and unknown refs."""
    registry = MCPRegistry()

    def _sync_prompt_complete(arg_name: str, arg_value: str) -> list[str]:
        assert arg_name == "language"
        return [item for item in ("python", "perl", "pascal") if item.startswith(arg_value)]

    async def _async_resource_complete(arg_name: str, arg_value: str) -> dict[str, Any]:
        assert arg_name == "path"
        return {"values": [f"{arg_value}/a.txt", f"{arg_value}/b.txt"], "total": 2, "hasMore": False}

    def _large_complete(arg_name: str, arg_value: str) -> list[str]:
        _ = (arg_name, arg_value)
        return [f"item-{idx}" for idx in range(150)]

    registry.register_completion("review_prompt", _sync_prompt_complete)
    registry.register_completion("file:///{path}", _async_resource_complete)
    registry.register_completion("large_prompt", _large_complete)

    core = MCPCore(registry=registry)

    discover_resp = await _post_mcp(
        core,
        {"jsonrpc": "2.0", "id": 1, "method": "server/discover", "params": {}},
        headers={"MCP-Protocol-Version": "2026-07-28"},
    )
    assert decode_json(discover_resp.body)["result"]["capabilities"]["completions"] == {}

    sync_resp = await _post_mcp(
        core,
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "completion/complete",
            "params": {
                "ref": {"type": "ref/prompt", "name": "review_prompt"},
                "argument": {"name": "language", "value": "p"},
            },
        },
        headers={"MCP-Protocol-Version": "2026-07-28"},
    )
    sync_comp = decode_json(sync_resp.body)["result"]["completion"]
    assert sync_comp == {"values": ["python", "perl", "pascal"], "total": 3, "hasMore": False}

    async_resp = await _post_mcp(
        core,
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "completion/complete",
            "params": {
                "ref": {"type": "ref/resource", "uri": "file:///{path}"},
                "argument": {"name": "path", "value": "docs"},
            },
        },
        headers={"MCP-Protocol-Version": "2026-07-28"},
    )
    async_comp = decode_json(async_resp.body)["result"]["completion"]
    assert async_comp == {"values": ["docs/a.txt", "docs/b.txt"], "total": 2, "hasMore": False}

    large_resp = await _post_mcp(
        core,
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "completion/complete",
            "params": {
                "ref": {"type": "ref/prompt", "name": "large_prompt"},
                "argument": {"name": "item", "value": ""},
            },
        },
        headers={"MCP-Protocol-Version": "2026-07-28"},
    )
    large_comp = decode_json(large_resp.body)["result"]["completion"]
    assert len(large_comp["values"]) == 100
    assert large_comp["total"] == 150
    assert large_comp["hasMore"] is True

    missing_resp = await _post_mcp(
        core,
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "completion/complete",
            "params": {
                "ref": {"type": "ref/prompt", "name": "unknown_prompt"},
                "argument": {"name": "x", "value": "y"},
            },
        },
        headers={"MCP-Protocol-Version": "2026-07-28"},
    )
    assert decode_json(missing_resp.body)["error"]["code"] == INVALID_PARAMS


@pytest.mark.anyio
async def test_mcp_task_store_ttl_pruning_and_queue_service_dispatch_cancellation() -> None:
    """Verify MCPTaskStore injected-clock TTL pruning and QueueService task dispatch/cancellation."""
    now = [1000.0]
    qs = QueueService(QueueBackendSettings(kind="memory"))
    store = MCPTaskStore(queue_service=qs, _clock=lambda: now[0])

    short_task = store.create(tool_name="ephemeral", owner_id="t1:u1", ttl_ms=500)
    long_task = store.create(tool_name="durable", owner_id="t1:u1", ttl_ms=5000)

    now[0] = 1000.5
    assert store.prune_expired() == 1
    with pytest.raises(TaskLookupError):
        store.get(short_task.task_id, owner_id="t1:u1")
    remaining, _ = store.list_for_owner("t1:u1")
    assert [t.task_id for t in remaining] == [long_task.task_id]

    core = MCPCore(task_store=store)
    executed: list[str] = []

    @tool(name="queued_job", description="Run via QueueService")
    async def queued_job(label: str) -> str:
        executed.append(label)
        return f"done:{label}"

    core.registry.register_tool(queued_job)
    ctx = AgentContext(identity=CallerIdentity(user_id="u1", tenant_id="t1"))

    run_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=ctx,
            headers={"MCP-Protocol-Version": "2026-07-28"},
            body=encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "tools/call",
                    "params": {
                        "name": "queued_job",
                        "arguments": {"label": "alpha"},
                        "_meta": {"io.modelcontextprotocol/task": True},
                    },
                }
            ),
        )
    )
    task_id = decode_json(run_resp.body)["result"]["task"]["taskId"]
    assert store.get(task_id, "t1:u1").status == "working"
    assert executed == []

    await qs.drain_once()
    completed = store.get(task_id, "t1:u1")
    assert completed.status == "completed"
    assert completed.result is not None
    assert completed.result["content"][0]["text"] == "done:alpha"
    assert executed == ["alpha"]

    cancel_resp = await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=ctx,
            headers={"MCP-Protocol-Version": "2026-07-28"},
            body=encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": 11,
                    "method": "tools/call",
                    "params": {
                        "name": "queued_job",
                        "arguments": {"label": "beta"},
                        "_meta": {"io.modelcontextprotocol/task": True},
                    },
                }
            ),
        )
    )
    cancelled_id = decode_json(cancel_resp.body)["result"]["task"]["taskId"]
    await core.handle(
        McpHttpRequest(
            method="POST",
            path="/mcp",
            context=ctx,
            headers={"MCP-Protocol-Version": "2026-07-28"},
            body=encode_json_bytes(
                {
                    "jsonrpc": "2.0",
                    "id": 12,
                    "method": "tasks/cancel",
                    "params": {"taskId": cancelled_id, "reason": "abort before worker"},
                }
            ),
        )
    )
    await qs.drain_once()
    assert store.get(cancelled_id, "t1:u1").status == "cancelled"
    assert executed == ["alpha"]


@pytest.mark.anyio
@pytest.mark.parametrize("bad_url", ["sqlite:///local.db", "postgresql://user:pass@localhost/db", "not-a-url"])
async def test_mcp_config_rejects_invalid_and_sql_cache_urls(bad_url: str) -> None:
    """Verify MCPConfig raises ValueError for unsupported or SQL cache URL schemes."""
    with pytest.raises(ValueError, match="Unsupported cache URL scheme"):
        MCPConfig(cache_url=bad_url)


@pytest.mark.anyio
async def test_mcp_distributed_cache_stores_cross_worker_and_aclose() -> None:
    """Verify CacheMCPSessionStore, CacheMCPTaskStore, and CacheSseStreamBroker across two MCPCore workers."""
    shared_async = AsyncCacheStore("memory://")
    shared_sync = SyncCacheStore("memory://")

    worker1 = MCPCore(
        session_store=CacheMCPSessionStore(shared_async, default_ttl=600.0),
        task_store=CacheMCPTaskStore(shared_sync),
        stream_broker=CacheSseStreamBroker(shared_sync),
    )
    worker2 = MCPCore(
        session_store=CacheMCPSessionStore(shared_async, default_ttl=600.0),
        task_store=CacheMCPTaskStore(shared_sync),
        stream_broker=CacheSseStreamBroker(shared_sync),
    )

    alice = AgentContext(identity=CallerIdentity(user_id="alice", tenant_id="t1"))
    bob = AgentContext(identity=CallerIdentity(user_id="bob", tenant_id="t2"))

    init_resp = await _post_mcp(
        worker1, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}, context=alice
    )
    assert init_resp.status == 200
    sid = init_resp.headers["Mcp-Session-Id"]

    ping_w2 = await _post_mcp(
        worker2,
        {"jsonrpc": "2.0", "id": 2, "method": "ping", "params": {}},
        headers={"Mcp-Session-Id": sid, "MCP-Protocol-Version": "2025-11-25"},
        context=alice,
    )
    assert ping_w2.status == 200
    assert decode_json(ping_w2.body)["result"] == {}

    cross_tenant_ping = await _post_mcp(
        worker2,
        {"jsonrpc": "2.0", "id": 3, "method": "ping", "params": {}},
        headers={"Mcp-Session-Id": sid, "MCP-Protocol-Version": "2025-11-25"},
        context=bob,
    )
    assert cross_tenant_ping.status == 404

    task = worker1.task_store.create("report", owner_id="t1:alice")
    worker1.task_store.complete(task.task_id, {"content": [{"type": "text", "text": "ok"}], "isError": False})
    fetched_w2 = worker2.task_store.get(task.task_id, "t1:alice")
    assert fetched_w2.status == "completed"
    regressed = worker2.task_store.cancel(task.task_id, "t1:alice", reason="late cancel")
    assert regressed.status == "completed"
    with pytest.raises(TaskLookupError):
        worker2.task_store.get(task.task_id, "t2:bob")

    worker1.stream_broker.publish(sid, SseFrame(id="evt-1", event="message", data='{"seq":1}'))
    worker1.stream_broker.publish(sid, SseFrame(id="evt-2", event="message", data='{"seq":2}'))

    sse_resp = await worker2.handle(
        McpHttpRequest(
            method="GET",
            path="/mcp",
            context=alice,
            headers={
                "Accept": "text/event-stream",
                "Mcp-Session-Id": sid,
                "MCP-Protocol-Version": "2025-11-25",
                "Last-Event-ID": "evt-1",
            },
        )
    )
    assert sse_resp.status == 200
    assert sse_resp.stream is not None
    chunk = await sse_resp.stream.__anext__()
    assert b"id: evt-2" in chunk
    assert b'{"seq":2}' in chunk
    await _aclose(sse_resp.stream)

    auto_core = MCPCore(config=MCPConfig(cache_url="memory://"))
    assert isinstance(auto_core.session_store, CacheMCPSessionStore)
    assert isinstance(auto_core.task_store, CacheMCPTaskStore)
    assert isinstance(auto_core.stream_broker, CacheSseStreamBroker)
    await auto_core.aclose()
    assert auto_core._owned_async_cache is None
    assert auto_core._owned_sync_cache is None

    await worker1.aclose()
    await worker2.aclose()
    await shared_async.aclose()
    shared_sync.close()
