"""Unit tests for Vertebra litestar-mcp integration."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from litestar import Litestar, get
from litestar.testing import TestClient

from vertebra.agent.mcp import MCPConfig
from vertebra.agent.plugin import AgentPlugin, AgentPluginConfig
from vertebra.extensions.litestar.mcp import (
    JSONRPCFrameAccelerator,
    ObjectStoreSkillSource,
    WellKnownAgentCardOffload,
    create_mcp_plugin,
    create_shm_mcp_task_config,
)
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json


def test_agent_card_offload_etag_and_304_matching() -> None:
    """Verify WellKnownAgentCardOffload generates valid ETags and evaluates 304 matches."""
    card = {"name": "TestAgent", "version": "1.0.0", "description": "A test assistant"}
    offload = WellKnownAgentCardOffload.from_card(card)

    assert offload.etag.startswith('"')
    assert offload.etag.endswith('"')

    conn_match = MagicMock()
    conn_match.headers = {"if-none-match": offload.etag}
    assert offload.matches_if_none_match(conn_match) is True
    resp_304 = offload.create_response(conn_match)
    assert resp_304.status_code == 304

    conn_mismatch = MagicMock()
    conn_mismatch.headers = {"if-none-match": '"stale-etag"'}
    assert offload.matches_if_none_match(conn_mismatch) is False
    resp_200 = offload.create_response(conn_mismatch)
    assert resp_200.status_code == 200
    assert b"TestAgent" in resp_200.content


def test_jsonrpc_frame_accelerator() -> None:
    """Verify JSONRPCFrameAccelerator classifies incoming JSON-RPC payloads."""
    accel = JSONRPCFrameAccelerator()

    call_payload = b'{"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "test"}, "id": 1}'
    kind, method, req_id = accel.classify(call_payload)
    assert kind == 1
    assert method == "tools/call"
    assert req_id == "1"

    notify_payload = b'{"jsonrpc": "2.0", "method": "notifications/progress", "params": {}}'
    notify_kind, notify_method, notify_id = accel.classify(notify_payload)
    assert notify_kind == 0
    assert notify_method == "notifications/progress"
    assert notify_id is None


@pytest.mark.anyio
async def test_object_store_skill_source_empty_list() -> None:
    """Verify ObjectStoreSkillSource lists skills from backing ObjectStore."""
    store_mock = MagicMock()
    store_mock.list_objects = AsyncMock(return_value=[])

    source = ObjectStoreSkillSource(store=store_mock, prefix="skills/")
    req_mock = MagicMock()
    skills = await source.list_skills(req_mock)
    assert skills == ()


def test_create_shm_mcp_task_config() -> None:
    """Verify create_shm_mcp_task_config configures Vertebra ResponseCacheStore."""
    task_cfg = create_shm_mcp_task_config()
    assert task_cfg.default_ttl_ms == 300_000
    assert task_cfg.max_ttl_ms == 3_600_000


def test_create_mcp_plugin() -> None:
    """Verify create_mcp_plugin initializes a LitestarMCP plugin."""
    plugin = create_mcp_plugin(name="Vertebra Agent MCP")
    assert type(plugin).__name__ == "LitestarMCP"


def test_missing_dependency_error_on_missing_mcp(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify MissingDependencyError with extra='mcp' when litestar_mcp is absent."""
    monkeypatch.setattr("vertebra.utils.module_loader.import_optional", lambda name: None)
    with pytest.raises(MissingDependencyError) as exc_info:
        ensure_module("litestar_mcp", extra="mcp")
    assert "mcp" in exc_info.value.install_package


def test_litestar_mcp_delegation_no_duplicate_opt_mcp_tool_and_canonical_well_known() -> None:
    """Litestar delegates /mcp to litestar-mcp without duplicate opt['mcp_tool'] entries and serves canonical /.well-known/mcp.json."""

    @get("/orders", opt={"mcp_tool": "list_orders"})
    async def list_orders() -> list[str]:
        """List active orders."""
        return ["ord-1", "ord-2"]

    mcp_plugin = create_mcp_plugin(name="litestar-orders-mcp", enable_tasks=False)
    agent_plugin = AgentPlugin(
        AgentPluginConfig(mcp=MCPConfig(server_name="litestar-orders-mcp", server_version="1.0.0"))
    )
    app = Litestar(route_handlers=[list_orders], plugins=[mcp_plugin, agent_plugin])

    with TestClient(app=app) as client:
        tools_resp = client.post(
            "/mcp",
            headers={
                "MCP-Protocol-Version": "2026-07-28",
                "Mcp-Method": "tools/list",
            },
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
                "params": {
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                        "io.modelcontextprotocol/clientCapabilities": {},
                    }
                },
            },
        )
        assert tools_resp.status_code == 200
        tools_list = tools_resp.json()["result"]["tools"]
        tool_names = [t["name"] for t in tools_list]
        assert tool_names.count("list_orders") == 1

        wk_resp = client.get("/.well-known/mcp.json")
        assert wk_resp.status_code == 200
        wk_doc = wk_resp.json()
        assert wk_doc["protocolVersion"] == "2026-07-28"
        assert wk_doc["mcp_version"] == "2026-07-28"
        assert wk_doc["server"] == "litestar-orders-mcp"
        assert wk_doc["serverInfo"]["name"] == "litestar-orders-mcp"
        assert wk_doc["endpoint"] == "/mcp"
        assert "list_orders" in wk_doc["tools"]

    offload = WellKnownAgentCardOffload.from_mcp_config(
        MCPConfig(server_name="litestar-orders-mcp", server_version="1.0.0"),
        agent_plugin.mcp_registry,
    )
    offload_doc = decode_json(offload.body_bytes)
    assert set(offload_doc.keys()) == {
        "protocolVersion",
        "mcp_version",
        "server",
        "serverInfo",
        "endpoint",
        "tools",
    }
    assert offload_doc["protocolVersion"] == "2026-07-28"
