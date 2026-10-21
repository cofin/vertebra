"""Unit tests for vertebra.framework.* subdomains, Wasm host, and Litestar ecosystem bridges."""

from __future__ import annotations

from types import SimpleNamespace

import msgspec
import pytest

from vertebra.extensions.litestar import LitestarEcosystemBridge, attach_litestar_ecosystem_bridges
from vertebra.framework.a2a import A2AServer, A2ATask, AgentCard, AgentSkill
from vertebra.framework.asyncapi import AsyncAPISpecBuilder, AsyncChannelDef
from vertebra.framework.channels import ChannelBus
from vertebra.framework.di import Container, DependencyGraph, Provide
from vertebra.framework.email import EmailMessage, EmailService
from vertebra.framework.htmx import HtmxRequest, HtmxResponse
from vertebra.framework.mcp import McpServer, mcp_resource, mcp_tool
from vertebra.framework.openapi import OpenAPISpecBuilder
from vertebra.framework.procedures import ProcedureRouter, mutation, query
from vertebra.framework.queues import QueueService, task
from vertebra.framework.security import (
    PermissionDeniedError,
    Principal,
    SecurityContext,
    requires_capability,
    requires_role,
    requires_scope,
    requires_tenant,
)
from vertebra.framework.ssr import encode_ssr_frame
from vertebra.framework.stores import KeyValueStore, NamespacedStore
from vertebra.wasm import WASM_MAGIC_HEADER, WasmExecutionError, WasmModuleConfig, WasmPluginHost, WasmSandboxPolicy


class ItemSchema(msgspec.Struct):
    """Sample msgspec schema for OpenAPI 3.1.0 generation."""

    id: int
    name: str


@pytest.mark.anyio
async def test_di_container_topological_resolution_and_cycle_detection() -> None:
    """Verify DependencyGraph topological compilation, singleton/request scopes, and cycle detection."""
    calls = 0

    def provide_config() -> dict[str, str]:
        nonlocal calls
        calls += 1
        return {"env": "prod"}

    async def provide_service(config: dict[str, str]) -> str:
        return f"svc:{config['env']}"

    graph = DependencyGraph(
        {
            "config": Provide(provide_config, scope="singleton"),
            "service": Provide(provide_service, scope="request"),
        }
    )
    assert graph.topological_order == ("config", "service")
    container = Container(graph)
    assert await container.resolve("service") == "svc:prod"
    assert await container.resolve("service") == "svc:prod"
    assert calls == 1

    cycle_graph = DependencyGraph()
    cycle_graph.register("a", lambda b: b)
    cycle_graph.register("b", lambda a: a)
    with pytest.raises(ValueError, match="Circular dependency"):
        cycle_graph.compile()


def test_security_principal_and_guards() -> None:
    """Verify Principal and RBAC/ABAC guards (requires_role, requires_scope, requires_tenant, requires_capability)."""
    principal = Principal(
        subject="user-1",
        tenant_id="org-acme",
        roles=frozenset({"admin", "editor"}),
        scopes=frozenset({"items:read", "items:write"}),
        capabilities=frozenset({"approve_po"}),
    )
    ctx = SecurityContext(principal=principal)
    verified = ctx.enforce(
        requires_role("admin"),
        requires_scope("items:write"),
        requires_tenant("org-acme"),
        requires_capability("approve_po"),
    )
    assert verified.subject == "user-1"

    with pytest.raises(PermissionDeniedError):
        ctx.enforce(requires_role("superadmin"))


@pytest.mark.anyio
async def test_queues_channels_stores_email_and_procedures() -> None:
    """Verify QueueService, ChannelBus, NamespacedStore, EmailService, and ProcedureRouter."""
    qsvc = QueueService()

    @task(lane="cpu")
    async def compute_square(n: int) -> int:
        return n * n

    qsvc.register(compute_square)
    env = await qsvc.enqueue("compute_square", 9)
    assert env.lane == "cpu"
    assert await qsvc.drain_once() == [81]

    bus = ChannelBus(history=5)
    await bus.on_startup()
    await bus.publish(b"evt", ["orders"])
    assert await bus.get_history("orders") == [b"evt"]
    await bus.on_shutdown()

    kv = KeyValueStore()
    ns = NamespacedStore(kv, "tenant-a")
    await ns.set("token", b"xyz")
    assert await ns.get("token") == b"xyz"

    esvc = EmailService()
    receipt = await esvc.send(
        EmailMessage(subject="Welcome", to=("user@example.com",), from_email="noreply@example.com", text_body="Hello")
    )
    assert receipt == "msg-1"
    assert len(esvc.outbox) == 1

    router = ProcedureRouter()

    @query()
    async def get_version() -> str:
        return "0.1.0"

    @mutation()
    def double_value(val: int) -> int:
        return val * 2

    router.register(get_version)
    router.register(double_value)
    assert await router.call("get_version") == "0.1.0"
    assert await router.call("double_value", 21) == 42


def test_htmx_ssr_openapi_and_asyncapi() -> None:
    """Verify HtmxRequest/HtmxResponse, UnixSocketSsrFrame, OpenAPI 3.1.0, and AsyncAPI 3.0.0 builders."""
    hx_req = HtmxRequest.from_headers({"HX-Request": "true", "HX-Target": "main-panel"})
    assert hx_req.is_htmx is True
    assert hx_req.target == "main-panel"

    hx_resp = HtmxResponse("<div>Updated</div>", reswap="outerHTML", trigger={"saved": True})
    headers = hx_resp.to_headers()
    assert headers["HX-Reswap"] == "outerHTML"
    assert headers["HX-Trigger"] == '{"saved":true}'

    ssr_frame = encode_ssr_frame("Dashboard", {"count": 3})
    assert int.from_bytes(ssr_frame[:4], "big") == len(ssr_frame) - 4

    oa = OpenAPISpecBuilder()
    oa.add_operation("/items", "GET", summary="List items", response_schema=ItemSchema, tags=("items",))
    oa_doc = oa.build()
    assert oa_doc["openapi"] == "3.1.0"
    assert "ItemSchema" in oa_doc["components"]["schemas"]

    aa = AsyncAPISpecBuilder()
    aa.add_channel("orders", AsyncChannelDef(address="/ws/orders", protocol="ws"))
    aa_doc = aa.build()
    assert aa_doc["asyncapi"] == "3.0.0"
    assert aa_doc["channels"]["orders"]["address"] == "/ws/orders"


@pytest.mark.anyio
async def test_mcp_a2a_wasm_and_litestar_ecosystem_bridge() -> None:
    """Verify McpServer, A2AServer, WasmPluginHost, and LitestarEcosystemBridge."""
    mcp_srv = McpServer(name="demo-mcp")

    @mcp_tool(description="Add two integers")
    async def add_ints(a: int, b: int) -> int:
        return a + b

    @mcp_resource("config://app")
    def app_cfg() -> dict[str, str]:
        return {"mode": "active"}

    mcp_srv.register_tool(add_ints)
    mcp_srv.register_resource(app_cfg)

    init_res = await mcp_srv.handle_jsonrpc({"jsonrpc": "2.0", "id": 1, "method": "initialize"})
    assert init_res["result"]["serverInfo"]["name"] == "demo-mcp"
    call_res = await mcp_srv.handle_jsonrpc(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "add_ints", "arguments": {"a": 10, "b": 15}},
        }
    )
    assert call_res["result"]["content"][0]["text"] == "25"

    card = AgentCard(
        name="OrderAgent",
        description="Processes orders",
        url="https://agent.example.com",
        skills=(AgentSkill(id="lookup", name="Lookup Order", description="Find order by ID"),),
    )

    async def handle_a2a(task_obj: A2ATask, _params: dict[str, object]) -> str:
        return f"Processed {task_obj.id}"

    a2a_srv = A2AServer(card, handler=handle_a2a)
    assert a2a_srv.agent_card_json()["name"] == "OrderAgent"
    a2a_res = await a2a_srv.handle_jsonrpc(
        {
            "jsonrpc": "2.0",
            "id": "req-1",
            "method": "tasks/send",
            "params": {"id": "t-100", "message": {"role": "user"}},
        }
    )
    assert a2a_res["result"]["status"]["state"] == "completed"

    host = WasmPluginHost()
    wasm_bytes = WASM_MAGIC_HEADER + b"\x00\x00\x00\x00"
    host.register_module(
        WasmModuleConfig(name="validator", wasm_source=wasm_bytes, policy=WasmSandboxPolicy(max_fuel=500)),
        exports={"check": lambda s: len(str(s)) > 0},
    )
    assert host.call_export("validator", "check", "ok", fuel_cost=100) is True
    with pytest.raises(WasmExecutionError, match="exceeded fuel budget"):
        host.call_export("validator", "check", "ok", fuel_cost=1000)

    bridge = LitestarEcosystemBridge.create(mcp_server=mcp_srv, a2a_server=a2a_srv)
    fake_app = SimpleNamespace(state={})
    attach_litestar_ecosystem_bridges(fake_app, bridge)
    assert fake_app.state["mcp_server"] is mcp_srv
    assert fake_app.state["a2a_server"] is a2a_srv
