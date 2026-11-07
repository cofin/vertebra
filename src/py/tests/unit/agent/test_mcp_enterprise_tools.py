"""Unit tests for ORDS, Oracle 26ai Duality Views, and Oracle APEX MCP tool synthesis."""

import ast
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from vertebra.agent.core import AgentContext, CallerIdentity, PermissionDeniedError, ToolSpec, tool
from vertebra.agent.mcp import SkillController, ToolRegistry, mcp_tool
from vertebra.agent.runtime import ConfirmationGate as RuntimeConfirmationGate
from vertebra.db.adapters.bigquery.tools import BigQueryToolProvider
from vertebra.db.adapters.spanner.tools import SpannerToolProvider
from vertebra.extensions.apex import (
    ApexAccessPolicy,
    ApexGateway,
    ApexOpenAPIGenerator,
    ApexPageToolSpec,
    ApexSessionContext,
    ConfirmationGate,
    DualityViewSpec,
    OrdsTransportProtocol,
)
from vertebra.typing import ToolPolicyError, ToolProvider


class MockOrdsTransport(OrdsTransportProtocol):
    """In-memory mock transport capturing outgoing HTTP calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, str], bytes | None]] = []

    async def send_request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        self.calls.append((method, url, dict(headers), body))
        return 200, {"Content-Type": "application/json", "ETag": '"etag-v1"'}, b'{"items": [{"id": 1, "name": "Test"}]}'


class MockOracleConn:
    """Mock database connection tracking execute calls."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.module: str | None = None
        self.action: str | None = None
        self.client_identifier: str | None = None
        self.ecid: str | None = None
        self.econtext_id: str | None = None

    async def execute(self, statement: str, parameters: Any = None) -> None:
        self.executed.append((statement, parameters))


class MockOraclePool:
    """Mock connection pool providing MockOracleConn."""

    def __init__(self) -> None:
        self.conn = MockOracleConn()

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[MockOracleConn]:
        yield self.conn


@pytest.mark.anyio
async def test_ords_openapi_tool_synthesis_and_execution() -> None:
    """Verify ApexOpenAPIGenerator synthesizes executable Tool instances interpolating path parameters."""
    transport = MockOrdsTransport()
    gen = ApexOpenAPIGenerator("https://db.example.com/ords/hr")
    gen.load_ords_openapi(
        "hr",
        {
            "paths": {
                "/employees/{emp_id}": {
                    "get": {
                        "operationId": "get_employee",
                        "summary": "Retrieve an employee by ID",
                        "parameters": [
                            {"name": "emp_id", "in": "path", "required": True, "schema": {"type": "integer"}},
                            {"name": "fields", "in": "query", "schema": {"type": "string"}},
                        ],
                    }
                }
            }
        },
    )

    tools = gen.to_mcp_tools(transport=transport)
    assert len(tools) == 1
    target_tool = tools[0]
    assert target_tool.name == "get_employee"
    assert "emp_id" in target_tool.input_schema["properties"]

    result = await target_tool.invoke({"emp_id": 101, "fields": "name,salary"})
    assert result == {"items": [{"id": 1, "name": "Test"}]}
    assert len(transport.calls) == 1
    method, url, _headers, _ = transport.calls[0]
    assert method == "GET"
    assert "/employees/101" in url
    assert "fields=name%2Csalary" in url


@pytest.mark.anyio
async def test_duality_view_mcp_tools_and_resources() -> None:
    """Verify 26ai Duality View tools and static ResourceEntry generation with ETag support."""
    transport = MockOrdsTransport()
    gen = ApexOpenAPIGenerator("https://db.example.com/ords/orders")
    dv_spec = DualityViewSpec(
        view_name="ORDER_DV",
        root_table="ORDERS",
        etag_checked=True,
        json_schema={
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "total": {"type": "number"},
            },
        },
    )
    gen.register_duality_view(dv_spec)

    tools = gen.to_mcp_tools(transport=transport)
    resources = gen.to_mcp_resources(transport=transport)

    assert len(tools) == 1
    assert tools[0].name == "duality_order_dv"
    assert len(resources) == 1
    assert resources[0].uri == "ords://duality/order_dv"

    get_res = await tools[0].invoke({"id": 5001})
    assert get_res is not None
    assert len(transport.calls) == 1
    assert transport.calls[0][1] == "https://db.example.com/ords/orders/duality/ORDER_DV/5001"

    await tools[0].invoke({"id": 5001, "document": {"total": 150.0}, "etag": "etag-v1"})
    assert len(transport.calls) == 2
    put_call = transport.calls[1]
    assert put_call[0] == "PUT"
    assert put_call[2].get("If-Match") == "etag-v1"


@pytest.mark.anyio
async def test_apex_gateway_page_and_plsql_tool_registration() -> None:
    """Verify ApexGateway builds executable APEX page and PL/SQL tools enforcing policy and confirmation."""
    gate = ConfirmationGate()
    pool = MockOraclePool()
    gateway = ApexGateway(
        agent_name="apex-order-agent",
        endpoint_url="https://gateway.example.com/a2a",
        confirmation_gate=gate,
    )
    policy = ApexAccessPolicy(
        principal_email="ops@example.com",
        app_id=200,
        workspace_id=1001,
        authorization_schemes=("IS_APPROVER",),
        max_mutation_amount=25000.0,
        allowed_targets=("APEX_PAYMENT_PKG",),
    )
    session_ctx = ApexSessionContext(app_id=200, page_id=12, session_id=999, app_user="OPS")

    page_spec = ApexPageToolSpec(
        app_id=200,
        page_id=12,
        process_name="PROCESS_PAYMENT",
        package_procedure="APEX_PAYMENT_PKG.PROCESS",
        description="Process vendor payment from APEX Page 12",
        is_mutating=True,
        page_items=("p_invoice_id",),
    )
    page_tool = gateway.register_page_tool(
        page_spec,
        policy=policy,
        pool=pool,
        context=session_ctx,
        amount=15000.0,
        required_scheme="IS_APPROVER",
    )
    assert page_tool.name == "apex_p12_process_payment"

    pending = await page_tool.invoke({"p_invoice_id": 9901})
    assert pending["status"] == "confirmation_required"

    gate.approve(
        "tok-pay-9901",
        target="APEX_PAYMENT_PKG.PROCESS",
        payload={"p_invoice_id": 9901},
    )
    approved = await page_tool.invoke({"p_invoice_id": 9901, "confirmation_token": "tok-pay-9901"})
    assert approved["status"] == "executed"
    assert len(pool.conn.executed) == 3

    mcp_tools = gateway.build_mcp_tools()
    assert "apex_p12_process_payment" in mcp_tools
    apex_tool = mcp_tools["apex_p12_process_payment"]
    assert apex_tool.spec.name == "apex_p12_process_payment"
    assert "webmcp" not in apex_tool.spec.exposure


class _CustomAnalyticsProvider(ToolProvider):
    """Neutral ToolProvider verifying bound owner preservation and per-call confirmation."""

    def __init__(self, prefix: str = "tenant-a") -> None:
        self.prefix = prefix
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def operations(self) -> Mapping[str, Callable[..., Awaitable[Any]]]:
        return {
            "analytics_read": self.analytics_read,
            "analytics_mutate": self.analytics_mutate,
        }

    def confirmation_reason(self, operation_name: str, arguments: Mapping[str, Any]) -> str | None:
        if operation_name == "analytics_mutate" and arguments.get("force"):
            return "Force mutation requires approval"
        return None

    async def analytics_read(self, metric: str, window_minutes: int = 15) -> dict[str, Any]:
        """Read metric aggregation over a sliding window."""
        if window_minutes <= 0:
            raise ToolPolicyError("window_minutes must be positive")
        self.calls.append(("read", {"metric": metric, "window_minutes": window_minutes}))
        return {"owner": self.prefix, "metric": metric, "window_minutes": window_minutes}

    async def analytics_mutate(self, metric: str, force: bool = False) -> dict[str, Any]:
        """Mutate metric configuration."""
        self.calls.append(("mutate", {"metric": metric, "force": force}))
        return {"owner": self.prefix, "metric": metric, "mutated": True, "force": force}


class _SampleSkillController(SkillController):
    """Controller exposing a bound @tool method."""

    skill_id = "ops_skill"
    prefix = "ops"

    def __init__(self, tag: str = "prod") -> None:
        self.tag = tag

    @tool(name="ops_ping", title="Ops Ping", webmcp=True)
    async def ping(self, target: str) -> dict[str, str]:
        """Ping an operational target."""
        return {"target": target, "tag": self.tag}


@pytest.mark.anyio
async def test_tool_registry_provider_controller_and_route_discovery() -> None:
    """Verify ToolRegistry ingests neutral ToolProviders, BigQuery/Spanner providers, SkillControllers, and routes."""
    adapters_root = Path(__file__).resolve().parents[3] / "vertebra" / "db" / "adapters"
    for py_path in adapters_root.rglob("*.py"):
        tree = ast.parse(py_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("vertebra.agent"), f"Upward import in {py_path}"
            elif isinstance(node, ast.ImportFrom) and node.module:
                assert not node.module.startswith("vertebra.agent"), f"Upward import in {py_path}"

    gate = RuntimeConfirmationGate()
    registry = ToolRegistry(confirmation_gate=gate)
    initial_etag = registry.etag()

    provider = _CustomAnalyticsProvider(prefix="cluster-west")
    registry.register_provider(provider)
    after_provider_etag = registry.etag()
    assert after_provider_etag != initial_etag

    registry.register_provider(BigQueryToolProvider())
    registry.register_provider(SpannerToolProvider())
    registry.register_controller(_SampleSkillController(tag="eu-central"))

    @mcp_tool(name="route_health", description="Check route health", webmcp=True)
    async def route_handler(service: str = "api") -> dict[str, str]:
        return {"service": service, "status": "healthy"}

    registry.register_routes([route_handler])
    final_etag = registry.etag()
    assert final_etag != after_provider_etag

    specs = {spec.name: spec for spec in registry.list_specs()}
    assert {
        "analytics_read",
        "analytics_mutate",
        "bigquery_dry_run",
        "bigquery_query",
        "bigquery_object_table_inspect",
        "spanner_query",
        "spanner_schema_inspect",
        "ops_ping",
        "route_health",
    }.issubset(specs.keys())

    webmcp_specs = {spec.name for spec in registry.list_specs(exposure="webmcp")}
    assert webmcp_specs == {"ops_ping", "route_health"}

    read_spec = registry.get_spec("analytics_read")
    assert isinstance(read_spec, ToolSpec)
    assert read_spec.input_schema["properties"]["window_minutes"]["default"] == 15

    read_tool = registry.get_tool("analytics_read")
    assert read_tool is not None
    read_out = await read_tool.invoke({"metric": "cpu"})
    assert read_out == {"owner": "cluster-west", "metric": "cpu", "window_minutes": 15}

    with pytest.raises(PermissionDeniedError, match="window_minutes must be positive"):
        await read_tool.invoke({"metric": "cpu", "window_minutes": 0})

    mutate_tool = registry.get_tool("analytics_mutate")
    assert mutate_tool is not None
    ctx = AgentContext(identity=CallerIdentity(user_id="alice", tenant_id="t1"))
    safe_out = await mutate_tool.invoke({"metric": "cpu", "force": False}, context=ctx)
    assert safe_out["mutated"] is True

    with pytest.raises(PermissionDeniedError, match="ticket=") as err_info:
        await mutate_tool.invoke({"metric": "cpu", "force": True}, context=ctx)

    ticket_token = str(err_info.value).split("ticket=", 1)[1].rstrip(")")
    assert gate.approve(ticket_token, owner_id=ctx.identity.owner_key()) is True

    approved_out = await mutate_tool.invoke(
        {"metric": "cpu", "force": True, "confirmation_token": ticket_token},
        context=ctx,
    )
    assert approved_out == {"owner": "cluster-west", "metric": "cpu", "mutated": True, "force": True}

    ping_tool = registry.get_tool("ops_ping")
    assert ping_tool is not None
    assert await ping_tool.invoke({"target": "db-1"}) == {"target": "db-1", "tag": "eu-central"}
