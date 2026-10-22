"""Unit tests for ORDS, Oracle 26ai Duality Views, and Oracle APEX MCP tool synthesis."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

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
