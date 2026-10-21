"""Unit tests for Oracle APEX session management, ORDS OpenAPI generator, and EBS Gateway."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from vertebra.extensions.apex import (
    ApexLangRunner,
    ApexOpenAPIGenerator,
    ApexSessionContext,
    ConfirmationGate,
    DualityViewSpec,
    EbsGateway,
    EbsResponsibilityMapping,
    OracleSessionManager,
)


class FakeOracleConn:
    """In-memory mock of an async python-oracledb Thin connection."""

    __slots__ = ("action", "client_identifier", "ecid", "executed", "module")

    def __init__(self) -> None:
        self.module: str | None = None
        self.action: str | None = None
        self.ecid: str | None = None
        self.client_identifier: str | None = None
        self.executed: list[tuple[str, Any]] = []

    async def execute(
        self,
        statement: str,
        parameters: dict[str, Any] | list[Any] | None = None,
    ) -> None:
        self.executed.append((statement, parameters))


class FakeOraclePool:
    """In-memory mock of an async python-oracledb connection pool."""

    __slots__ = ("conn",)

    def __init__(self) -> None:
        self.conn = FakeOracleConn()

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[FakeOracleConn]:
        yield self.conn


@pytest.mark.anyio
async def test_oracle_session_manager_attach_and_guaranteed_detach() -> None:
    """Verify OracleSessionManager attaches EBS + MOAC + APEX and detaches on exit."""
    pool = FakeOraclePool()
    ctx = ApexSessionContext(
        app_id=105,
        page_id=12,
        session_id=987654321,
        app_user="CODY.FINCHER",
        ebs_user_id=1010,
        ebs_resp_id=50201,
        ebs_resp_appl_id=201,
        ebs_org_id=204,
        page_items={"P12_PO_ID": "45001"},
    )
    async with OracleSessionManager.acquire_scoped(
        pool,
        ctx,
        trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
        span_id="00f067aa0ba902b7",
        module="VertebraEBS",
        action="po.approve",
    ) as conn:
        assert conn.module == "VertebraEBS"
        assert conn.action == "po.approve"
        assert conn.client_identifier == "CODY.FINCHER"
        assert conn.ecid == "4bf92f3577b34da6a3ce929d0e0e4736:00f067aa0ba902b7"

    assert len(pool.conn.executed) == 2
    attach_sql, attach_binds = pool.conn.executed[0]
    assert "APPS.FND_GLOBAL.APPS_INITIALIZE" in attach_sql
    assert "MO_GLOBAL.SET_POLICY_CONTEXT" in attach_sql
    assert "APEX_SESSION.ATTACH" in attach_sql
    assert "APEX_UTIL.SET_SESSION_STATE" in attach_sql
    assert attach_binds["ebs_resp_id"] == 50201

    detach_sql, _ = pool.conn.executed[1]
    assert "APEX_SESSION.DETACH;" in detach_sql
    assert "DBMS_SESSION.CLEAR_IDENTIFIER;" in detach_sql
    assert pool.conn.client_identifier is None
    assert pool.conn.ecid is None


@pytest.mark.anyio
async def test_ebs_gateway_policy_and_confirmation_gate() -> None:
    """Verify EbsGateway enforces responsibility policy and ConfirmationGate tokens."""
    pool = FakeOraclePool()
    gate = ConfirmationGate()
    gateway = EbsGateway(
        agent_name="ebs-procurement-agent",
        endpoint_url="https://agents.example.com/a2a",
        confirmation_gate=gate,
    )
    mapping = EbsResponsibilityMapping(
        principal_email="buyer@example.com",
        ebs_user_id=1010,
        ebs_resp_id=50201,
        ebs_resp_appl_id=201,
        ebs_org_id=204,
        max_approval_amount=50000.0,
        allowed_packages=("PO_DOCUMENT_ACTION_PVT",),
    )
    ctx = ApexSessionContext(
        app_id=105,
        page_id=12,
        session_id=111222,
        app_user="BUYER",
    )

    pending = await gateway.execute_plsql_package(
        pool,
        mapping,
        ctx,
        "PO_DOCUMENT_ACTION_PVT.APPROVE",
        {"p_document_id": 45001},
        is_mutating=True,
        amount=12000.0,
    )
    assert pending["status"] == "confirmation_required"
    assert len(pool.conn.executed) == 0

    gateway.confirmation_gate.approve("tok-approve-45001")
    executed = await gateway.execute_plsql_package(
        pool,
        mapping,
        ctx,
        "PO_DOCUMENT_ACTION_PVT.APPROVE",
        {"p_document_id": 45001},
        is_mutating=True,
        amount=12000.0,
        confirmation_token="tok-approve-45001",
    )
    assert executed["status"] == "executed"
    assert len(pool.conn.executed) == 3
    assert "PO_DOCUMENT_ACTION_PVT.APPROVE" in pool.conn.executed[1][0]


def test_ords_openapi_and_duality_view_generator_and_apexlang() -> None:
    """Verify ApexOpenAPIGenerator converts ORDS OpenAPI and 26ai Duality Views into MCP/A2A specs."""
    generator = ApexOpenAPIGenerator("https://adb.example.com/ords/hr")
    generator.load_ords_openapi(
        "hr",
        {
            "paths": {
                "/employees/": {
                    "get": {
                        "operationId": "list_employees",
                        "summary": "List HR employees",
                    }
                }
            }
        },
    )
    generator.register_duality_view(DualityViewSpec(view_name="ORDER_DV", root_table="ORDERS"))
    tools = generator.to_mcp_tools()
    skills = generator.to_a2a_skills()
    assert len(tools) == 2
    assert len(skills) == 2
    assert tools[0].name == "list_employees"
    assert tools[1].name == "duality_order_dv"

    runner = ApexLangRunner(workspace_name="EBS_APEX_WS")
    artifact = runner.compile_spec(105, "orders.apexlang", "page 1 { title: 'Orders' }")
    assert artifact.app_id == 105
    assert "EBS_APEX_WS" in artifact.output_sql
