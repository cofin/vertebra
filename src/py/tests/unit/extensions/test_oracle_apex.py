"""Unit tests for Oracle APEX session management, islands, offline sync, federation, and MCP/A2A gateway."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from vertebra.agent.runtime import ConfirmationGate as RuntimeConfirmationGate
from vertebra.extensions.apex import (
    ApexAccessPolicy,
    ApexFederatedQuery,
    ApexFederatedSource,
    ApexGateway,
    ApexHandoffClaims,
    ApexIslandRegistry,
    ApexIslandSpec,
    ApexOfflineMutation,
    ApexOfflineSyncEngine,
    ApexOpenAPIGenerator,
    ApexPageToolSpec,
    ApexSessionAuth,
    ApexSessionContext,
    ConfirmationGate,
    DualityViewSpec,
    OracleSessionManager,
)
from vertebra.extensions.apex.gateway import ConfirmationGate as GatewayConfirmationGate
from vertebra.framework.live import LiveState
from vertebra.typing import UnsupportedBackendError


class FakeOracleConn:
    """In-memory mock of an async python-oracledb Thin connection."""

    __slots__ = ("action", "client_identifier", "ecid", "econtext_id", "executed", "module")

    def __init__(self) -> None:
        self.module: str | None = None
        self.action: str | None = None
        self.ecid: str | None = None
        self.econtext_id: str | None = None
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


class OrderLiveState(LiveState):
    """Reactive LiveState model bound to Oracle APEX page items."""

    order_id: str = ""
    status: str = "DRAFT"


@pytest.mark.anyio
async def test_oracle_session_manager_attach_vpd_and_guaranteed_detach() -> None:
    """Verify OracleSessionManager attaches VPD + APEX session state and detaches on exit."""
    pool = FakeOraclePool()
    ctx = ApexSessionContext(
        app_id=105,
        page_id=12,
        session_id=987654321,
        app_user="CODY.FINCHER",
        workspace_id=1001,
        client_info="vertebra-edge",
        vpd_context={"APEX_TENANT_CTX": {"TENANT_ID": "ACME", "REGION": "US_WEST"}},
        page_items={"P12_ORDER_ID": "45001"},
    )
    async with OracleSessionManager.acquire_scoped(
        pool,
        ctx,
        trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
        span_id="00f067aa0ba902b7",
        module="VertebraAPEX",
        action="order.approve",
    ) as conn:
        assert conn.module == "VertebraAPEX"
        assert conn.action == "order.approve"
        assert conn.client_identifier == "CODY.FINCHER"
        assert conn.ecid == "4bf92f3577b34da6a3ce929d0e0e4736:00f067aa0ba902b7"
        assert conn.econtext_id == "4bf92f3577b34da6a3ce929d0e0e4736:00f067aa0ba902b7"

    assert len(pool.conn.executed) == 2
    attach_sql, attach_binds = pool.conn.executed[0]
    assert "DBMS_SESSION.SET_IDENTIFIER(:app_user);" in attach_sql
    assert "DBMS_APPLICATION_INFO.SET_CLIENT_INFO(:client_info);" in attach_sql
    assert "DBMS_SESSION.SET_CONTEXT(" in attach_sql
    assert "APEX_SESSION.ATTACH" in attach_sql
    assert "APEX_UTIL.SET_SESSION_STATE" in attach_sql
    assert attach_binds["vpd_ns_0"] == "APEX_TENANT_CTX"
    assert attach_binds["vpd_val_0"] == "ACME"

    detach_sql, _ = pool.conn.executed[1]
    assert "APEX_SESSION.DETACH;" in detach_sql
    assert "DBMS_SESSION.CLEAR_IDENTIFIER;" in detach_sql
    assert pool.conn.client_identifier is None
    assert pool.conn.ecid is None


def test_apex_session_auth_handoff_token_verification_and_expiry() -> None:
    """Verify ApexSessionAuth signs, verifies, and enforces expiry on APEX-to-Vertebra handoff tokens."""
    auth = ApexSessionAuth("apex-shared-secret-key")
    claims = ApexHandoffClaims(
        app_id=105,
        page_id=12,
        session_id=987654321,
        app_user="CODY.FINCHER",
        exp=2000,
        workspace_id=1001,
        roles=("APEX_APPROVER",),
        page_items={"P12_ORDER_ID": "45001"},
    )
    token = auth.issue_handoff_token(claims)
    verified = auth.verify_handoff_token(token, now_epoch=1500)
    assert verified is not None
    assert verified.app_user == "CODY.FINCHER"
    assert verified.roles == ("APEX_APPROVER",)

    expired = auth.verify_handoff_token(token, now_epoch=2000)
    assert expired is None

    tampered = f"{token[:-4]}dead"
    assert auth.verify_handoff_token(tampered, now_epoch=1500) is None

    ctx = auth.authenticate_headers({"X-APEX-Handoff-Token": token}, now_epoch=1500)
    assert ctx is not None
    assert ctx.app_id == 105
    assert ctx.page_items["P12_ORDER_ID"] == "45001"


def test_apex_island_registry_and_live_state_two_way_sync() -> None:
    """Verify ApexIslandRegistry renders <vertebra-island> mounts and syncs page items with LiveState."""
    registry = ApexIslandRegistry()
    spec = ApexIslandSpec(
        component_name="OrderInspectorIsland",
        region_static_id="order_inspector_region",
        live_channel="apex:order:45001",
        bound_page_items=("P12_ORDER_ID", "P12_STATUS"),
        props={"theme": "redwood"},
    )
    registry.register(spec)
    ctx = ApexSessionContext(
        app_id=105,
        page_id=12,
        session_id=987654321,
        app_user="CODY.FINCHER",
        page_items={"P12_ORDER_ID": "45001", "P12_STATUS": "PENDING"},
    )
    rendered = registry.render_island_html("order_inspector_region", ctx)
    assert 'data-component="OrderInspectorIsland"' in rendered
    assert 'data-region-id="order_inspector_region"' in rendered
    assert 'data-live-channel="apex:order:45001"' in rendered
    assert 'data-bound-items="P12_ORDER_ID,P12_STATUS"' in rendered
    assert "45001" in rendered

    live = OrderLiveState()
    updated_fields = ApexIslandRegistry.sync_context_to_live_state(
        ctx,
        live,
        item_to_field_map={"P12_ORDER_ID": "order_id", "P12_STATUS": "status"},
    )
    assert updated_fields == ["order_id", "status"]
    assert live.order_id == "45001"
    assert live.status == "PENDING"

    live.status = "APPROVED"
    updated_ctx = ApexIslandRegistry.sync_live_state_to_context(
        live,
        ctx,
        field_to_item_map={"order_id": "P12_ORDER_ID", "status": "P12_STATUS"},
    )
    assert updated_ctx.page_items["P12_STATUS"] == "APPROVED"


@pytest.mark.anyio
async def test_apex_offline_sync_engine_hlc_lww_and_etag_conflicts() -> None:
    """Verify ApexOfflineSyncEngine reconciles offline APEX mutations with HLC watermarks and ETags."""
    pool = FakeOraclePool()
    row_events: list[str] = []

    async def handle_row(_ctx: ApexSessionContext, mutation: ApexOfflineMutation) -> bool:
        row_events.append(mutation.mutation_id)
        return True

    engine = ApexOfflineSyncEngine(row_handler=handle_row)
    engine.register_etag("orders_ig:mut-etag-conflict", "etag-v2")
    engine.record_server_item_write("P12_STALE_ITEM", hlc_timestamp=5000)

    ctx = ApexSessionContext(
        app_id=105,
        page_id=12,
        session_id=987654321,
        app_user="CODY.FINCHER",
        page_items={"P12_STATUS": "DRAFT", "P12_STALE_ITEM": "SERVER_NEW"},
    )
    live = OrderLiveState()
    mutations = [
        ApexOfflineMutation(
            mutation_id="mut-1",
            region_id="orders_ig",
            operation="set_item",
            hlc_timestamp=6000,
            item_name="P12_STATUS",
            value="SUBMITTED",
        ),
        ApexOfflineMutation(
            mutation_id="mut-stale",
            region_id="orders_ig",
            operation="set_item",
            hlc_timestamp=4000,
            item_name="P12_STALE_ITEM",
            value="CLIENT_OLD",
        ),
        ApexOfflineMutation(
            mutation_id="mut-etag-conflict",
            region_id="orders_ig",
            operation="duality_patch",
            hlc_timestamp=7000,
            value={"orderId": 45001},
            etag="etag-v1",
        ),
        ApexOfflineMutation(
            mutation_id="mut-row-ok",
            region_id="orders_ig",
            operation="row_upsert",
            hlc_timestamp=8000,
            value={"orderId": 45001, "qty": 5},
        ),
    ]
    result = await engine.sync_batch(ctx, mutations, pool=pool, live_state=live)
    assert result.applied_ids == ("mut-1", "mut-row-ok")
    assert result.conflict_ids == ("mut-stale", "mut-etag-conflict")
    assert result.updated_context.page_items["P12_STATUS"] == "SUBMITTED"
    assert result.updated_context.page_items["P12_STALE_ITEM"] == "SERVER_NEW"
    assert row_events == ["mut-row-ok"]
    assert len(pool.conn.executed) == 2


def test_apex_federated_query_plan_with_session_bindings() -> None:
    """Verify ApexFederatedQuery registers multi-engine sources and injects APEX session bindings."""
    federated = ApexFederatedQuery()
    federated.register_source(ApexFederatedSource(alias="APEX_ORDERS", uri="oracle://orders_dv", format="oracle"))
    federated.register_source(
        ApexFederatedSource(
            alias="ARCHIVE_SHIPMENTS",
            uri="gs://warehouse/shipments.parquet",
            format="parquet",
        )
    )
    ctx = ApexSessionContext(
        app_id=105,
        page_id=12,
        session_id=987654321,
        app_user="CODY.FINCHER",
        page_items={"P12_WAREHOUSE_ID": "WH_WEST"},
    )
    plan = federated.build_federated_plan(
        ctx,
        "SELECT o.order_id, s.tracking_no FROM APEX_ORDERS o JOIN ARCHIVE_SHIPMENTS s ON o.id = s.order_id",
    )
    assert plan["referenced_tables"] == ["APEX_ORDERS", "ARCHIVE_SHIPMENTS"]
    assert plan["session_binds"]["APP_USER"] == "CODY.FINCHER"
    assert plan["session_binds"]["P12_WAREHOUSE_ID"] == "WH_WEST"


@pytest.mark.anyio
async def test_apex_gateway_policy_page_tools_and_confirmation_gate() -> None:
    """Verify ApexGateway exposes APEX page tools and enforces ApexAccessPolicy + ConfirmationGate."""
    pool = FakeOraclePool()
    gate = ConfirmationGate()
    gateway = ApexGateway(
        agent_name="apex-order-agent",
        endpoint_url="https://agents.example.com/a2a",
        confirmation_gate=gate,
    )
    page_tool = ApexPageToolSpec(
        app_id=105,
        page_id=12,
        process_name="APPROVE_ORDER",
        package_procedure="APEX_ORDER_PKG.APPROVE",
        description="Approve an order from APEX Page 12",
        is_mutating=True,
        page_items=("P12_ORDER_ID",),
    )
    registered_tool = gateway.register_page_tool(page_tool)
    assert registered_tool.name == "apex_p12_approve_order"
    assert len(gateway.build_agent_card().skills) == 1

    policy = ApexAccessPolicy(
        principal_email="buyer@example.com",
        app_id=105,
        workspace_id=1001,
        authorization_schemes=("IS_ORDER_APPROVER",),
        max_mutation_amount=50000.0,
        allowed_targets=("APEX_ORDER_PKG", "ORDS_ORDERS"),
    )
    ctx = ApexSessionContext(
        app_id=105,
        page_id=12,
        session_id=111222,
        app_user="BUYER",
    )

    pending = await gateway.execute_plsql_process(
        pool,
        policy,
        ctx,
        "APEX_ORDER_PKG.APPROVE",
        {"p_order_id": 45001},
        is_mutating=True,
        amount=12000.0,
        required_scheme="IS_ORDER_APPROVER",
    )
    assert pending["status"] == "confirmation_required"
    assert len(pool.conn.executed) == 0

    gateway.confirmation_gate.approve(
        "tok-approve-45001",
        target="APEX_ORDER_PKG.APPROVE",
        payload={"p_order_id": 45001},
    )
    executed = await gateway.execute_plsql_process(
        pool,
        policy,
        ctx,
        "APEX_ORDER_PKG.APPROVE",
        {"p_order_id": 45001},
        is_mutating=True,
        amount=12000.0,
        required_scheme="IS_ORDER_APPROVER",
        confirmation_token="tok-approve-45001",
    )
    assert executed["status"] == "executed"
    assert len(pool.conn.executed) == 3
    assert "APEX_ORDER_PKG.APPROVE" in pool.conn.executed[1][0]

    replayed = await gateway.execute_plsql_process(
        pool,
        policy,
        ctx,
        "APEX_ORDER_PKG.APPROVE",
        {"p_order_id": 45001},
        is_mutating=True,
        amount=12000.0,
        required_scheme="IS_ORDER_APPROVER",
        confirmation_token="tok-approve-45001",
    )
    assert replayed["status"] == "confirmation_required"

    with pytest.raises(PermissionError, match="Missing APEX authorization scheme"):
        await gateway.execute_plsql_process(
            pool,
            policy,
            ctx,
            "APEX_ORDER_PKG.APPROVE",
            {"p_order_id": 45001},
            required_scheme="IS_EXECUTIVE_APPROVER",
        )

    with pytest.raises(ValueError, match="Invalid PL/SQL package procedure identifier"):
        await gateway.execute_plsql_process(pool, policy, ctx, "ORDER_DOC; DROP TABLE", {})

    with pytest.raises(ValueError, match="Invalid PL/SQL bind variable identifier"):
        await gateway.execute_plsql_process(pool, policy, ctx, "APEX_ORDER_PKG.APPROVE", {"bad-bind": 1})

    with pytest.raises(UnsupportedBackendError, match="requires an explicit ords_transport"):
        await gateway.execute_ords_rest(policy, "ORDS_ORDERS", {"id": 1}, bearer_token="tok")


def test_apex_confirmation_gate_unification_wildcard_and_owner_isolation() -> None:
    """Verify APEX uses runtime ConfirmationGate with single-use target/wildcard and owner isolation."""
    assert ConfirmationGate is RuntimeConfirmationGate
    assert GatewayConfirmationGate is RuntimeConfirmationGate

    gate = ConfirmationGate()
    assert gate.approve("tok-wild", target="*") is True
    assert gate.is_approved("tok-wild", target="ANY_PKG.PROC", payload={"x": 1}) is True
    assert gate.consume("tok-wild", target="ANY_PKG.PROC", payload={"x": 1}) is True
    assert gate.consume("tok-wild", target="ANY_PKG.PROC", payload={"x": 1}) is False

    assert (
        gate.approve("tok-scoped", target="APEX_ORDER_PKG.APPROVE", payload={"id": 42}, owner_id="acme:alice") is True
    )
    assert (
        gate.is_approved("tok-scoped", target="APEX_ORDER_PKG.APPROVE", payload={"id": 42}, owner_id="acme:bob")
        is False
    )
    assert gate.consume("tok-scoped", target="APEX_ORDER_PKG.APPROVE", payload={"id": 42}, owner_id="acme:bob") is False
    assert (
        gate.consume("tok-scoped", target="APEX_ORDER_PKG.APPROVE", payload={"id": 99}, owner_id="acme:alice") is False
    )
    assert (
        gate.consume("tok-scoped", target="apex_order_pkg.approve", payload={"id": 42}, owner_id="acme:alice") is True
    )
    assert (
        gate.consume("tok-scoped", target="APEX_ORDER_PKG.APPROVE", payload={"id": 42}, owner_id="acme:alice") is False
    )

    ticket = gate.request_confirmation("drop_table", "call-9", {"table": "orders"}, owner_id="acme:alice")
    assert gate.approve(ticket.token, owner_id="acme:bob") is False
    assert gate.approve(ticket.token, owner_id="acme:alice") is True
    assert gate.consume(ticket.token, target="drop_table", payload={"table": "orders"}, owner_id="acme:bob") is False
    assert gate.consume(ticket.token, target="drop_table", payload={"table": "orders"}, owner_id="acme:alice") is True
    assert gate.consume(ticket.token, target="drop_table", payload={"table": "orders"}, owner_id="acme:alice") is False


def test_ords_openapi_and_duality_view_generator() -> None:
    """Verify ApexOpenAPIGenerator converts ORDS OpenAPI and 26ai Duality Views into MCP/A2A specs."""
    with pytest.raises(ValueError, match="Invalid APEX page item identifier"):
        ApexSessionContext(app_id=105, page_id=1, session_id=1, app_user="ADMIN", page_items={"BAD;ITEM": "1"})

    generator = ApexOpenAPIGenerator("https://adb.example.com/ords/hr")
    generator.load_ords_openapi(
        "hr",
        {
            "paths": {
                "/employees/{id}": {
                    "get": {
                        "operationId": "get_employee",
                        "summary": "Get HR employee",
                        "parameters": [
                            {"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}},
                        ],
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
    assert tools[0].name == "get_employee"
    assert "parameters" in tools[0].input_schema
    assert tools[1].name == "duality_order_dv"


@pytest.mark.anyio
async def test_oracle_session_manager_trusted_package_and_evict_on_failed_detach() -> None:
    """Verify OracleSessionManager supports trusted context procedures, set_session_attrs, and detach eviction."""

    class _NativeLikeOracleConn:
        def __init__(self, *, fail_detach: bool = False) -> None:
            self.fail_detach = fail_detach
            self.module: str | None = None
            self.action: str | None = None
            self.ecid: str | None = None
            self.econtext_id: str | None = None
            self.client_identifier: str | None = None
            self.executed: list[tuple[str, Any]] = []
            self.staged_attrs: list[dict[str, str | None]] = []
            self.evicted = False

        def set_session_attrs(
            self,
            *,
            client_identifier: str | None = None,
            module: str | None = None,
            action: str | None = None,
            ecid: str | None = None,
        ) -> None:
            self.staged_attrs.append(
                {
                    "client_identifier": client_identifier,
                    "module": module,
                    "action": action,
                    "ecid": ecid,
                }
            )

        async def execute(
            self,
            statement: str,
            parameters: dict[str, Any] | list[Any] | None = None,
        ) -> None:
            self.executed.append((statement, parameters))
            if self.fail_detach and "APEX_SESSION.DETACH" in statement:
                raise RuntimeError("ORA-03113: detach failed")

        async def evict_connection(self) -> None:
            self.evicted = True

    class _NativeLikePool:
        def __init__(self, *, fail_detach: bool = False) -> None:
            self.conn = _NativeLikeOracleConn(fail_detach=fail_detach)

        @asynccontextmanager
        async def acquire(self) -> AsyncIterator[_NativeLikeOracleConn]:
            yield self.conn

    ctx = ApexSessionContext(
        app_id=200,
        page_id=5,
        session_id=123456,
        app_user="ADMIN",
        vpd_context={"APEX_TENANT_CTX": {"TENANT_ID": "ACME"}},
    )
    ok_pool = _NativeLikePool(fail_detach=False)
    async with OracleSessionManager.acquire_scoped(
        ok_pool,
        ctx,
        trace_id="trace-1",
        span_id="span-2",
        context_package_proc="APEX_CTX_PKG.SET_VAL",
        context_clear_proc="APEX_CTX_PKG.CLEAR_VAL",
    ):
        assert ok_pool.conn.staged_attrs == [
            {
                "client_identifier": "ADMIN",
                "module": "Vertebra",
                "action": "apex.request",
                "ecid": "trace-1:span-2",
            }
        ]
    assert ok_pool.conn.evicted is False
    assert len(ok_pool.conn.executed) == 2
    attach_sql, attach_binds = ok_pool.conn.executed[0]
    assert "APEX_CTX_PKG.SET_VAL(:vpd_ns_0, :vpd_attr_0, :vpd_val_0);" in attach_sql
    assert "DBMS_SESSION.SET_CONTEXT" not in attach_sql
    assert attach_binds["vpd_ns_0"] == "APEX_TENANT_CTX"
    assert attach_binds["vpd_attr_0"] == "TENANT_ID"
    assert attach_binds["vpd_val_0"] == "ACME"

    detach_sql, detach_binds = ok_pool.conn.executed[1]
    assert "APEX_CTX_PKG.CLEAR_VAL(:vpd_ns_0, :vpd_attr_0);" in detach_sql
    assert detach_binds == {"vpd_ns_0": "APEX_TENANT_CTX", "vpd_attr_0": "TENANT_ID"}

    bad_pool = _NativeLikePool(fail_detach=True)
    with pytest.raises(RuntimeError, match="ORA-03113: detach failed"):
        async with OracleSessionManager.acquire_scoped(
            bad_pool,
            ctx,
            context_package_proc="APEX_CTX_PKG.SET_VAL",
            context_clear_proc="APEX_CTX_PKG.CLEAR_VAL",
        ):
            pass
    assert bad_pool.conn.evicted is True
    assert bad_pool.conn.client_identifier is None
    assert bad_pool.conn.ecid is None
