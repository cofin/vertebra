"""Unit tests for PostgreSQL CTE query pipeline, RLS tenant predicates, CDC spill bridge, DataFusion, and memfd SHM."""

from __future__ import annotations

import os
import socket

import pytest

from vertebra.db import (
    ARROW_STREAM_MEDIA_TYPE,
    CDCConfig,
    ChangeFeed,
    CTEStep,
    DataFusionConfig,
    DataFusionEngine,
    QueryPipeline,
    RelOverlay,
    TableRegistration,
    TenantPredicate,
    TenantScopeConfig,
)
from vertebra.server.platform import SHMAllocationPolicy


def test_query_pipeline_reloverlay_and_chunked_jsonb() -> None:
    """Verify QueryPipeline.build_jsonb_object (>50 args), RelOverlay, and QueryPipeline compilation."""
    pairs = [(f"k{i}", f"v{i}") for i in range(30)]
    chunked = QueryPipeline.build_jsonb_object(pairs, max_pairs_per_call=25)
    assert " || " in chunked
    assert chunked.count("jsonb_build_object(") == 2

    pipeline = (
        QueryPipeline()
        .add_step(
            "inserted_order",
            "INSERT INTO orders (id, total) VALUES (:id, :total) RETURNING *",
            params={"id": 1, "total": 99},
        )
        .add_overlay("orders_view", RelOverlay(base_table="orders", inserted_cte="inserted_order"))
        .add_step("summary", "SELECT count(*) AS cnt FROM orders_view")
    )
    sql, params = pipeline.compile()
    assert "WITH inserted_order AS" in sql
    assert "orders_view AS (SELECT * FROM orders UNION ALL SELECT * FROM inserted_order)" in sql
    assert params == {"id": 1, "total": 99}

    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        CTEStep(name="bad;name", sql="SELECT 1")
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        RelOverlay(base_table="orders; DROP TABLE users")


def test_query_pipeline_build_jsonb_object_googlesql_dialect() -> None:
    """Verify QueryPipeline.build_jsonb_object compiles GoogleSQL TO_JSON(STRUCT(...)) expressions."""
    assert QueryPipeline.build_jsonb_object([], dialect="googlesql") == "JSON '{}'"
    assert (
        QueryPipeline.build_jsonb_object([("id", "t.id"), "name"], dialect="googlesql")
        == "TO_JSON(STRUCT(t.id AS id, name AS name))"
    )


@pytest.mark.anyio
async def test_query_pipeline_googlesql_compiles_struct_json_and_rewrites_named_params() -> None:
    """Verify GoogleSQL QueryPipeline rewrites :param -> @param safely and executes over select() drivers."""
    pipeline = (
        QueryPipeline(dialect="googlesql")
        .add_step(
            "recent_events",
            (
                "SELECT id, '14:30:00' AS ts, col::INT64 AS casted "
                "FROM `proj.ds.events:raw` "
                "WHERE org_id = :org_id AND status = @already_at"
            ),
            params={"org_id": "org_42", "@already_at": "active"},
        )
        .add_overlay(
            "events_view",
            RelOverlay(base_table="proj.ds.events_base", inserted_cte="recent_events"),
        )
        .add_step(
            "summary",
            "SELECT COUNT(*) AS total FROM events_view WHERE org_id = :org_id",
            params={"org_id": "org_42"},
        )
    )
    assert pipeline.dialect == "googlesql"
    sql, params = pipeline.compile()
    assert "@org_id" in sql
    assert ":org_id" not in sql
    assert "'14:30:00'" in sql
    assert "`proj.ds.events:raw`" in sql
    assert "::INT64" in sql
    assert "@already_at" in sql
    assert "TO_JSON_STRING(TO_JSON(STRUCT(" in sql
    assert "ARRAY_AGG(TO_JSON(_s))" in sql
    assert params == {"org_id": "org_42", "already_at": "active"}

    class _StubGoogleSQLDriver:
        def __init__(self) -> None:
            self.captured_sql = ""
            self.captured_params: dict[str, object] = {}

        def select(self, query: str, bind_params: dict[str, object]) -> list[dict[str, str]]:
            self.captured_sql = query
            self.captured_params = dict(bind_params)
            return [{"payload": '{"recent_events":[{"id":1}],"summary":[{"total":1}]}'}]

        async def execute_with_description(
            self,
            query: str,
            bind_params: dict[str, object],
        ) -> tuple[list[dict[str, str]], tuple[str, ...]]:
            self.captured_sql = query
            self.captured_params = dict(bind_params)
            return ([{"payload": '{"recent_events":[{"id":1}],"summary":[{"total":1}]}'}], ("payload",))

    stub = _StubGoogleSQLDriver()
    sync_out = pipeline.execute(stub)
    assert sync_out == {"recent_events": [{"id": 1}], "summary": [{"total": 1}]}
    assert stub.captured_sql == sql
    assert stub.captured_params == params

    async_out = await pipeline.execute_async(stub)
    assert async_out == {"recent_events": [{"id": 1}], "summary": [{"total": 1}]}


def test_rls_planner_unfenced_predicate_and_session_guc() -> None:
    """Verify TenantPredicate and TenantPredicate.build_session_guc_sql."""
    guc_sql, guc_params = TenantPredicate.build_session_guc_sql("tenant-123")
    assert guc_sql == "SELECT set_config($1, $2, $3)"
    assert guc_params == ("app.tenant_id", "tenant-123", True)

    pred = TenantPredicate(
        table_alias="o",
        tenant_value="tenant-123",
        config=TenantScopeConfig(tenant_column="org_id", parameter_cast="uuid"),
    )
    assert pred.to_sql_fragment(2) == "o.org_id = $2::uuid"
    with pytest.raises(ValueError, match="table_alias"):
        TenantPredicate(table_alias="o; DROP", tenant_value="t1")
    with pytest.raises(ValueError, match="tenant_column"):
        TenantScopeConfig(tenant_column="org_id; --")


def test_cdc_listen_notify_spill_and_logical_emit() -> None:
    """Verify ChangeFeed >7500-byte spill hydration, replay_since, bounded eviction, and logical emit SQL."""
    sql, params = ChangeFeed.build_emit_message_sql("orders", '{"id":1}')
    assert "pg_logical_emit_message" in sql
    assert params == (True, "orders", b'{"id":1}')

    bridge = ChangeFeed(CDCConfig(max_history_size=2))
    small_wire, small_evt = bridge.publish_event("INSERT", {"id": 1})
    assert small_evt.spilled is False
    assert bridge.hydrate_notification(small_wire).payload == {"id": 1}

    large_payload = {"blob": "x" * 8000}
    large_wire, large_evt = bridge.publish_event("UPDATE", large_payload)
    assert large_evt.spilled is True
    assert len(large_wire) < 200
    hydrated = bridge.hydrate_notification(large_wire)
    assert hydrated.spilled is True
    assert hydrated.payload == large_payload
    assert len(bridge.replay_since(1)) == 1


def test_embedded_datafusion_and_memfd_scm_rights() -> None:
    """Verify DataFusionEngine Cloud Run / GKE modes and anonymous memfd + SCM_RIGHTS passing."""
    engine = DataFusionEngine(DataFusionConfig(mode="in_process"))
    assert engine.supports_cloud_run_ingress is True
    engine.register_table(TableRegistration(name="events_archive", uri="gs://bucket/events.parquet"))
    plan = engine.explain_plan("SELECT * FROM events_archive")
    assert plan["media_type"] == ARROW_STREAM_MEDIA_TYPE
    assert plan["referenced_tables"] == ["events_archive"]
    assert engine.execute_sql("SELECT 1 AS ok") == [{"ok": 1}]

    fd, _backend = SHMAllocationPolicy.allocate_fd("ring-test", 4096)
    try:
        os.write(fd, b"VERTEBRA_MEMFD")
        s1, s2 = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            SHMAllocationPolicy.send_fd(s1, fd, b"HELLO")
            recv_fd = SHMAllocationPolicy.recv_fd(s2)
            try:
                os.lseek(recv_fd, 0, os.SEEK_SET)
                assert os.read(recv_fd, 14) == b"VERTEBRA_MEMFD"
            finally:
                os.close(recv_fd)
        finally:
            s1.close()
            s2.close()
    finally:
        os.close(fd)
