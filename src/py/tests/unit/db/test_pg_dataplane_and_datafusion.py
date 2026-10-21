"""Unit tests for PostgreSQL CTE query pipeline, RLS tenant predicates, CDC spill bridge, DataFusion, and memfd SHM."""

from __future__ import annotations

import os
import socket

from vertebra.db import (
    ARROW_STREAM_MEDIA_TYPE,
    DataFusionConfig,
    DataFusionTableRegistration,
    EmbeddedDataFusionEngine,
    ListenNotifySpillBridge,
    PlannerUnfencedTenantPredicate,
    QueryPipeline,
    RelOverlay,
    TenantScopeConfig,
    build_logical_emit_message_sql,
    build_session_tenant_guc_sql,
    chunked_jsonb_build_object,
)
from vertebra.server._shm_hardening import (
    allocate_anonymous_shm_fd,
    recv_fd_over_unix_socket,
    send_fd_over_unix_socket,
)


def test_query_pipeline_reloverlay_and_chunked_jsonb() -> None:
    """Verify chunked_jsonb_build_object (>50 args), RelOverlay, and QueryPipeline compilation."""
    pairs = [(f"k{i}", f"v{i}") for i in range(30)]
    chunked = chunked_jsonb_build_object(pairs, max_pairs_per_call=25)
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


def test_rls_planner_unfenced_predicate_and_session_guc() -> None:
    """Verify PlannerUnfencedTenantPredicate and build_session_tenant_guc_sql."""
    guc_sql, guc_params = build_session_tenant_guc_sql("tenant-123")
    assert guc_sql == "SELECT set_config($1, $2, $3)"
    assert guc_params == ("app.tenant_id", "tenant-123", True)

    pred = PlannerUnfencedTenantPredicate(
        table_alias="o",
        tenant_value="tenant-123",
        config=TenantScopeConfig(tenant_column="org_id", parameter_cast="uuid"),
    )
    assert pred.to_sql_fragment(2) == "o.org_id = $2::uuid"


def test_cdc_listen_notify_spill_and_logical_emit() -> None:
    """Verify ListenNotifySpillBridge >7500-byte spill hydration, replay_since, and logical emit SQL."""
    sql, params = build_logical_emit_message_sql("orders", '{"id":1}')
    assert "pg_logical_emit_message" in sql
    assert params == (True, "orders", b'{"id":1}')

    bridge = ListenNotifySpillBridge()
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
    """Verify EmbeddedDataFusionEngine Cloud Run / GKE modes and anonymous memfd + SCM_RIGHTS passing."""
    engine = EmbeddedDataFusionEngine(DataFusionConfig(mode="in_process"))
    assert engine.supports_cloud_run_ingress is True
    engine.register_table(DataFusionTableRegistration(name="events_archive", uri="gs://bucket/events.parquet"))
    plan = engine.explain_plan("SELECT * FROM events_archive")
    assert plan["media_type"] == ARROW_STREAM_MEDIA_TYPE
    assert plan["referenced_tables"] == ["events_archive"]

    fd = allocate_anonymous_shm_fd("ring-test", 4096)
    try:
        os.write(fd, b"VERTEBRA_MEMFD")
        s1, s2 = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            send_fd_over_unix_socket(s1, fd, b"HELLO")
            recv_fd, msg = recv_fd_over_unix_socket(s2)
            try:
                assert msg == b"HELLO"
                os.lseek(recv_fd, 0, os.SEEK_SET)
                assert os.read(recv_fd, 14) == b"VERTEBRA_MEMFD"
            finally:
                os.close(recv_fd)
        finally:
            s1.close()
            s2.close()
    finally:
        os.close(fd)
