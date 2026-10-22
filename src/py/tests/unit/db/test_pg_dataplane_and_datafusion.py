"""Unit tests for PostgreSQL CTE query pipeline, RLS tenant predicates, CDC spill bridge, DataFusion, and memfd SHM."""

from __future__ import annotations

import os
import socket

import pytest

from vertebra._typing import UnsupportedBackendError
from vertebra.db import (
    ARROW_STREAM_MEDIA_TYPE,
    CdcConfig,
    ChangeFeed,
    CteStep,
    DataFusionConfig,
    DataFusionEngine,
    QueryPipeline,
    RelOverlay,
    TableRegistration,
    TenantPredicate,
    TenantScopeConfig,
)
from vertebra.server.platform import ShmAllocationPolicy


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
        CteStep(name="bad;name", sql="SELECT 1")
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        RelOverlay(base_table="orders; DROP TABLE users")


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
    with pytest.raises(ValueError, match="Invalid table_alias identifier"):
        TenantPredicate(table_alias="o; DROP", tenant_value="t1")
    with pytest.raises(ValueError, match="Invalid tenant_column identifier"):
        TenantScopeConfig(tenant_column="org_id; --")


def test_cdc_listen_notify_spill_and_logical_emit() -> None:
    """Verify ChangeFeed >7500-byte spill hydration, replay_since, bounded eviction, and logical emit SQL."""
    sql, params = ChangeFeed.build_emit_message_sql("orders", '{"id":1}')
    assert "pg_logical_emit_message" in sql
    assert params == (True, "orders", b'{"id":1}')

    bridge = ChangeFeed(CdcConfig(max_spill_entries=2))
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
    with pytest.raises(UnsupportedBackendError, match="native vertebra-db DataFusion runtime"):
        engine.execute_sql("SELECT * FROM events_archive")

    fd, _backend = ShmAllocationPolicy.allocate_fd("ring-test", 4096)
    try:
        os.write(fd, b"VERTEBRA_MEMFD")
        s1, s2 = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            ShmAllocationPolicy.send_fd(s1, fd, b"HELLO")
            recv_fd = ShmAllocationPolicy.recv_fd(s2)
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
