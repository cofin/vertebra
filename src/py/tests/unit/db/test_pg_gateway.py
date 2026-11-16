"""Unit tests for CDC ChangeFeed ObjectStore spill, tenant validation, and DataFusionEngine."""

from __future__ import annotations

import re
import sys
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq
import pytest

from vertebra.db.adapters.datafusion import DataFusionConfig, DataFusionEngine
from vertebra.db.adapters.oracle.core import OracleCore
from vertebra.db.cdc import CDCConfig, ChangeFeed
from vertebra.db.rls import TenantPredicate, TenantScopeConfig
from vertebra.storage import SyncObjectStore
from vertebra.typing import MissingDependencyError, reset_dependency_cache
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json, encode_json_bytes


class _GatewayDataFrame:
    """In-memory Arrow DataFrame stand-in for DataFusionEngine unit tests."""

    __slots__ = ("_table",)

    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def to_arrow_table(self) -> pa.Table:
        """Return the underlying normalized PyArrow table."""
        return self._table


class _GatewaySessionContext:
    """Deterministic PyArrow-backed SessionContext double that rejects unknown tables and invalid SQL."""

    __slots__ = ("tables",)

    def __init__(self) -> None:
        self.tables: dict[str, pa.Table] = {}

    def register_table(self, name: str, table: Any) -> None:
        """Require normalized PyArrow tables on registration."""
        if not isinstance(table, pa.Table):
            msg = f"Expected pyarrow.Table, got {type(table)!r}"
            raise TypeError(msg)
        self.tables[name] = table

    def sql(self, query: str) -> _GatewayDataFrame:
        """Execute simple table scans and counts over registered PyArrow tables or raise."""
        normalized = " ".join(query.strip().split())
        select_all = re.fullmatch(r"SELECT \* FROM ([A-Za-z_][A-Za-z0-9_]*)", normalized, flags=re.IGNORECASE)
        if select_all is not None:
            tbl_name = select_all.group(1)
            if tbl_name not in self.tables:
                msg = f"Unknown DataFusion table: {tbl_name!r}"
                raise RuntimeError(msg)
            return _GatewayDataFrame(self.tables[tbl_name])
        count_match = re.fullmatch(
            r"SELECT count\(\*\) AS ([A-Za-z_][A-Za-z0-9_]*) FROM ([A-Za-z_][A-Za-z0-9_]*)",
            normalized,
            flags=re.IGNORECASE,
        )
        if count_match is not None:
            alias, tbl_name = count_match.group(1), count_match.group(2)
            if tbl_name not in self.tables:
                msg = f"Unknown DataFusion table: {tbl_name!r}"
                raise RuntimeError(msg)
            return _GatewayDataFrame(pa.Table.from_pylist([{alias: self.tables[tbl_name].num_rows}]))
        msg = f"Invalid DataFusion SQL: {query}"
        raise RuntimeError(msg)


def test_changefeed_bounded_history_and_spill_to_object_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """ChangeFeed must bound in-memory history, spill >7500B payloads to ObjectStore, and flush Parquet."""
    store = SyncObjectStore("memory://")
    config = CDCConfig(max_history_size=2, spill_threshold_bytes=100, spill_prefix="cdc/spill")
    feed = ChangeFeed(config, store=store)

    feed.publish_event("INSERT", {"id": 1, "small": "data"})
    large_payload = {"id": 2, "large": "x" * 500}
    env2, evt2 = feed.publish_event("UPDATE", large_payload)
    assert evt2.spilled is True

    assert store.get("cdc/spill/2.json") is not None

    hydrated = feed.hydrate_notification(env2)
    assert hydrated.event_id == 2
    assert hydrated.spilled is True
    assert hydrated.payload["id"] == 2

    feed.publish_event("DELETE", {"id": 3})
    assert len(feed.replay_since(0)) == 2
    assert [e.event_id for e in feed.replay_since(0)] == [2, 3]

    put_res = feed.flush_to_parquet("cdc/archive/batch.parquet")
    assert put_res.path == "cdc/archive/batch.parquet"
    parquet_bytes = store.get("cdc/archive/batch.parquet")
    decoded_table = pq.read_table(pa.BufferReader(parquet_bytes))
    assert [row["event_id"] for row in decoded_table.to_pylist()] == [2, 3]

    store.delete("cdc/spill/2.json")
    with pytest.raises(FileNotFoundError, match="cdc/spill/2.json"):
        feed.hydrate_notification(env2)

    monkeypatch.setitem(sys.modules, "pyarrow", None)
    reset_dependency_cache("pyarrow")
    try:
        with pytest.raises(MissingDependencyError):
            feed.flush_to_parquet("cdc/archive/missing.parquet")
    finally:
        reset_dependency_cache("pyarrow")


def test_datafusion_engine_table_registration_and_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    """DataFusionEngine must register normalized PyArrow tables, execute via SessionContext, and fail closed."""
    ctx = None if import_optional("datafusion") is not None else _GatewaySessionContext()
    engine = DataFusionEngine(DataFusionConfig(), ctx=ctx)
    store = SyncObjectStore("memory://")
    metrics_sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist([{"id": 1, "val": 100}]), metrics_sink)
    store.put("data/metrics.parquet", metrics_sink.getvalue().to_pybytes())

    reg = engine.register_parquet_from_store("metrics", store, "data/metrics.parquet")
    assert reg.name == "metrics"
    assert "metrics" in engine.tables

    class MockArrowTable:
        def to_pylist(self) -> list[dict[str, Any]]:
            return [{"id": 1, "val": 100}, {"id": 2, "val": 200}]

    engine.register_arrow_table("mock_arrow", MockArrowTable())
    rows = engine.execute_dicts("SELECT * FROM mock_arrow")
    assert len(rows) == 2
    assert rows[0]["id"] == 1

    ipc_bytes = engine.stream_arrow_ipc("SELECT * FROM mock_arrow")
    assert isinstance(ipc_bytes, bytes)
    decoded_ipc = pa_ipc.open_stream(pa.BufferReader(ipc_bytes)).read_all()
    assert decoded_ipc.to_pylist() == [{"id": 1, "val": 100}, {"id": 2, "val": 200}]

    with pytest.raises(Exception, match="nonexistent_table"):
        engine.execute_dicts("SELECT * FROM nonexistent_table")

    monkeypatch.setitem(sys.modules, "datafusion", None)
    reset_dependency_cache("datafusion")
    try:
        with pytest.raises(MissingDependencyError):
            DataFusionEngine().execute_dicts("SELECT 1")
    finally:
        reset_dependency_cache("datafusion")


def test_cdc_changefeed_parquet_spill_to_object_store_and_datafusion_arrow_query() -> None:
    """ChangeFeed Parquet spill and DataFusion queries propagate missing-storage errors."""
    store = SyncObjectStore("memory://")
    feed = ChangeFeed(CDCConfig(spill_threshold_bytes=100), store=store)

    feed.publish_event("INSERT", {"account_id": 10, "amount": 50})
    env2, evt2 = feed.publish_event("INSERT", {"account_id": 20, "amount": 100, "blob": "y" * 300})
    assert evt2.spilled is True

    hydrated = feed.hydrate_notification(env2)
    assert hydrated.event_id == 2
    assert hydrated.payload["account_id"] == 20

    put_res = feed.flush_to_parquet("cdc/events.parquet")
    assert put_res.path == "cdc/events.parquet"

    ctx = None if import_optional("datafusion") is not None else _GatewaySessionContext()
    engine = DataFusionEngine(ctx=ctx)
    reg = engine.register_parquet_from_store("events", store, "cdc/events.parquet")
    assert reg.name == "events"

    class EventLogArrowTable:
        def to_pylist(self) -> list[dict[str, Any]]:
            return [
                {"account_id": 10, "amount": 50},
                {"account_id": 20, "amount": 100},
            ]

    engine.register_arrow_table("events", EventLogArrowTable())
    summary = engine.execute_dicts("SELECT count(*) AS total_events FROM events")
    assert len(summary) == 1

    ipc_data = engine.stream_arrow_ipc("SELECT * FROM events")
    assert isinstance(ipc_data, bytes)
    assert pa_ipc.open_stream(pa.BufferReader(ipc_data)).read_all().num_rows == 2

    store.delete("cdc/spill/2.json")
    with pytest.raises(FileNotFoundError, match="cdc/spill/2.json"):
        feed.hydrate_notification(env2)


def test_cdc_engine_neutral_defaults() -> None:
    """CDC configuration keeps its neutral defaults and no SQL emission surface."""
    config = CDCConfig()
    assert config.to_dict() == {
        "channel": "vertebra_events",
        "spill_threshold_bytes": 7500,
        "spill_prefix": "cdc/spill",
        "max_history_size": 1024,
        "max_memory_spills": 1024,
    }
    assert not hasattr(ChangeFeed(), "build_emit_message_sql")


@pytest.mark.parametrize(
    "field", ["replication_slot_name", "publication_name", "max_slot_wal_keep_size_mb", "spill_table"]
)
def test_cdc_rejects_postgres_configuration(field: str) -> None:
    """Engine-specific options are excluded from CDCConfig's constructor."""
    bad_kwargs: dict[str, Any] = {field: "legacy"}
    with pytest.raises(TypeError):
        CDCConfig(**bad_kwargs)


@pytest.mark.parametrize("limit", [0, -1])
def test_cdc_rejects_unbounded_memory_spills(limit: int) -> None:
    """A spill cache must retain at least one entry."""
    with pytest.raises(ValueError, match="max_memory_spills"):
        CDCConfig(max_memory_spills=limit)


@pytest.mark.parametrize(
    "prefix,expected",
    [("cdc/spill/", "cdc/spill/1.json"), ("cdc/spill///", "cdc/spill/1.json"), ("/", "1.json"), ("", "1.json")],
)
def test_cdc_spill_key_normalization(prefix: str, expected: str) -> None:
    """Publication and hydration without an explicit key use the same normalized prefix."""
    store = SyncObjectStore("memory://")
    feed = ChangeFeed(CDCConfig(spill_threshold_bytes=16, spill_prefix=prefix), store=store)
    payload = {"blob": "x" * 100}
    wire, _ = feed.publish_event("UPDATE", payload)
    envelope = decode_json(wire)
    assert envelope.pop("spill_key") == expected
    assert store.get(expected) == encode_json_bytes(payload)
    assert feed.hydrate_notification(encode_json(envelope)).payload == payload


@pytest.mark.parametrize("threshold,spilled", [(13, True), (14, False), (15, False)])
def test_cdc_spill_threshold_boundary(threshold: int, spilled: bool) -> None:
    """Only encoded payloads strictly larger than the byte threshold spill."""
    feed = ChangeFeed(CDCConfig(spill_threshold_bytes=threshold))
    wire, event = feed.publish_event("INSERT", {"value": "é"})
    assert event.spilled is spilled
    assert feed.hydrate_notification(wire).payload == {"value": "é"}


def test_cdc_memory_spills_evict_oldest() -> None:
    """Oversized payloads respect the configured cache bound through public hydration."""
    feed = ChangeFeed(CDCConfig(spill_threshold_bytes=16, max_memory_spills=2))
    first, _ = feed.publish_event("INSERT", {"blob": "a" * 100})
    second, _ = feed.publish_event("INSERT", {"blob": "b" * 100})
    third, _ = feed.publish_event("INSERT", {"blob": "c" * 100})
    with pytest.raises(KeyError, match="Evicted or unknown in-memory CDC spill id: 1"):
        feed.hydrate_notification(first)
    assert feed.hydrate_notification(second).payload == {"blob": "b" * 100}
    assert feed.hydrate_notification(third).payload == {"blob": "c" * 100}


def test_cdc_memory_spills_refresh_recency_on_hydration() -> None:
    """Reading a spill keeps it resident ahead of an unread older entry."""
    feed = ChangeFeed(CDCConfig(spill_threshold_bytes=16, max_memory_spills=2))
    first, _ = feed.publish_event("INSERT", {"blob": "a" * 100})
    second, _ = feed.publish_event("INSERT", {"blob": "b" * 100})
    feed.hydrate_notification(first)
    third, _ = feed.publish_event("INSERT", {"blob": "c" * 100})
    with pytest.raises(KeyError, match="spill id: 2"):
        feed.hydrate_notification(second)
    assert feed.hydrate_notification(first).payload == {"blob": "a" * 100}
    assert feed.hydrate_notification(third).payload == {"blob": "c" * 100}


def test_cdc_store_hydration_propagates_storage_error() -> None:
    """A backing-store failure reaches the caller without fallback or error translation."""
    store = MagicMock(spec=SyncObjectStore)
    failure = RuntimeError("storage unavailable")
    store.get.side_effect = failure
    feed = ChangeFeed(CDCConfig(spill_threshold_bytes=16), store=store)
    wire, _ = feed.publish_event("UPDATE", {"blob": "x" * 100})
    with pytest.raises(RuntimeError, match="storage unavailable") as raised:
        feed.hydrate_notification(wire)
    assert raised.value is failure
    store.get.assert_called_once_with("cdc/spill/1.json")


def test_rls_identifier_validation_and_predicate() -> None:
    """Tenant identifiers remain validated and tenant values remain bound."""
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        TenantPredicate(table_alias="t; DROP TABLE users", tenant_value=1)
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        TenantScopeConfig(tenant_column="col; bad")
    tenant = TenantPredicate(table_alias="a", tenant_value="tenant-abc")
    assert tenant.to_sql_fragment() == "a.tenant_id = ?"


def test_oracle_tenant_context_builder_binds_values_and_validates_identifiers() -> None:
    """Oracle context names are validated while the tenant stays a bind value."""
    scope = TenantScopeConfig(setting_name="app.tenant_id")
    assert OracleCore.build_session_context_sql("tenant'; DROP", scope) == (
        "BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;",
        {"value": "tenant'; DROP"},
    )
    assert OracleCore.build_session_context_sql("tenant'; DROP", scope, namespace="VERTEBRA_CTX") == (
        "BEGIN DBMS_SESSION.SET_CONTEXT(:namespace, :attribute, :value); END;",
        {"namespace": "VERTEBRA_CTX", "attribute": "tenant_id", "value": "tenant'; DROP"},
    )
    with pytest.raises(ValueError, match="namespace"):
        OracleCore.build_session_context_sql("t1", scope, namespace="CTX.BAD")
    with pytest.raises(ValueError, match="attribute"):
        OracleCore.build_session_context_sql("t1", TenantScopeConfig(setting_name="app.bad;name"))
