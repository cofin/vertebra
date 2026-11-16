"""Unit tests for vertebra.extensions.sqlspec.adapters.spanner, session modes, data dictionary, and stores."""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import math
import re
import threading
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, cast

import pytest
from sqlspec import StatementStack
from sqlspec.data_dictionary import get_data_dictionary_loader
from sqlspec.exceptions import NotFoundError

from tests.unit.db.test_spanner import FakeAsyncSpannerClient, FakeSpannerClient
from vertebra.db import NotSupportedError
from vertebra.db.adapters.spanner import AsyncSpannerDriver, SpannerConfig, StalenessBound, SyncSpannerDriver
from vertebra.db.rls import TenantPredicate
from vertebra.extensions.sqlspec._driver import (
    AsyncAdapterConfig,
    BaseAsyncDriver,
    BaseSyncDriver,
    SyncAdapterConfig,
)
from vertebra.extensions.sqlspec.adapters.spanner import (
    SpannerAsyncADKArtifactStore,
    SpannerAsyncADKMemoryStore,
    SpannerAsyncADKStore,
    SpannerAsyncConfig,
    SpannerEventQueueStore,
    SpannerLitestarStore,
    SpannerSyncADKArtifactStore,
    SpannerSyncADKMemoryStore,
    SpannerSyncADKStore,
    SpannerSyncConfig,
)
from vertebra.extensions.sqlspec.adapters.spanner import (
    SpannerAsyncDriver as SQLSpecAsyncSpannerDriver,
)
from vertebra.extensions.sqlspec.adapters.spanner import (
    SpannerSyncDriver as SQLSpecSyncSpannerDriver,
)
from vertebra.typing import ImproperConfigurationError


class _ClosableFakeSpannerClient(FakeSpannerClient):
    """FakeSpannerClient implementing SyncSpannerClientProtocol.close."""

    __slots__ = ()

    def close(self) -> None:
        return None


class _ClosableFakeAsyncSpannerClient(FakeAsyncSpannerClient):
    """FakeAsyncSpannerClient implementing AsyncSpannerClientProtocol.close."""

    __slots__ = ()

    async def close(self) -> None:
        return None


def test_spanner_sync_config_sessions_connection_shim_and_data_dictionary() -> None:
    """Verify SpannerSyncConfig ReadOnly vs ReadWrite sessions, SpannerConnectionShim, and SpannerDataDictionary."""
    core_driver = SyncSpannerDriver(
        SpannerConfig(
            project_id="test-proj",
            instance_id="test-inst",
            database_id="sqlspec-sync-db",
        ),
        client=_ClosableFakeSpannerClient(),
    )
    config = SpannerSyncConfig(
        project_id="test-proj",
        instance_id="test-inst",
        database_id="sqlspec-sync-db",
        storage_backend="memory://",
        connection_instance=core_driver,
    )
    shim = config.create_connection()
    shim.update_ddl(
        [
            """
            CREATE TABLE parent_users (
                user_id STRING(64) NOT NULL,
                email STRING(256) NOT NULL
            ) PRIMARY KEY (user_id)
            """,
            """
            CREATE TABLE child_orders (
                user_id STRING(64) NOT NULL,
                order_id STRING(64) NOT NULL,
                total INT64 NOT NULL
            ) PRIMARY KEY (user_id, order_id),
            INTERLEAVE IN PARENT parent_users ON DELETE CASCADE
            """,
            """
            CREATE NULL_FILTERED INDEX idx_child_orders_total
            ON child_orders (total)
            """,
        ]
    )

    with shim.batch() as batch:
        batch.insert_or_update("parent_users", ("user_id", "email"), [("u1", "u1@example.com")])
        batch.insert_or_update("child_orders", ("user_id", "order_id", "total"), [("u1", "o1", 100)])

    rows = shim.execute_sql("SELECT user_id, email FROM parent_users WHERE user_id = @uid", {"uid": "u1"})
    assert len(rows) == 1
    assert rows[0]["email"] == "u1@example.com"

    with config.provide_session(transaction=False, staleness=StalenessBound.exact_staleness(500)) as ro_session:
        assert ro_session.is_read_only is True
        assert ro_session.in_transaction is False
        fetched = ro_session.select_one(
            "SELECT order_id, total FROM child_orders WHERE user_id = @uid",
            {"uid": "u1"},
        )
        assert fetched["total"] == 100
        with pytest.raises(NotFoundError):
            ro_session.select_one(
                "SELECT order_id, total FROM child_orders WHERE user_id = @uid",
                {"uid": "missing-user"},
            )
        assert (
            ro_session.select_one_or_none(
                "SELECT order_id, total FROM child_orders WHERE user_id = @uid",
                {"uid": "missing-user"},
            )
            is None
        )
        with pytest.raises(ImproperConfigurationError):
            ro_session.execute("DELETE FROM child_orders WHERE user_id = 'u1'")

    with config.provide_session(transaction=True) as rw_session:
        assert rw_session.is_read_only is False
        assert rw_session.in_transaction is True
        rw_session.execute(
            "INSERT INTO child_orders (user_id, order_id, total) VALUES (@uid, @oid, @total)",
            {"uid": "u1", "oid": "o2", "total": 250},
        )
        stack_out = rw_session.execute_stack(
            StatementStack()
            .push_execute("SELECT user_id, order_id, total FROM child_orders")
            .push_execute("SELECT COUNT(*) AS cnt FROM child_orders")
        )
        row_data = stack_out[0].get_result().get_data()
        assert len(row_data) == 2
        assert {row["order_id"] for row in row_data} == {"o1", "o2"}
        assert stack_out[1].get_result().get_data() == [{"cnt": 2}]

        dd = rw_session.data_dictionary
        tables = dd.get_tables(rw_session)
        assert any(t["table_name"] == "parent_users" for t in tables)
        interleaved = dd.get_interleaved_tables(rw_session, "parent_users")
        assert len(interleaved) == 1
        assert interleaved[0]["table_name"] == "child_orders"
        assert interleaved[0]["parent_table_name"] == "parent_users"
        assert interleaved[0]["on_delete_action"] == "CASCADE"

        cols = dd.get_columns(rw_session, "child_orders")
        assert any(c["column_name"] == "total" for c in cols)
        idxs = dd.get_indexes(rw_session, "child_orders")
        assert any(i["index_name"] == "idx_child_orders_total" for i in idxs)

        export_arrow = rw_session.select_to_arrow(
            "SELECT user_id, order_id, total FROM child_orders ORDER BY order_id ASC"
        )
        backend = config.resolve_storage_backend()
        assert backend is not None
        backend.write_arrow_sync("backups/orders.parquet", export_arrow.data, file_format="parquet")
        assert export_arrow.rows_affected == 2

    pg_cfg = SpannerSyncConfig(
        dsn="spangres://proj/inst/db?endpoint=http://127.0.0.1:9010",
    )
    pg_dd = pg_cfg.provide_driver().data_dictionary
    assert pg_dd.dialect == "spangres"
    pg_sql = get_data_dictionary_loader().get_domain_query_text("spanner", "tables", "by_schema", mode="postgresql")
    assert pg_sql is not None
    assert "information_schema.tables" in pg_sql.lower()
    shim.close()


class _StoreFakeSpannerClient(_ClosableFakeSpannerClient):
    """FakeSpannerClient extended with Spanner JSON, ScaNN, FTS, and MERGE shims for SQLite."""

    def __init__(self) -> None:
        super().__init__()
        self._conn.create_function("PARSE_JSON", 1, lambda val: val)
        self._conn.create_function("APPROX_COSINE_DISTANCE", 2, self._cosine_distance)
        self._conn.create_function("COSINE_DISTANCE", 2, self._cosine_distance)
        self._conn.create_function("SEARCH", 2, self._fts_search)
        self._conn.create_function("FARM_FINGERPRINT", 1, lambda val: abs(hash(str(val))))

    @staticmethod
    def _cosine_distance(lhs: Any, rhs: Any) -> float:
        vec_a = json.loads(lhs) if isinstance(lhs, str) else list(lhs or ())
        vec_b = json.loads(rhs) if isinstance(rhs, str) else list(rhs or ())
        if not vec_a or not vec_b or len(vec_a) != len(vec_b):
            return 1.0
        dot = sum(float(a) * float(b) for a, b in zip(vec_a, vec_b, strict=False))
        norm_a = math.sqrt(sum(float(a) * float(a) for a in vec_a))
        norm_b = math.sqrt(sum(float(b) * float(b) for b in vec_b))
        if norm_a == 0.0 or norm_b == 0.0:
            return 1.0
        return 1.0 - (dot / (norm_a * norm_b))

    @staticmethod
    def _fts_search(text: Any, query: Any) -> int:
        haystack = str(text or "").lower()
        tokens = [tok for tok in re.split(r"\s+", str(query or "").lower()) if tok]
        return 1 if tokens and all(tok in haystack for tok in tokens) else 0

    @staticmethod
    def _coerce_cell(val: Any) -> Any:
        if isinstance(val, dict):
            typ = val.get("type")
            if typ == "BYTES":
                encoded = val.get("value") if "value" in val else val.get("base64")
                if isinstance(encoded, str):
                    return base64.b64decode(encoded)
            if typ in {"TIMESTAMP", "DATE", "JSON", "JSONB", "PG_JSONB", "NUMERIC"} and "value" in val:
                return val["value"]
        return FakeSpannerClient._coerce_cell(val)

    @staticmethod
    def _translate_ddl(ddl: str) -> str:
        cleaned = re.sub(
            r"DEFAULT\s*\(MOD\(ABS\(FARM_FINGERPRINT\([^)]+\)\)\s*,\s*\d+\)\)",
            "DEFAULT 0",
            ddl,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(r"\s+STORING\s*\([^)]+\)", "", cleaned, flags=re.IGNORECASE)
        return FakeSpannerClient._translate_ddl(cleaned)

    def _adapt_sql(self, sql: str, params_json: str | None) -> tuple[str, list[Any]]:
        rewritten = re.sub(r",\s*options\s*=>\s*JSON\s*'[^']*'", "", sql, flags=re.IGNORECASE)
        rewritten = re.sub(r"SEARCH\(\s*content_tokens\s*,", "SEARCH(content_text,", rewritten, flags=re.IGNORECASE)
        upper_rewritten = rewritten.upper()
        insert_marker = "WHEN NOT MATCHED THEN INSERT"
        insert_idx = upper_rewritten.find(insert_marker)
        if upper_rewritten.lstrip().startswith("MERGE") and insert_idx != -1:
            table_match = re.search(r"MERGE\s+INTO\s+([A-Za-z0-9_]+)", rewritten, re.IGNORECASE)
            rest = rewritten[insert_idx + len(insert_marker) :].strip()
            vals_idx = rest.upper().find("VALUES")
            if table_match and vals_idx != -1:
                table = table_match.group(1)
                cols_part = rest[:vals_idx].strip()
                vals_part = re.sub(r"\bsource\.", "@", rest[vals_idx + len("VALUES") :].strip())
                verb = "INSERT OR REPLACE" if "WHEN MATCHED THEN UPDATE" in upper_rewritten else "INSERT OR IGNORE"
                rewritten = f"{verb} INTO {table} {cols_part} VALUES {vals_part}"
        return super()._adapt_sql(rewritten, params_json)


def test_spanner_sync_stores_adk_memory_artifact_kv_and_event_queue() -> None:
    """Verify all 5 synchronous SQLSpec Spanner stores (ADK, Memory, Artifact, KV Store, EventQueue)."""
    core_driver = SyncSpannerDriver(
        SpannerConfig(
            project_id="test-proj",
            instance_id="test-inst",
            database_id="stores-sync-db",
        ),
        client=_StoreFakeSpannerClient(),
    )
    config = SpannerSyncConfig(
        project_id="test-proj",
        instance_id="test-inst",
        database_id="stores-sync-db",
        storage_backend="memory://",
        connection_instance=core_driver,
        extension_config={
            "events": {"cleanup_on_ack": False},
            "adk": {"vector_dimensions": 4, "memory_use_fts": True},
        },
    )

    adk_store = SpannerSyncADKStore(config)
    ddls = adk_store.ddl_statements()
    assert "INTERLEAVE IN PARENT" in ddls[1]
    assert "ON DELETE CASCADE" in ddls[1]
    assert "ROW DELETION POLICY" in ddls[1]
    adk_store.create_tables()
    created = adk_store.create_session("s-sync-1", "agent_app", "user_1", state={"step": 1})
    sid = created["id"]
    adk_store.append_event(
        {
            "id": "ev-1",
            "app_name": "agent_app",
            "user_id": "user_1",
            "session_id": sid,
            "invocation_id": "inv-1",
            "timestamp": datetime.now(UTC),
            "event_data": {"author": "user", "content": {"text": "hello"}, "actions": {"route": "plan"}},
        }
    )
    adk_store.update_session_state("agent_app", "user_1", sid, {"step": 2})
    loaded_session = adk_store.get_session("agent_app", "user_1", sid)
    assert loaded_session is not None
    assert loaded_session["state"] == {"step": 2}
    assert len(adk_store.get_events("agent_app", "user_1", sid)) == 1
    assert len(adk_store.list_sessions(app_name="agent_app", user_id="user_1")) == 1
    adk_store.delete_session("agent_app", "user_1", sid)
    assert adk_store.get_session("agent_app", "user_1", sid) is None

    mem_store = SpannerSyncADKMemoryStore(config)
    assert "ARRAY<FLOAT32>(vector_length=>4)" in "\n".join(mem_store._memory_table_ddl())
    assert "tree_type = 'SCANN'" in mem_store.build_vector_index_sql("idx_vec")
    assert "CREATE SEARCH INDEX" in mem_store.build_search_index_sql("idx_fts")
    assert "CREATE OR REPLACE PROPERTY GRAPH" in mem_store.build_property_graph_sql("mem_g")
    mem_store.create_tables()
    now = datetime.now(UTC)
    mem_entries = [
        {
            "id": "m1",
            "session_id": "s1",
            "app_name": "agent_app",
            "user_id": "user_1",
            "scope": "user",
            "event_id": "ev-m1",
            "author": "user",
            "timestamp": now,
            "content_json": {"text": "Cloud Spanner supports TrueTime external consistency"},
            "content_text": "Cloud Spanner supports TrueTime external consistency",
            "metadata_json": {"topic": "spanner"},
            "inserted_at": now,
            "embedding": [1.0, 0.0, 0.0, 0.0],
        },
        {
            "id": "m2",
            "session_id": "s1",
            "app_name": "agent_app",
            "user_id": "user_1",
            "scope": "user",
            "event_id": "ev-m2",
            "author": "user",
            "timestamp": now,
            "content_json": {"text": "BigQuery executes columnar Arrow Storage Read streams"},
            "content_text": "BigQuery executes columnar Arrow Storage Read streams",
            "metadata_json": {"topic": "bigquery"},
            "inserted_at": now,
            "embedding": [0.0, 1.0, 0.0, 0.0],
        },
    ]
    assert mem_store.insert_memory_entries(mem_entries) == 2

    vec_hits = mem_store.search_entries("TrueTime", "agent_app", "user_1", embedding=[1.0, 0.0, 0.0, 0.0])
    assert vec_hits[0]["id"] == "m1"
    rrf_hits = mem_store.hybrid_rrf_search(
        app_name="agent_app",
        user_id="user_1",
        query="TrueTime",
        embedding=[1.0, 0.0, 0.0, 0.0],
        rrf_k=60,
    )
    assert rrf_hits[0]["id"] == "m1"

    art_store = SpannerSyncADKArtifactStore(config)
    art_store.create_table()
    saved_ref = art_store.save_artifact_object(
        "agent_app",
        "user_1",
        "report.pdf",
        b"%PDF-1.7-content",
        session_id="s1",
        mime_type="application/pdf",
    )
    assert saved_ref.size == 16
    loaded_art = art_store.get_artifact("agent_app", "user_1", "report.pdf", session_id="s1")
    assert loaded_art is not None
    assert loaded_art["canonical_uri"] == saved_ref.uri
    assert art_store.list_artifact_keys("agent_app", "user_1", session_id="s1") == ["report.pdf"]
    art_store.delete_artifact("agent_app", "user_1", "report.pdf", session_id="s1")
    assert art_store.get_artifact("agent_app", "user_1", "report.pdf", session_id="s1") is None

    kv_store = SpannerLitestarStore(config)
    assert "ROW DELETION POLICY" in kv_store.create_statements()[0]
    asyncio.run(kv_store.create_table())
    asyncio.run(kv_store.set("sess:abc", b"payload-bytes", expires_in=timedelta(seconds=120)))
    assert asyncio.run(kv_store.exists("sess:abc")) is True
    assert asyncio.run(kv_store.get("sess:abc")) == b"payload-bytes"
    ttl_rem = asyncio.run(kv_store.expires_in("sess:abc"))
    assert ttl_rem is not None and ttl_rem > 0
    asyncio.run(kv_store.delete("sess:abc"))
    assert asyncio.run(kv_store.get("sess:abc")) is None

    eq_store = SpannerEventQueueStore(config)
    eq_ddls = eq_store.ddl_statements()
    assert "ROW DELETION POLICY" in eq_ddls[0]
    assert "STORING" in eq_ddls[1]
    eq_store.create_tables_sync()
    pub = eq_store.publish_sync("orders", {"order_id": "o-100"})
    pending = eq_store.consume_sync("orders")
    assert len(pending) == 1
    assert pending[0]["event_id"] == pub["event_id"]
    assert eq_store.ack_sync(pub["event_id"]) is True
    assert eq_store.consume_sync("orders") == []


@pytest.mark.anyio
async def test_spanner_async_config_and_async_stores() -> None:
    """Verify SpannerAsyncConfig and all 5 async SQLSpec Spanner stores."""
    core_driver = SyncSpannerDriver(
        SpannerConfig(
            project_id="test-proj",
            instance_id="test-inst",
            database_id="stores-async-db",
        ),
        client=_StoreFakeSpannerClient(),
    )
    config = SpannerAsyncConfig(
        project_id="test-proj",
        instance_id="test-inst",
        database_id="stores-async-db",
        storage_backend="memory://",
        connection_instance=core_driver,
        extension_config={"adk": {"vector_dimensions": 4}},
    )
    async with config.provide_session(transaction=False) as ro_session:
        assert ro_session.is_read_only is True

    adk = SpannerAsyncADKStore(config)
    await adk.create_tables()
    s = await adk.create_session("s-async-1", "app", "u1", state={"ok": True})
    await adk.append_event(
        {
            "id": "aev-1",
            "app_name": "app",
            "user_id": "u1",
            "session_id": s["id"],
            "invocation_id": "inv-1",
            "timestamp": datetime.now(UTC),
            "event_data": {"author": "assistant", "content": {"reply": "done"}},
        }
    )
    events = await adk.get_events("app", "u1", s["id"])
    assert len(events) == 1

    mem = SpannerAsyncADKMemoryStore(config)
    await mem.create_tables()
    now = datetime.now(UTC)
    await mem.insert_memory_entries(
        [
            {
                "id": "am-1",
                "session_id": s["id"],
                "app_name": "app",
                "user_id": "u1",
                "scope": "user",
                "event_id": "aev-1",
                "author": "assistant",
                "timestamp": now,
                "content_json": {"text": "Spanner async vector search"},
                "content_text": "Spanner async vector search",
                "metadata_json": None,
                "inserted_at": now,
                "embedding": [0.5, 0.5, 0.0, 0.0],
            }
        ]
    )
    hits = await mem.hybrid_rrf_search(app_name="app", user_id="u1", query="Spanner", embedding=[0.5, 0.5, 0.0, 0.0])
    assert len(hits) == 1

    art = SpannerAsyncADKArtifactStore(config)
    await art.create_table()
    ref = await art.save_artifact_object("app", "u1", "blob.bin", b"\x00\x01\x02", session_id=s["id"])
    got_art = await art.get_artifact("app", "u1", "blob.bin", session_id=s["id"])
    assert got_art is not None and got_art["canonical_uri"] == ref.uri

    kv = SpannerLitestarStore(config)
    await kv.create_table()
    await kv.set("k1", "v1", expires_in=timedelta(seconds=60))
    assert await kv.get("k1") == b"v1"
    await kv.delete_all()

    eq = SpannerEventQueueStore(config)
    await eq.create_tables()
    evt = await eq.publish("notifications", {"msg": "hi"})
    items = await eq.consume("notifications")
    assert len(items) == 1
    assert await eq.ack(evt["event_id"]) is True


class _ArrowOptionsSpannerClient(_ClosableFakeSpannerClient):
    """External Spanner client port recording partition and staleness options."""

    def __init__(self) -> None:
        super().__init__()
        self.arrow_requests: list[tuple[int, bool, str, int]] = []

    def partitioned_query(
        self,
        sql: str,
        params_json: str | None = None,
        max_partitions: int = 8,
        data_boost_enabled: bool = True,
        staleness_mode: str = "strong",
        staleness_ms: int = 0,
    ) -> dict[str, Any]:
        self.arrow_requests.append((max_partitions, data_boost_enabled, staleness_mode, staleness_ms))
        return super().partitioned_query(
            sql, params_json, max_partitions, data_boost_enabled, staleness_mode, staleness_ms
        )


@pytest.mark.parametrize("config_created", [False, True])
@pytest.mark.parametrize("dialect", ["googlesql", "spangres"])
def test_spanner_native_driver_stack_results_and_batch_dml(
    config_created: bool, dialect: Literal["googlesql", "spangres"]
) -> None:
    """SQLSpec retains independent native Spanner reads and the raw batch DML contract."""
    client = _ArrowOptionsSpannerClient()
    core = SyncSpannerDriver(
        SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="stack-db", dialect=dialect),
        client=client,
    )
    driver = (
        SpannerSyncConfig(core.config, connection_instance=core).create_driver()
        if config_created
        else SQLSpecSyncSpannerDriver(core)
    )
    select_sql = (
        "SELECT id FROM stack_items WHERE id = $1"
        if dialect == "spangres"
        else "SELECT id FROM stack_items WHERE id = @id"
    )
    args = (1,) if dialect == "spangres" else ()
    kwargs: dict[str, Any] = {} if dialect == "spangres" else {"id": 1}
    kwargs_two: dict[str, Any] = {} if dialect == "spangres" else {"id": 2}
    try:
        core.update_ddl(["CREATE TABLE stack_items (id INT64 NOT NULL) PRIMARY KEY (id)"])
        dml_res = driver.execute_stack(
            StatementStack()
            .push_execute("INSERT INTO stack_items (id) VALUES (1)")
            .push_execute("INSERT INTO stack_items (id) VALUES (2)")
        )
        assert [item.rows_affected for item in dml_res] == [1, 1]
        result = driver.execute_stack(
            StatementStack()
            .push_execute(select_sql, *args, **kwargs)
            .push_execute("SELECT COUNT(*) AS n FROM stack_items")
        )
        assert result[0].get_result().get_data() == [{"id": 1}]
        assert result[1].get_result().get_data() == [{"n": 2}]
        single = driver.execute_stack(
            StatementStack().push_execute(select_sql, *((2,) if dialect == "spangres" else ()), **kwargs_two)
        )
        assert single[0].get_result().get_data() == [{"id": 2}]
        bound = StalenessBound.exact_staleness(750)
        arrow = driver.execute_stack(
            StatementStack().push_execute_arrow(
                select_sql, *args, data_boost=False, max_partitions=2, staleness=bound, **kwargs
            )
        )[0]
        assert arrow.is_arrow_result()
        assert arrow.rows_affected == 1
        assert arrow.get_result().data.to_pylist() == [{"id": 1}]
        assert arrow.get_result().statement.operation_type == "SELECT"
        assert client.arrow_requests == [(2, False, bound.mode, bound.effective_value())]
    finally:
        core.close()


@pytest.mark.anyio
@pytest.mark.parametrize("config_created", [False, True])
@pytest.mark.parametrize("dialect", ["googlesql", "spangres"])
async def test_spanner_async_native_driver_stack_results_and_batch_dml(
    config_created: bool, dialect: Literal["googlesql", "spangres"]
) -> None:
    """Async SQLSpec uses the real Spanner driver with its external client boundary."""
    client = _ArrowOptionsSpannerClient()
    core = AsyncSpannerDriver(
        SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="stack-async-db", dialect=dialect),
        client=_ClosableFakeAsyncSpannerClient(client),
    )
    driver = (
        SpannerAsyncConfig(core.config, connection_instance=core).create_driver()
        if config_created
        else SQLSpecAsyncSpannerDriver(core)
    )
    select_sql = (
        "SELECT id FROM stack_items WHERE id = $1"
        if dialect == "spangres"
        else "SELECT id FROM stack_items WHERE id = @id"
    )
    args = (1,) if dialect == "spangres" else ()
    kwargs: dict[str, Any] = {} if dialect == "spangres" else {"id": 1}
    kwargs_two: dict[str, Any] = {} if dialect == "spangres" else {"id": 2}
    try:
        await core.update_ddl(["CREATE TABLE stack_items (id INT64 NOT NULL) PRIMARY KEY (id)"])
        dml_res = await driver.execute_stack(
            StatementStack()
            .push_execute("INSERT INTO stack_items (id) VALUES (1)")
            .push_execute("INSERT INTO stack_items (id) VALUES (2)")
        )
        assert [item.rows_affected for item in dml_res] == [1, 1]
        result = await driver.execute_stack(
            StatementStack()
            .push_execute(select_sql, *args, **kwargs)
            .push_execute("SELECT COUNT(*) AS n FROM stack_items")
        )
        assert result[0].get_result().get_data() == [{"id": 1}]
        assert result[1].get_result().get_data() == [{"n": 2}]
        single = await driver.execute_stack(
            StatementStack().push_execute(select_sql, *((2,) if dialect == "spangres" else ()), **kwargs_two)
        )
        assert single[0].get_result().get_data() == [{"id": 2}]
        bound = StalenessBound.exact_staleness(750)
        arrow = (
            await driver.execute_stack(
                StatementStack().push_execute_arrow(
                    select_sql, *args, data_boost=False, max_partitions=2, staleness=bound, **kwargs
                )
            )
        )[0]
        assert arrow.is_arrow_result()
        assert arrow.rows_affected == 1
        assert arrow.get_result().data.to_pylist() == [{"id": 1}]
        assert arrow.get_result().statement.operation_type == "SELECT"
        assert client.arrow_requests == [(2, False, bound.mode, bound.effective_value())]
    finally:
        await core.close()


@pytest.mark.parametrize("kind", ["sync-driver", "async-driver", "sync-config", "async-config"])
def test_configured_tenant_predicate_rejected_before_constructor_effects(
    kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unsupported isolation fails before base, core, resolver, broker, or native access."""

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Constructor performed a side effect before rejecting tenant isolation")

    class ForbiddenCore:
        def __getattribute__(self, name: str) -> Any:
            forbidden(name)

    monkeypatch.setattr(BaseSyncDriver, "__init__", forbidden)
    monkeypatch.setattr(BaseAsyncDriver, "__init__", forbidden)
    monkeypatch.setattr(SyncAdapterConfig, "__init__", forbidden)
    monkeypatch.setattr(AsyncAdapterConfig, "__init__", forbidden)
    predicate = TenantPredicate(table_alias="t", tenant_value="tenant")
    constructors: dict[str, Callable[[], Any]] = {
        "sync-driver": lambda: SQLSpecSyncSpannerDriver(ForbiddenCore(), tenant_predicate=predicate),
        "async-driver": lambda: SQLSpecAsyncSpannerDriver(ForbiddenCore(), tenant_predicate=predicate),
        "sync-config": lambda: SpannerSyncConfig(tenant_predicate=predicate),
        "async-config": lambda: SpannerAsyncConfig(tenant_predicate=predicate),
    }
    monkeypatch.setattr(SpannerSyncConfig, "resolve_spanner_config", forbidden)
    monkeypatch.setattr(SpannerAsyncConfig, "resolve_spanner_config", forbidden)
    monkeypatch.setattr(SQLSpecSyncSpannerDriver, "__setattr__", forbidden)
    monkeypatch.setattr(SQLSpecAsyncSpannerDriver, "__setattr__", forbidden)
    monkeypatch.setattr(SpannerSyncConfig, "__setattr__", forbidden)
    monkeypatch.setattr(SpannerAsyncConfig, "__setattr__", forbidden)
    with pytest.raises(NotSupportedError):
        constructors[kind]()


class _StalenessAndThreadTrackingSpannerClient(_StoreFakeSpannerClient):
    """External Spanner client port recording staleness bounds and caller thread identities."""

    def __init__(self) -> None:
        super().__init__()
        self.read_staleness: list[tuple[str, int]] = []
        self.call_threads: list[int] = []

    def query(
        self,
        sql: str,
        params_json: str | None = None,
        staleness_mode: str = "strong",
        staleness_ms: int = 0,
    ) -> dict[str, Any]:
        self.call_threads.append(threading.get_ident())
        self.read_staleness.append((staleness_mode, staleness_ms))
        return super().query(
            sql,
            params_json=params_json,
            staleness_mode=staleness_mode,
            staleness_ms=staleness_ms,
        )

    def execute_dml(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> int:
        self.call_threads.append(threading.get_ident())
        return super().execute_dml(
            sql,
            params_json=params_json,
            request_tag=request_tag,
            transaction_tag=transaction_tag,
        )

    def apply_mutations(
        self,
        mutations_json: str,
        transaction_tag: str | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> dict[str, Any]:
        self.call_threads.append(threading.get_ident())
        return super().apply_mutations(
            mutations_json,
            transaction_tag=transaction_tag,
            exclude_txn_from_change_streams=exclude_txn_from_change_streams,
            max_commit_delay_ms=max_commit_delay_ms,
        )

    def update_ddl(self, statements: Sequence[str]) -> str:
        self.call_threads.append(threading.get_ident())
        return super().update_ddl(statements)


@pytest.mark.anyio
async def test_spanner_sync_and_async_select_value_routes_through_active_snapshot() -> None:
    """ReadOnly snapshot sessions route select_value through _active_snapshot with the bound StalenessBound."""
    sync_client = _StalenessAndThreadTrackingSpannerClient()
    sync_core = SyncSpannerDriver(
        SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="snap-val-sync"),
        client=sync_client,
    )
    sync_cfg = SpannerSyncConfig(sync_core.config, connection_instance=sync_core)
    bound = StalenessBound.exact_staleness(425)
    try:
        sync_core.update_ddl(["CREATE TABLE counters (id STRING(32) NOT NULL, total INT64 NOT NULL) PRIMARY KEY (id)"])
        sync_core.execute("INSERT INTO counters (id, total) VALUES (@id, @total)", {"id": "c1", "total": 19})
        sync_client.read_staleness.clear()
        with sync_cfg.provide_session(transaction=False, staleness=bound) as ro_sync:
            val = ro_sync.select_value("SELECT total FROM counters WHERE id = @id", {"id": "c1"}, value_type=str)
            assert val == "19"
        assert sync_client.read_staleness == [(bound.mode, bound.effective_value())]
    finally:
        sync_core.close()

    async_raw = _StalenessAndThreadTrackingSpannerClient()
    async_core = AsyncSpannerDriver(
        SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="snap-val-async"),
        client=_ClosableFakeAsyncSpannerClient(async_raw),
    )
    async_cfg = SpannerAsyncConfig(async_core.config, connection_instance=async_core)
    try:
        await async_core.update_ddl(
            ["CREATE TABLE counters (id STRING(32) NOT NULL, total INT64 NOT NULL) PRIMARY KEY (id)"]
        )
        await async_core.execute("INSERT INTO counters (id, total) VALUES (@id, @total)", {"id": "c1", "total": 27})
        async_raw.read_staleness.clear()
        async with async_cfg.provide_session(transaction=False, staleness=bound) as ro_async:
            aval = await ro_async.select_value(
                "SELECT total FROM counters WHERE id = @id", {"id": "c1"}, value_type=str
            )
            assert aval == "27"
        assert async_raw.read_staleness == [(bound.mode, bound.effective_value())]
    finally:
        await async_core.close()


@pytest.mark.anyio
async def test_spanner_async_stores_offload_sync_driver_calls_off_event_loop() -> None:
    """SpannerLitestarStore and SpannerAsyncADKStore offload synchronous driver operations onto worker threads."""
    tracking_client = _StalenessAndThreadTrackingSpannerClient()
    sync_core = SyncSpannerDriver(
        SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="offload-async-db"),
        client=tracking_client,
    )
    config = SpannerAsyncConfig(
        project_id="test-proj",
        instance_id="test-inst",
        database_id="offload-async-db",
        connection_instance=sync_core,
    )
    loop_tid = threading.get_ident()
    try:
        kv_store = SpannerLitestarStore(config)
        tracking_client.call_threads.clear()
        await kv_store.create_table()
        await kv_store.set("sess-1", b"val-1", expires_in=timedelta(seconds=60))
        assert await kv_store.get("sess-1") == b"val-1"
        assert tracking_client.call_threads
        assert set(tracking_client.call_threads).isdisjoint({loop_tid})

        adk_store = SpannerAsyncADKStore(config)
        tracking_client.call_threads.clear()
        await adk_store.create_tables()
        created = await adk_store.create_session("s-offload-1", "demo", "u1", state={"step": 1})
        fetched = await adk_store.get_session("demo", "u1", created["id"])
        assert fetched is not None
        assert tracking_client.call_threads
        assert set(tracking_client.call_threads).isdisjoint({loop_tid})
    finally:
        sync_core.close()


@pytest.mark.anyio
async def test_sqlspec_spanner_batch_write_rejects_max_commit_delay_ms() -> None:
    """SQLSpec sync and async Spanner drivers reject max_commit_delay_ms on batch_write."""
    sync_backing = _ClosableFakeSpannerClient()
    sync_core = SyncSpannerDriver(SpannerConfig(), client=sync_backing)
    async_core = AsyncSpannerDriver(SpannerConfig(), client=_ClosableFakeAsyncSpannerClient(sync_backing))
    sync_cfg = SpannerSyncConfig(sync_core.config, connection_instance=sync_core)
    async_cfg = SpannerAsyncConfig(async_core.config, connection_instance=async_core)
    try:
        with sync_cfg.provide_session() as sync_driver:
            assert "max_commit_delay_ms" not in inspect.signature(sync_driver.batch_write).parameters
            with pytest.raises(TypeError, match="max_commit_delay_ms"):
                cast("Any", sync_driver).batch_write([], max_commit_delay_ms=10)
        async with async_cfg.provide_session() as async_driver:
            assert "max_commit_delay_ms" not in inspect.signature(async_driver.batch_write).parameters
            with pytest.raises(TypeError, match="max_commit_delay_ms"):
                await cast("Any", async_driver).batch_write([], max_commit_delay_ms=10)
    finally:
        sync_core.close()
        await async_core.close()
