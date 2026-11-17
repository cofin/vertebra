"""Unit tests for vertebra.extensions.sqlspec.adapters.spanner, session modes, data dictionary, and stores."""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import math
import re
import subprocess
import sys
import threading
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

import pytest
from sqlspec import StatementStack
from sqlspec.config import ADKConfig
from sqlspec.data_dictionary import get_data_dictionary_loader
from sqlspec.exceptions import MigrationError, NotFoundError
from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore
from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore, BaseSyncADKArtifactStore
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.extensions.litestar.store import CompanionIndex
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.runner import SyncMigrationRunner

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
    SpangresAsyncConfig,
    SpangresSyncConfig,
    SpannerADKConfig,
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
from vertebra.extensions.sqlspec.adapters.spanner import adk as spanner_adk_module
from vertebra.extensions.sqlspec.adapters.spanner import reconcile as spanner_reconcile
from vertebra.extensions.sqlspec.adapters.spanner.core import (
    farm_fingerprint_shard,
    farmhash_fingerprint64,
)
from vertebra.extensions.sqlspec.adapters.spanner.driver import (
    _execute_async_select,
    _execute_sync_select,
)
from vertebra.extensions.sqlspec.adapters.sqlite import SQLiteSyncConfig
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
        self.executed_queries: list[str] = []
        self._conn.create_function("PARSE_JSON", 1, lambda val: val)
        self._conn.create_function("APPROX_COSINE_DISTANCE", 2, self._cosine_distance)
        self._conn.create_function("COSINE_DISTANCE", 2, self._cosine_distance)
        self._conn.create_function("SEARCH", 2, self._fts_search)
        self._conn.create_function("SCORE", 2, self._fts_score)
        self._conn.create_function("FARM_FINGERPRINT", 1, lambda val: abs(hash(str(val))))

    @staticmethod
    def _fts_score(text: Any, query: Any) -> float:
        haystack = str(text or "").lower()
        tokens = [tok for tok in re.split(r"\s+", str(query or "").lower()) if tok]
        if not tokens:
            return 0.0
        return float(sum(haystack.count(tok) for tok in tokens))

    def query(
        self,
        sql: str,
        params_json: str | None = None,
        staleness_mode: str = "strong",
        staleness_ms: int = 0,
    ) -> dict[str, Any]:
        self.executed_queries.append(sql)
        return super().query(sql, params_json, staleness_mode, staleness_ms)

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
        rewritten = re.sub(r"SCORE\(\s*content_tokens\s*,", "SCORE(content_text,", rewritten, flags=re.IGNORECASE)
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
    mem_entries: list[Any] = [
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
    async with config.provide_session(transaction=False) as autocommit_session:
        assert autocommit_session.is_read_only is False
    async with config.provide_session(transaction=False, read_only=True) as ro_session:
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


@pytest.mark.anyio
async def test_spanner_provide_session_rw_transactions_autocommit_and_snapshot_precedence() -> None:
    """provide_session begins/commits/rolls back RW transactions, defaults transaction=False to autocommit, and prioritizes active_snapshot."""
    fake_client = _ClosableFakeSpannerClient()
    sync_core = SyncSpannerDriver(
        SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="tx-lifecycle-db"),
        client=fake_client,
    )
    async_core = AsyncSpannerDriver(
        SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="tx-lifecycle-db"),
        client=_ClosableFakeAsyncSpannerClient(fake_client),
    )
    sync_cfg = SpannerSyncConfig(sync_core.config, connection_instance=sync_core, share_pool=False)
    async_cfg = SpannerAsyncConfig(async_core.config, connection_instance=async_core, share_pool=False)
    try:
        with sync_cfg.provide_session(transaction=False) as autocommit:
            assert autocommit.is_read_only is False
            assert autocommit.in_transaction is False
            autocommit.execute("CREATE TABLE ledger (id STRING(64) NOT NULL, amount INT64 NOT NULL) PRIMARY KEY (id)")
            autocommit.execute("INSERT INTO ledger (id, amount) VALUES (@id, @amount)", {"id": "row-0", "amount": 10})

        before_tx = len(fake_client.begin_tx_calls)
        with sync_cfg.provide_session(transaction=True) as rw_sync:
            assert rw_sync.in_transaction is True
            rw_sync.execute("INSERT INTO ledger (id, amount) VALUES (@id, @amount)", {"id": "row-1", "amount": 25})
        assert len(fake_client.begin_tx_calls) == before_tx + 1
        assert len(fake_client.tx_rollbacks) == 0

        with pytest.raises(RuntimeError, match="abort sync tx"), sync_cfg.provide_session(transaction=True) as rw_fail:
            rw_fail.execute("INSERT INTO ledger (id, amount) VALUES (@id, @amount)", {"id": "row-bad", "amount": 99})
            raise RuntimeError("abort sync tx")
        assert len(fake_client.tx_rollbacks) == 1

        with sync_cfg.provide_session(transaction=False, read_only=True) as ro_sync:
            assert ro_sync.is_read_only is True
            rows = ro_sync.select("SELECT id, amount FROM ledger ORDER BY id")
            assert [r["id"] for r in rows] == ["row-0", "row-1"]

        async with async_cfg.provide_session(transaction=True) as rw_async:
            assert rw_async.in_transaction is True
            await rw_async.execute(
                "INSERT INTO ledger (id, amount) VALUES (@id, @amount)", {"id": "row-2", "amount": 50}
            )

        with pytest.raises(RuntimeError, match="abort async tx"):
            async with async_cfg.provide_session(transaction=True) as rw_async_fail:
                await rw_async_fail.execute(
                    "INSERT INTO ledger (id, amount) VALUES (@id, @amount)", {"id": "row-async-bad", "amount": 77}
                )
                raise RuntimeError("abort async tx")
        assert len(fake_client.tx_rollbacks) == 2

        async with async_cfg.provide_session(transaction=False, read_only=True) as ro_async:
            assert ro_async.is_read_only is True
            arows = await ro_async.select("SELECT id, amount FROM ledger ORDER BY id")
            assert [r["id"] for r in arows] == ["row-0", "row-1", "row-2"]

        cursor_called = False

        class _CursorStub:
            def execute_with_description(self, sql: str, params: Any = None) -> tuple[list[Any], list[str]]:
                nonlocal cursor_called
                _ = (sql, params)
                cursor_called = True
                return [(999,)], ["val"]

        class _SyncSnapshotStub:
            def execute_with_description(self, sql: str, params: Any = None) -> tuple[list[Any], list[str]]:
                _ = (sql, params)
                return [(42,)], ["snap_val"]

        class _AsyncSnapshotStub:
            async def execute_with_description(self, sql: str, params: Any = None) -> tuple[list[Any], list[str]]:
                _ = (sql, params)
                return [(84,)], ["async_snap_val"]

        sync_rows, sync_cols = _execute_sync_select(
            _CursorStub(), "SELECT 1", None, active_snapshot=_SyncSnapshotStub()
        )
        assert cursor_called is False
        assert sync_rows == [{"snap_val": 42}]
        assert sync_cols == ["snap_val"]

        async_rows, async_cols = await _execute_async_select(
            _CursorStub(), "SELECT 1", None, active_snapshot=_AsyncSnapshotStub()
        )
        assert cursor_called is False
        assert async_rows == [{"async_snap_val": 84}]
        assert async_cols == ["async_snap_val"]
    finally:
        sync_cfg.close()
        await async_cfg.close()


@pytest.mark.anyio
async def test_spanner_farmhash_fingerprint_cached_core_close_and_adk_error_propagation() -> None:
    """farmhash_fingerprint64 matches Google reference vectors, close() cleans cached cores, and ADK search propagates errors."""
    assert farmhash_fingerprint64("") == -7286425919675154353
    assert farmhash_fingerprint64("abc") == 2640714258260161385
    assert farmhash_fingerprint64("alphabet") == -2427165924636348523
    medium_key = "user-session-identifier-with-forty-bytes"
    long_key = "x" * 96
    assert farm_fingerprint_shard(medium_key, 16) == abs(farmhash_fingerprint64(medium_key)) % 16
    assert farm_fingerprint_shard(long_key, 32) == abs(farmhash_fingerprint64(long_key)) % 32

    class _ClosableCoreStub:
        def __init__(self) -> None:
            self.closed_count = 0

        def close(self) -> None:
            self.closed_count += 1

    class _AsyncClosableCoreStub:
        def __init__(self) -> None:
            self.closed_count = 0

        async def close(self) -> None:
            self.closed_count += 1

    sync_stub = _ClosableCoreStub()
    sync_cfg = SpannerSyncConfig(project_id="p", instance_id="i", database_id="d", share_pool=False)
    cast(Any, sync_cfg)._cached_core = sync_stub
    sync_cfg.close()
    assert sync_stub.closed_count == 1
    assert sync_cfg._cached_core is None

    async_stub = _AsyncClosableCoreStub()
    sync_helper_stub = _ClosableCoreStub()
    async_cfg = SpannerAsyncConfig(project_id="p", instance_id="i", database_id="d", share_pool=False)
    cast(Any, async_cfg)._cached_core = async_stub
    cast(Any, async_cfg)._cached_sync_core = sync_helper_stub
    await async_cfg.close()
    assert async_stub.closed_count == 1
    assert sync_helper_stub.closed_count == 1
    assert async_cfg._cached_core is None
    assert async_cfg._cached_sync_core is None

    class _BrokenSpannerClient(_ClosableFakeSpannerClient):
        def query(
            self,
            sql: str,
            params_json: str | None = None,
            staleness_mode: str = "strong",
            staleness_ms: int = 0,
        ) -> dict[str, Any]:
            if "COSINE_DISTANCE" in sql or "SEARCH(" in sql:
                raise RuntimeError("broken search index")
            return super().query(sql, params_json, staleness_mode, staleness_ms)

    broken_core = SyncSpannerDriver(SpannerConfig(), client=_BrokenSpannerClient())
    broken_sync_cfg = SpannerSyncConfig(broken_core.config, connection_instance=broken_core, share_pool=False)
    mem_store = SpannerSyncADKMemoryStore(broken_sync_cfg)
    with pytest.raises(RuntimeError, match="broken search index"):
        mem_store.search_entries("hello", "app", "u1", embedding=[0.1, 0.2])

    broken_async_cfg = SpannerAsyncConfig(broken_core.config, connection_instance=broken_core, share_pool=False)
    async_mem_store = SpannerAsyncADKMemoryStore(broken_async_cfg)
    with pytest.raises(RuntimeError, match="broken search index"):
        await async_mem_store.search_entries("hello", "app", "u1", embedding=[0.1, 0.2])


def test_spanner_adk_upstream_bases_slots_config_and_missing_dependency() -> None:
    """Verify upstream ADK base inheritance, __slots__, removed Python ranking helpers, config keys, and MissingDependencyError."""
    for removed_symbol in (
        "_rank_memory_rows",
        "_cosine_similarity",
        "_keyword_score",
        "_decode_embedding",
        "_FallbackArtifactStoreBase",
        "_FallbackSyncADKArtifactStore",
        "_FallbackAsyncADKArtifactStore",
        "_FallbackMemoryStoreBase",
        "_FallbackSyncADKMemoryStore",
        "_FallbackAsyncADKMemoryStore",
    ):
        assert not hasattr(spanner_adk_module, removed_symbol), f"{removed_symbol} must not exist"

    assert issubclass(SpannerSyncADKStore, BaseSyncADKStore)
    assert issubclass(SpannerAsyncADKStore, BaseAsyncADKStore)
    assert issubclass(SpannerSyncADKMemoryStore, BaseSyncADKMemoryStore)
    assert issubclass(SpannerAsyncADKMemoryStore, BaseAsyncADKMemoryStore)
    assert issubclass(SpannerSyncADKArtifactStore, BaseSyncADKArtifactStore)
    assert issubclass(SpannerAsyncADKArtifactStore, BaseAsyncADKArtifactStore)

    expected_keys = {
        "shard_count",
        "session_table_options",
        "events_table_options",
        "retention",
        "enable_memory",
        "memory_table",
        "artifact_table",
        "use_fts",
        "vector_dimensions",
        "scann_num_leaves",
        "scann_quantizer",
        "owner_id_column",
    }
    config_keys = set(SpannerADKConfig.__optional_keys__) | set(ADKConfig.__optional_keys__)
    assert expected_keys <= config_keys

    sync_cfg = SpannerSyncConfig(
        project_id="p",
        instance_id="i",
        database_id="d",
        share_pool=False,
        extension_config={
            "adk": {
                "shard_count": 4,
                "session_table_options": "row_deletion_policy = 'none'",
                "events_table_options": "row_deletion_policy = 'none'",
                "retention": {"session_ttl_seconds": 86400, "event_ttl_seconds": 172800},
                "enable_memory": True,
                "memory_table": "adk_memory",
                "artifact_table": "adk_artifact",
                "use_fts": True,
                "vector_dimensions": 8,
                "scann_num_leaves": 128,
                "scann_quantizer": "SQ8",
                "owner_id_column": "tenant_id STRING(64)",
            }
        },
    )
    async_cfg = SpannerAsyncConfig(
        project_id="p",
        instance_id="i",
        database_id="d",
        share_pool=False,
        extension_config=sync_cfg.extension_config,
    )
    for store in (
        SpannerSyncADKStore(sync_cfg),
        SpannerAsyncADKStore(async_cfg),
        SpannerSyncADKMemoryStore(sync_cfg),
        SpannerAsyncADKMemoryStore(async_cfg),
        SpannerSyncADKArtifactStore(sync_cfg),
        SpannerAsyncADKArtifactStore(async_cfg),
    ):
        assert not hasattr(store, "__dict__"), f"{type(store).__name__} must define __slots__"

    missing_script = (
        "import sys\n"
        "from vertebra.typing import MissingDependencyError\n"
        "sys.modules['google.adk'] = None\n"
        "try:\n"
        "    import vertebra.extensions.sqlspec.adapters.spanner.adk\n"
        "except MissingDependencyError as exc:\n"
        "    assert exc.install_package == 'vertebra-core[adk]'\n"
        "else:\n"
        "    raise AssertionError('Expected MissingDependencyError when google.adk is absent')\n"
    )
    subprocess.run([sys.executable, "-c", missing_script], check=True)


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_spanner_adk_state_deltas_sql_ranking_and_owner_scoped_delete(is_async_mode: bool) -> None:
    """Verify app/user state delta merging, SQL-native vector/FTS/RRF ranking, and owner-scoped deletion."""
    client = _StoreFakeSpannerClient()
    core = SyncSpannerDriver(
        SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="spanner-adk-db"),
        client=client,
    )
    adk_ext: SpannerADKConfig = {
        "memory_use_fts": True,
        "memory_max_results": 5,
        "vector_dimensions": 4,
    }
    try:
        if is_async_mode:
            cfg: Any = SpannerAsyncConfig(
                project_id="test-proj",
                instance_id="test-inst",
                database_id="spanner-adk-db",
                connection_instance=core,
                share_pool=False,
                extension_config={"adk": adk_ext},
            )
            adk_store: Any = SpannerAsyncADKStore(cfg)
            mem_store: Any = SpannerAsyncADKMemoryStore(cfg)
            await adk_store.create_tables()
            await mem_store.create_tables()
            created = await adk_store.create_session(
                "sess-delta",
                "agent-app",
                "user-a",
                state={"local": "v1"},
                app_state_delta={"app:theme": "dark"},
                user_state_delta={"user:lang": "en"},
            )
            assert created["state"] == {"local": "v1", "app:theme": "dark", "user:lang": "en"}
            updated = await adk_store.append_event_and_update_state(
                {
                    "id": "ev-delta-1",
                    "app_name": "agent-app",
                    "user_id": "user-a",
                    "session_id": "sess-delta",
                    "invocation_id": "inv-delta-1",
                    "timestamp": datetime.now(UTC),
                    "event_data": {"author": "user"},
                },
                "agent-app",
                "user-a",
                "sess-delta",
                state={"local": "v2"},
                app_state_delta={"app:tier": "pro"},
                user_state_delta={"user:tz": "UTC"},
            )
            assert updated["state"] == {
                "local": "v2",
                "app:theme": "dark",
                "app:tier": "pro",
                "user:lang": "en",
                "user:tz": "UTC",
            }
            assert await adk_store.get_app_state("agent-app") == {"app:theme": "dark", "app:tier": "pro"}
            assert await adk_store.get_user_state("agent-app", "user-a") == {"user:lang": "en", "user:tz": "UTC"}
            with pytest.raises(ValueError, match="does not match"):
                await adk_store.append_event_and_update_state(
                    {
                        "id": "ev-bad",
                        "app_name": "other-app",
                        "user_id": "user-a",
                        "session_id": "sess-delta",
                        "invocation_id": "inv-bad",
                        "timestamp": datetime.now(UTC),
                        "event_data": {},
                    },
                    "agent-app",
                    "user-a",
                    "sess-delta",
                    state={},
                )
        else:
            cfg = SpannerSyncConfig(
                project_id="test-proj",
                instance_id="test-inst",
                database_id="spanner-adk-db",
                connection_instance=core,
                share_pool=False,
                extension_config={"adk": adk_ext},
            )
            adk_store = SpannerSyncADKStore(cfg)
            mem_store = SpannerSyncADKMemoryStore(cfg)
            adk_store.create_tables()
            mem_store.create_tables()
            created = adk_store.create_session(
                "sess-delta",
                "agent-app",
                "user-a",
                state={"local": "v1"},
                app_state_delta={"app:theme": "dark"},
                user_state_delta={"user:lang": "en"},
            )
            assert created["state"] == {"local": "v1", "app:theme": "dark", "user:lang": "en"}
            updated = adk_store.append_event_and_update_state(
                {
                    "id": "ev-delta-1",
                    "app_name": "agent-app",
                    "user_id": "user-a",
                    "session_id": "sess-delta",
                    "invocation_id": "inv-delta-1",
                    "timestamp": datetime.now(UTC),
                    "event_data": {"author": "user"},
                },
                "agent-app",
                "user-a",
                "sess-delta",
                state={"local": "v2"},
                app_state_delta={"app:tier": "pro"},
                user_state_delta={"user:tz": "UTC"},
            )
            assert updated["state"] == {
                "local": "v2",
                "app:theme": "dark",
                "app:tier": "pro",
                "user:lang": "en",
                "user:tz": "UTC",
            }
            assert adk_store.get_app_state("agent-app") == {"app:theme": "dark", "app:tier": "pro"}
            assert adk_store.get_user_state("agent-app", "user-a") == {"user:lang": "en", "user:tz": "UTC"}
            with pytest.raises(ValueError, match="does not match"):
                adk_store.append_event_and_update_state(
                    {
                        "id": "ev-bad",
                        "app_name": "other-app",
                        "user_id": "user-a",
                        "session_id": "sess-delta",
                        "invocation_id": "inv-bad",
                        "timestamp": datetime.now(UTC),
                        "event_data": {},
                    },
                    "agent-app",
                    "user-a",
                    "sess-delta",
                    state={},
                )

        t0 = datetime(2026, 11, 17, 1, 0, tzinfo=UTC)
        t1 = datetime(2026, 11, 17, 2, 0, tzinfo=UTC)
        entries: list[Any] = [
            {
                "id": "mem-1",
                "session_id": "sess-1",
                "app_name": "agent-app",
                "user_id": "user-a",
                "scope": "user",
                "event_id": "e1",
                "author": "user",
                "timestamp": t0,
                "content_json": {"text": "alpha spanner memory"},
                "content_text": "alpha spanner memory",
                "metadata_json": None,
                "inserted_at": t0,
                "embedding": [1.0, 0.0, 0.0, 0.0],
            },
            {
                "id": "mem-2",
                "session_id": "sess-1",
                "app_name": "agent-app",
                "user_id": "user-a",
                "scope": "user",
                "event_id": "e2",
                "author": "user",
                "timestamp": t1,
                "content_json": {"text": "alpha alpha spanner memory newer"},
                "content_text": "alpha alpha spanner memory newer",
                "metadata_json": None,
                "inserted_at": t1,
                "embedding": [0.0, 1.0, 0.0, 0.0],
            },
            {
                "id": "mem-other-user",
                "session_id": "sess-1",
                "app_name": "agent-app",
                "user_id": "user-b",
                "scope": "user",
                "event_id": "e3",
                "author": "user",
                "timestamp": t1,
                "content_json": {"text": "alpha other user session"},
                "content_text": "alpha other user session",
                "metadata_json": None,
                "inserted_at": t1,
                "embedding": [0.0, 1.0, 0.0, 0.0],
            },
        ]

        client.executed_queries.clear()
        if is_async_mode:
            await mem_store.insert_memory_entries(entries)
            vec_only = await mem_store.search_entries(
                "", "agent-app", "user-a", limit=2, embedding=[0.9, 0.1, 0.0, 0.0]
            )
            fts_only = await mem_store.search_entries("alpha", "agent-app", "user-a", limit=2)
            rrf_hits = await mem_store.hybrid_rrf_search(
                app_name="agent-app",
                user_id="user-a",
                query="alpha",
                embedding=[0.9, 0.1, 0.0, 0.0],
                top_k=2,
            )
            deleted = await mem_store.delete_entries_by_session("sess-1", "agent-app", "user-a")
            remaining_other = await mem_store.search_entries("other", "agent-app", "user-b", limit=2)
        else:
            mem_store.insert_memory_entries(entries)
            vec_only = mem_store.search_entries("", "agent-app", "user-a", limit=2, embedding=[0.9, 0.1, 0.0, 0.0])
            fts_only = mem_store.search_entries("alpha", "agent-app", "user-a", limit=2)
            rrf_hits = mem_store.hybrid_rrf_search(
                app_name="agent-app",
                user_id="user-a",
                query="alpha",
                embedding=[0.9, 0.1, 0.0, 0.0],
                top_k=2,
            )
            deleted = mem_store.delete_entries_by_session("sess-1", "agent-app", "user-a")
            remaining_other = mem_store.search_entries("other", "agent-app", "user-b", limit=2)

        assert [hit["id"] for hit in vec_only] == ["mem-1", "mem-2"]
        assert [hit["id"] for hit in fts_only] == ["mem-2", "mem-1"]
        assert len(rrf_hits) == 2
        assert deleted == 2
        assert [hit["id"] for hit in remaining_other] == ["mem-other-user"]

        assert any(
            "APPROX_COSINE_DISTANCE" in sql and "ORDER BY distance ASC" in sql and "LIMIT @top_k" in sql
            for sql in client.executed_queries
        )
        assert any(
            "SEARCH(content_tokens, @query)" in sql and "ORDER BY SCORE(content_tokens, @query) DESC" in sql
            for sql in client.executed_queries
        )
        assert any(
            "WITH vector_matches AS" in sql
            and "RANK() OVER" in sql
            and "text_matches AS" in sql
            and "ORDER BY rrf_score DESC" in sql
            for sql in client.executed_queries
        )
    finally:
        core.close()


@pytest.mark.anyio
@pytest.mark.parametrize("dialect", ["googlesql", "spangres"])
async def test_spanner_adk_dialect_sql_shapes_and_missing_table_tolerance(
    dialect: Literal["googlesql", "spangres"],
) -> None:
    """Verify GoogleSQL and Spangres SQL ranking builders and missing-table empty returns."""

    class _MissingTableClient(_ClosableFakeSpannerClient):
        def query(
            self,
            sql: str,
            params_json: str | None = None,
            staleness_mode: str = "strong",
            staleness_ms: int = 0,
        ) -> dict[str, Any]:
            self.executed_queries.append(sql)
            raise RuntimeError("Table not found: adk_memory")

        def __init__(self) -> None:
            super().__init__()
            self.executed_queries: list[str] = []

    client = _MissingTableClient()
    core = SyncSpannerDriver(
        SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="missing-db", dialect=dialect),
        client=client,
    )
    try:
        cfg_cls = SpangresSyncConfig if dialect == "spangres" else SpannerSyncConfig
        async_cfg_cls = SpangresAsyncConfig if dialect == "spangres" else SpannerAsyncConfig
        sync_cfg = cfg_cls(
            project_id="test-proj",
            instance_id="test-inst",
            database_id="missing-db",
            connection_instance=core,
            share_pool=False,
            extension_config={"adk": {"use_fts": True}},
        )
        async_cfg = async_cfg_cls(
            project_id="test-proj",
            instance_id="test-inst",
            database_id="missing-db",
            connection_instance=core,
            share_pool=False,
            extension_config={"adk": {"use_fts": True}},
        )
        sync_mem = SpannerSyncADKMemoryStore(sync_cfg)
        async_mem = SpannerAsyncADKMemoryStore(async_cfg)

        vec_sql, _ = sync_mem.build_vector_search_sql(app_name="app", user_id="u1", top_k=3)
        fts_sql, _ = sync_mem.build_fts_search_sql(app_name="app", user_id="u1", query="hello", top_k=3)
        rrf_sql, _ = sync_mem.build_hybrid_rrf_search_sql(app_name="app", user_id="u1", query="hello", top_k=3)
        if dialect == "spangres":
            assert "spanner.cosine_distance" in vec_sql
            assert "plainto_tsquery" in fts_sql and "ts_rank" in fts_sql
            assert "WITH vector_matches AS" in rrf_sql and "spanner.cosine_distance" in rrf_sql
        else:
            assert "APPROX_COSINE_DISTANCE" in vec_sql
            assert "SEARCH(content_tokens, @query)" in fts_sql and "SCORE(content_tokens, @query)" in fts_sql
            assert "WITH vector_matches AS" in rrf_sql and "RANK() OVER" in rrf_sql

        assert sync_mem.search_entries("hello", "app", "u1", embedding=[0.1, 0.2]) == []
        assert (
            await async_mem.hybrid_rrf_search(app_name="app", user_id="u1", query="hello", embedding=[0.1, 0.2]) == []
        )
        if dialect == "spangres":
            assert any("$1" in sql for sql in client.executed_queries)
        else:
            assert any("@app_name" in sql for sql in client.executed_queries)
    finally:
        core.close()


class _ReconcileFakeSpannerClient(_StoreFakeSpannerClient):
    """Recording Spanner fake tracking non-transactional DDL batches, commits, and catalog column types."""

    def __init__(self) -> None:
        super().__init__()
        self.commit_calls: int = 0
        self.ddl_batches: list[list[str]] = []
        self._table_columns: dict[str, list[dict[str, Any]]] = {}

    def commit(self) -> None:
        self.commit_calls += 1

    def _record_table_schema(self, stmt: str) -> None:
        m_create = re.match(
            r"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?([A-Za-z0-9_]+)`?\s*\((.*)",
            stmt,
            re.IGNORECASE | re.DOTALL,
        )
        if m_create:
            tname = m_create.group(1)
            body = m_create.group(2)
            depth = 1
            end_pos = len(body)
            for idx, ch in enumerate(body):
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        end_pos = idx
                        break
            inner = body[:end_pos]
            parts: list[str] = []
            cur_buf: list[str] = []
            pdepth = 0
            for ch in inner:
                if ch == "(":
                    pdepth += 1
                elif ch == ")":
                    pdepth -= 1
                elif ch == "," and pdepth == 0:
                    parts.append("".join(cur_buf).strip())
                    cur_buf.clear()
                    continue
                cur_buf.append(ch)
            if cur_buf:
                parts.append("".join(cur_buf).strip())
            cols: list[dict[str, Any]] = []
            ordinal = 0
            for part in parts:
                upper_part = part.upper().lstrip()
                if not part or upper_part.startswith(("PRIMARY KEY", "CONSTRAINT", "FOREIGN KEY", "CHECK", "UNIQUE")):
                    continue
                tokens = part.split(None, 1)
                if len(tokens) < 2:
                    continue
                ordinal += 1
                col_name = tokens[0].strip('`"')
                rest = tokens[1]
                is_nullable = "NO" if re.search(r"\bNOT\s+NULL\b", rest, re.IGNORECASE) else "YES"
                type_match = re.match(
                    r"(ARRAY\s*<[^>]+>(?:\([^)]*\))?|STRING\s*\([^)]*\)|BYTES\s*\([^)]*\)|"
                    r"VARCHAR\s*\([^)]*\)|DOUBLE\s+PRECISION|TOKENLIST|TIMESTAMPTZ|TIMESTAMP|"
                    r"INT64|BIGINT|FLOAT64|FLOAT32|REAL|BOOL|BOOLEAN|JSONB|JSON|BYTEA|TEXT)",
                    rest.strip(),
                    re.IGNORECASE,
                )
                dtype = type_match.group(1).upper() if type_match else "STRING(MAX)"
                dtype = re.sub(r"\(\s*vector_length\s*=>\s*\d+\s*\)", "", dtype, flags=re.IGNORECASE)
                is_generated = "ALWAYS" if re.search(r"\bAS\s*\(", rest, re.IGNORECASE) else "NEVER"
                cols.append(
                    {
                        "table_name": tname,
                        "column_name": col_name,
                        "ordinal_position": ordinal,
                        "is_nullable": is_nullable,
                        "spanner_type": dtype,
                        "is_generated": is_generated,
                        "generation_expression": None,
                    }
                )
            self._table_columns[tname] = cols
            return

        m_alter = re.match(
            r"^\s*ALTER\s+TABLE\s+`?([A-Za-z0-9_]+)`?\s+ADD\s+COLUMN\s+`?([A-Za-z0-9_]+)`?\s+(.+)$",
            stmt,
            re.IGNORECASE | re.DOTALL,
        )
        if m_alter:
            tname, col_name, rest = m_alter.group(1), m_alter.group(2), m_alter.group(3).strip()
            existing = self._table_columns.setdefault(tname, [])
            is_nullable = "NO" if re.search(r"\bNOT\s+NULL\b", rest, re.IGNORECASE) else "YES"
            dtype = rest.split()[0].upper()
            existing.append(
                {
                    "table_name": tname,
                    "column_name": col_name,
                    "ordinal_position": len(existing) + 1,
                    "is_nullable": is_nullable,
                    "spanner_type": dtype,
                    "is_generated": "NEVER",
                    "generation_expression": None,
                }
            )

    def update_ddl(self, statements: Sequence[str]) -> str:
        batch = [stmt.strip().rstrip(";") for stmt in statements if stmt.strip()]
        self.ddl_batches.append(batch)
        for stmt in batch:
            self._record_table_schema(stmt)
        return super().update_ddl(statements)

    def _try_information_schema(self, sql: str, params_json: str | None) -> dict[str, Any] | None:
        if "INFORMATION_SCHEMA.COLUMNS" in sql.upper():
            params_map = self._decode_params_map(params_json)
            table_filter = str(params_map.get("table_name") or params_map.get("p2") or "")
            rows_out: list[list[Any]] = []
            for tname, cols in self._table_columns.items():
                if table_filter and tname.lower() != table_filter.lower():
                    continue
                for col in cols:
                    rows_out.append(
                        [
                            col["table_name"],
                            col["column_name"],
                            col["ordinal_position"],
                            col["is_nullable"],
                            col["spanner_type"],
                            col["is_generated"],
                            col["generation_expression"],
                        ]
                    )
            return {
                "columns": [
                    "table_name",
                    "column_name",
                    "ordinal_position",
                    "is_nullable",
                    "spanner_type",
                    "is_generated",
                    "generation_expression",
                ],
                "rows": rows_out,
            }
        return super()._try_information_schema(sql, params_json)


@pytest.mark.parametrize("dialect", ["googlesql", "spangres"])
@pytest.mark.parametrize("store_cls", [SpannerEventQueueStore, SpannerLitestarStore])
def test_spanner_events_litestar_slots_and_reconcile_hooks(
    store_cls: type[Any],
    dialect: Literal["googlesql", "spangres"],
) -> None:
    """Verify Spanner event/Litestar stores declare ``__slots__``, schema targets, indexes, and drops."""
    assert "__slots__" in store_cls.__dict__
    cfg_cls = SpangresSyncConfig if dialect == "spangres" else SpannerSyncConfig
    cfg = cfg_cls(
        project_id="test-proj",
        instance_id="test-inst",
        database_id="test-db",
        extension_config={
            "litestar": {"session_table": "vtb_sessions"},
            "events": {"queue_table": "vtb_events"},
        },
    )
    store = store_cls(cfg)
    assert not hasattr(store, "__dict__")

    targets = store.get_schema_targets()
    assert len(targets) == 1
    target = targets[0]
    assert isinstance(target.create_statement, str)

    drops = [d.statement for d in store.downgrade_drops()]
    assert len(drops) == 2
    assert drops[0].startswith("DROP INDEX IF EXISTS ")
    assert drops[1].startswith("DROP TABLE IF EXISTS ")

    if store_cls is SpannerEventQueueStore:
        assert target.table_name == "vtb_events"
        companion = store.companion_index_statement()
        assert companion is not None
        if dialect == "spangres":
            assert "INCLUDE (" in companion
            assert "TTL INTERVAL" in target.create_statement
        else:
            assert "STORING (" in companion
            assert "ROW DELETION POLICY" in target.create_statement
        assert drops == ["DROP INDEX IF EXISTS idx_vtb_events_channel_status", "DROP TABLE IF EXISTS vtb_events"]
    else:
        assert target.table_name == "vtb_sessions"
        indexes = store.get_companion_indexes()
        assert len(indexes) == 1
        assert isinstance(indexes[0], CompanionIndex)
        assert indexes[0].name == "idx_vtb_sessions_expires_at"
        if dialect == "spangres":
            assert "WHERE expires_at IS NOT NULL" in indexes[0].ddl
            assert "TTL INTERVAL" in target.create_statement
        else:
            assert "CREATE NULL_FILTERED INDEX IF NOT EXISTS" in indexes[0].ddl
            assert "ROW DELETION POLICY" in target.create_statement
        assert drops == ["DROP INDEX IF EXISTS idx_vtb_sessions_expires_at", "DROP TABLE IF EXISTS vtb_sessions"]

    assert spanner_reconcile.down is spanner_reconcile.downgrade
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import vertebra.extensions.sqlspec.adapters.spanner.reconcile; "
                "assert 'google.adk' not in sys.modules; "
                "assert 'vertebra.extensions.sqlspec.adapters.spanner.adk' not in sys.modules"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0


@pytest.mark.anyio
@pytest.mark.parametrize("dialect", ["googlesql", "spangres"])
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_spanner_reconcile_and_downgrade_litestar_events_adk(
    dialect: Literal["googlesql", "spangres"],
    is_async_mode: bool,
) -> None:
    """Verify Spanner GoogleSQL and Spangres reconcile, idempotent companion indexes, and reverse-order drops."""
    client = _ReconcileFakeSpannerClient()
    core = SyncSpannerDriver(
        SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="reconcile-db", dialect=dialect),
        client=client,
    )
    ext_cfg: dict[str, Any] = {
        "litestar": {"session_table": "vtb_sessions"},
        "events": {"queue_table": "vtb_events"},
        "adk": {
            "session_table": "vertebra_adk_sessions",
            "events_table": "vertebra_adk_events",
            "memory_table": "vertebra_adk_memories",
            "vector_dimensions": 4,
        },
    }
    try:
        if is_async_mode:
            acfg_cls = SpangresAsyncConfig if dialect == "spangres" else SpannerAsyncConfig
            acfg: Any = acfg_cls(
                project_id="test-proj",
                instance_id="test-inst",
                database_id="reconcile-db",
                connection_instance=core,
                share_pool=False,
                extension_config=ext_cfg,
            )
            async with acfg.provide_session(transaction=False) as adriver:
                awaitable = spanner_reconcile.reconcile(MigrationContext(config=acfg, driver=adriver))
                assert awaitable is not None
                await awaitable

                executed = list(client.ddl_history)
                assert any("CREATE TABLE IF NOT EXISTS vtb_sessions" in s for s in executed)
                assert any("CREATE TABLE IF NOT EXISTS vtb_events" in s for s in executed)
                assert any("CREATE TABLE IF NOT EXISTS vertebra_adk_sessions" in s for s in executed)
                assert any("CREATE TABLE IF NOT EXISTS vertebra_adk_events" in s for s in executed)
                assert any("CREATE TABLE IF NOT EXISTS vertebra_adk_memories" in s for s in executed)
                if dialect == "spangres":
                    assert any(
                        "CREATE INDEX IF NOT EXISTS idx_vtb_sessions_expires_at" in s
                        and "WHERE expires_at IS NOT NULL" in s
                        for s in executed
                    )
                    assert any(
                        "CREATE INDEX IF NOT EXISTS idx_vtb_events_channel_status" in s and "INCLUDE (" in s
                        for s in executed
                    )
                else:
                    assert any(
                        "CREATE NULL_FILTERED INDEX IF NOT EXISTS idx_vtb_sessions_expires_at" in s for s in executed
                    )
                    assert any(
                        "CREATE INDEX IF NOT EXISTS idx_vtb_events_channel_status" in s and "STORING (" in s
                        for s in executed
                    )
                    assert any("CREATE VECTOR INDEX IF NOT EXISTS vertebra_adk_memories_vec_idx" in s for s in executed)
                    assert any("CREATE OR REPLACE PROPERTY GRAPH vertebra_adk_memories_graph" in s for s in executed)
                assert client.commit_calls == 0

                client.ddl_batches.clear()
                second_awaitable = spanner_reconcile.reconcile(MigrationContext(config=acfg, driver=adriver))
                assert second_awaitable is not None
                await second_awaitable
                assert not any("CREATE INDEX" in stmt for batch in client.ddl_batches for stmt in batch)

                client.ddl_batches.clear()
                down_awaitable = spanner_reconcile.downgrade(MigrationContext(config=acfg, driver=adriver))
                assert down_awaitable is not None
                await down_awaitable
        else:
            scfg_cls = SpangresSyncConfig if dialect == "spangres" else SpannerSyncConfig
            scfg: Any = scfg_cls(
                project_id="test-proj",
                instance_id="test-inst",
                database_id="reconcile-db",
                connection_instance=core,
                share_pool=False,
                extension_config=ext_cfg,
            )
            with scfg.provide_session(transaction=False) as sdriver:
                assert spanner_reconcile.reconcile(MigrationContext(config=scfg, driver=sdriver)) is None

                executed = list(client.ddl_history)
                assert any("CREATE TABLE IF NOT EXISTS vtb_sessions" in s for s in executed)
                assert any("CREATE TABLE IF NOT EXISTS vtb_events" in s for s in executed)
                assert any("CREATE TABLE IF NOT EXISTS vertebra_adk_sessions" in s for s in executed)
                assert any("CREATE TABLE IF NOT EXISTS vertebra_adk_events" in s for s in executed)
                assert any("CREATE TABLE IF NOT EXISTS vertebra_adk_memories" in s for s in executed)
                if dialect == "spangres":
                    assert any(
                        "CREATE INDEX IF NOT EXISTS idx_vtb_sessions_expires_at" in s
                        and "WHERE expires_at IS NOT NULL" in s
                        for s in executed
                    )
                    assert any(
                        "CREATE INDEX IF NOT EXISTS idx_vtb_events_channel_status" in s and "INCLUDE (" in s
                        for s in executed
                    )
                else:
                    assert any(
                        "CREATE NULL_FILTERED INDEX IF NOT EXISTS idx_vtb_sessions_expires_at" in s for s in executed
                    )
                    assert any(
                        "CREATE INDEX IF NOT EXISTS idx_vtb_events_channel_status" in s and "STORING (" in s
                        for s in executed
                    )
                    assert any("CREATE VECTOR INDEX IF NOT EXISTS vertebra_adk_memories_vec_idx" in s for s in executed)
                    assert any("CREATE OR REPLACE PROPERTY GRAPH vertebra_adk_memories_graph" in s for s in executed)
                assert client.commit_calls == 0

                client.ddl_batches.clear()
                assert spanner_reconcile.reconcile(MigrationContext(config=scfg, driver=sdriver)) is None
                assert not any("CREATE INDEX" in stmt for batch in client.ddl_batches for stmt in batch)

                client.ddl_batches.clear()
                assert spanner_reconcile.downgrade(MigrationContext(config=scfg, driver=sdriver)) is None

        drop_stmts = [stmt for batch in client.ddl_batches for stmt in batch if stmt.startswith("DROP ")]
        expected_drops = [
            "DROP INDEX IF EXISTS idx_vtb_events_channel_status",
            "DROP TABLE IF EXISTS vtb_events",
        ]
        if dialect == "googlesql":
            expected_drops.extend(
                [
                    "DROP PROPERTY GRAPH IF EXISTS vertebra_adk_memories_graph",
                    "DROP VECTOR INDEX IF EXISTS vertebra_adk_memories_vec_idx",
                ]
            )
        expected_drops.extend(
            [
                "DROP TABLE IF EXISTS vertebra_adk_memories",
                "DROP TABLE IF EXISTS vertebra_adk_events",
                "DROP TABLE IF EXISTS vertebra_adk_sessions",
                "DROP INDEX IF EXISTS idx_vtb_sessions_expires_at",
                "DROP TABLE IF EXISTS vtb_sessions",
            ]
        )
        assert drop_stmts == expected_drops
    finally:
        core.close()


def test_spanner_add_extension_migrations_runner_discovery_and_errors(tmp_path: Path) -> None:
    """Verify ``add_extension_migrations`` discovers Spanner reconciler, additive ALTER, and error contracts."""
    migrations_dir = tmp_path / "spanner_migrations"
    migrations_dir.mkdir()
    client = _ReconcileFakeSpannerClient()
    client.update_ddl(
        [
            "CREATE TABLE vtb_sessions ("
            "shard_id INT64 NOT NULL, "
            "session_id STRING(256) NOT NULL, "
            "data BYTES(MAX) NOT NULL"
            ") PRIMARY KEY (shard_id, session_id)"
        ]
    )
    core = SyncSpannerDriver(
        SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="runner-db"),
        client=client,
    )
    try:
        cfg = SpannerSyncConfig(
            project_id="test-proj",
            instance_id="test-inst",
            database_id="runner-db",
            connection_instance=core,
            share_pool=False,
            migration_config={"script_location": str(migrations_dir)},
            extension_config={
                "litestar": {"session_table": "vtb_sessions"},
                "events": {"queue_table": "vtb_events"},
                "adk": {"session_table": "vertebra_adk_sessions", "events_table": "vertebra_adk_events"},
            },
        )
        cfg.add_extension_migrations(
            "litestar",
            "vertebra.extensions.sqlspec:adapters/spanner",
            migration_mode="reconcile",
        )
        cfg.add_extension_migrations(
            "events",
            "vertebra.extensions.sqlspec:adapters/spanner",
            migration_mode="reconcile",
        )
        cfg.add_extension_migrations(
            "adk",
            "vertebra.extensions.sqlspec:adapters/spanner",
            migration_mode="reconcile",
        )
        commands = cfg.get_migration_commands()
        assert {desc.name for desc in commands.extension_reconcilers} == {"litestar", "events", "adk"}
        assert all(desc.module_path.name == "reconcile.py" for desc in commands.extension_reconcilers)
        assert isinstance(commands.runner, SyncMigrationRunner)

        with cfg.provide_session(transaction=False) as driver:
            for desc in commands.extension_reconcilers:
                commands.runner.execute_reconciliation(driver, desc)

            assert any(
                "ALTER TABLE `vtb_sessions`" in sql and "ADD COLUMN `expires_at` TIMESTAMP" in sql
                for sql in client.ddl_history
            )
            assert any(
                "CREATE NULL_FILTERED INDEX IF NOT EXISTS idx_vtb_sessions_expires_at" in s for s in client.ddl_history
            )
            assert any("CREATE TABLE IF NOT EXISTS vtb_events" in s for s in client.ddl_history)
            assert any("CREATE TABLE IF NOT EXISTS vertebra_adk_sessions" in s for s in client.ddl_history)

            async_cfg = SpannerAsyncConfig(project_id="test-proj", instance_id="test-inst", database_id="runner-db")
            async_driver = SQLSpecAsyncSpannerDriver(FakeAsyncSpannerClient())
            for bad_ctx in (
                MigrationContext(config=None, driver=driver),
                MigrationContext(config=cfg, driver=None),
                MigrationContext(config=cfg, driver=driver, extension_config={"migration_mode": "invalid"}),
                MigrationContext(config=async_cfg, driver=driver),
                MigrationContext(config=cfg, driver=async_driver),
                MigrationContext(config=SQLiteSyncConfig(database=":memory:"), driver=driver),
            ):
                with pytest.raises(MigrationError):
                    spanner_reconcile.reconcile(bad_ctx)
                with pytest.raises(MigrationError):
                    spanner_reconcile.downgrade(bad_ctx)
    finally:
        core.close()
