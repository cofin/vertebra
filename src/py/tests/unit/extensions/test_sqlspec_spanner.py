"""Unit tests for vertebra.extensions.sqlspec.spanner adapter, session modes, data dictionary, and zero-SDK stores."""

from __future__ import annotations

import pytest

from tests.unit.db.test_spanner import FakeSpannerClient
from vertebra.db.adapters.spanner import SpannerConfig, StalenessBound, SyncSpannerDriver
from vertebra.extensions.sqlspec.spanner import (
    SpannerAsyncADKArtifactStore,
    SpannerAsyncADKMemoryStore,
    SpannerAsyncADKStore,
    SpannerAsyncConfig,
    SpannerAsyncEventQueueStore,
    SpannerAsyncStore,
    SpannerSyncADKArtifactStore,
    SpannerSyncADKMemoryStore,
    SpannerSyncADKStore,
    SpannerSyncConfig,
    SpannerSyncEventQueueStore,
    SpannerSyncStore,
)
from vertebra.typing import ImproperConfigurationError


def test_spanner_sync_config_sessions_connection_shim_and_data_dictionary() -> None:
    """Verify SpannerSyncConfig ReadOnly vs ReadWrite sessions, SpannerConnectionShim, and SpannerDataDictionary."""
    core_driver = SyncSpannerDriver(
        SpannerConfig(
            project_id="test-proj",
            instance_id="test-inst",
            database_id="sqlspec-sync-db",
        ),
        client=FakeSpannerClient(),
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
            [
                "SELECT user_id, order_id, total FROM child_orders",
                "SELECT COUNT(*) AS cnt FROM step_0",
            ]
        )
        assert stack_out[0][0]["cnt"] == 2

        dd = rw_session.data_dictionary
        tables = dd.get_tables()
        assert any(t["table_name"] == "parent_users" for t in tables)
        interleaved = dd.get_interleaved_tables("parent_users")
        assert len(interleaved) == 1
        assert interleaved[0]["table_name"] == "child_orders"
        assert interleaved[0]["parent_table_name"] == "parent_users"
        assert interleaved[0]["on_delete_action"] == "CASCADE"

        cols = dd.get_columns("child_orders")
        assert any(c["column_name"] == "total" for c in cols)
        idxs = dd.get_indexes("child_orders")
        assert any(i["index_name"] == "idx_child_orders_total" for i in idxs)

        export_res = rw_session.select_to_storage(
            "SELECT user_id, order_id, total FROM child_orders ORDER BY order_id ASC",
            "backups/orders.jsonl",
            format="jsonl",
        )
        assert export_res["rows_exported"] == 2

    pg_cfg = SpannerSyncConfig(
        dsn="spangres://proj/inst/db?endpoint=http://127.0.0.1:9010",
    )
    pg_dd = pg_cfg.provide_driver().data_dictionary
    pg_sql, _ = pg_dd.build_tables_sql()
    assert "information_schema.tables" in pg_sql
    shim.close()


def test_spanner_sync_stores_adk_memory_artifact_kv_and_event_queue() -> None:
    """Verify all 5 synchronous SQLSpec Spanner stores (ADK, Memory, Artifact, KV Store, EventQueue)."""
    core_driver = SyncSpannerDriver(
        SpannerConfig(
            project_id="test-proj",
            instance_id="test-inst",
            database_id="stores-sync-db",
        ),
        client=FakeSpannerClient(),
    )
    config = SpannerSyncConfig(
        project_id="test-proj",
        instance_id="test-inst",
        database_id="stores-sync-db",
        storage_backend="memory://",
        connection_instance=core_driver,
    )

    adk_store = SpannerSyncADKStore(config)
    ddls = adk_store.ddl_statements()
    assert "INTERLEAVE IN PARENT" in ddls[1]
    assert "ON DELETE CASCADE" in ddls[1]
    assert "ROW DELETION POLICY" in ddls[1]
    adk_store.create_tables()
    created = adk_store.create_session(app_name="agent_app", user_id="user_1", state={"step": 1})
    sid = created["session_id"]
    adk_store.append_event(sid, author="user", content={"text": "hello"}, actions={"route": "plan"})
    adk_store.update_session_state(sid, {"step": 2})
    loaded_session = adk_store.get_session(sid)
    assert loaded_session is not None
    assert loaded_session["state"] == {"step": 2}
    assert len(loaded_session["events"]) == 1
    assert len(adk_store.list_sessions(app_name="agent_app", user_id="user_1")) == 1
    assert adk_store.delete_session(sid) is True

    mem_store = SpannerSyncADKMemoryStore(config, vector_dimensions=4)
    mem_ddls = mem_store.ddl_statements()
    assert any("ARRAY<FLOAT32>(vector_length=>4)" in s for s in mem_ddls)
    assert any("tree_type = 'SCANN'" in s for s in mem_ddls)
    assert any("TOKENIZE_FULLTEXT" in s for s in mem_ddls)
    assert any("CREATE OR REPLACE PROPERTY GRAPH" in s for s in mem_ddls)
    mem_store.create_tables()
    mem_store.store_memory(
        memory_id="m1",
        app_name="agent_app",
        user_id="user_1",
        content="Cloud Spanner supports TrueTime external consistency",
        embedding=[1.0, 0.0, 0.0, 0.0],
        entity_links=["spanner", "truetime"],
    )
    batch_saved = mem_store.store_memories_batch(
        [
            {
                "memory_id": "m1",
                "app_name": "agent_app",
                "user_id": "user_1",
                "content": "Cloud Spanner supports TrueTime external consistency",
                "embedding": [1.0, 0.0, 0.0, 0.0],
                "entity_links": ["spanner", "truetime"],
            },
            {
                "memory_id": "m2",
                "app_name": "agent_app",
                "user_id": "user_1",
                "content": "BigQuery executes columnar Arrow Storage Read streams",
                "embedding": [0.0, 1.0, 0.0, 0.0],
                "entity_links": ["bigquery", "arrow"],
            },
        ]
    )
    assert batch_saved[0]["deduplicated"] is True
    assert batch_saved[1]["deduplicated"] is False

    vec_hits = mem_store.search_memory("user_1", embedding=[1.0, 0.0, 0.0, 0.0], mode="vector")
    assert vec_hits[0]["memory_id"] == "m1"
    fts_hits = mem_store.search_memory("user_1", query="TrueTime consistency", mode="fts")
    assert fts_hits[0]["memory_id"] == "m1"
    rrf_hits = mem_store.search_memory(
        "user_1",
        query="TrueTime",
        embedding=[1.0, 0.0, 0.0, 0.0],
        mode="hybrid_rrf",
        rrf_k=60,
    )
    assert rrf_hits[0]["memory_id"] == "m1"
    assert rrf_hits[0]["rrf_score"] > 0
    graph_hits = mem_store.search_memory("user_1", entity_id="truetime", mode="graph")
    assert graph_hits[0]["memory_id"] == "m1"

    art_store = SpannerSyncADKArtifactStore(config)
    art_store.create_tables()
    saved_art = art_store.save_artifact("s1", "report.pdf", b"%PDF-1.7-content", content_type="application/pdf")
    assert saved_art["version"] == 1
    loaded_art = art_store.get_artifact("s1", "report.pdf")
    assert loaded_art is not None
    assert loaded_art["data"] == b"%PDF-1.7-content"
    assert loaded_art["object_ref"] is not None
    assert len(art_store.list_artifacts("s1")) == 1
    assert art_store.delete_artifact("s1", "report.pdf") is True

    kv_store = SpannerSyncStore(config)
    assert "ROW DELETION POLICY" in kv_store.ddl_statements()[0]
    kv_store.create_tables()
    kv_store.set("sess:abc", b"payload-bytes", expires_in=120)
    assert kv_store.exists("sess:abc") is True
    assert kv_store.get("sess:abc") == b"payload-bytes"
    ttl_rem = kv_store.expires_in("sess:abc")
    assert ttl_rem is not None and ttl_rem > 0
    assert kv_store.delete("sess:abc") is True
    assert kv_store.get("sess:abc") is None

    eq_store = SpannerSyncEventQueueStore(config, cleanup_on_ack=False)
    eq_ddls = eq_store.ddl_statements()
    assert "ROW DELETION POLICY" in eq_ddls[0]
    assert "STORING" in eq_ddls[1]
    eq_store.create_tables()
    pub = eq_store.publish("orders", {"order_id": "o-100"})
    pending = eq_store.consume("orders")
    assert len(pending) == 1
    assert pending[0]["event_id"] == pub["event_id"]
    assert eq_store.ack(pub["event_id"]) is True
    assert eq_store.consume("orders") == []


@pytest.mark.anyio
async def test_spanner_async_config_and_async_stores() -> None:
    """Verify SpannerAsyncConfig and all 5 async SQLSpec Spanner stores."""
    core_driver = SyncSpannerDriver(
        SpannerConfig(
            project_id="test-proj",
            instance_id="test-inst",
            database_id="stores-async-db",
        ),
        client=FakeSpannerClient(),
    )
    config = SpannerAsyncConfig(
        project_id="test-proj",
        instance_id="test-inst",
        database_id="stores-async-db",
        storage_backend="memory://",
        connection_instance=core_driver,
    )
    async with config.provide_session(transaction=False) as ro_session:
        assert ro_session.is_read_only is True

    adk = SpannerAsyncADKStore(config)
    await adk.create_tables()
    s = await adk.create_session(app_name="app", user_id="u1", state={"ok": True})
    await adk.append_event(s["session_id"], author="assistant", content={"reply": "done"})
    fetched = await adk.get_session(s["session_id"])
    assert fetched is not None and len(fetched["events"]) == 1

    mem = SpannerAsyncADKMemoryStore(config, vector_dimensions=4)
    await mem.create_tables()
    await mem.store_memory(
        app_name="app",
        user_id="u1",
        content="Spanner async vector search",
        embedding=[0.5, 0.5, 0.0, 0.0],
    )
    hits = await mem.search_memory("u1", query="Spanner", embedding=[0.5, 0.5, 0.0, 0.0], mode="hybrid_rrf")
    assert len(hits) == 1

    art = SpannerAsyncADKArtifactStore(config)
    await art.create_tables()
    await art.save_artifact(s["session_id"], "blob.bin", b"\x00\x01\x02")
    got_art = await art.get_artifact(s["session_id"], "blob.bin")
    assert got_art is not None and got_art["data"] == b"\x00\x01\x02"

    kv = SpannerAsyncStore(config)
    await kv.create_tables()
    await kv.set("k1", "v1", expires_in=60)
    assert await kv.get("k1") == b"v1"
    await kv.delete_all()

    eq = SpannerAsyncEventQueueStore(config)
    await eq.create_tables()
    evt = await eq.publish("notifications", {"msg": "hi"})
    items = await eq.consume("notifications")
    assert len(items) == 1
    assert await eq.ack(evt["event_id"]) is True
