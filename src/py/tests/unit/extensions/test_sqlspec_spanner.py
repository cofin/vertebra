"""Unit tests for vertebra.extensions.sqlspec.spanner adapter, session modes, data dictionary, and zero-SDK stores."""

from __future__ import annotations

import inspect
import threading
from typing import Any, Literal, cast

import pytest
from sqlspec import StatementStack

from tests.unit.db.test_spanner import FakeAsyncSpannerClient, FakeSpannerClient
from vertebra.db import NotFoundError, NotSupportedError
from vertebra.db.adapters.spanner import AsyncSpannerDriver, SpannerConfig, StalenessBound, SyncSpannerDriver
from vertebra.db.rls import TenantPredicate
from vertebra.extensions.sqlspec.adapters import (
    AsyncAdapterConfig,
    BaseAsyncDriver,
    BaseSyncDriver,
    SyncAdapterConfig,
)
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
from vertebra.extensions.sqlspec.spanner import (
    SpannerAsyncDriver as SQLSpecAsyncSpannerDriver,
)
from vertebra.extensions.sqlspec.spanner import (
    SpannerSyncDriver as SQLSpecSyncSpannerDriver,
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
            [
                "SELECT user_id, order_id, total FROM child_orders",
                "SELECT COUNT(*) AS cnt FROM child_orders",
            ]
        )
        assert len(stack_out[0]) == 2
        assert {row["order_id"] for row in stack_out[0]} == {"o1", "o2"}
        assert stack_out[1] == [{"cnt": 2}]

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


class _ArrowOptionsSpannerClient(FakeSpannerClient):
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
    kwargs = {} if dialect == "spangres" else {"id": 1}
    try:
        core.update_ddl(["CREATE TABLE stack_items (id INT64 NOT NULL) PRIMARY KEY (id)"])
        assert driver.execute_stack(
            ["INSERT INTO stack_items (id) VALUES (1)", "INSERT INTO stack_items (id) VALUES (2)"]
        ) == [1, 1]
        result = driver.execute_stack(
            StatementStack()
            .push_execute(select_sql, *args, **kwargs)
            .push_execute("SELECT COUNT(*) AS n FROM stack_items")
        )
        assert result[0].get_result().get_data() == [{"id": 1}]
        assert result[1].get_result().get_data() == [{"n": 2}]
        single = driver.execute_stack(
            StatementStack().push_execute(
                select_sql, *((2,) if dialect == "spangres" else ()), **({} if dialect == "spangres" else {"id": 2})
            )
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
        client=FakeAsyncSpannerClient(client),
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
    kwargs = {} if dialect == "spangres" else {"id": 1}
    try:
        await core.update_ddl(["CREATE TABLE stack_items (id INT64 NOT NULL) PRIMARY KEY (id)"])
        assert await driver.execute_stack(
            ["INSERT INTO stack_items (id) VALUES (1)", "INSERT INTO stack_items (id) VALUES (2)"]
        ) == [1, 1]
        result = await driver.execute_stack(
            StatementStack()
            .push_execute(select_sql, *args, **kwargs)
            .push_execute("SELECT COUNT(*) AS n FROM stack_items")
        )
        assert result[0].get_result().get_data() == [{"id": 1}]
        assert result[1].get_result().get_data() == [{"n": 2}]
        single = await driver.execute_stack(
            StatementStack().push_execute(
                select_sql, *((2,) if dialect == "spangres" else ()), **({} if dialect == "spangres" else {"id": 2})
            )
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
    constructors = {
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


class _StalenessAndThreadTrackingSpannerClient(FakeSpannerClient):
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

    def commit_mutations(
        self,
        mutations_json: str,
        max_commit_delay_ms: int = 0,
    ) -> dict[str, Any]:
        self.call_threads.append(threading.get_ident())
        return super().commit_mutations(mutations_json, max_commit_delay_ms=max_commit_delay_ms)

    def update_ddl(self, statements: list[str]) -> str:
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
        client=FakeAsyncSpannerClient(async_raw),
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
    """SpannerAsyncStore and SpannerAsyncADKStore offload synchronous driver operations onto worker threads."""
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
        kv_store = SpannerAsyncStore(config)
        tracking_client.call_threads.clear()
        await kv_store.create_tables()
        await kv_store.set("sess-1", b"val-1", expires_in=60)
        assert await kv_store.get("sess-1") == b"val-1"
        assert tracking_client.call_threads
        assert set(tracking_client.call_threads).isdisjoint({loop_tid})

        adk_store = SpannerAsyncADKStore(config)
        tracking_client.call_threads.clear()
        await adk_store.create_tables()
        created = await adk_store.create_session(app_name="demo", user_id="u1", state={"step": 1})
        fetched = await adk_store.get_session(created["session_id"])
        assert fetched is not None
        assert tracking_client.call_threads
        assert set(tracking_client.call_threads).isdisjoint({loop_tid})
    finally:
        sync_core.close()


@pytest.mark.anyio
async def test_sqlspec_spanner_batch_write_rejects_max_commit_delay_ms() -> None:
    """SQLSpec sync and async Spanner drivers reject max_commit_delay_ms on batch_write."""
    sync_backing = FakeSpannerClient()
    sync_core = SyncSpannerDriver(SpannerConfig(), client=sync_backing)
    async_core = AsyncSpannerDriver(SpannerConfig(), client=FakeAsyncSpannerClient(sync_backing))
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
