"""Unit tests for BigQuery SQLSpec configs, drivers, data dictionary, and zero-SDK sub-extension stores."""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from typing import Any

import pytest
from sqlspec import StatementStack

from tests.unit.db.test_bigquery import _MockBigQueryRestServer
from vertebra.db import ArrowCursor, NotFoundError
from vertebra.db.adapters.bigquery import (
    AsyncBigQueryDriver as CoreAsyncBigQueryDriver,
)
from vertebra.db.adapters.bigquery import BigQueryConfig, BigQueryDryRunResult, ObjectTableConfig
from vertebra.db.adapters.bigquery import SyncBigQueryDriver as CoreSyncBigQueryDriver
from vertebra.extensions.sqlspec import (
    BigQueryAsyncADKArtifactStore,
    BigQueryAsyncADKMemoryStore,
    BigQueryAsyncADKStore,
    BigQueryAsyncConfig,
    BigQueryAsyncDriver,
    BigQueryAsyncEventQueueStore,
    BigQueryAsyncStore,
    BigQueryConnectionShim,
    BigQuerySyncADKArtifactStore,
    BigQuerySyncADKMemoryStore,
    BigQuerySyncADKStore,
    BigQuerySyncConfig,
    BigQuerySyncDriver,
    BigQuerySyncEventQueueStore,
    BigQuerySyncStore,
    ObjectStoreBackend,
)
from vertebra.storage import SigningConfig, SyncObjectStore


class _InMemoryBigQueryEngine:
    """In-memory GoogleSQL simulator backing sync and async BigQuery drivers for store unit tests."""

    def __init__(self, project_id: str = "test-proj", dataset_id: str = "app_ds") -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.config = BigQueryConfig(
            project_id=project_id,
            dataset_id=dataset_id,
            rest_endpoint="http://127.0.0.1:9050",
        )
        self.ddl_statements: list[str] = []
        self.executed_sql: list[tuple[str, Any]] = []
        self.sessions: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.memories: list[dict[str, Any]] = []
        self.artifacts: list[dict[str, Any]] = []
        self.kv_store: dict[str, dict[str, Any]] = {}
        self.event_queue: dict[str, dict[str, Any]] = {}
        self.metrics_rows: list[dict[str, Any]] = [
            {"id": 1, "name": "alpha", "score": 10.0},
            {"id": 2, "name": "beta", "score": 20.0},
        ]
        self.last_query_metadata: dict[str, Any] = {
            "job_id": "job-sim-1",
            "session_id": "sess-sim-1",
            "total_rows": 2,
            "total_bytes_processed": 1024,
            "total_bytes_billed": 1024,
            "cache_hit": False,
            "num_dml_affected_rows": 1,
        }

    def execute_sql(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> int:
        params = dict(parameters) if isinstance(parameters, Mapping) else {}
        self.executed_sql.append((sql, params))
        upper = sql.strip().upper()

        if upper.startswith("CREATE "):
            self.ddl_statements.append(sql)
            return 0

        if "VERTEBRA_ADK_SESSIONS" in upper:
            if upper.startswith("INSERT "):
                sid = str(params["session_id"])
                self.sessions[sid] = dict(params)
                return 1
            if upper.startswith("UPDATE "):
                sid = str(params["session_id"])
                if sid in self.sessions:
                    self.sessions[sid]["state_json"] = params["state_json"]
                    self.sessions[sid]["updated_at"] = params["updated_at"]
                    return 1
                return 0
            if upper.startswith("DELETE "):
                sid = str(params.get("session_id", ""))
                self.sessions.pop(sid, None)
                return 1

        if "VERTEBRA_ADK_EVENTS" in upper:
            if upper.startswith("INSERT "):
                self.events.append(dict(params))
                return 1
            if upper.startswith("DELETE "):
                sid = str(params.get("session_id", ""))
                self.events = [e for e in self.events if e.get("session_id") != sid]
                return 1

        if "VERTEBRA_ADK_MEMORIES" in upper and upper.startswith("INSERT "):
            self.memories.append(dict(params))
            return 1

        if "VERTEBRA_ADK_ARTIFACTS" in upper:
            if upper.startswith("INSERT "):
                self.artifacts.append(dict(params))
                return 1
            if upper.startswith("DELETE "):
                sid = str(params.get("session_id", ""))
                key = str(params.get("artifact_key", ""))
                ver = params.get("version")
                self.artifacts = [
                    a
                    for a in self.artifacts
                    if not (
                        a.get("session_id") == sid
                        and a.get("artifact_key") == key
                        and (ver is None or a.get("version") == ver)
                    )
                ]
                return 1

        if "VERTEBRA_KV_STORE" in upper:
            if upper.startswith("MERGE "):
                k = str(params["store_key"])
                self.kv_store[k] = {
                    "store_key": k,
                    "value_bytes": params["value_bytes"],
                    "expires_at": params.get("expires_at"),
                }
                return 1
            if upper.startswith("DELETE "):
                if "STORE_KEY = @STORE_KEY" in upper:
                    self.kv_store.pop(str(params.get("store_key", "")), None)
                elif "EXPIRES_AT <=" in upper:
                    now_str = str(params.get("now", ""))
                    self.kv_store = {
                        k: v
                        for k, v in self.kv_store.items()
                        if not (v.get("expires_at") is not None and str(v["expires_at"]) <= now_str)
                    }
                else:
                    self.kv_store.clear()
                return 1

        if "VERTEBRA_EVENT_QUEUE" in upper:
            if upper.startswith("INSERT "):
                eid = str(params["event_id"])
                self.event_queue[eid] = {
                    "event_id": eid,
                    "channel": str(params["channel"]),
                    "payload_json": str(params["payload_json"]),
                    "status": "PENDING",
                    "published_at": params.get("published_at"),
                }
                return 1
            if upper.startswith("UPDATE "):
                eid = str(params.get("event_id", ""))
                if eid in self.event_queue:
                    self.event_queue[eid]["status"] = "ACKED"
                    return 1
                return 0

        return 1

    def select_sql(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        params = dict(parameters) if isinstance(parameters, Mapping) else {}
        self.executed_sql.append((sql, params))
        upper = sql.strip().upper()

        if "INFORMATION_SCHEMA.TABLES" in upper:
            return [
                {
                    "table_catalog": self.project_id,
                    "table_schema": self.dataset_id,
                    "table_name": "metrics",
                    "table_type": "BASE TABLE",
                    "creation_time": "2026-11-02T00:00:00Z",
                    "ddl": "CREATE TABLE metrics (id INT64, name STRING, score FLOAT64)",
                }
            ]
        if "INFORMATION_SCHEMA.COLUMNS" in upper:
            return [
                {
                    "table_name": params.get("table_name", "metrics"),
                    "column_name": "id",
                    "ordinal_position": 1,
                    "is_nullable": "NO",
                    "data_type": "INT64",
                    "is_partitioning_column": "NO",
                    "clustering_ordinal_position": 1,
                }
            ]
        if "INFORMATION_SCHEMA.PARTITIONS" in upper:
            return [
                {
                    "table_name": params.get("table_name", "metrics"),
                    "partition_id": "20261102",
                    "total_rows": 2,
                    "total_logical_bytes": 1024,
                    "total_billable_bytes": 1024,
                    "last_modified_time": "2026-11-02T00:00:00Z",
                    "storage_tier": "ACTIVE",
                }
            ]

        if "VERTEBRA_ADK_SESSIONS" in upper:
            if "SESSION_ID = @SESSION_ID" in upper:
                sid = str(params.get("session_id", ""))
                found = self.sessions.get(sid)
                return [found] if found is not None else []
            uid = str(params.get("user_id", ""))
            app = params.get("app_name")
            return [
                s
                for s in self.sessions.values()
                if s.get("user_id") == uid and (app is None or s.get("app_name") == app)
            ]

        if "VERTEBRA_ADK_EVENTS" in upper:
            sid = str(params.get("session_id", ""))
            return [e for e in self.events if e.get("session_id") == sid]

        if "VERTEBRA_ADK_MEMORIES" in upper:
            uid = str(params.get("user_id", ""))
            app = params.get("app_name")
            return [m for m in self.memories if m.get("user_id") == uid and (app is None or m.get("app_name") == app)]

        if "VERTEBRA_ADK_ARTIFACTS" in upper:
            sid = str(params.get("session_id", ""))
            key = params.get("artifact_key")
            ver = params.get("version")
            matched = [
                a
                for a in self.artifacts
                if a.get("session_id") == sid
                and (key is None or a.get("artifact_key") == key)
                and (ver is None or a.get("version") == ver)
            ]
            matched.sort(key=lambda r: int(r.get("version", 1)), reverse=True)
            return matched

        if "VERTEBRA_KV_STORE" in upper:
            k = str(params.get("store_key", ""))
            entry = self.kv_store.get(k)
            return [entry] if entry is not None else []

        if "VERTEBRA_EVENT_QUEUE" in upper:
            ch = str(params.get("channel", ""))
            return [ev for ev in self.event_queue.values() if ev.get("channel") == ch and ev.get("status") == "PENDING"]

        return list(self.metrics_rows)


class _SimulatedSyncCoreDriver:
    """Sync core driver backed by ``_InMemoryBigQueryEngine``."""

    def __init__(self, engine: _InMemoryBigQueryEngine) -> None:
        self._engine = engine
        self.config = engine.config
        self.project_id = engine.project_id
        self.dataset_id = engine.dataset_id
        self.last_query_metadata = engine.last_query_metadata

    def connect(self) -> _SimulatedSyncCoreDriver:
        return self

    def close(self) -> None:
        return None

    def execute(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        **_kwargs: Any,
    ) -> int:
        return self._engine.execute_sql(sql, parameters)

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Mapping[str, Any] | Sequence[Any]],
    ) -> int:
        return sum(self._engine.execute_sql(sql, p) for p in parameter_sets)

    def execute_script(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        return self._engine.select_sql(sql, parameters)

    def select(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        _ = schema_type
        return self._engine.select_sql(sql, parameters)

    def select_one(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        rows = self.select(sql, parameters, schema_type=schema_type)
        if not rows:
            msg = "BigQuery query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

    def select_one_or_none(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        rows = self.select(sql, parameters, schema_type=schema_type)
        return rows[0] if rows else None

    def select_value(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> Any:
        rows = self.select(sql, parameters)
        if not rows:
            msg = "BigQuery query returned no rows"
            raise NotFoundError(msg)
        first = rows[0]
        return next(iter(first.values())) if first else None

    def dry_run(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> BigQueryDryRunResult:
        _ = (sql, parameters)
        return BigQueryDryRunResult(
            total_bytes_processed=2048,
            total_bytes_billed=2048,
            cache_hit=True,
            referenced_tables=("test-proj.app_ds.metrics",),
            estimated_cost_usd=0.0,
        )

    def select_to_arrow(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        **_kwargs: Any,
    ) -> ArrowCursor:
        rows = self.select(sql, parameters)
        return ArrowCursor.from_rows(rows, threshold_rows=1)

    def load_from_arrow(self, table_name: str, data: ArrowCursor | bytes | Any, **_kwargs: Any) -> int:
        _ = table_name
        cursor = data if isinstance(data, ArrowCursor) else ArrowCursor(bytes(data))
        rows = cursor.to_pylist()
        self._engine.metrics_rows.extend(rows)
        return len(rows)

    def export_to_object_store(
        self,
        sql_or_table: str,
        destination_uri: str,
        *,
        store: Any | None = None,
        format: str = "parquet",
        compression: str = "ZSTD",
        overwrite: bool = True,
    ) -> int:
        _ = overwrite
        cursor = self.select_to_arrow(sql_or_table)
        if isinstance(store, ObjectStoreBackend):
            key = destination_uri.split("://", 1)[-1].split("/", 1)[-1]
            store.write_arrow_cursor_sync(key, cursor, file_format=format, compression=compression)
        return cursor.row_count

    def load_from_object_store(
        self,
        table: str,
        source_uris: str | Sequence[str],
        *,
        store: Any | None = None,
        format: str = "parquet",
        write_disposition: str = "WRITE_APPEND",
    ) -> int:
        _ = (table, write_disposition)
        uris = [source_uris] if isinstance(source_uris, str) else list(source_uris)
        total = 0
        for u in uris:
            if isinstance(store, ObjectStoreBackend):
                key = u.split("://", 1)[-1].split("/", 1)[-1]
                tbl = store.read_arrow_sync(key, file_format=format)
                total += len(tbl.to_pylist())
        return total

    def create_object_table(self, config: ObjectTableConfig, *, if_not_exists: bool = True) -> None:
        self._engine.ddl_statements.append(config.create_object_table_sql(if_not_exists=if_not_exists))


class _SimulatedAsyncCoreDriver:
    """Async core driver backed by ``_InMemoryBigQueryEngine``."""

    def __init__(self, engine: _InMemoryBigQueryEngine) -> None:
        self._sync = _SimulatedSyncCoreDriver(engine)
        self.config = engine.config
        self.project_id = engine.project_id
        self.dataset_id = engine.dataset_id
        self.last_query_metadata = engine.last_query_metadata

    async def connect(self) -> _SimulatedAsyncCoreDriver:
        return self

    async def close(self) -> None:
        return None

    async def execute(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> int:
        return self._sync.execute(sql, parameters, **kwargs)

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Mapping[str, Any] | Sequence[Any]],
    ) -> int:
        return self._sync.execute_many(sql, parameter_sets)

    async def execute_script(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        return self._sync.execute_script(sql, parameters)

    async def select(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        return self._sync.select(sql, parameters, schema_type=schema_type)

    async def select_one(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        return self._sync.select_one(sql, parameters, schema_type=schema_type)

    async def select_one_or_none(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        return self._sync.select_one_or_none(sql, parameters, schema_type=schema_type)

    async def select_value(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> Any:
        return self._sync.select_value(sql, parameters)

    async def dry_run(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> BigQueryDryRunResult:
        return self._sync.dry_run(sql, parameters)

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> ArrowCursor:
        return self._sync.select_to_arrow(sql, parameters, **kwargs)

    async def load_from_arrow(self, table_name: str, data: ArrowCursor | bytes | Any, **kwargs: Any) -> int:
        return self._sync.load_from_arrow(table_name, data, **kwargs)

    async def export_to_object_store(
        self,
        sql_or_table: str,
        destination_uri: str,
        **kwargs: Any,
    ) -> int:
        return self._sync.export_to_object_store(sql_or_table, destination_uri, **kwargs)

    async def load_from_object_store(
        self,
        table: str,
        source_uris: str | Sequence[str],
        **kwargs: Any,
    ) -> int:
        return self._sync.load_from_object_store(table, source_uris, **kwargs)


def test_bigquery_sync_config_driver_data_dictionary_and_connection_shim() -> None:
    """Verify BigQuerySyncConfig, BigQuerySyncDriver, BigQueryDataDictionary, and BigQueryConnectionShim."""
    engine = _InMemoryBigQueryEngine()
    core_sync = _SimulatedSyncCoreDriver(engine)
    storage = ObjectStoreBackend("memory://bq-stage")

    cfg = BigQuerySyncConfig(
        project_id="test-proj",
        dataset_id="app_ds",
        storage_backend=storage,
        connection_instance=core_sync,
    )
    driver = cfg.provide_driver()
    assert isinstance(driver, BigQuerySyncDriver)
    assert driver.dialect == "bigquery"

    tables = driver.data_dictionary.get_tables()
    assert len(tables) == 1
    assert tables[0]["table_name"] == "metrics"

    columns = driver.data_dictionary.get_columns("metrics")
    assert columns[0]["column_name"] == "id"

    partitions = driver.data_dictionary.get_partitions("metrics")
    assert partitions[0]["partition_id"] == "20261102"

    arrow_cur = driver.select_to_arrow("SELECT id, name, score FROM metrics")
    assert arrow_cur.row_count == 2
    appended = driver.load_from_arrow("metrics", arrow_cur)
    assert appended == 2

    with pytest.raises(NotFoundError):
        driver.select_one(
            "SELECT store_key FROM vertebra_kv_store WHERE store_key = @store_key", {"store_key": "missing"}
        )
    assert (
        driver.select_one_or_none(
            "SELECT store_key FROM vertebra_kv_store WHERE store_key = @store_key",
            {"store_key": "missing"},
        )
        is None
    )

    exported = driver.select_to_storage(
        "SELECT id, name, score FROM metrics",
        "memory://bq-stage/exports/metrics.parquet",
        format="parquet",
    )
    assert exported == 4
    loaded = driver.load_from_storage(
        "metrics",
        "memory://bq-stage/exports/metrics.parquet",
        format="parquet",
    )
    assert loaded == 4

    dry = driver.dry_run("SELECT * FROM metrics")
    assert dry.cache_hit is True
    assert dry.estimated_cost_usd == 0.0

    stack_out = driver.execute_stack(
        [
            "CREATE TABLE IF NOT EXISTS t1 (id INT64)",
            ("SELECT id, name FROM metrics", None),
        ]
    )
    assert len(stack_out) == 2

    conn = cfg.create_connection()
    assert isinstance(conn, BigQueryConnectionShim)
    assert conn.project == "test-proj"
    assert conn.dataset_id == "app_ds"
    job = conn.query("SELECT id, name FROM metrics")
    assert len(job.result()) == 4
    assert job.job_id == "job-sim-1"


def test_bigquery_sync_adk_memory_artifact_litestar_and_event_stores() -> None:
    """Verify synchronous ADK, Memory, Artifact, Litestar KV, and EventQueue stores on BigQuery."""
    engine = _InMemoryBigQueryEngine()
    core_sync = _SimulatedSyncCoreDriver(engine)
    store = SyncObjectStore(
        "memory://",
        signing=SigningConfig(
            provider="hmac",
            hmac_secret=base64.b64encode(b"secret-signing-key").decode("ascii"),
            endpoint="https://artifacts.example.com",
        ),
    )
    backend = ObjectStoreBackend("memory://artifacts", store=store)
    driver = BigQuerySyncDriver(core_sync, storage_backend=backend)

    adk_store = BigQuerySyncADKStore(driver)
    adk_store.create_tables()
    assert any("CLUSTER BY user_id, session_id" in ddl for ddl in engine.ddl_statements)
    assert any("CLUSTER BY session_id, timestamp" in ddl for ddl in engine.ddl_statements)

    sess = adk_store.create_session("s-1", "u-1", app_name="agent-app", state={"step": 1})
    assert sess["session_id"] == "s-1"
    assert sess["state"] == {"step": 1}

    fetched_sess = adk_store.get_session("s-1", user_id="u-1")
    assert fetched_sess is not None
    assert fetched_sess["state"] == {"step": 1}

    adk_store.update_session_state("s-1", {"step": 2, "ready": True})
    updated_sess = adk_store.get_session("s-1")
    assert updated_sess is not None
    assert updated_sess["state"] == {"step": 2, "ready": True}

    assert len(adk_store.list_sessions("u-1", app_name="agent-app")) == 1
    adk_store.append_event("s-1", "ev-1", "user", {"text": "hi"}, actions={"tool": "none"})
    events = adk_store.get_events("s-1")
    assert len(events) == 1
    assert events[0]["content"] == {"text": "hi"}
    adk_store.delete_session("s-1")
    assert adk_store.get_session("s-1") is None

    mem_store = BigQuerySyncADKMemoryStore(driver)
    mem_store.create_tables()
    mem_store.store_memory(
        "m-1",
        "u-1",
        "Cloud Spanner supports multiplexed sessions",
        embedding=[1.0, 0.0, 0.0],
        metadata={"topic": "spanner"},
    )
    mem_store.store_memory(
        "m-2",
        "u-1",
        "BigQuery Storage Read API streams Arrow RecordBatches",
        embedding=[0.0, 1.0, 0.0],
        metadata={"topic": "bigquery"},
    )
    hits_vec = mem_store.search_memory("u-1", embedding=[0.95, 0.05, 0.0], top_k=1)
    assert len(hits_vec) == 1
    assert hits_vec[0]["memory_id"] == "m-1"

    hits_kw = mem_store.search_memory("u-1", query="Arrow RecordBatches", top_k=1)
    assert len(hits_kw) == 1
    assert hits_kw[0]["memory_id"] == "m-2"

    art_store = BigQuerySyncADKArtifactStore(driver, storage_backend=backend)
    art_store.create_tables()
    ref = art_store.save_artifact("s-1", "report.pdf", b"%PDF-1.7-payload", content_type="application/pdf")
    assert ref.size == 16
    assert ref.signed_url is not None
    loaded_art = art_store.get_artifact("s-1", "report.pdf")
    assert loaded_art is not None
    assert loaded_art[0] == b"%PDF-1.7-payload"
    assert len(art_store.list_artifacts("s-1")) == 1
    art_store.delete_artifact("s-1", "report.pdf")
    assert art_store.get_artifact("s-1", "report.pdf") is None

    kv_store = BigQuerySyncStore(driver)
    kv_store.create_tables()
    kv_store.set("sess-k1", b"payload-1", expires_in=300)
    assert kv_store.exists("sess-k1") is True
    assert kv_store.get("sess-k1") == b"payload-1"
    ttl = kv_store.expires_in("sess-k1")
    assert ttl is not None and ttl > 0
    kv_store.delete("sess-k1")
    assert kv_store.get("sess-k1") is None

    eq_store = BigQuerySyncEventQueueStore(driver)
    eq_store.create_tables()
    eid = eq_store.publish("orders", {"order_id": 42})
    pending = eq_store.consume("orders")
    assert len(pending) == 1
    assert pending[0]["event_id"] == eid
    assert pending[0]["payload"] == {"order_id": 42}
    eq_store.ack(eid)
    assert eq_store.consume("orders") == []


@pytest.mark.anyio
async def test_bigquery_async_config_driver_and_async_stores() -> None:
    """Verify BigQueryAsyncConfig, BigQueryAsyncDriver, and all async sub-extension stores."""
    engine = _InMemoryBigQueryEngine()
    core_async = _SimulatedAsyncCoreDriver(engine)
    store = SyncObjectStore(
        "memory://",
        signing=SigningConfig(
            provider="hmac",
            hmac_secret=base64.b64encode(b"secret-signing-key").decode("ascii"),
            endpoint="https://artifacts.example.com",
        ),
    )
    backend = ObjectStoreBackend("memory://async-artifacts", store=store)

    cfg = BigQueryAsyncConfig(
        project_id="test-proj",
        dataset_id="app_ds",
        storage_backend=backend,
        connection_instance=core_async,
    )
    async with cfg.provide_session() as driver:
        assert isinstance(driver, BigQueryAsyncDriver)
        tables = await driver.data_dictionary.get_tables()
        assert tables[0]["table_name"] == "metrics"

        adk = BigQueryAsyncADKStore(driver)
        await adk.create_tables()
        await adk.create_session("as-1", "au-1", state={"ok": True})
        sess = await adk.get_session("as-1")
        assert sess is not None and sess["state"] == {"ok": True}
        await adk.append_event("as-1", "aev-1", "assistant", {"reply": "done"})
        assert len(await adk.get_events("as-1")) == 1
        await adk.delete_session("as-1")

        mem = BigQueryAsyncADKMemoryStore(driver)
        await mem.create_tables()
        await mem.store_memory("am-1", "au-1", "Async Spanner multiplexed read-write", embedding=[0.5, 0.5])
        results = await mem.search_memory("au-1", query="multiplexed")
        assert len(results) == 1

        art = BigQueryAsyncADKArtifactStore(driver, storage_backend=backend)
        await art.create_tables()
        ref = await art.save_artifact("as-1", "chart.png", b"\x89PNG", content_type="image/png")
        assert ref.signed_url is not None
        got = await art.get_artifact("as-1", "chart.png")
        assert got is not None and got[0] == b"\x89PNG"
        await art.delete_artifact("as-1", "chart.png")

        kv = BigQueryAsyncStore(driver)
        await kv.create_tables()
        await kv.set("ak-1", "async-val", expires_in=120)
        assert await kv.get("ak-1") == b"async-val"
        await kv.delete_all()
        assert await kv.exists("ak-1") is False

        eq = BigQueryAsyncEventQueueStore(driver)
        await eq.create_tables()
        eid = await eq.publish("jobs", {"task": "compact"})
        items = await eq.consume("jobs")
        assert len(items) == 1 and items[0]["event_id"] == eid
        await eq.ack(eid)
        assert await eq.consume("jobs") == []


@pytest.mark.parametrize("config_created", [False, True])
def test_bigquery_statement_stack_preserves_rows_over_native_rest_port(config_created: bool) -> None:
    """Real BigQuery driver requests retain each SQLSpec read result and session transaction."""
    server = _MockBigQueryRestServer()
    core = CoreSyncBigQueryDriver(BigQueryConfig(project_id="test-proj", rest_endpoint=f"http://{server.host_port}"))
    driver = (
        BigQuerySyncConfig(project_id="test-proj", connection_instance=core).create_driver()
        if config_created
        else BigQuerySyncDriver(core)
    )
    try:
        result = driver.execute_stack(
            StatementStack()
            .push_execute("SELECT id FROM metrics WHERE id >= @min_id", min_id=1)
            .push_execute("SELECT name FROM metrics")
        )
        assert len(result) == 2
        assert [item.rows_affected for item in result] == [2, 2]
        assert result[0].get_result().get_data()[0]["id"] == 1
        assert result[1].get_result().get_data()[1]["name"] == "beta"
        single = driver.execute_stack(
            StatementStack().push_execute("SELECT id FROM metrics WHERE id >= @min_id", min_id=2)
        )
        assert single[0].get_result().get_data()[1]["id"] == 2
        arrow = driver.execute_stack(
            StatementStack().push_execute_arrow("SELECT id FROM metrics WHERE id >= @min_id", min_id=1)
        )[0]
        assert arrow.is_arrow_result()
        assert arrow.rows_affected == 2
        assert [row["id"] for row in arrow.get_result().data.to_pylist()] == [1, 2]
        assert arrow.get_result().statement.named_parameters == {"min_id": 1}
        queries = [request["body"].get("query", "") for request in server.requests]
        assert any("BEGIN TRANSACTION" in query for query in queries)
        assert any("COMMIT TRANSACTION" in query for query in queries)
        assert sum("FROM metrics" in query for query in queries) == 4
        bound_request = next(
            request["body"] for request in server.requests if "min_id" in request["body"].get("query", "")
        )
        assert "@min_id" in bound_request["query"]
        assert bound_request["queryParameters"][0]["name"] == "min_id"
        assert bound_request["queryParameters"][0]["parameterValue"]["value"] == "1"
    finally:
        core.close()
        server.close()


@pytest.mark.anyio
@pytest.mark.parametrize("config_created", [False, True])
async def test_bigquery_async_statement_stack_preserves_rows_over_native_rest_port(config_created: bool) -> None:
    """Async BigQuery stack execution retains native session requests and separate row sets."""
    server = _MockBigQueryRestServer()
    core = CoreAsyncBigQueryDriver(BigQueryConfig(project_id="test-proj", rest_endpoint=f"http://{server.host_port}"))
    driver = (
        BigQueryAsyncConfig(project_id="test-proj", connection_instance=core).create_driver()
        if config_created
        else BigQueryAsyncDriver(core)
    )
    try:
        result = await driver.execute_stack(
            StatementStack()
            .push_execute("SELECT id FROM metrics WHERE id >= @min_id", min_id=1)
            .push_execute("SELECT name FROM metrics")
        )
        assert len(result) == 2
        assert [item.rows_affected for item in result] == [2, 2]
        assert result[0].get_result().get_data()[0]["id"] == 1
        assert result[1].get_result().get_data()[1]["name"] == "beta"
        single = await driver.execute_stack(
            StatementStack().push_execute("SELECT id FROM metrics WHERE id >= @min_id", min_id=2)
        )
        assert single[0].get_result().get_data()[1]["id"] == 2
        arrow = (
            await driver.execute_stack(
                StatementStack().push_execute_arrow("SELECT id FROM metrics WHERE id >= @min_id", min_id=1)
            )
        )[0]
        assert arrow.is_arrow_result()
        assert arrow.rows_affected == 2
        assert [row["id"] for row in arrow.get_result().data.to_pylist()] == [1, 2]
        assert arrow.get_result().statement.named_parameters == {"min_id": 1}
        queries = [request["body"].get("query", "") for request in server.requests]
        assert any("BEGIN TRANSACTION" in query for query in queries)
        assert any("COMMIT TRANSACTION" in query for query in queries)
        assert sum("FROM metrics" in query for query in queries) == 4
        bound_request = next(
            request["body"] for request in server.requests if "min_id" in request["body"].get("query", "")
        )
        assert "@min_id" in bound_request["query"]
        assert bound_request["queryParameters"][0]["name"] == "min_id"
        assert bound_request["queryParameters"][0]["parameterValue"]["value"] == "1"
    finally:
        await core.close()
        server.close()


class _ArrowOptionsBigQueryClient:
    """External Storage Read client port recording native Arrow request options."""

    def __init__(self) -> None:
        self.requests: list[tuple[Any, ...]] = []

    def query(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"session_id": "arrow-options-session"}

    def read_table_arrow(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        selected_fields: list[str] | None,
        row_restriction: str | None,
        max_streams: int,
        compression: str,
    ) -> None:
        self.requests.append(
            (project_id, dataset_id, table_id, selected_fields, row_restriction, max_streams, compression)
        )
        raise RuntimeError("external Arrow read reached")


class _AsyncArrowOptionsBigQueryClient:
    """Async Storage Read client port sharing the exact native request observations."""

    def __init__(self, sync: _ArrowOptionsBigQueryClient) -> None:
        self.sync = sync

    async def query(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.sync.query(*args, **kwargs)

    async def read_table_arrow(self, *args: Any, **kwargs: Any) -> None:
        self.sync.read_table_arrow(*args, **kwargs)


def test_bigquery_arrow_stack_preserves_native_storage_options() -> None:
    """BigQuery stack options reach the real native driver's external Storage Read port."""
    client = _ArrowOptionsBigQueryClient()
    core = CoreSyncBigQueryDriver(BigQueryConfig(project_id="test-proj", dataset_id="analytics"), client=client)
    driver = BigQuerySyncDriver(core)
    try:
        with pytest.raises(RuntimeError, match="external Arrow read reached"):
            driver.execute_stack(
                StatementStack().push_execute_arrow(
                    "metrics", selected_fields=["id"], row_restriction="id = 7", max_streams=1
                )
            )
        assert client.requests == [
            ("test-proj", "analytics", "metrics", ["id"], "id = 7", 1, core.config.arrow_compression)
        ]
    finally:
        core.close()


@pytest.mark.anyio
async def test_bigquery_async_arrow_stack_preserves_native_storage_options() -> None:
    """Async BigQuery forwards owning Arrow options through native session execution."""
    client = _ArrowOptionsBigQueryClient()
    core = CoreAsyncBigQueryDriver(
        BigQueryConfig(project_id="test-proj", dataset_id="analytics"), client=_AsyncArrowOptionsBigQueryClient(client)
    )
    driver = BigQueryAsyncDriver(core)
    try:
        with pytest.raises(RuntimeError, match="external Arrow read reached"):
            await driver.execute_stack(
                StatementStack().push_execute_arrow(
                    "metrics", selected_fields=["id"], row_restriction="id = 7", max_streams=1
                )
            )
        assert client.requests == [
            ("test-proj", "analytics", "metrics", ["id"], "id = 7", 1, core.config.arrow_compression)
        ]
    finally:
        await core.close()


@pytest.mark.parametrize("kind", ["sync-driver", "async-driver", "sync-config", "async-config"])
def test_configured_tenant_predicate_rejected_before_constructor_effects(
    kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unsupported isolation fails before base, core, resolver, broker, or native access."""
    from vertebra.db import NotSupportedError
    from vertebra.db.rls import TenantPredicate
    from vertebra.extensions.sqlspec.adapters import (
        AsyncAdapterConfig,
        BaseAsyncDriver,
        BaseSyncDriver,
        SyncAdapterConfig,
    )

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Constructor performed a side effect before rejecting tenant isolation")

    class ForbiddenCore:
        def __getattribute__(self, name: str) -> Any:
            forbidden(name)

    for base in (BaseSyncDriver, BaseAsyncDriver, SyncAdapterConfig, AsyncAdapterConfig):
        monkeypatch.setattr(base, "__init__", forbidden)
    predicate = TenantPredicate(table_alias="t", tenant_value="tenant")
    constructors = {
        "sync-driver": lambda: BigQuerySyncDriver(ForbiddenCore(), tenant_predicate=predicate),
        "async-driver": lambda: BigQueryAsyncDriver(ForbiddenCore(), tenant_predicate=predicate),
        "sync-config": lambda: BigQuerySyncConfig(tenant_predicate=predicate),
        "async-config": lambda: BigQueryAsyncConfig(tenant_predicate=predicate),
    }
    for config_type in (BigQuerySyncConfig, BigQueryAsyncConfig):
        monkeypatch.setattr(config_type, "resolve_bigquery_config", forbidden)
    for constructor_type in (BigQuerySyncDriver, BigQueryAsyncDriver, BigQuerySyncConfig, BigQueryAsyncConfig):
        monkeypatch.setattr(constructor_type, "__setattr__", forbidden)
    with pytest.raises(NotSupportedError):
        constructors[kind]()
