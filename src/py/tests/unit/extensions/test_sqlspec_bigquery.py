"""Unit tests for BigQuery SQLSpec configs, drivers, data dictionary, and zero-SDK sub-extension stores."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pyarrow as pa
import pytest
from sqlspec import StatementStack
from sqlspec.exceptions import NotFoundError as SQLSpecNotFoundError
from sqlspec.exceptions import PermissionDeniedError, StackExecutionError

from tests.unit.db.test_bigquery import _MockBigQueryRestServer
from vertebra._vertebra import ArrowCursor as RustArrowCursor
from vertebra._vertebra import BigQueryError
from vertebra.db import ArrowCursor, NotFoundError, NotSupportedError
from vertebra.db.adapters.bigquery import (
    AsyncBigQueryDriver as CoreAsyncBigQueryDriver,
)
from vertebra.db.adapters.bigquery import BigQueryConfig, BigQueryDryRunResult, ObjectTableConfig
from vertebra.db.adapters.bigquery import SyncBigQueryDriver as CoreSyncBigQueryDriver
from vertebra.db.rls import TenantPredicate
from vertebra.extensions.sqlspec import ObjectStoreBackend
from vertebra.extensions.sqlspec._driver import (
    AsyncAdapterConfig,
    BaseAsyncDriver,
    BaseSyncDriver,
    SyncAdapterConfig,
)
from vertebra.extensions.sqlspec.adapters.bigquery import (
    BigQueryAsyncADKArtifactStore,
    BigQueryAsyncADKMemoryStore,
    BigQueryAsyncADKStore,
    BigQueryAsyncConfig,
    BigQueryAsyncDriver,
    BigQueryConnectionShim,
    BigQueryEventQueueStore,
    BigQueryLitestarStore,
    BigQuerySyncADKArtifactStore,
    BigQuerySyncADKMemoryStore,
    BigQuerySyncADKStore,
    BigQuerySyncConfig,
    BigQuerySyncDriver,
)
from vertebra.extensions.sqlspec.adapters.bigquery import config as bq_config_mod
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
                sid = str(params["id"])
                self.sessions[sid] = dict(params)
                return 1
            if upper.startswith("UPDATE "):
                sid = str(params["id"])
                if sid in self.sessions:
                    self.sessions[sid]["state"] = params["state"]
                    self.sessions[sid]["update_time"] = params["update_time"]
                    return 1
                return 0
            if upper.startswith("DELETE "):
                sid = str(params.get("id", ""))
                self.sessions.pop(sid, None)
                return 1

        if "VERTEBRA_ADK_EVENTS" in upper:
            if upper.startswith("INSERT "):
                self.events.append(dict(params))
                return 1
            if upper.startswith("DELETE "):
                sid = str(params.get("id", params.get("session_id", "")))
                self.events = [e for e in self.events if e.get("session_id") != sid]
                return 1

        if "VERTEBRA_ADK_MEMORIES" in upper and (upper.startswith("INSERT ") or upper.startswith("MERGE ")):
            self.memories = [m for m in self.memories if m.get("id") != params.get("id")]
            self.memories.append(dict(params))
            return 1

        if "VERTEBRA_ADK_ARTIFACTS" in upper:
            if upper.startswith("INSERT ") or upper.startswith("MERGE "):
                self.artifacts.append(dict(params))
                return 1
            if upper.startswith("DELETE "):
                art_sid = params.get("session_id")
                fname = str(params.get("filename", ""))
                self.artifacts = [
                    a
                    for a in self.artifacts
                    if not (
                        a.get("filename") == fname
                        and a.get("app_name") == params.get("app_name")
                        and a.get("user_id") == params.get("user_id")
                        and (art_sid is None or a.get("session_id") == art_sid)
                    )
                ]
                return 1

        if "VERTEBRA_KV_STORE" in upper:
            if upper.startswith("MERGE "):
                k = str(params["session_id"])
                self.kv_store[k] = {
                    "session_id": k,
                    "data": params["data"],
                    "expires_at": params.get("expires_at"),
                }
                return 1
            if upper.startswith("DELETE "):
                if "SESSION_ID = @SESSION_ID" in upper:
                    self.kv_store.pop(str(params.get("session_id", "")), None)
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
            if "WHERE ID = @ID" in upper:
                sid = str(params.get("id", ""))
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
            if "MAX(VERSION)" in upper or "COALESCE(MAX(" in upper:
                fname = params.get("filename")
                matched_vers = [
                    int(a.get("version", 0))
                    for a in self.artifacts
                    if a.get("app_name") == params.get("app_name")
                    and a.get("user_id") == params.get("user_id")
                    and a.get("filename") == fname
                ]
                return [{"next_version": (max(matched_vers) + 1) if matched_vers else 1}]
            art_sid = params.get("session_id")
            fname = params.get("filename")
            ver = params.get("version")
            matched = [
                a
                for a in self.artifacts
                if a.get("app_name") == params.get("app_name")
                and a.get("user_id") == params.get("user_id")
                and (art_sid is None or a.get("session_id") == art_sid)
                and (fname is None or a.get("filename") == fname)
                and (ver is None or a.get("version") == ver)
            ]
            matched.sort(key=lambda r: int(r.get("version", 1)), reverse=True)
            return matched

        if "VERTEBRA_KV_STORE" in upper:
            k = str(params.get("session_id", params.get("store_key", "")))
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

    def begin(self) -> None:
        return None

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
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

    async def begin(self) -> None:
        return None

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
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
    driver = cfg.create_driver()
    assert isinstance(driver, BigQuerySyncDriver)
    assert driver.dialect == "bigquery"

    tables = driver.data_dictionary.get_tables(driver)
    assert len(tables) == 1
    assert tables[0]["table_name"] == "metrics"

    columns = driver.data_dictionary.get_columns(driver, "metrics")
    assert columns[0]["column_name"] == "id"

    partitions = driver.data_dictionary.get_partitions(driver, "metrics")
    assert cast("dict[str, Any]", partitions[0])["partition_id"] == "20261102"

    arrow_cur = driver.select_to_arrow("SELECT id, name, score FROM metrics")
    assert arrow_cur.rows_affected == 2
    appended = driver.load_from_arrow("metrics", arrow_cur)
    assert appended.telemetry["rows_processed"] == 2

    with pytest.raises(SQLSpecNotFoundError):
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

    arrow_export = driver.select_to_arrow("SELECT id, name, score FROM metrics")
    storage.write_arrow_sync("exports/metrics.parquet", arrow_export.data, file_format="parquet")
    exported_table = storage.read_arrow_sync("exports/metrics.parquet", file_format="parquet")
    assert exported_table.num_rows == 4
    loaded = driver.load_from_arrow("metrics", exported_table)
    assert loaded.telemetry["rows_processed"] == 4

    dry = driver.dry_run("SELECT * FROM metrics")
    assert dry.cache_hit is True
    assert dry.estimated_cost_usd == 0.0

    stack_out = driver.execute_stack(
        StatementStack()
        .push_execute("CREATE TABLE IF NOT EXISTS t1 (id INT64)")
        .push_execute("SELECT id, name FROM metrics")
    )
    assert len(stack_out) == 2

    conn = cfg.create_connection()
    assert isinstance(conn, BigQueryConnectionShim)
    assert conn.project == "test-proj"
    assert conn.dataset_id == "app_ds"
    job = conn.query("SELECT id, name FROM metrics")
    assert len(job.result()) == 8
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
    cfg = BigQuerySyncConfig(
        project_id="test-proj",
        dataset_id="app_ds",
        storage_backend=backend,
        connection_instance=core_sync,
        extension_config={
            "adk": {
                "session_table": "vertebra_adk_sessions",
                "events_table": "vertebra_adk_events",
                "memory_table": "vertebra_adk_memories",
                "artifact_table": "vertebra_adk_artifacts",
            },
            "litestar": {"session_table": "vertebra_kv_store"},
            "events": {"queue_table": "vertebra_event_queue"},
        },
    )

    adk_store = BigQuerySyncADKStore(cfg)
    adk_store.create_tables()
    assert any("CLUSTER BY app_name, user_id, id" in ddl for ddl in engine.ddl_statements)
    assert any("CLUSTER BY app_name, user_id, session_id" in ddl for ddl in engine.ddl_statements)

    sess = adk_store.create_session("s-1", "agent-app", "u-1", state={"step": 1})
    assert sess["id"] == "s-1"
    assert sess["state"] == {"step": 1}

    fetched_sess = adk_store.get_session("agent-app", "u-1", "s-1")
    assert fetched_sess is not None
    assert fetched_sess["state"] == {"step": 1}

    adk_store.update_session_state("agent-app", "u-1", "s-1", {"step": 2, "ready": True})
    updated_sess = adk_store.get_session("agent-app", "u-1", "s-1")
    assert updated_sess is not None
    assert updated_sess["state"] == {"step": 2, "ready": True}

    assert len(adk_store.list_sessions("agent-app", user_id="u-1")) == 1
    adk_store.append_event(
        {
            "id": "ev-1",
            "app_name": "agent-app",
            "user_id": "u-1",
            "session_id": "s-1",
            "invocation_id": "inv-1",
            "timestamp": datetime.now(UTC),
            "event_data": {"author": "user", "content": {"text": "hi"}, "actions": {"tool": "none"}},
        }
    )
    events = adk_store.get_events("agent-app", "u-1", "s-1")
    assert len(events) == 1
    assert events[0]["event_data"]["content"] == {"text": "hi"}
    adk_store.delete_session("agent-app", "u-1", "s-1")
    assert adk_store.get_session("agent-app", "u-1", "s-1") is None

    mem_store = BigQuerySyncADKMemoryStore(cfg)
    mem_store.create_tables()
    now = datetime.now(UTC)
    assert (
        mem_store.insert_memory_entries(
            [
                {
                    "id": "m-1",
                    "session_id": "s-1",
                    "app_name": "agent-app",
                    "user_id": "u-1",
                    "scope": "user",
                    "event_id": "ev-1",
                    "author": "user",
                    "timestamp": now,
                    "content_json": {"text": "Cloud Spanner supports multiplexed sessions"},
                    "content_text": "Cloud Spanner supports multiplexed sessions",
                    "metadata_json": {"topic": "spanner"},
                    "inserted_at": now,
                    "embedding": [1.0, 0.0, 0.0],
                },
                {
                    "id": "m-2",
                    "session_id": "s-1",
                    "app_name": "agent-app",
                    "user_id": "u-1",
                    "scope": "user",
                    "event_id": "ev-2",
                    "author": "user",
                    "timestamp": now,
                    "content_json": {"text": "BigQuery Storage Read API streams Arrow RecordBatches"},
                    "content_text": "BigQuery Storage Read API streams Arrow RecordBatches",
                    "metadata_json": {"topic": "bigquery"},
                    "inserted_at": now,
                    "embedding": [0.0, 1.0, 0.0],
                },
            ]
        )
        == 2
    )
    hits_vec = mem_store.search_entries("multiplexed", "agent-app", "u-1", limit=1, embedding=[0.95, 0.05, 0.0])
    assert len(hits_vec) == 1
    assert hits_vec[0]["id"] == "m-1"

    hits_kw = mem_store.search_entries("Arrow RecordBatches", "agent-app", "u-1", limit=1)
    assert len(hits_kw) == 1
    assert hits_kw[0]["id"] == "m-2"

    art_store = BigQuerySyncADKArtifactStore(cfg, storage_backend=backend)
    art_store.create_table()
    ref = art_store.save_artifact_object(
        "agent-app",
        "u-1",
        "report.pdf",
        b"%PDF-1.7-payload",
        session_id="s-1",
        mime_type="application/pdf",
    )
    assert ref.size == 16
    loaded_art = art_store.get_artifact("agent-app", "u-1", "report.pdf", session_id="s-1")
    assert loaded_art is not None
    assert loaded_art["canonical_uri"] == ref.uri
    assert len(art_store.list_artifact_keys("agent-app", "u-1", session_id="s-1")) == 1
    art_store.delete_artifact("agent-app", "u-1", "report.pdf", session_id="s-1")
    assert art_store.get_artifact("agent-app", "u-1", "report.pdf", session_id="s-1") is None

    kv_store = BigQueryLitestarStore(cfg)
    asyncio.run(kv_store.create_table())
    asyncio.run(kv_store.set("sess-k1", b"payload-1", expires_in=timedelta(seconds=300)))
    assert asyncio.run(kv_store.exists("sess-k1")) is True
    assert asyncio.run(kv_store.get("sess-k1")) == b"payload-1"
    ttl = asyncio.run(kv_store.expires_in("sess-k1"))
    assert ttl is not None and ttl > 0
    asyncio.run(kv_store.delete("sess-k1"))
    assert asyncio.run(kv_store.get("sess-k1")) is None

    eq_store = BigQueryEventQueueStore(cfg)
    eq_ddls = eq_store.create_statements()
    assert any("PARTITION BY DATE(available_at)" in ddl and "CLUSTER BY" in ddl for ddl in eq_ddls)
    assert eq_store.drop_statements() == ["DROP TABLE IF EXISTS `test-proj`.`app_ds`.`vertebra_event_queue`"]


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
        extension_config={
            "adk": {
                "session_table": "vertebra_adk_sessions",
                "events_table": "vertebra_adk_events",
                "memory_table": "vertebra_adk_memories",
                "artifact_table": "vertebra_adk_artifacts",
            },
            "litestar": {"session_table": "vertebra_kv_store"},
            "events": {"queue_table": "vertebra_event_queue"},
        },
    )
    async with cfg.provide_session() as driver:
        assert isinstance(driver, BigQueryAsyncDriver)
        tables = await driver.data_dictionary.get_tables(driver)
        assert tables[0]["table_name"] == "metrics"

        adk = BigQueryAsyncADKStore(cfg)
        await adk.create_tables()
        await adk.create_session("as-1", "agent-app", "au-1", state={"ok": True})
        sess = await adk.get_session("agent-app", "au-1", "as-1")
        assert sess is not None and sess["state"] == {"ok": True}
        await adk.append_event(
            {
                "id": "aev-1",
                "app_name": "agent-app",
                "user_id": "au-1",
                "session_id": "as-1",
                "invocation_id": "ainv-1",
                "timestamp": datetime.now(UTC),
                "event_data": {"author": "assistant", "content": {"reply": "done"}},
            }
        )
        assert len(await adk.get_events("agent-app", "au-1", "as-1")) == 1
        await adk.delete_session("agent-app", "au-1", "as-1")

        mem = BigQueryAsyncADKMemoryStore(cfg)
        await mem.create_tables()
        now = datetime.now(UTC)
        await mem.insert_memory_entries(
            [
                {
                    "id": "am-1",
                    "session_id": "as-1",
                    "app_name": "agent-app",
                    "user_id": "au-1",
                    "scope": "user",
                    "event_id": "aev-1",
                    "author": "assistant",
                    "timestamp": now,
                    "content_json": {"text": "Async Spanner multiplexed read-write"},
                    "content_text": "Async Spanner multiplexed read-write",
                    "metadata_json": None,
                    "inserted_at": now,
                    "embedding": [0.5, 0.5],
                }
            ]
        )
        results = await mem.search_entries("multiplexed", "agent-app", "au-1")
        assert len(results) == 1

        art = BigQueryAsyncADKArtifactStore(cfg, storage_backend=backend)
        await art.create_table()
        ref = await art.save_artifact_object(
            "agent-app",
            "au-1",
            "chart.png",
            b"\x89PNG",
            session_id="as-1",
            mime_type="image/png",
        )
        got = await art.get_artifact("agent-app", "au-1", "chart.png", session_id="as-1")
        assert got is not None and got["canonical_uri"] == ref.uri
        await art.delete_artifact("agent-app", "au-1", "chart.png", session_id="as-1")

        kv = BigQueryLitestarStore(cfg)
        await kv.create_table()
        await kv.set("ak-1", "async-val", expires_in=timedelta(seconds=120))
        assert await kv.get("ak-1") == b"async-val"
        await kv.delete_all()
        assert await kv.exists("ak-1") is False

        eq = BigQueryEventQueueStore(cfg)
        assert any("PARTITION BY DATE(available_at)" in ddl for ddl in eq.create_statements())


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

    def query(
        self,
        sql: str,
        params_json: str | None = None,
        session_id: str | None = None,
        create_session: bool = False,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        job_timeout_ms: int | None = None,
        priority: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        _ = (
            sql,
            params_json,
            session_id,
            create_session,
            destination_table,
            write_disposition,
            maximum_bytes_billed,
            job_timeout_ms,
            priority,
            labels,
        )
        return {"session_id": "arrow-options-session"}

    def dry_run(self, sql: str, params_json: str | None = None) -> dict[str, Any]:
        _ = (sql, params_json)
        return {}

    def read_table_arrow(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        selected_fields: Sequence[str] | None = None,
        row_restriction: str | None = None,
        max_streams: int = 4,
        compression: str = "LZ4_FRAME",
    ) -> RustArrowCursor:
        self.requests.append(
            (project_id, dataset_id, table_id, selected_fields, row_restriction, max_streams, compression)
        )
        raise RuntimeError("external Arrow read reached")

    def append_arrow(
        self,
        parent_table: str,
        arrow_ipc_bytes: bytes,
        mode: str = "_default",
    ) -> dict[str, Any]:
        _ = (parent_table, arrow_ipc_bytes, mode)
        raise NotImplementedError

    def close(self) -> None:
        return None


class _AsyncArrowOptionsBigQueryClient:
    """Async Storage Read client port sharing the exact native request observations."""

    def __init__(self, sync: _ArrowOptionsBigQueryClient) -> None:
        self.sync = sync

    async def query(
        self,
        sql: str,
        params_json: str | None = None,
        session_id: str | None = None,
        create_session: bool = False,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        job_timeout_ms: int | None = None,
        priority: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        return self.sync.query(
            sql,
            params_json,
            session_id,
            create_session,
            destination_table,
            write_disposition,
            maximum_bytes_billed,
            job_timeout_ms,
            priority,
            labels,
        )

    async def dry_run(self, sql: str, params_json: str | None = None) -> dict[str, Any]:
        return self.sync.dry_run(sql, params_json)

    async def read_table_arrow(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        selected_fields: Sequence[str] | None = None,
        row_restriction: str | None = None,
        max_streams: int = 4,
        compression: str = "LZ4_FRAME",
    ) -> RustArrowCursor:
        return self.sync.read_table_arrow(
            project_id,
            dataset_id,
            table_id,
            selected_fields,
            row_restriction,
            max_streams,
            compression,
        )

    async def append_arrow(
        self,
        parent_table: str,
        arrow_ipc_bytes: bytes,
        mode: str = "_default",
    ) -> dict[str, Any]:
        return self.sync.append_arrow(parent_table, arrow_ipc_bytes, mode)

    def close(self) -> None:
        self.sync.close()


def test_bigquery_arrow_stack_preserves_native_storage_options() -> None:
    """BigQuery stack options reach the real native driver's external Storage Read port."""
    client = _ArrowOptionsBigQueryClient()
    core = CoreSyncBigQueryDriver(BigQueryConfig(project_id="test-proj", dataset_id="analytics"), client=client)
    driver = BigQuerySyncDriver(core)
    try:
        with pytest.raises((RuntimeError, StackExecutionError), match="external Arrow read reached"):
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
        with pytest.raises((RuntimeError, StackExecutionError), match="external Arrow read reached"):
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

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Constructor performed a side effect before rejecting tenant isolation")

    class ForbiddenCore:
        def __getattribute__(self, name: str) -> Any:
            forbidden(name)

    for base in (BaseSyncDriver, BaseAsyncDriver, SyncAdapterConfig, AsyncAdapterConfig):
        monkeypatch.setattr(base, "__init__", forbidden)
    predicate = TenantPredicate(table_alias="t", tenant_value="tenant")
    constructors: dict[str, Callable[[], Any]] = {
        "sync-driver": lambda: BigQuerySyncDriver(ForbiddenCore(), tenant_predicate=predicate),
        "async-driver": lambda: BigQueryAsyncDriver(ForbiddenCore(), tenant_predicate=predicate),
        "sync-config": lambda: BigQuerySyncConfig(tenant_predicate=predicate),
        "async-config": lambda: BigQueryAsyncConfig(tenant_predicate=predicate),
    }
    monkeypatch.setattr(bq_config_mod, "_merge_connection_config", forbidden)
    for constructor_type in (BigQuerySyncDriver, BigQueryAsyncDriver, BigQuerySyncConfig, BigQueryAsyncConfig):
        monkeypatch.setattr(constructor_type, "__setattr__", forbidden)
    with pytest.raises(NotSupportedError):
        constructors[kind]()


@pytest.mark.parametrize(
    "method_name",
    [
        "execute",
        "execute_many",
        "execute_stack",
        "select",
        "select_one",
        "select_one_or_none",
        "select_value",
        "dry_run",
    ],
)
def test_bigquery_sync_driver_rejects_per_call_tenant_id(method_name: str) -> None:
    """Sync BigQuery query methods raise NotSupportedError when tenant_id is supplied."""
    engine = _InMemoryBigQueryEngine()
    driver = BigQuerySyncDriver(_SimulatedSyncCoreDriver(engine))
    calls: dict[str, Callable[[], Any]] = {
        "execute": lambda: driver.execute("SELECT 1", tenant_id="tenant-a"),
        "execute_many": lambda: driver.execute_many("INSERT INTO metrics VALUES (1)", [(1,)], tenant_id="tenant-a"),
        "execute_stack": lambda: driver.execute_stack(StatementStack().push_execute("SELECT 1"), tenant_id="tenant-a"),
        "select": lambda: driver.select("SELECT 1", tenant_id="tenant-a"),
        "select_one": lambda: driver.select_one("SELECT 1", tenant_id="tenant-a"),
        "select_one_or_none": lambda: driver.select_one_or_none("SELECT 1", tenant_id="tenant-a"),
        "select_value": lambda: driver.select_value("SELECT 1", tenant_id="tenant-a"),
        "dry_run": lambda: driver.dry_run("SELECT 1", tenant_id="tenant-a"),
    }
    with pytest.raises(NotSupportedError, match="Tenant session enforcement is unsupported for BigQuery"):
        calls[method_name]()
    assert engine.executed_sql == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    "method_name",
    [
        "execute",
        "execute_many",
        "execute_stack",
        "select",
        "select_one",
        "select_one_or_none",
        "select_value",
        "dry_run",
    ],
)
async def test_bigquery_async_driver_rejects_per_call_tenant_id(method_name: str) -> None:
    """Async BigQuery query methods raise NotSupportedError when tenant_id is supplied."""
    engine = _InMemoryBigQueryEngine()
    driver = BigQueryAsyncDriver(_SimulatedAsyncCoreDriver(engine))
    calls: dict[str, Callable[[], Awaitable[Any]]] = {
        "execute": lambda: driver.execute("SELECT 1", tenant_id="tenant-a"),
        "execute_many": lambda: driver.execute_many("INSERT INTO metrics VALUES (1)", [(1,)], tenant_id="tenant-a"),
        "execute_stack": lambda: driver.execute_stack(StatementStack().push_execute("SELECT 1"), tenant_id="tenant-a"),
        "select": lambda: driver.select("SELECT 1", tenant_id="tenant-a"),
        "select_one": lambda: driver.select_one("SELECT 1", tenant_id="tenant-a"),
        "select_one_or_none": lambda: driver.select_one_or_none("SELECT 1", tenant_id="tenant-a"),
        "select_value": lambda: driver.select_value("SELECT 1", tenant_id="tenant-a"),
        "dry_run": lambda: driver.dry_run("SELECT 1", tenant_id="tenant-a"),
    }
    with pytest.raises(NotSupportedError, match="Tenant session enforcement is unsupported for BigQuery"):
        await calls[method_name]()
    assert engine.executed_sql == []


@pytest.mark.anyio
async def test_bigquery_execute_many_compiles_parameters_and_maps_arrow_dry_run_exceptions() -> None:
    """BigQuery drivers compile execute_many parameters and map native errors on select_to_arrow, load_from_arrow, and dry_run."""
    recorded_many: list[tuple[str, list[Any]]] = []

    class _ManyRecordingSyncDriver(_SimulatedSyncCoreDriver):
        def execute_many(
            self,
            sql: str,
            parameter_sets: Sequence[Mapping[str, Any] | Sequence[Any]],
        ) -> int:
            recorded_many.append((sql, [dict(p) if isinstance(p, Mapping) else list(p) for p in parameter_sets]))
            return len(parameter_sets)

    class _ManyRecordingAsyncDriver(_SimulatedAsyncCoreDriver):
        async def execute_many(
            self,
            sql: str,
            parameter_sets: Sequence[Mapping[str, Any] | Sequence[Any]],
        ) -> int:
            recorded_many.append((sql, [dict(p) if isinstance(p, Mapping) else list(p) for p in parameter_sets]))
            return len(parameter_sets)

    engine = _InMemoryBigQueryEngine()
    sync_driver = BigQuerySyncDriver(_ManyRecordingSyncDriver(engine))
    sync_driver.execute_many(
        "INSERT INTO metrics (id, name, score) VALUES (?, ?, ?)",
        [(10, "ten", 1.5), (20, "twenty", 2.5)],
    )
    assert len(recorded_many) == 1
    compiled_sql, compiled_sets = recorded_many[0]
    assert "@" in compiled_sql
    assert "?" not in compiled_sql
    assert len(compiled_sets) == 2
    assert all(isinstance(item, dict) and len(item) == 3 for item in compiled_sets)
    assert list(compiled_sets[0].values()) == [10, "ten", 1.5]
    assert list(compiled_sets[1].values()) == [20, "twenty", 2.5]

    recorded_many.clear()
    async_driver = BigQueryAsyncDriver(_ManyRecordingAsyncDriver(engine))
    await async_driver.execute_many(
        "INSERT INTO metrics (id, name, score) VALUES (:id, :name, :score)",
        [
            {"id": 30, "name": "thirty", "score": 3.5},
            {"id": 40, "name": "forty", "score": 4.5},
        ],
    )
    assert len(recorded_many) == 1
    acompiled_sql, acompiled_sets = recorded_many[0]
    assert "@id" in acompiled_sql
    assert ":id" not in acompiled_sql
    assert acompiled_sets == [
        {"id": 30, "name": "thirty", "score": 3.5},
        {"id": 40, "name": "forty", "score": 4.5},
    ]

    class _FailingSyncCore(_SimulatedSyncCoreDriver):
        def select_to_arrow(
            self,
            sql: str,
            parameters: Mapping[str, Any] | Sequence[Any] | None = None,
            **_kwargs: Any,
        ) -> ArrowCursor:
            _ = (sql, parameters)
            raise BigQueryError("Permission denied on dataset")

        def load_from_arrow(self, table_name: str, data: Any, **_kwargs: Any) -> int:
            _ = (table_name, data)
            raise BigQueryError("Permission denied on dataset")

        def dry_run(
            self,
            sql: str,
            parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        ) -> BigQueryDryRunResult:
            _ = (sql, parameters)
            raise BigQueryError("Permission denied on dataset")

    class _FailingAsyncCore(_SimulatedAsyncCoreDriver):
        async def select_to_arrow(
            self,
            sql: str,
            parameters: Mapping[str, Any] | Sequence[Any] | None = None,
            **_kwargs: Any,
        ) -> ArrowCursor:
            _ = (sql, parameters)
            raise BigQueryError("Permission denied on dataset")

        async def load_from_arrow(self, table_name: str, data: Any, **_kwargs: Any) -> int:
            _ = (table_name, data)
            raise BigQueryError("Permission denied on dataset")

        async def dry_run(
            self,
            sql: str,
            parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        ) -> BigQueryDryRunResult:
            _ = (sql, parameters)
            raise BigQueryError("Permission denied on dataset")

    sample_tbl = pa.table({"id": [1], "name": ["x"], "score": [1.0]})
    failing_sync = BigQuerySyncDriver(_FailingSyncCore(engine))
    with pytest.raises(PermissionDeniedError):
        failing_sync.select_to_arrow("SELECT * FROM metrics")
    with pytest.raises(PermissionDeniedError):
        failing_sync.load_from_arrow("metrics", sample_tbl)
    with pytest.raises(PermissionDeniedError):
        failing_sync.dry_run("SELECT * FROM metrics")

    failing_async = BigQueryAsyncDriver(_FailingAsyncCore(engine))
    with pytest.raises(PermissionDeniedError):
        await failing_async.select_to_arrow("SELECT * FROM metrics")
    with pytest.raises(PermissionDeniedError):
        await failing_async.load_from_arrow("metrics", sample_tbl)
    with pytest.raises(PermissionDeniedError):
        await failing_async.dry_run("SELECT * FROM metrics")


@pytest.mark.anyio
async def test_bigquery_adk_memory_search_propagates_non_missing_table_errors() -> None:
    """BigQuery sync and async ADK memory search propagate query errors instead of silently scanning."""

    class _BrokenSearchSyncCore(_SimulatedSyncCoreDriver):
        def select(
            self,
            sql: str,
            parameters: Mapping[str, Any] | Sequence[Any] | None = None,
            *,
            schema_type: type[Any] | None = None,
        ) -> list[Any]:
            if "COSINE" in sql or "SEARCH(" in sql:
                raise RuntimeError("vector index corrupted")
            return super().select(sql, parameters, schema_type=schema_type)

    class _BrokenSearchAsyncCore(_SimulatedAsyncCoreDriver):
        async def select(
            self,
            sql: str,
            parameters: Mapping[str, Any] | Sequence[Any] | None = None,
            *,
            schema_type: type[Any] | None = None,
        ) -> list[Any]:
            if "COSINE" in sql or "SEARCH(" in sql:
                raise RuntimeError("vector index corrupted")
            return await super().select(sql, parameters, schema_type=schema_type)

    engine = _InMemoryBigQueryEngine()
    sync_cfg = BigQuerySyncConfig(
        project_id="test-proj",
        dataset_id="app_ds",
        connection_instance=_BrokenSearchSyncCore(engine),
        share_pool=False,
    )
    sync_mem = BigQuerySyncADKMemoryStore(sync_cfg)
    with pytest.raises(RuntimeError, match="vector index corrupted"):
        sync_mem.search_entries("alpha", "app", "u1", embedding=[0.1, 0.2])

    async_cfg = BigQueryAsyncConfig(
        project_id="test-proj",
        dataset_id="app_ds",
        connection_instance=_BrokenSearchAsyncCore(engine),
        share_pool=False,
    )
    async_mem = BigQueryAsyncADKMemoryStore(async_cfg)
    with pytest.raises(RuntimeError, match="vector index corrupted"):
        await async_mem.search_entries("alpha", "app", "u1", embedding=[0.1, 0.2])
