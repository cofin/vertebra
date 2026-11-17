"""Unit tests for Vertebra SQLSpec BigQuery modular adapter package."""

import asyncio
import subprocess
import sys
from datetime import timedelta
from importlib import import_module
from typing import Any

import pyarrow as pa
import pytest
from sqlspec.core import ParameterStyle, StatementConfig, TypedParameter
from sqlspec.exceptions import (
    ConnectionTimeoutError,
    DatabaseConnectionError,
    DataError,
    DialectNotSupportedError,
    IntegrityError,
    NotFoundError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
)
from sqlspec.exceptions import ImproperConfigurationError as SQLSpecImproperConfigurationError
from sqlspec.utils.adapters import resolve_adapter_name, resolve_extension_store_class

from vertebra._vertebra import (
    BigQueryError,
    BigQueryRowError,
    GoogleCloudAuthError,
    NotSupportedError,
)
from vertebra.db.adapters.bigquery import BigQueryDryRunResult, ObjectRef, ObjectTableConfig
from vertebra.db.arrow import ArrowCursor
from vertebra.extensions.sqlspec.adapters.bigquery import (
    BigQueryAsyncADKArtifactStore,
    BigQueryAsyncConfig,
    BigQueryAsyncConnectionContext,
    BigQueryAsyncDataDictionary,
    BigQueryAsyncDriver,
    BigQueryAsyncSessionContext,
    BigQueryConnectionParams,
    BigQueryConnectionQueryJob,
    BigQueryConnectionShim,
    BigQueryDriverFeatures,
    BigQueryEventQueueStore,
    BigQueryLitestarStore,
    BigQueryMetadataScope,
    BigQueryPoolParams,
    BigQuerySyncADKArtifactStore,
    BigQuerySyncConfig,
    BigQuerySyncConnectionContext,
    BigQuerySyncDataDictionary,
    BigQuerySyncDriver,
    BigQuerySyncSessionContext,
    default_statement_config,
    map_bigquery_exception,
)
from vertebra.extensions.sqlspec.adapters.bigquery.core import (
    bigquery_partition_clauses,
    encode_typed_bigquery_parameters,
    is_compound_googlesql_script,
    object_ref_to_artifact_reference,
)
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.storage import ArtifactReference
from vertebra.typing import ImproperConfigurationError, MissingDependencyError


class _RecordingBigQuerySyncNative:
    """Offline double simulating native `SyncBigQueryDriver` calls for SQLSpec adapter verification."""

    def __init__(self) -> None:
        self.project_id = "proj-test"
        self.dataset_id = "ds_test"
        self.calls: list[tuple[str, Any, dict[str, Any]]] = []
        self.description_rows: list[Any] = []
        self.description_cols: list[str] = []
        self.select_rows: list[dict[str, Any]] = []
        self.last_query_metadata: dict[str, Any] = {
            "job_id": "job-123",
            "session_id": "sess-456",
            "total_bytes_processed": 1024,
            "total_bytes_billed": 1048576,
            "cache_hit": True,
            "num_dml_affected_rows": 2,
        }
        self.closed = False
        self._session_id: str | None = None
        self._in_transaction: bool = False
        self._client: Any = None

    def connect(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> int:
        self.calls.append(("execute", (sql, parameters), dict(kwargs)))
        return 1

    def execute_many(self, sql: str, param_sets: Any = None, **kwargs: Any) -> int:
        batch = list(param_sets or [])
        self.calls.append(("execute_many", (sql, batch), dict(kwargs)))
        return len(batch)

    def execute_script(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(("execute_script", (sql, parameters), dict(kwargs)))
        return [{"ok": True}]

    def execute_with_description(self, sql: str, parameters: Any = None, **kwargs: Any) -> tuple[list[Any], list[str]]:
        self.calls.append(("execute_with_description", (sql, parameters), dict(kwargs)))
        return self.description_rows, self.description_cols

    def select(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(("select", (sql, parameters), dict(kwargs)))
        return list(self.select_rows)

    def select_to_arrow(self, sql: str, parameters: Any = None, **kwargs: Any) -> ArrowCursor:
        self.calls.append(("select_to_arrow", (sql, parameters), dict(kwargs)))
        return ArrowCursor.from_rows([{"id": 1, "label": "ok"}], threshold_rows=1)

    def load_from_arrow(self, table: str, payload: bytes, **kwargs: Any) -> int:
        self.calls.append(("load_from_arrow", (table, len(payload)), dict(kwargs)))
        return 3

    def dry_run(self, sql: str, parameters: Any = None) -> BigQueryDryRunResult:
        self.calls.append(("dry_run", (sql, parameters), {}))
        return BigQueryDryRunResult(total_bytes_processed=2048, total_bytes_billed=2048, cache_hit=False)

    def create_object_table(self, config: ObjectTableConfig, *, if_not_exists: bool = True) -> None:
        self.calls.append(("create_object_table", config.table_name, {"if_not_exists": if_not_exists}))

    def list_object_table_refs(self, config: ObjectTableConfig, **kwargs: Any) -> list[ObjectRef]:
        self.calls.append(("list_object_table_refs", config.table_name, dict(kwargs)))
        return [
            ObjectRef(
                uri="gs://bkt/obj.json",
                version="7",
                content_type="application/json",
                size=64,
                updated="2026-11-16T00:00:00Z",
                signed_url="https://signed.invalid/obj.json",
            )
        ]

    def begin(self) -> None:
        self.calls.append(("begin", None, {}))

    def commit(self) -> None:
        self.calls.append(("commit", None, {}))

    def rollback(self) -> None:
        self.calls.append(("rollback", None, {}))


class _RecordingBigQueryAsyncNative:
    """Offline async double simulating `AsyncBigQueryDriver` calls for SQLSpec adapter verification."""

    def __init__(self) -> None:
        self.project_id = "proj-async"
        self.dataset_id = "ds_async"
        self.calls: list[tuple[str, Any, dict[str, Any]]] = []
        self.description_rows: list[Any] = []
        self.description_cols: list[str] = []
        self.select_rows: list[dict[str, Any]] = []
        self.closed = False
        self._session_id: str | None = None
        self._in_transaction: bool = False
        self._client: Any = None

    async def close(self) -> None:
        self.closed = True

    async def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> int:
        self.calls.append(("execute", (sql, parameters), dict(kwargs)))
        return 1

    async def execute_many(self, sql: str, param_sets: Any = None, **kwargs: Any) -> int:
        batch = list(param_sets or [])
        self.calls.append(("execute_many", (sql, batch), dict(kwargs)))
        return len(batch)

    async def execute_script(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(("execute_script", (sql, parameters), dict(kwargs)))
        return [{"ok": True}]

    async def execute_with_description(
        self, sql: str, parameters: Any = None, **kwargs: Any
    ) -> tuple[list[Any], list[str]]:
        self.calls.append(("execute_with_description", (sql, parameters), dict(kwargs)))
        return self.description_rows, self.description_cols

    async def select(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(("select", (sql, parameters), dict(kwargs)))
        return list(self.select_rows)

    async def select_to_arrow(self, sql: str, parameters: Any = None, **kwargs: Any) -> ArrowCursor:
        self.calls.append(("select_to_arrow", (sql, parameters), dict(kwargs)))
        return ArrowCursor.from_rows([{"id": 2, "label": "async"}], threshold_rows=1)

    async def load_from_arrow(self, table: str, payload: bytes, **kwargs: Any) -> int:
        self.calls.append(("load_from_arrow", (table, len(payload)), dict(kwargs)))
        return 4

    async def dry_run(self, sql: str, parameters: Any = None) -> BigQueryDryRunResult:
        self.calls.append(("dry_run", (sql, parameters), {}))
        return BigQueryDryRunResult(total_bytes_processed=4096, total_bytes_billed=4096, cache_hit=True)


class _HttpStatusError(Exception):
    """Synthetic HTTP status error carrying `status_code` for mapping verification."""

    def __init__(self, message: str, *, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


def test_bigquery_pep562_exports_and_zero_sdk_import_isolation() -> None:
    """Verify PEP 562 lazy exports and zero eager import of `pyarrow` or `google.adk`."""
    probe = (
        "import sys; "
        "import vertebra.extensions.sqlspec.adapters.bigquery as bq; "
        "cfg = bq.BigQuerySyncConfig(project_id='p', dataset_id='d'); "
        "assert cfg.project_id == 'p'; "
        "assert 'pyarrow' not in sys.modules; "
        "assert 'google.adk' not in sys.modules"
    )
    proc = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr

    pkg = import_module("vertebra.extensions.sqlspec.adapters.bigquery")
    assert pkg.BigQuerySyncConfig is BigQuerySyncConfig
    assert pkg.BigQuerySyncConnectionContext is BigQuerySyncConnectionContext
    assert pkg.BigQueryAsyncConnectionContext is BigQueryAsyncConnectionContext
    assert pkg.BigQuerySyncSessionContext is BigQuerySyncSessionContext
    assert pkg.BigQueryAsyncSessionContext is BigQueryAsyncSessionContext
    assert pkg.BigQueryConnectionParams is BigQueryConnectionParams
    assert pkg.BigQueryPoolParams is BigQueryPoolParams
    assert pkg.BigQueryDriverFeatures is BigQueryDriverFeatures
    assert pkg.BigQueryAsyncADKArtifactStore is BigQueryAsyncADKArtifactStore
    with pytest.raises(AttributeError):
        _ = pkg.NonExistentBigQueryExport


def test_bigquery_config_identity_connection_instance_and_tenant_rejection() -> None:
    """Verify adapter registration, `connection_instance` preservation, and tenant rejection."""
    from vertebra.extensions.sqlspec.adapters.bigquery.adk import (
        BigQueryAsyncADKStore,
        BigQuerySyncADKMemoryStore,
        BigQuerySyncADKStore,
    )

    native = _RecordingBigQuerySyncNative()
    sync_cfg = BigQuerySyncConfig(
        project_id="proj-test",
        dataset_id="ds_test",
        connection_instance=native,
    )
    async_cfg = BigQueryAsyncConfig(
        project_id="proj-async",
        dataset_id="ds_async",
        connection_instance=_RecordingBigQueryAsyncNative(),
    )

    assert resolve_adapter_name(sync_cfg) == "vertebra_bigquery"
    assert resolve_adapter_name(async_cfg) == "vertebra_bigquery"
    assert sync_cfg.dialect == "bigquery"
    assert async_cfg.dialect == "bigquery"
    assert (
        resolve_extension_store_class(sync_cfg, extension="adk", store_attr="adk_store_class", store_suffix="ADKStore")
        is BigQuerySyncADKStore
    )
    assert (
        resolve_extension_store_class(async_cfg, extension="adk", store_attr="adk_store_class", store_suffix="ADKStore")
        is BigQueryAsyncADKStore
    )
    assert (
        resolve_extension_store_class(
            sync_cfg, extension="adk", store_attr="adk_memory_store_class", store_suffix="ADKMemoryStore"
        )
        is BigQuerySyncADKMemoryStore
    )
    assert (
        resolve_extension_store_class(
            sync_cfg,
            extension="adk",
            store_attr="adk_artifact_store_class",
            store_suffix="ADKArtifactStore",
        )
        is BigQuerySyncADKArtifactStore
    )
    assert (
        resolve_extension_store_class(
            async_cfg,
            extension="adk",
            store_attr="adk_artifact_store_class",
            store_suffix="ADKArtifactStore",
        )
        is BigQueryAsyncADKArtifactStore
    )
    assert (
        resolve_extension_store_class(
            sync_cfg, extension="events", store_attr="events_store_class", store_suffix="EventQueueStore"
        )
        is BigQueryEventQueueStore
    )
    assert (
        resolve_extension_store_class(
            sync_cfg, extension="litestar", store_attr="litestar_store_class", store_suffix="Store"
        )
        is BigQueryLitestarStore
    )

    drv = sync_cfg.create_driver()
    assert isinstance(drv, BigQuerySyncDriver)
    assert drv.connection is native
    shim = sync_cfg.create_connection()
    assert isinstance(shim, BigQueryConnectionShim)
    job = shim.query("SELECT @id AS id", {"id": 1})
    assert isinstance(job, BigQueryConnectionQueryJob)
    assert job.job_id == "job-123"
    assert job.session_id == "sess-456"
    assert job.cache_hit is True
    assert job.total_bytes_processed == 1024

    with pytest.raises(NotSupportedError):
        BigQuerySyncConfig(project_id="p", dataset_id="d", tenant_predicate=object())

    with pytest.raises(NotSupportedError):
        BigQueryAsyncConfig(project_id="p", dataset_id="d", tenant_predicate=object())


def test_bigquery_statement_config_typed_parameters_and_compound_scripts() -> None:
    """Verify `@name` GoogleSQL compilation, `TypedParameter` encoding, and procedural script detection."""
    stmt_cfg = default_statement_config()
    assert isinstance(stmt_cfg, StatementConfig)
    assert stmt_cfg.dialect == "bigquery"
    assert stmt_cfg.parameter_config.default_execution_parameter_style == ParameterStyle.NAMED_AT

    encoded = encode_typed_bigquery_parameters(
        {
            "vec": TypedParameter([0.1, 0.2], list, semantic_name="ARRAY<FLOAT64>"),
            "meta": TypedParameter({"ok": True}, dict, semantic_name="JSON"),
            "id": 9,
        }
    )
    assert encoded["vec"] == {"type": "ARRAY<FLOAT64>", "values": [0.1, 0.2]}
    assert encoded["meta"] == {"type": "JSON", "value": '{"ok":true}'}
    assert encoded["id"] == 9

    compound = "DECLARE x INT64 DEFAULT 1; BEGIN SELECT x; END;"
    assert is_compound_googlesql_script(compound) is True
    assert is_compound_googlesql_script("SELECT 'DECLARE BEGIN END'; SELECT 1;") is False


def test_bigquery_sync_driver_zero_row_arrow_object_table_and_dry_run() -> None:
    """Verify zero-row metadata preservation, compound script routing, Arrow APIs, and ObjectTable RPCs."""
    native = _RecordingBigQuerySyncNative()
    native.description_rows = []
    native.description_cols = ["id", "title"]
    cfg = BigQuerySyncConfig(project_id="proj-test", dataset_id="ds_test", connection_instance=native)

    with cfg.provide_session() as driver:
        res = driver.execute("SELECT id, title FROM items WHERE id = @id", {"id": 5})
        assert res.data == []
        assert res.column_names == ["id", "title"]

        compound_sql = "DECLARE v INT64 DEFAULT 1; BEGIN SELECT v; END;"
        script_res = driver.execute_script(compound_sql)
        assert script_res.total_statements == 1
        assert script_res.successful_statements == 1
        assert native.calls[-1][0] == "execute_script"
        assert native.calls[-1][1][0] == compound_sql

        arrow_res = driver.select_to_arrow(
            "SELECT id, label FROM items",
            selected_fields=["id", "label"],
            max_streams=2,
            threshold_rows=10,
        )
        assert isinstance(arrow_res.data, pa.Table)
        assert arrow_res.rows_affected == 1
        assert native.calls[-1][0] == "select_to_arrow"
        assert native.calls[-1][2] == {
            "threshold_rows": 10,
            "selected_fields": ["id", "label"],
            "max_streams": 2,
        }

        tbl = pa.table({"id": [1, 2], "name": ["a", "b"]})
        job = driver.load_from_arrow("`proj-test`.`ds_test`.`items`", tbl, overwrite=True)
        assert job.telemetry is not None
        assert job.telemetry["rows_processed"] == 3
        assert native.calls[-1][0] == "load_from_arrow"
        assert native.calls[-1][2] == {"mode": "PENDING", "write_disposition": "WRITE_TRUNCATE"}

        dry = driver.dry_run("SELECT 1")
        assert dry.total_bytes_processed == 2048

        ot_cfg = ObjectTableConfig(
            table_name="`proj-test`.`ds_test`.`assets`",
            connection_id="us.conn",
            uris=("gs://bkt/*",),
        )
        driver.create_object_table(ot_cfg)
        refs = driver.list_object_table_refs(ot_cfg, limit=5)
        assert len(refs) == 1
        artifact_ref = object_ref_to_artifact_reference(refs[0])
        assert isinstance(artifact_ref, ArtifactReference)
        assert artifact_ref.uri == "gs://bkt/obj.json"
        assert artifact_ref.version == 7

        with pytest.raises(NotImplementedError):
            driver.create_savepoint("sp_unsupported")


@pytest.mark.anyio
async def test_bigquery_async_driver_dispatch_and_arrow() -> None:
    """Verify `BigQueryAsyncDriver` query dispatch, Arrow I/O, and dry-run delegation."""
    native = _RecordingBigQueryAsyncNative()
    native.description_rows = [{"id": 2}]
    native.description_cols = ["id"]
    cfg = BigQueryAsyncConfig(project_id="proj-async", dataset_id="ds_async", connection_instance=native)

    async with cfg.provide_session() as driver:
        assert isinstance(driver, BigQueryAsyncDriver)
        res = await driver.execute("SELECT id FROM items WHERE id = @id", {"id": 2})
        assert res.data == [{"id": 2}]

        arrow_res = await driver.select_to_arrow("SELECT id FROM items", max_streams=1)
        assert isinstance(arrow_res.data, pa.Table)
        assert arrow_res.rows_affected == 1

        load_job = await driver.load_from_arrow("items", pa.table({"id": [1]}))
        assert load_job.telemetry is not None
        assert load_job.telemetry["rows_processed"] == 4
        assert native.calls[-1][2] == {"mode": "PENDING", "write_disposition": "WRITE_APPEND"}

        dry = await driver.dry_run("SELECT 1")
        assert dry.cache_hit is True


def test_bigquery_data_dictionary_metadata_scope_and_feature_flags() -> None:
    """Verify `BigQueryMetadataScope` and data dictionary version, tables, and feature capability flags."""
    scope = BigQueryMetadataScope(project="proj-test", dataset="ds_test", region="us")
    assert scope.project == "proj-test"
    assert scope.dataset == "ds_test"
    assert scope.region == "us"

    native = _RecordingBigQuerySyncNative()
    native.select_rows = [{"table_name": "events", "table_type": "BASE TABLE", "schema_name": "ds_test"}]
    native.description_rows = [{"table_name": "events", "table_type": "BASE TABLE", "schema_name": "ds_test"}]
    native.description_cols = ["table_name", "table_type", "schema_name"]
    driver = BigQuerySyncDriver(native)

    dd = BigQuerySyncDataDictionary()
    assert dd.get_feature_flag(driver, "supports_json") is True
    assert dd.get_feature_flag(driver, "supports_vectors") is True
    assert dd.get_feature_flag(driver, "supports_transactions") is True
    assert dd.get_feature_flag(driver, "supports_savepoints") is False
    tables = dd.get_tables(driver, schema="ds_test")
    assert len(tables) == 1
    assert tables[0]["table_name"] == "events"
    assert isinstance(BigQueryAsyncDataDictionary(), BigQueryAsyncDataDictionary)


def test_bigquery_stores_adk_artifacts_events_and_litestar() -> None:
    """Verify BigQuery ADK, Artifact (`ArtifactReference`), EventQueue, and Litestar DDL & store flows."""
    from vertebra.extensions.sqlspec.adapters.bigquery.adk import (
        BigQuerySyncADKMemoryStore,
        BigQuerySyncADKStore,
    )

    native = _RecordingBigQuerySyncNative()
    backend = ObjectStoreBackend("memory://")
    cfg = BigQuerySyncConfig(
        project_id="proj-test",
        dataset_id="ds_test",
        connection_instance=native,
        extension_config={
            "adk": {"enable_memory": True, "memory_use_fts": True},
            "events": {"with_partitioning": True, "require_partition_filter": True},
            "litestar": {"with_partitioning": True, "partition_expiration_days": 7},
        },
    )

    partition_clause, options_clause = bigquery_partition_clauses(
        "available_at", enabled=True, expiration_days=14, require_filter=True
    )
    assert "PARTITION BY DATE(available_at)" in partition_clause
    assert "partition_expiration_days = 14" in options_clause
    assert "require_partition_filter = TRUE" in options_clause

    events_store = BigQueryEventQueueStore(cfg)
    ev_stmts = events_store.create_statements()
    assert any("PARTITION BY DATE(available_at)" in s and "CLUSTER BY" in s for s in ev_stmts)

    lit_store = BigQueryLitestarStore(cfg)
    lit_ddl = lit_store._table_ddl()
    assert "BYTES NOT NULL" in lit_ddl
    assert "PARTITION BY DATE(expires_at)" in lit_ddl
    assert lit_store._drop_table_sql() == ["DROP TABLE IF EXISTS `proj-test`.`ds_test`.`litestar_session`"]

    asyncio.run(lit_store.create_table())
    asyncio.run(lit_store.set("sid-1", b"val-bytes", expires_in=timedelta(seconds=30)))
    assert any("MERGE" in call[1][0] for call in native.calls if call[0] == "execute")

    art_store = BigQuerySyncADKArtifactStore(cfg, storage_backend=backend)
    saved_ref = art_store.save_artifact_object("app1", "user1", "notes.txt", b"hello-bq", mime_type="text/plain")
    assert isinstance(saved_ref, ArtifactReference)
    assert saved_ref.size == 8
    stored_keys = backend.list_objects_sync()
    assert len(stored_keys) == 1
    assert backend.read_bytes_sync(stored_keys[0]) == b"hello-bq"

    converted_ref = BigQuerySyncADKArtifactStore.to_artifact_reference(
        ObjectRef(
            uri="gs://bucket/app1/user1/report.pdf",
            version="2",
            content_type="application/pdf",
            size=128,
            updated="2026-11-16T00:00:00Z",
        )
    )
    assert isinstance(converted_ref, ArtifactReference)
    assert converted_ref.uri == "gs://bucket/app1/user1/report.pdf"
    assert converted_ref.version == 2

    mem_store = BigQuerySyncADKMemoryStore(cfg)
    vec_sql, vec_params = mem_store.build_vector_search_sql(app_name="app1", user_id="user1", top_k=5)
    assert "VECTOR_SEARCH" in vec_sql
    assert vec_params["app_name"] == "app1"
    fts_sql, fts_params = mem_store.build_fts_search_sql(app_name="app1", user_id="user1", query="memory", top_k=5)
    assert "SEARCH(" in fts_sql
    assert "memory" in fts_params.values()
    native.calls.clear()
    adk_store = BigQuerySyncADKStore(cfg)
    adk_store.create_tables()
    assert sum(1 for call in native.calls if call[0] == "execute" and "CREATE TABLE IF NOT EXISTS" in call[1][0]) == 5


@pytest.mark.parametrize(
    ("exc", "expected_cls"),
    [
        (NotSupportedError("unsupported feature"), DialectNotSupportedError),
        (GoogleCloudAuthError("missing adc"), PermissionDeniedError),
        (BigQueryRowError("bad row"), DataError),
        (_HttpStatusError("forbidden", status_code=403), PermissionDeniedError),
        (_HttpStatusError("missing", status_code=404), NotFoundError),
        (_HttpStatusError("timeout", status_code=408), ConnectionTimeoutError),
        (_HttpStatusError("gateway timeout", status_code=504), QueryTimeoutError),
        (_HttpStatusError("conflict", status_code=409), IntegrityError),
        (_HttpStatusError("rate limited", status_code=429), OperationalError),
        (_HttpStatusError("syntax error", status_code=400), SQLParsingError),
        (_HttpStatusError("bad input", status_code=400), DataError),
        (BigQueryError("network connect refused"), DatabaseConnectionError),
    ],
)
def test_bigquery_exception_mapping(exc: BaseException, expected_cls: type[Exception]) -> None:
    """Verify deterministic mapping of Vertebra BigQuery and HTTP status exceptions."""
    mapped = map_bigquery_exception(exc)
    assert isinstance(mapped, expected_cls)


@pytest.mark.parametrize("method_name", ["select_to_arrow", "load_from_arrow"])
def test_bigquery_missing_pyarrow_raises_missing_dependency_error(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
) -> None:
    """Verify Arrow methods raise `MissingDependencyError` naming `extra='arrow'` when `pyarrow` is absent."""
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    sync_cfg = BigQuerySyncConfig(
        project_id="proj-test",
        dataset_id="ds_test",
        connection_instance=_RecordingBigQuerySyncNative(),
    )
    async_cfg = BigQueryAsyncConfig(
        project_id="proj-async",
        dataset_id="ds_async",
        connection_instance=_RecordingBigQueryAsyncNative(),
    )

    with sync_cfg.provide_session() as sync_driver, pytest.raises(MissingDependencyError, match="arrow"):
        if method_name == "select_to_arrow":
            sync_driver.select_to_arrow("SELECT 1")
        else:
            sync_driver.load_from_arrow("items", b"arrow-bytes")

    async def _run_async() -> None:
        async with async_cfg.provide_session() as async_driver:
            with pytest.raises(MissingDependencyError, match="arrow"):
                if method_name == "select_to_arrow":
                    await async_driver.select_to_arrow("SELECT 1")
                else:
                    await async_driver.load_from_arrow("items", b"arrow-bytes")

    asyncio.run(_run_async())


def test_bigquery_sessions_create_distinct_drivers_and_mark_closed() -> None:
    """Verify repeated `provide_session` entries yield fresh drivers and mark exited drivers closed."""
    sync_native = _RecordingBigQuerySyncNative()
    sync_cfg = BigQuerySyncConfig(
        project_id="proj-test",
        dataset_id="ds_test",
        connection_instance=sync_native,
        driver_features={"reset_session_on_release": True},
    )
    assert not hasattr(sync_cfg, "_driver_instance")

    with sync_cfg.provide_session(statement_config=default_statement_config()) as driver_one:
        assert driver_one.connection is sync_native
    assert not driver_one.connection
    assert repr(driver_one.connection) == "<ClosedConnection>"

    with sync_cfg.provide_session() as driver_two:
        assert driver_two is not driver_one
        assert driver_two.connection is sync_native
    assert not driver_two.connection
    assert repr(driver_two.connection) == "<ClosedConnection>"
    assert sync_native.calls == []

    with sync_cfg.provide_connection("ignored-arg", custom_flag=True) as conn:
        assert conn is sync_native

    async_native = _RecordingBigQueryAsyncNative()
    async_cfg = BigQueryAsyncConfig(
        project_id="proj-async",
        dataset_id="ds_async",
        connection_instance=async_native,
        reset_session_on_release=True,
    )
    assert not hasattr(async_cfg, "_driver_instance")

    async def _verify_async() -> None:
        async with async_cfg.provide_session(statement_config=default_statement_config()) as a_one:
            assert a_one.connection is async_native
        assert not a_one.connection
        assert repr(a_one.connection) == "<ClosedConnection>"

        async with async_cfg.provide_session() as a_two:
            assert a_two is not a_one
            assert a_two.connection is async_native
        assert not a_two.connection
        assert repr(a_two.connection) == "<ClosedConnection>"
        assert async_native.calls == []

        async with async_cfg.provide_connection("ignored-arg", custom_flag=True) as a_conn:
            assert a_conn is async_native

    asyncio.run(_verify_async())


def test_bigquery_reset_session_on_release_clears_active_session() -> None:
    """Verify `reset_session_on_release=True` resets active session state without failing on stateless REST."""
    sync_native = _RecordingBigQuerySyncNative()
    sync_native._session_id = "sess-sync-1"
    sync_native._in_transaction = True
    sync_native._client = object()
    sync_cfg = BigQuerySyncConfig(
        project_id="proj-test",
        dataset_id="ds_test",
        connection_instance=sync_native,
        reset_session_on_release=True,
    )
    with sync_cfg.provide_session():
        pass
    assert sync_native._session_id is None
    assert sync_native._in_transaction is False
    assert any("ABORT_SESSION" in call[1][0] for call in sync_native.calls if call[0] == "execute")


@pytest.mark.parametrize(
    ("replica_spec", "expected_project", "expected_dataset"),
    [
        ("bigquery://reader-proj/ds_ro", "reader-proj", "ds_ro"),
        ({"project": "reader-dict", "dataset_id": "ds_dict"}, "reader-dict", "ds_dict"),
        ("reader-plain-project", "reader-plain-project", "ds_primary"),
    ],
)
def test_bigquery_replicas_routing_and_validation(
    replica_spec: str | dict[str, Any],
    expected_project: str,
    expected_dataset: str | None,
) -> None:
    """Verify `replicas=` configures derived replica pools and rejects boolean `replicas=True`."""
    sync_primary = BigQuerySyncConfig(
        project_id="primary-proj",
        dataset_id="ds_primary",
        replicas=[replica_spec],
    )
    assert "default" in sync_primary.replica_groups
    replica_cfg = sync_primary.replica_groups["default"].specs[0].config
    assert isinstance(replica_cfg, BigQuerySyncConfig)
    assert replica_cfg.is_replica is True
    assert replica_cfg.project_id == expected_project
    assert replica_cfg.dataset_id == expected_dataset

    async_primary = BigQueryAsyncConfig(
        project_id="primary-proj",
        dataset_id="ds_primary",
        replicas=[replica_spec],
    )
    assert "default" in async_primary.replica_groups
    async_replica = async_primary.replica_groups["default"].specs[0].config
    assert isinstance(async_replica, BigQueryAsyncConfig)
    assert async_replica.is_replica is True
    assert async_replica.project_id == expected_project
    assert async_replica.dataset_id == expected_dataset

    with pytest.raises((ImproperConfigurationError, SQLSpecImproperConfigurationError)):
        BigQuerySyncConfig(project_id="primary-proj", dataset_id="ds_primary", replicas=True)


def test_bigquery_select_to_storage_native_export_and_streaming() -> None:
    """Verify `select_to_storage` uses native `EXPORT DATA` for GCS and streams local destinations."""
    native = _RecordingBigQuerySyncNative()
    native.select_rows = [{"id": 1, "label": "row-1"}]
    native.description_rows = [{"id": 1, "label": "row-1"}]
    native.description_cols = ["id", "label"]
    cfg = BigQuerySyncConfig(
        project_id="proj-test",
        dataset_id="ds_test",
        connection_instance=native,
        driver_features={"native_export_connection": "proj-test.us.conn1"},
    )
    with cfg.provide_session() as driver:
        gcs_job = driver.select_to_storage("SELECT id, label FROM items", "gs://my-bucket/out/*.parquet")
        assert gcs_job.telemetry is not None
        assert gcs_job.telemetry["extra"] == {"native_export": True}
        assert native.calls[-1][0] == "execute"
        assert "EXPORT DATA" in native.calls[-1][1][0]
        assert "gs://my-bucket/out/*.parquet" in native.calls[-1][1][0]

        s3_job = driver.select_to_storage(
            "SELECT id, label FROM items",
            "s3://my-bucket/out/*.csv",
            format_hint="csv",
        )
        assert s3_job.telemetry is not None
        assert "WITH CONNECTION `proj-test.us.conn1`" in native.calls[-1][1][0]

        local_backend = ObjectStoreBackend("memory://")
        stream_job = driver.select_to_storage(
            "SELECT id, label FROM items",
            "memory://local-export/items.parquet",
            format_hint="parquet",
        )
        assert stream_job.telemetry is not None
        assert local_backend is not None

    async_native = _RecordingBigQueryAsyncNative()
    async_native.select_rows = [{"id": 2, "label": "async-row"}]
    async_native.description_rows = [{"id": 2, "label": "async-row"}]
    async_native.description_cols = ["id", "label"]
    async_cfg = BigQueryAsyncConfig(
        project_id="proj-async",
        dataset_id="ds_async",
        connection_instance=async_native,
    )

    async def _run_async_storage() -> None:
        async with async_cfg.provide_session() as a_driver:
            a_gcs = await a_driver.select_to_storage("SELECT id FROM items", "gcs://bkt/part-*.parquet")
            assert a_gcs.telemetry is not None
            assert a_gcs.telemetry["extra"] == {"native_export": True}
            assert "gs://bkt/part-*.parquet" in async_native.calls[-1][1][0]

            a_local = await a_driver.select_to_storage(
                "SELECT id, label FROM items",
                "memory://async-export/items.parquet",
                format_hint="parquet",
            )
            assert a_local.telemetry is not None

    asyncio.run(_run_async_storage())


def test_bigquery_module_invariants_slots_and_no_top_level_pyarrow() -> None:
    """Verify `from __future__ import annotations`, `__slots__`, and absence of top-level `pyarrow`."""
    from pathlib import Path

    pkg_dir = Path(__file__).resolve().parents[3] / "vertebra" / "extensions" / "sqlspec" / "adapters" / "bigquery"
    for filename in ("__init__.py", "_typing.py", "config.py", "driver.py"):
        text = (pkg_dir / filename).read_text(encoding="utf-8")
        assert "from __future__ import annotations" in text
    driver_text = (pkg_dir / "driver.py").read_text(encoding="utf-8")
    assert "import pyarrow" not in driver_text
    config_text = (pkg_dir / "config.py").read_text(encoding="utf-8")
    assert "_driver_instance" not in config_text

    for cls in (
        BigQuerySyncConnectionContext,
        BigQueryAsyncConnectionContext,
        BigQuerySyncSessionContext,
        BigQueryAsyncSessionContext,
        BigQuerySyncConfig,
        BigQueryAsyncConfig,
        BigQuerySyncDriver,
        BigQueryAsyncDriver,
    ):
        assert "__slots__" in cls.__dict__
