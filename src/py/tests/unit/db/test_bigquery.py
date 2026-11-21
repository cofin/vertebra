"""Unit tests for pure-Rust Google Cloud BigQuery drivers, Arrow Storage APIs, Object Tables, and DataFusion."""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
import threading
from collections.abc import Sequence
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from types import MappingProxyType
from typing import Any

import msgspec
import pyarrow as pa
import pyarrow.compute as pc
import pytest

import vertebra.db.adapters.bigquery as bq_pkg
import vertebra.db.adapters.bigquery.config as bq_config_mod
import vertebra.db.adapters.bigquery.object_tables as bq_object_tables_mod
from vertebra._bigquery import (
    AsyncBigQueryClient,
    AsyncBigQueryCursor,
    SyncBigQueryClient,
    SyncBigQueryCursor,
)
from vertebra._vertebra import (
    BigQueryError,
    InterfaceError,
    NotSupportedError,
    ProgrammingError,
)
from vertebra.agent import ConfirmationGate, PermissionDeniedError, build_tools
from vertebra.db import (
    ArrowCursor,
    DatabaseRegistry,
)
from vertebra.db.adapters.bigquery import (
    AsyncBigQueryDriver,
    BigQueryConfig,
    BigQueryCore,
    BigQueryDryRunResult,
    BigQueryEmbeddingPushdown,
    BigQueryToolProvider,
    ObjectRef,
    ObjectTableConfig,
    SyncBigQueryDriver,
)
from vertebra.db.adapters.bigquery.object_tables import (
    hydrate_object_refs_async,
    hydrate_object_refs_sync,
)
from vertebra.db.adapters.datafusion import DataFusionEngine
from vertebra.db.driver import DataError
from vertebra.embeddings import PushdownEmbedder
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.storage import SigningConfig, SyncObjectStore
from vertebra.typing import (
    FrozenBaseStruct,
    ImproperConfigurationError,
    MissingDependencyError,
    VertebraError,
    reset_dependency_cache,
)
from vertebra.utils.module_loader import import_optional


class _BigQueryDataFrame:
    """In-memory Arrow DataFrame stand-in for BigQuery DataFusion federation tests."""

    __slots__ = ("_table",)

    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def to_arrow_table(self) -> pa.Table:
        """Return the underlying normalized PyArrow table."""
        return self._table


class _BigQuerySessionContext:
    """Deterministic PyArrow-backed SessionContext double for BigQuery federation tests."""

    __slots__ = ("tables",)

    def __init__(self) -> None:
        self.tables: dict[str, pa.Table] = {}

    def register_table(self, name: str, table: Any) -> None:
        """Require a normalized pyarrow.Table from ArrowCursor registration."""
        if not isinstance(table, pa.Table):
            msg = f"Expected pyarrow.Table, got {type(table)!r}"
            raise TypeError(msg)
        self.tables[name] = table

    def sql(self, query: str) -> _BigQueryDataFrame:
        """Execute filtered projections over registered BigQuery PyArrow tables or raise."""
        normalized = " ".join(query.strip().split())
        if normalized == "SELECT id, name FROM bq_metrics WHERE id = 1":
            if "bq_metrics" not in self.tables:
                msg = "Table 'bq_metrics' is not registered"
                raise RuntimeError(msg)
            tbl = self.tables["bq_metrics"]
            filtered = tbl.filter(pc.equal(tbl["id"], 1)).select(["id", "name"])
            return _BigQueryDataFrame(filtered)
        msg = f"Invalid DataFusion SQL: {query}"
        raise RuntimeError(msg)


class MetricRow(FrozenBaseStruct, frozen=True, gc=False):
    """Typed row struct for BigQuery hydration tests."""

    id: int
    """Row identifier."""

    name: str
    """Metric name."""

    score: float
    """Metric score."""


class _MockBigQueryRestServer:
    """Local HTTP/1.1 BigQuery REST v2 server for end-to-end wire tests."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.inserted_rows: list[dict[str, Any]] = []
        self.session_counter: int = 0

        parent = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                _ = (format, args)

            def do_GET(self) -> None:
                parent.requests.append({"method": "GET", "path": self.path, "body": {}})
                self._send_json(
                    200,
                    {
                        "jobComplete": True,
                        "jobReference": {"projectId": "test-proj", "jobId": "job-123", "location": "US"},
                        "schema": {"fields": []},
                        "rows": [],
                        "totalRows": "0",
                        "numDmlAffectedRows": "1",
                    },
                )

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
                body = json.loads(raw_body) if raw_body else {}
                parent.requests.append({"method": "POST", "path": self.path, "body": body})

                if self.path.endswith("/jobs"):
                    query_cfg = body.get("configuration", {}).get("query", {})
                    is_dry_run = bool(body.get("configuration", {}).get("dryRun", False))
                    if is_dry_run:
                        tib_bytes = 1 << 40
                        resp = {
                            "jobReference": {"projectId": "test-proj", "jobId": "dryrun-job-1", "location": "US"},
                            "status": {"state": "DONE"},
                            "statistics": {
                                "totalBytesProcessed": str(tib_bytes),
                                "query": {
                                    "totalBytesProcessed": str(tib_bytes),
                                    "totalBytesBilled": str(tib_bytes),
                                    "cacheHit": False,
                                    "referencedTables": [
                                        {
                                            "projectId": "test-proj",
                                            "datasetId": "analytics",
                                            "tableId": "events",
                                        }
                                    ],
                                    "schema": {
                                        "fields": [
                                            {"name": "id", "type": "INT64", "mode": "NULLABLE"},
                                            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                                        ]
                                    },
                                },
                            },
                        }
                        self._send_json(200, resp)
                        return
                    sql = str(query_cfg.get("query", ""))
                    resp = parent._build_query_response(sql, body)
                    self._send_json(200, resp)
                    return

                if self.path.endswith("/queries"):
                    sql = str(body.get("query", ""))
                    resp = parent._build_query_response(sql, body)
                    self._send_json(200, resp)
                    return

                self._send_json(404, {"error": {"code": 404, "message": "Not found"}})

            def _send_json(self, status: int, payload: dict[str, Any]) -> None:
                encoded = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self.host_port = f"127.0.0.1:{self._server.server_port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def _build_query_response(self, sql: str, body: dict[str, Any]) -> dict[str, Any]:
        upper = sql.strip().upper()
        create_session = bool(body.get("createSession", False))
        conn_props = body.get("connectionProperties") or []
        session_id = None
        for prop in conn_props:
            if prop.get("key") == "session_id":
                session_id = prop.get("value")
        if create_session and not session_id:
            self.session_counter += 1
            session_id = f"bq-session-{self.session_counter}"

        base_resp: dict[str, Any] = {
            "jobComplete": True,
            "jobReference": {"projectId": "test-proj", "jobId": "job-123", "location": "US"},
            "totalBytesProcessed": "4096",
            "cacheHit": False,
        }
        if session_id:
            base_resp["sessionInfo"] = {"sessionId": session_id}

        if "ML.GENERATE_EMBEDDING" in upper:
            base_resp["schema"] = {
                "fields": [
                    {"name": "content", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "ml_generate_embedding_result", "type": "FLOAT64", "mode": "REPEATED"},
                ]
            }
            params = body.get("queryParameters") or []
            texts: list[str] = ["hello"]
            for param in params:
                pval = param.get("parameterValue", {})
                if "arrayValue" in pval:
                    arr = pval["arrayValue"].get("arrayValues") or pval["arrayValue"].get("values") or []
                    texts = [str(item.get("value", "")) for item in arr]
                    break
                if "arrayValues" in pval:
                    arr = pval.get("arrayValues") or []
                    texts = [str(item.get("value", "")) for item in arr]
                    break
            base_resp["rows"] = [
                {
                    "f": [
                        {"v": txt},
                        {"v": [{"v": "0.25"}, {"v": "0.5"}, {"v": "0.75"}]},
                    ]
                }
                for txt in texts
            ]
            base_resp["totalRows"] = str(len(texts))
            return base_resp

        if "FROM" in upper and "OBJECT_TABLE_DOCS" in upper:
            base_resp["schema"] = {
                "fields": [
                    {"name": "uri", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "generation", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "content_type", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "size", "type": "INT64", "mode": "NULLABLE"},
                    {"name": "md5_hash", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "updated", "type": "STRING", "mode": "NULLABLE"},
                ]
            }
            base_resp["rows"] = [
                {
                    "f": [
                        {"v": "gs://my-bucket/docs/report.pdf"},
                        {"v": "101"},
                        {"v": "application/pdf"},
                        {"v": "2048"},
                        {"v": "abc123md5"},
                        {"v": "2026-11-02T00:00:00Z"},
                    ]
                }
            ]
            base_resp["totalRows"] = "1"
            return base_resp

        if upper.startswith(("INSERT ", "UPDATE ", "DELETE ", "MERGE ", "EXPORT DATA", "LOAD DATA", "CREATE ")):
            if upper.startswith("INSERT "):
                self.inserted_rows.append(body)
            base_resp["schema"] = {"fields": []}
            base_resp["rows"] = []
            base_resp["totalRows"] = "0"
            base_resp["numDmlAffectedRows"] = "1"
            return base_resp

        if upper.startswith(("BEGIN", "COMMIT", "ROLLBACK")):
            base_resp["schema"] = {"fields": []}
            base_resp["rows"] = []
            base_resp["totalRows"] = "0"
            return base_resp

        base_resp["schema"] = {
            "fields": [
                {"name": "id", "type": "INT64", "mode": "NULLABLE"},
                {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "score", "type": "FLOAT64", "mode": "NULLABLE"},
            ]
        }
        base_resp["rows"] = [
            {"f": [{"v": "1"}, {"v": "alpha"}, {"v": "9.5"}]},
            {"f": [{"v": "2"}, {"v": "beta"}, {"v": "4.25"}]},
        ]
        base_resp["totalRows"] = "2"
        return base_resp


def test_bigquery_config_dsn_and_registry() -> None:
    """Verify BigQueryConfig.from_dsn, auth options, and DatabaseRegistry routing."""
    cfg = BigQueryConfig.from_dsn(
        "bigquery://analytics-proj/events_ds?location=EU&maximum_bytes_billed=10485760"
        "&priority=BATCH&pool_size=8&arrow_compression=ZSTD&quota_project_id=quota-proj"
    )
    assert cfg.effective_project_id() == "analytics-proj"
    assert cfg.effective_dataset_id() == "events_ds"
    assert cfg.location == "EU"
    assert cfg.maximum_bytes_billed == 10485760
    assert cfg.priority == "BATCH"
    assert cfg.pool_size == 8
    assert cfg.arrow_compression == "ZSTD"
    assert cfg.quota_project_id == "quota-proj"

    with pytest.raises(ImproperConfigurationError, match="Unsupported BigQuery DSN scheme"):
        BigQueryConfig.from_dsn("postgresql://localhost/db")

    reg = DatabaseRegistry()
    dsn = "bigquery://analytics-proj/events_ds?rest_endpoint=http://127.0.0.1:9050"
    reg.register("bq", dsn)
    fp = DatabaseRegistry.pool_fingerprint(dsn)
    assert fp is not None
    assert fp[0] == "bigquery"
    assert fp[1] == "analytics-proj"
    assert fp[2] == "events_ds"
    sync_drv = reg.get_sync_driver("bq")
    assert isinstance(sync_drv, SyncBigQueryDriver)
    assert sync_drv.project_id == "analytics-proj"
    assert sync_drv.dataset_id == "events_ds"


def test_sync_bigquery_driver_queries_dry_run_and_transactions(tmp_path: Path) -> None:
    """Verify SyncBigQueryDriver over the pure-Rust BigQuery REST v2 wire engine."""
    server = _MockBigQueryRestServer()
    try:
        cfg = BigQueryConfig(
            project_id="test-proj",
            dataset_id="analytics",
            rest_endpoint=f"http://{server.host_port}",
            labels={"Env Name!": "Prod-01"},
        )
        store = SyncObjectStore(f"file://{tmp_path}")
        backend = ObjectStoreBackend(f"file://{tmp_path}/warehouse", store=store)
        with SyncBigQueryDriver(cfg) as driver:
            rows = driver.select("SELECT id, name, score FROM metrics WHERE id >= @min_id", {"min_id": 1})
            assert rows == [
                {"id": 1, "name": "alpha", "score": 9.5},
                {"id": 2, "name": "beta", "score": 4.25},
            ]

            exported_sync = driver.export_to_object_store(
                "SELECT id, name, score FROM metrics",
                f"file://{tmp_path}/warehouse/exports/sync_metrics.parquet",
                store=backend,
                format="parquet",
            )
            assert exported_sync == 2

            loaded_sync = driver.load_from_object_store(
                "analytics.metrics",
                f"file://{tmp_path}/warehouse/exports/sync_metrics.parquet",
                store=backend,
                format="parquet",
            )
            assert loaded_sync == 2

            typed_rows = driver.select("SELECT id, name, score FROM metrics", schema_type=MetricRow)
            assert len(typed_rows) == 2
            assert isinstance(typed_rows[0], MetricRow)
            assert typed_rows[0].name == "alpha"

            first_row = driver.select_one("SELECT id, name, score FROM metrics")
            assert first_row == {"id": 1, "name": "alpha", "score": 9.5}
            assert driver.select_value("SELECT id, name, score FROM metrics") == 1

            dry: BigQueryDryRunResult = driver.dry_run("SELECT id, name FROM analytics.events")
            assert dry.total_bytes_processed == (1 << 40)
            assert dry.total_bytes_billed == (1 << 40)
            assert dry.cache_hit is False
            assert dry.estimated_cost_usd == pytest.approx(6.25)
            assert dry.referenced_tables == ("test-proj.analytics.events",)

            with driver.transaction():
                assert driver.in_transaction is True
                assert driver.session_id == "bq-session-1"
                affected = driver.execute(
                    "INSERT INTO metrics (id, name, score) VALUES (@id, @name, @score)",
                    {"id": 3, "name": "gamma", "score": 7.0},
                )
                assert affected == 1
            assert driver.in_transaction is False
    finally:
        server.close()


@pytest.mark.anyio
async def test_async_bigquery_driver_arrow_object_store_and_object_tables(tmp_path: Path) -> None:
    """Verify AsyncBigQueryDriver, ArrowCursor, multicloud ObjectStore stage/load, and ObjectRef presigning."""
    server = _MockBigQueryRestServer()
    try:
        cfg = BigQueryConfig(
            project_id="test-proj",
            dataset_id="analytics",
            rest_endpoint=f"http://{server.host_port}",
        )
        store = SyncObjectStore(
            f"file://{tmp_path}",
            signing=SigningConfig(
                provider="hmac",
                hmac_secret="test-bq-signing-secret",
                endpoint="https://storage.example.com",
            ),
        )
        backend = ObjectStoreBackend(f"file://{tmp_path}/warehouse", store=store)

        async with AsyncBigQueryDriver(cfg) as driver:
            cursor = await driver.select_to_arrow("SELECT id, name, score FROM metrics")
            assert isinstance(cursor, ArrowCursor)
            assert cursor.row_count == 2
            assert cursor.to_pylist() == [
                {"id": 1, "name": "alpha", "score": 9.5},
                {"id": 2, "name": "beta", "score": 4.25},
            ]

            exported_count = await driver.export_to_object_store(
                "SELECT id, name, score FROM metrics",
                f"file://{tmp_path}/warehouse/exports/metrics.parquet",
                store=backend,
                format="parquet",
            )
            assert exported_count == 2

            loaded_count = await driver.load_from_object_store(
                "analytics.metrics",
                f"file://{tmp_path}/warehouse/exports/metrics.parquet",
                store=backend,
                format="parquet",
            )
            assert loaded_count == 2

            gcs_export_rows = await driver.export_to_object_store(
                "analytics.metrics",
                "gs://my-export-bucket/metrics/*.parquet",
                format="parquet",
                compression="ZSTD",
            )
            assert gcs_export_rows == 1

            obj_cfg = ObjectTableConfig(
                table_name="analytics.object_table_docs",
                connection_id="us.gcs-conn",
                uris=("gs://my-bucket/docs/*",),
                max_staleness="INTERVAL 1 HOUR",
                metadata_cache_mode="AUTOMATIC",
            )
            ddl = obj_cfg.create_object_table_sql()
            assert "CREATE EXTERNAL TABLE IF NOT EXISTS `analytics`.`object_table_docs`" in ddl
            assert "WITH CONNECTION `us.gcs-conn`" in ddl
            assert "WITH MAX STALENESS INTERVAL 1 HOUR" in ddl
            assert "object_metadata = 'SIMPLE'" in ddl

            hyphen_ddl = ObjectTableConfig(
                table_name="analytics.object-table-docs",
                connection_id="us.gcs-conn",
                uris=("gs://my-bucket/docs/*",),
            ).create_object_table_sql()
            assert "CREATE EXTERNAL TABLE IF NOT EXISTS `analytics`.`object-table-docs`" in hyphen_ddl

            with pytest.raises(ValueError, match="table_name"):
                ObjectTableConfig(
                    table_name="bad-dataset.object_table_docs",
                    connection_id="us.gcs-conn",
                    uris=("gs://my-bucket/docs/*",),
                ).create_object_table_sql()

            with pytest.raises(ValueError, match="connection_id"):
                ObjectTableConfig(
                    table_name="analytics.object_table_docs",
                    connection_id="us.gcs-conn`; DROP TABLE x",
                    uris=("gs://my-bucket/docs/*",),
                ).create_object_table_sql()

            await driver.create_object_table(obj_cfg)
            refs: list[ObjectRef] = await driver.list_object_table_refs(
                obj_cfg,
                store=store,
                expires_in_secs=1800,
            )
            assert len(refs) == 1
            assert refs[0].uri == "gs://my-bucket/docs/report.pdf"
            assert refs[0].content_type == "application/pdf"
            assert refs[0].size == 2048
            assert refs[0].signed_url is not None
            assert "X-Vtb-Signature=" in refs[0].signed_url
    finally:
        server.close()


@pytest.mark.anyio
async def test_bigquery_embedder_and_datafusion_registration(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify BigQueryEmbeddingPushdown ML.GENERATE_EMBEDDING and DataFusionEngine.register_arrow_table."""
    server = _MockBigQueryRestServer()
    try:
        cfg = BigQueryConfig(
            project_id="test-proj",
            dataset_id="analytics",
            rest_endpoint=f"http://{server.host_port}",
        )
        sync_driver = SyncBigQueryDriver(cfg)
        async_driver = AsyncBigQueryDriver(cfg)

        embedder_sync = BigQueryEmbeddingPushdown(
            sync_driver,
            model="analytics.text_embedding_model",
            dimensions=3,
            normalize=False,
        )
        vec = PushdownEmbedder(embedder_sync, dimensions=3, normalize=False).embed_query_sync("hello world")
        assert vec.to_list() == [0.25, 0.5, 0.75]

        embedder_async = BigQueryEmbeddingPushdown(
            async_driver,
            model="analytics.text_embedding_model",
            dimensions=3,
            normalize=False,
        )
        vecs = await embedder_async.embed_documents(["doc one", "doc two"])
        assert len(vecs.to_list()) == 2
        assert vecs.to_list()[0] == [0.25, 0.5, 0.75]

        ctx = None if import_optional("datafusion") is not None else _BigQuerySessionContext()
        engine = DataFusionEngine(ctx=ctx)
        cursor = sync_driver.select_to_arrow("SELECT id, name, score FROM metrics")
        engine.register_arrow_table("bq_metrics", cursor)
        rows = engine.execute_dicts("SELECT id, name FROM bq_metrics WHERE id = 1")
        assert rows == [{"id": 1, "name": "alpha"}]
        with pytest.raises(Exception, match="missing_bq_table"):
            engine.execute_dicts("SELECT * FROM missing_bq_table")

        monkeypatch.setitem(sys.modules, "datafusion", None)
        reset_dependency_cache("datafusion")
        try:
            with pytest.raises(MissingDependencyError):
                DataFusionEngine().execute_dicts("SELECT 1")
        finally:
            reset_dependency_cache("datafusion")
    finally:
        server.close()


class _StubBigQuerySelectDriver:
    """BigQuery driver stand-in returning fixed rows from select."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = list(rows)

    def select(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
        return list(self.rows)


def test_bigquery_embedder_builders_reject_invalid_identifiers() -> None:
    """BigQuery SQL builders validate model, table, column, and distance values."""
    stub_port = _StubBigQuerySelectDriver([])
    with pytest.raises(ValueError, match="model_endpoint"):
        BigQueryEmbeddingPushdown(stub_port, model="analytics.model`; DROP TABLE x; --").build_generate_embedding_sql()
    embedder = BigQueryEmbeddingPushdown(stub_port, model="analytics-proj.analytics.text_embedding_model")
    assert "MODEL `analytics-proj.analytics.text_embedding_model`" in embedder.build_generate_embedding_sql()
    search_sql = embedder.build_vector_search_sql("my-proj.analytics.docs-v1")
    assert "TABLE `my-proj`.`analytics`.`docs-v1`, " in search_sql
    with pytest.raises(ValueError, match="base_table"):
        embedder.build_vector_search_sql("analytics.docs) --")
    with pytest.raises(ValueError, match="base_table"):
        embedder.build_vector_search_sql("my-proj.bad-dataset.docs")
    with pytest.raises(ValueError, match="embedding_column"):
        embedder.build_vector_search_sql("analytics.docs", embedding_column="emb' OR '1")
    bad_dist: Any = "MANHATTAN"
    with pytest.raises(ValueError, match="distance_type"):
        embedder.build_vector_search_sql("analytics.docs", distance_type=bad_dist)


@pytest.mark.parametrize("use_ai_function", [False, True], ids=["ml-generate", "ai-generate"])
def test_bigquery_embedder_generate_sql_orders_rows_by_input_index(use_ai_function: bool) -> None:
    """The generated SQL carries the input offset through the model and orders the output by it."""
    sql = BigQueryEmbeddingPushdown(
        _StubBigQuerySelectDriver([]), model="analytics.text_embedding_model", use_ai_function=use_ai_function
    ).build_generate_embedding_sql()
    assert sql.startswith("SELECT ") and ", idx FROM " in sql
    assert "(SELECT content, idx FROM UNNEST(@texts) AS content WITH OFFSET AS idx)" in sql
    assert sql.endswith(") ORDER BY idx")


@pytest.mark.anyio
@pytest.mark.parametrize(
    "rows",
    [[{"embedding": [0.25, 0.5]}], [{"embedding": [0.25, 0.5]}, {"embedding": []}]],
    ids=["missing-row", "empty-vector"],
)
async def test_bigquery_embedder_rejects_incomplete_results(rows: list[dict[str, Any]]) -> None:
    """A short result or an empty vector raises DataError."""
    embedder = BigQueryEmbeddingPushdown(
        _StubBigQuerySelectDriver(rows), model="analytics.text_embedding_model", dimensions=2, normalize=False
    )
    with pytest.raises(DataError):
        await embedder.embed_documents(["alpha", "beta"])


def test_bigquery_client_invalid_params_raise_typed_error() -> None:
    """Sync BigQuery binding raises BigQueryError with INVALID_ARGUMENT for malformed parameters."""
    client = SyncBigQueryClient(project_id="p1", rest_endpoint="http://127.0.0.1:9")
    with pytest.raises(BigQueryError) as excinfo:
        client.query("SELECT 1", "not json")
    assert excinfo.value.code == 3
    assert excinfo.value.code_name == "INVALID_ARGUMENT"
    assert isinstance(excinfo.value, VertebraError)


@pytest.mark.anyio
async def test_bigquery_client_invalid_params_raise_typed_error_async() -> None:
    """Async BigQuery binding raises the same BigQueryError as the sync binding."""
    client = AsyncBigQueryClient(project_id="p1", rest_endpoint="http://127.0.0.1:9")
    with pytest.raises(BigQueryError) as excinfo:
        await client.query("SELECT 1", "not json")
    assert excinfo.value.code == 3


@pytest.mark.parametrize(
    ("code", "expected"),
    [(7, True), (9, True), (12, True), (14, True), (5, False), (3, False)],
)
def test_storage_api_fallback_uses_status_codes(code: int, expected: bool) -> None:
    """BigQuery Storage API fallback decisions use canonical codes, not message text."""
    err = BigQueryError("storage read failure")
    err.code = code
    assert BigQueryCore.is_storage_api_fallback_error(err) is expected


def test_storage_api_fallback_ignores_untyped_errors() -> None:
    """A RuntimeError mentioning HTTP 403 is not a Storage API fallback signal."""
    assert BigQueryCore.is_storage_api_fallback_error(RuntimeError("status 403")) is False


@pytest.mark.anyio
async def test_bigquery_select_to_arrow_keeps_small_rest_fallback_in_memory_and_direct_object_store() -> None:
    """Verify 1-row REST fallback cursors remain in-memory by default and ObjectStore export/load works directly."""
    server = _MockBigQueryRestServer()
    try:
        cfg = BigQueryConfig(
            project_id="test-proj",
            dataset_id="analytics",
            rest_endpoint=f"http://{server.host_port}",
        )
        mem_store = SyncObjectStore("memory://")
        with SyncBigQueryDriver(cfg) as sync_driver:
            with sync_driver.select_to_arrow("SELECT uri, size FROM object_table_docs") as sync_cursor:
                assert sync_cursor.row_count == 1
                assert not sync_cursor.used_shm
            exported = sync_driver.export_to_object_store(
                "SELECT id, name, score FROM metrics",
                "memory://warehouse/one_metric.parquet",
                store=mem_store,
                format="parquet",
            )
            assert exported == 2
            loaded = sync_driver.load_from_object_store(
                "analytics.metrics",
                "memory://warehouse/one_metric.parquet",
                store=mem_store,
                format="parquet",
            )
            assert loaded == 2

        async with AsyncBigQueryDriver(cfg) as async_driver:
            with await async_driver.select_to_arrow("SELECT uri, size FROM object_table_docs") as async_cursor:
                assert async_cursor.row_count == 1
                assert not async_cursor.used_shm
            exported_async = await async_driver.export_to_object_store(
                "SELECT id, name, score FROM metrics",
                "memory://warehouse/async_one_metric.parquet",
                store=mem_store,
                format="parquet",
            )
            assert exported_async == 2
            loaded_async = await async_driver.load_from_object_store(
                "analytics.metrics",
                "memory://warehouse/async_one_metric.parquet",
                store=mem_store,
                format="parquet",
            )
            assert loaded_async == 2
    finally:
        server.close()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("provider_cap", "caller_cap", "expected"),
    [(1000, None, 700), (500, 900, 500), (500, 200, 200)],
)
async def test_bigquery_tool_provider_enforces_native_request_billing_caps(
    asynchronous: bool,
    provider_cap: int,
    caller_cap: int | None,
    expected: int,
) -> None:
    """Driver overrides and provider ceilings reach actual BigQuery REST requests."""
    server = _MockBigQueryRestServer()
    config = BigQueryConfig(
        project_id="test-proj",
        dataset_id="analytics",
        rest_endpoint=f"http://{server.host_port}",
        maximum_bytes_billed=700,
    )
    driver: Any = AsyncBigQueryDriver(config) if asynchronous else SyncBigQueryDriver(config)
    try:
        if asynchronous:
            await driver.select("SELECT id, name, score FROM metrics", maximum_bytes_billed=300)
        else:
            driver.select("SELECT id, name, score FROM metrics", maximum_bytes_billed=300)
        assert server.requests[-1]["body"]["maximumBytesBilled"] == "300"
        if asynchronous:
            await driver.select("SELECT id, name, score FROM metrics")
        else:
            driver.select("SELECT id, name, score FROM metrics")
        assert server.requests[-1]["body"]["maximumBytesBilled"] == "700"

        gate = ConfirmationGate()
        tool = build_tools(
            BigQueryToolProvider(driver, maximum_bytes_billed=provider_cap, enforce_dry_run_limit=False),
            confirmation_gate=gate,
        )["bigquery_query"]
        await tool.invoke({"sql": "SELECT id, name, score FROM metrics", "max_bytes_billed": caller_cap})
        assert server.requests[-1]["body"]["maximumBytesBilled"] == str(expected)
        sql = "INSERT INTO metrics (id, name, score) VALUES (3, 'gamma', 7.0)"
        with pytest.raises(PermissionDeniedError) as exc:
            await tool.invoke({"sql": sql, "max_bytes_billed": caller_cap})
        token = str(exc.value).split("ticket=", 1)[1].rstrip(")")
        gate.approve(token)
        result = await tool.invoke({"sql": sql, "max_bytes_billed": caller_cap, "confirmation_token": token})
        assert result["affected_rows"] == 1
        assert server.requests[-1]["body"]["maximumBytesBilled"] == str(expected)

        guarded = build_tools(BigQueryToolProvider(driver, maximum_bytes_billed=1000))["bigquery_query"]
        before = len(server.requests)
        with pytest.raises(PermissionDeniedError, match="exceeds"):
            await guarded.invoke({"sql": "SELECT id FROM metrics"})
        assert len(server.requests) == before + 1
        assert server.requests[-1]["body"]["configuration"]["dryRun"] is True
    finally:
        if asynchronous:
            await driver.close()
        else:
            driver.close()
        server.close()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("caller_cap", [0, -1])
async def test_bigquery_tool_provider_rejects_non_positive_caller_caps(
    asynchronous: bool,
    caller_cap: int,
) -> None:
    """Non-positive caller max_bytes_billed values fail policy before making a REST request."""
    server = _MockBigQueryRestServer()
    config = BigQueryConfig(
        project_id="test-proj", rest_endpoint=f"http://{server.host_port}", maximum_bytes_billed=700
    )
    driver: Any = AsyncBigQueryDriver(config) if asynchronous else SyncBigQueryDriver(config)
    try:
        tool = build_tools(BigQueryToolProvider(driver, maximum_bytes_billed=500, enforce_dry_run_limit=False))[
            "bigquery_query"
        ]
        with pytest.raises(PermissionDeniedError, match="positive"):
            await tool.invoke({"sql": "SELECT 1", "max_bytes_billed": caller_cap})
        assert server.requests == []
    finally:
        if asynchronous:
            await driver.close()
        else:
            driver.close()
        server.close()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(("provider_cap", "native_cap"), [(0, 700), (1000, 0), (1000, -1)])
async def test_bigquery_tool_provider_rejects_non_positive_configured_caps(
    asynchronous: bool,
    provider_cap: int,
    native_cap: int,
) -> None:
    """Non-positive provider or driver maximum_bytes_billed values fail before issuing any RPC."""
    server = _MockBigQueryRestServer()
    config = BigQueryConfig(
        project_id="test-proj",
        rest_endpoint=f"http://{server.host_port}",
        maximum_bytes_billed=native_cap,
    )
    driver: Any = AsyncBigQueryDriver(config) if asynchronous else SyncBigQueryDriver(config)
    try:
        tool = build_tools(
            BigQueryToolProvider(driver, maximum_bytes_billed=provider_cap, enforce_dry_run_limit=False)
        )["bigquery_query"]
        with pytest.raises(PermissionDeniedError, match="positive"):
            await tool.invoke({"sql": "SELECT 1"})
        assert server.requests == []
    finally:
        if asynchronous:
            await driver.close()
        else:
            driver.close()
        server.close()


@pytest.mark.parametrize(
    ("raw_table", "expected_quoted"),
    [
        ("metrics", "`metrics`"),
        ("analytics.metrics-2026", "`analytics`.`metrics-2026`"),
        ("my-proj.analytics.metrics", "`my-proj`.`analytics`.`metrics`"),
        ("google.com:corp-proj.analytics.metrics", "`google.com:corp-proj`.`analytics`.`metrics`"),
        ("`google.com:corp-proj`.analytics.`metrics-1`", "`google.com:corp-proj`.`analytics`.`metrics-1`"),
        ("`my-proj.analytics.metrics`", "`my-proj`.`analytics`.`metrics`"),
        (
            "projects/google.com:corp-proj/datasets/analytics/tables/metrics-1",
            "`google.com:corp-proj`.`analytics`.`metrics-1`",
        ),
    ],
)
def test_bigquery_core_validate_and_quote_table_accepts_supported_shapes(
    raw_table: str,
    expected_quoted: str,
) -> None:
    """BigQueryCore and ObjectTableConfig normalize resource paths, domain projects, and backticks."""
    assert BigQueryCore.validate_and_quote_table(raw_table) == expected_quoted
    ddl = ObjectTableConfig(
        table_name=raw_table,
        connection_id="us.gcs-conn",
        uris=("gs://my-bucket/docs/*",),
    ).create_object_table_sql()
    assert f"CREATE EXTERNAL TABLE IF NOT EXISTS {expected_quoted}" in ddl


@pytest.mark.parametrize(
    "bad_table",
    [
        "",
        "   ",
        "analytics.metrics; DROP TABLE x",
        "analytics.metrics -- comment",
        "analytics.metrics /* comment */",
        "analytics.metrics other",
        "analytics.'metrics'",
        "projects/p/datasets/d",
        "projects/`p`/datasets/d/tables/t",
        "a:b:c.ds.tbl",
    ],
)
def test_bigquery_core_validate_and_quote_table_rejects_unsafe_identifiers(bad_table: str) -> None:
    """BigQueryCore.validate_and_quote_table rejects SQL injection tokens and malformed table references."""
    with pytest.raises(ValueError):
        BigQueryCore.validate_and_quote_table(bad_table)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("table_name", "columns"),
    [
        ("analytics.metrics; DROP TABLE x", None),
        ("analytics.metrics", ["id", "name) VALUES (1); --"]),
    ],
    ids=["bad-table", "bad-column"],
)
async def test_bigquery_load_from_arrow_validates_identifiers_before_io(
    asynchronous: bool,
    table_name: str,
    columns: list[str] | None,
) -> None:
    """load_from_arrow validates table and column identifiers before touching the wire client."""
    server = _MockBigQueryRestServer()
    config = BigQueryConfig(project_id="test-proj", dataset_id="analytics", rest_endpoint=f"http://{server.host_port}")
    driver: Any = AsyncBigQueryDriver(config) if asynchronous else SyncBigQueryDriver(config)
    try:
        with pytest.raises(ValueError):
            if asynchronous:
                await driver.load_from_arrow(table_name, [{"id": 1, "name": "a"}], columns=columns)
            else:
                driver.load_from_arrow(table_name, [{"id": 1, "name": "a"}], columns=columns)
        assert server.requests == []
    finally:
        if asynchronous:
            await driver.close()
        else:
            driver.close()
        server.close()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "bad_uri",
    [
        "exports/metrics.parquet",
        "memory://exports/metrics.parquet",
        "s3://",
    ],
)
async def test_bigquery_staging_rejects_missing_or_ephemeral_memory_store(
    asynchronous: bool,
    bad_uri: str,
) -> None:
    """BigQuery ObjectStore export/load rejects relative, memory://, or malformed URIs when store is None."""
    server = _MockBigQueryRestServer()
    config = BigQueryConfig(project_id="test-proj", dataset_id="analytics", rest_endpoint=f"http://{server.host_port}")
    try:
        if asynchronous:
            driver = AsyncBigQueryDriver(config)
            try:
                with pytest.raises(ImproperConfigurationError):
                    await driver.export_to_object_store("SELECT id, name, score FROM metrics", bad_uri, store=None)
                with pytest.raises(ImproperConfigurationError):
                    await driver.load_from_object_store("analytics.metrics", bad_uri, store=None)
            finally:
                await driver.close()
        else:
            sync_driver = SyncBigQueryDriver(config)
            try:
                with pytest.raises(ImproperConfigurationError):
                    sync_driver.export_to_object_store("SELECT id, name, score FROM metrics", bad_uri, store=None)
                with pytest.raises(ImproperConfigurationError):
                    sync_driver.load_from_object_store("analytics.metrics", bad_uri, store=None)
            finally:
                sync_driver.close()
    finally:
        server.close()


def test_bigquery_core_arrow_ipc_coercion_preserves_bytes_casts_dictionaries_and_splits_oversized_batches() -> None:
    """coerce_arrow_ipc_bytes preserves bytes identity, casts dictionary columns, and splits >9 MiB batches."""
    small_table = pa.Table.from_pydict({"id": [1, 2], "name": ["alpha", "beta"]})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, small_table.schema) as writer:
        writer.write_table(small_table)
    raw_bytes = sink.getvalue().to_pybytes()

    assert BigQueryCore.coerce_arrow_ipc_bytes(raw_bytes) is raw_bytes
    in_memory_cursor = ArrowCursor(raw_bytes, used_shm=False)
    assert BigQueryCore.coerce_arrow_ipc_bytes(in_memory_cursor) is raw_bytes

    dict_col = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0], type=pa.int32()),
        pa.array(["alpha", "beta"], type=pa.string()),
    )
    dict_table = pa.Table.from_arrays([pa.array([10, 20, 30]), dict_col], names=["id", "tag"])
    coerced_dict_ipc = BigQueryCore.coerce_arrow_ipc_bytes(dict_table)
    decoded_dict_table = pa.ipc.open_stream(coerced_dict_ipc).read_all()
    assert not pa.types.is_dictionary(decoded_dict_table.schema.field("tag").type)
    assert decoded_dict_table.to_pylist() == [
        {"id": 10, "tag": "alpha"},
        {"id": 20, "tag": "beta"},
        {"id": 30, "tag": "alpha"},
    ]

    row_payload = b"x" * 1024
    oversized_rows = 10_000
    oversized_table = pa.Table.from_pydict(
        {
            "id": list(range(oversized_rows)),
            "payload": [row_payload] * oversized_rows,
        }
    )
    coerced_oversized_ipc = BigQueryCore.coerce_arrow_ipc_bytes(oversized_table)
    reader = pa.ipc.open_stream(coerced_oversized_ipc)
    batches = list(reader)
    assert len(batches) >= 2
    assert sum(batch.num_rows for batch in batches) == oversized_rows
    for batch in batches:
        batch_sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(batch_sink, batch.schema) as batch_writer:
            batch_writer.write_batch(batch)
        assert batch_sink.getvalue().size <= 9 * 1024 * 1024


@pytest.mark.anyio
async def test_bigquery_pushdown_embed_batch() -> None:
    """BigQueryEmbeddingPushdown exposes embed_batch delegating to embed_documents."""
    driver = _StubBigQuerySelectDriver([{"embedding": [3.0, 4.0]}, {"embedding": [0.0, 2.0]}])
    embedder = BigQueryEmbeddingPushdown(driver, model="analytics.text_embedding_model", dimensions=2, normalize=False)
    batch = await embedder.embed_batch(["alpha", "beta"], task="retrieval_document")
    assert batch.to_list() == [[3.0, 4.0], [0.0, 2.0]]


@pytest.mark.anyio
@pytest.mark.parametrize("bad_top_k", [0, -2])
async def test_bigquery_pushdown_rejects_non_positive_top_k(bad_top_k: int) -> None:
    """BigQueryEmbeddingPushdown.similarity_search rejects non-positive top_k."""
    embedder = BigQueryEmbeddingPushdown(_StubBigQuerySelectDriver([]), dimensions=2, normalize=False)
    with pytest.raises(ValueError, match="top_k must be positive"):
        await embedder.similarity_search([1.0, 0.0], table="proj.ds.docs", top_k=bad_top_k)


@pytest.mark.anyio
@pytest.mark.parametrize("bad_task", ["", "   ", "unknown_task", "summarize"])
async def test_bigquery_pushdown_rejects_unsupported_task(bad_task: str) -> None:
    """BigQueryEmbeddingPushdown.embed_documents rejects unknown embedding task names."""
    embedder = BigQueryEmbeddingPushdown(_StubBigQuerySelectDriver([]), dimensions=2, normalize=False)
    with pytest.raises(ValueError, match="Unsupported embedding task"):
        await embedder.embed_documents(["alpha"], task=bad_task)


@pytest.mark.anyio
@pytest.mark.parametrize("bad_vec", [[True, False], [1.0], [1.0, 2.0, 3.0]])
async def test_bigquery_pushdown_rejects_invalid_vector_rows(bad_vec: list[Any]) -> None:
    """BigQueryEmbeddingPushdown rejects boolean elements and dimension drift with DataError."""
    bad_embedder = BigQueryEmbeddingPushdown(
        _StubBigQuerySelectDriver([{"embedding": bad_vec}]),
        model="analytics.text_embedding_model",
        dimensions=2,
        normalize=False,
    )
    with pytest.raises(DataError, match="BigQuery embedding row"):
        await bad_embedder.embed_documents(["alpha"])


@pytest.mark.parametrize(
    ("symbol_name", "target_module", "expected_module"),
    [
        ("SyncBigQueryDriver", "vertebra.db.adapters.bigquery", "vertebra.db.adapters.bigquery._sync"),
        ("SyncBigQueryDriver", "vertebra.db.adapters.bigquery.driver", "vertebra.db.adapters.bigquery._sync"),
        ("SyncBigQueryDriver", "vertebra.db.adapters.bigquery._sync", "vertebra.db.adapters.bigquery._sync"),
        ("AsyncBigQueryDriver", "vertebra.db.adapters.bigquery", "vertebra.db.adapters.bigquery._async"),
        ("AsyncBigQueryDriver", "vertebra.db.adapters.bigquery.driver", "vertebra.db.adapters.bigquery._async"),
        ("AsyncBigQueryDriver", "vertebra.db.adapters.bigquery._async", "vertebra.db.adapters.bigquery._async"),
        ("BigQueryCore", "vertebra.db.adapters.bigquery", "vertebra.db.adapters.bigquery.core"),
        ("BigQueryTableIdentifier", "vertebra.db.adapters.bigquery.core", "vertebra.db.adapters.bigquery.core"),
        ("ObjectRef", "vertebra.db.adapters.bigquery", "vertebra.db.adapters.bigquery.object_tables"),
        ("ObjectTableConfig", "vertebra.db.adapters.bigquery", "vertebra.db.adapters.bigquery.object_tables"),
        (
            "hydrate_object_refs_sync",
            "vertebra.db.adapters.bigquery.object_tables",
            "vertebra.db.adapters.bigquery.object_tables",
        ),
        (
            "hydrate_object_refs_async",
            "vertebra.db.adapters.bigquery.object_tables",
            "vertebra.db.adapters.bigquery.object_tables",
        ),
    ],
)
def test_bigquery_submodule_import_identity(
    symbol_name: str,
    target_module: str,
    expected_module: str,
) -> None:
    """Verify BigQuery package exports resolve identically to their owning submodules."""
    mod = importlib.import_module(target_module)
    exp_mod = importlib.import_module(expected_module)
    assert getattr(mod, symbol_name) is getattr(exp_mod, symbol_name)


def test_bigquery_package_import_is_lazy() -> None:
    """Verify importing vertebra.db.adapters.bigquery defers driver and object_tables loading."""
    saved = {
        name: sys.modules.pop(name)
        for name in list(sys.modules)
        if name == "vertebra.db.adapters.bigquery"
        or name.startswith("vertebra.db.adapters.bigquery.")
        or name == "vertebra._bigquery"
    }
    reset_dependency_cache("vertebra._bigquery")
    try:
        fresh_pkg = importlib.import_module("vertebra.db.adapters.bigquery")
        assert "vertebra.db.adapters.bigquery.driver" not in sys.modules
        assert "vertebra.db.adapters.bigquery.object_tables" not in sys.modules
        assert "vertebra._bigquery" not in sys.modules
        assert fresh_pkg.ObjectRef is importlib.import_module("vertebra.db.adapters.bigquery.object_tables").ObjectRef
        assert fresh_pkg.ObjectRef is not None
    finally:
        for name in list(sys.modules):
            if name == "vertebra.db.adapters.bigquery" or name.startswith("vertebra.db.adapters.bigquery."):
                sys.modules.pop(name, None)
        sys.modules.update(saved)
        reset_dependency_cache("vertebra._bigquery")


@pytest.mark.parametrize("symbol", ["ObjectRef", "ObjectTableConfig", "BigQueryTableIdentifier"])
def test_bigquery_config_module_rejects_moved_symbols(symbol: str) -> None:
    """Verify bigquery.config does not export or retain shims for moved domain symbols."""
    with pytest.raises(AttributeError):
        getattr(bq_config_mod, symbol)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_bigquery_object_table_hydration_and_presigning(
    tmp_path: Path,
    asynchronous: bool,
) -> None:
    """Verify ObjectTableConfig and module functions hydrate and presign ObjectRef entries."""
    signing = SigningConfig(
        provider="hmac",
        hmac_secret="test-secret-key-32-bytes-minimum!",
        endpoint="https://storage.example.com",
    )
    store = SyncObjectStore(f"file://{tmp_path}", signing=signing)
    raw_rows = [
        {
            "uri": "gs://test-bucket/docs/report.pdf",
            "generation": 101,
            "content_type": "application/pdf",
            "size": 2048,
            "md5_hash": "abc123md5",
            "updated": "2026-11-14T00:00:00Z",
        },
        {
            "uri": "gs://test-bucket/images/logo.png",
            "generation": 102,
            "content_type": "image/png",
            "size": 4096,
            "md5_hash": "def456md5",
            "updated": "2026-11-14T01:00:00Z",
        },
    ]
    cfg = ObjectTableConfig(
        table_name="analytics.docs_obj",
        connection_id="us.gcs-conn",
        uris=("gs://test-bucket/docs/*",),
    )
    try:
        if asynchronous:
            async_store = store.to_async()
            try:
                refs = await cfg.hydrate_object_refs_async(raw_rows, async_store)
                assert len(refs) == 2
                assert refs[0].signed_url is not None
                assert refs[0].signed_url.startswith("https://storage.example.com/")
                assert "docs/report.pdf" in refs[0].signed_url
                assert "X-Vtb-Signature=" in refs[0].signed_url
                assert refs[0].uri == "gs://test-bucket/docs/report.pdf"
                assert refs[0].size == 2048
                unsigned_refs = [msgspec.structs.replace(r, signed_url=None) for r in refs]
                fn_refs = await hydrate_object_refs_async(unsigned_refs, async_store)
                assert len(fn_refs) == 2
                assert fn_refs[1].signed_url is not None
                assert fn_refs[1].signed_url.startswith("https://storage.example.com/")
                assert "images/logo.png" in fn_refs[1].signed_url
                assert "X-Vtb-Signature=" in fn_refs[1].signed_url
                assert fn_refs[1].size == 4096
            finally:
                async_store.close()
        else:
            refs = cfg.hydrate_object_refs_sync(raw_rows, store)
            assert len(refs) == 2
            assert refs[0].signed_url is not None
            assert refs[0].signed_url.startswith("https://storage.example.com/")
            assert "docs/report.pdf" in refs[0].signed_url
            assert "X-Vtb-Signature=" in refs[0].signed_url
            assert refs[0].uri == "gs://test-bucket/docs/report.pdf"
            assert refs[0].size == 2048
            unsigned_refs = [msgspec.structs.replace(r, signed_url=None) for r in refs]
            fn_refs = hydrate_object_refs_sync(unsigned_refs, store)
            assert len(fn_refs) == 2
            assert fn_refs[1].signed_url is not None
            assert fn_refs[1].signed_url.startswith("https://storage.example.com/")
            assert "images/logo.png" in fn_refs[1].signed_url
            assert "X-Vtb-Signature=" in fn_refs[1].signed_url
            assert fn_refs[1].size == 4096
    finally:
        store.close()


def test_bigquery_object_table_limit_zero_and_empty_bucket_scheme(tmp_path: Path) -> None:
    """Verify list_from_store_sync respects limit=0 and empty-bucket URIs retain URL scheme."""
    store = SyncObjectStore(f"file://{tmp_path}")
    try:
        (tmp_path / "docs").mkdir()
        (tmp_path / "docs" / "sample.parquet").write_bytes(b"PAR1")
        cfg = ObjectTableConfig(
            table_name="analytics.local_docs",
            connection_id="us.file-conn",
            uris=(f"file://{tmp_path}/docs/*",),
        )
        assert cfg.list_from_store_sync(store, limit=0) == []
        row = ObjectTableConfig._normalize_store_item_to_row(
            {"path": "docs/sample.parquet", "size": 4},
            scheme="file",
            bucket="",
        )
        assert row["uri"] == "file://docs/sample.parquet"
        assert row["size"] == 4
    finally:
        store.close()


def test_bigquery_select_one_forwards_maximum_bytes_billed() -> None:
    """Verify select_one and select_one_or_none forward kwargs and preserve explicit zero billing cap."""
    captured_caps: list[int | None] = []

    class _RecordingBqCursor:
        arraysize: int = 1
        description: tuple[tuple[Any, ...], ...] | None = (("id", "INT64", None, None, None, None, False),)
        rowcount: int = 1
        lastrowid: int | None = None
        query_metadata: dict[str, Any] | None = {"job_id": "rec-job", "total_rows": 1}

        def execute(self, sql: str, params: Any = ()) -> Any:
            _ = (sql, params)
            return self

        def executemany(self, sql: str, seq_of_params: Sequence[Any]) -> Any:
            _ = (sql, seq_of_params)
            return self

        def fetchone(self) -> tuple[Any, ...] | None:
            return (1,)

        def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
            _ = size
            return [(1,)]

        def fetchall(self) -> list[tuple[Any, ...]]:
            return [(1,)]

        def close(self) -> None:
            return None

    class _RecordingBqClient:
        def cursor(
            self,
            *,
            session_id: str | None = None,
            destination_table: str | None = None,
            write_disposition: str | None = None,
            maximum_bytes_billed: int | None = None,
            job_timeout_ms: int | None = None,
            priority: str | None = None,
            labels: Any = None,
            param_encoder: Any = None,
            lazy_begin: bool = False,
        ) -> _RecordingBqCursor:
            _ = (
                session_id,
                destination_table,
                write_disposition,
                job_timeout_ms,
                priority,
                labels,
                param_encoder,
                lazy_begin,
            )
            captured_caps.append(maximum_bytes_billed)
            return _RecordingBqCursor()

    cfg = BigQueryConfig(project_id="test-proj", maximum_bytes_billed=10_000)
    driver = SyncBigQueryDriver(cfg)
    rec_client: Any = _RecordingBqClient()
    driver._client = rec_client
    row = driver.select_one("SELECT 1 AS id", maximum_bytes_billed=0)
    assert row == {"id": 1}
    row_opt = driver.select_one_or_none("SELECT 1 AS id", maximum_bytes_billed=250)
    assert row_opt == {"id": 1}
    assert captured_caps == [0, 250]
    assert bq_pkg.ObjectRef is bq_object_tables_mod.ObjectRef


@pytest.mark.parametrize(
    ("with_tx", "fail_rollback", "fail_close"),
    [
        (False, False, False),
        (True, False, False),
        (True, True, False),
        (False, False, True),
        (True, False, True),
    ],
)
def test_sync_bigquery_driver_close_invokes_client_close_and_clears_state(
    with_tx: bool,
    fail_rollback: bool,
    fail_close: bool,
) -> None:
    """Verify SyncBigQueryDriver.close rolls back before client.close and always clears client state."""
    events: list[tuple[str, bool]] = []
    driver_ref: list[SyncBigQueryDriver] = []

    class _SpySyncBigQueryClient:
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
            if sql == "BEGIN TRANSACTION":
                return {"session_id": "spy-session-1", "columns": [], "rows": []}
            if sql == "ROLLBACK TRANSACTION":
                events.append(("rollback", driver_ref[0]._client is not None))
                if fail_rollback:
                    msg = "rollback boom"
                    raise RuntimeError(msg)
            return {"columns": [], "rows": []}

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
        ) -> Any:
            _ = (project_id, dataset_id, table_id, selected_fields, row_restriction, max_streams, compression)
            raise NotImplementedError

        def append_arrow(
            self,
            parent_table: str,
            arrow_ipc_bytes: bytes,
            mode: str = "_default",
        ) -> dict[str, Any]:
            _ = (parent_table, arrow_ipc_bytes, mode)
            return {}

        def cursor(self, **kwargs: Any) -> Any:
            _ = kwargs
            client = self

            class _SpyCursor:
                arraysize: int = 1
                description: tuple[tuple[Any, ...], ...] | None = None
                rowcount: int = 0
                lastrowid: int | None = None
                query_metadata: dict[str, Any] | None = None

                def execute(self, sql: str, params: Any = ()) -> Any:
                    _ = params
                    res = client.query(sql)
                    self.query_metadata = res
                    return self

                def fetchall(self) -> list[tuple[Any, ...]]:
                    return []

                def close(self) -> None:
                    return None

            return _SpyCursor()

        def close(self) -> None:
            events.append(("close", driver_ref[0]._client is not None))
            if fail_close:
                msg = "close boom"
                raise RuntimeError(msg)

    spy: Any = _SpySyncBigQueryClient()
    driver = SyncBigQueryDriver(BigQueryConfig(project_id="test-proj"), client=spy)
    driver_ref.append(driver)
    if with_tx:
        driver.begin()

    if fail_close:
        with pytest.raises(RuntimeError, match="close boom"):
            driver.close()
    else:
        driver.close()

    expected: list[tuple[str, bool]] = []
    if with_tx:
        expected.append(("rollback", True))
    expected.append(("close", True))
    assert events == expected
    assert driver._client is None
    assert driver._closed is True
    assert driver._in_transaction is False
    assert driver.session_id is None

    driver.close()
    assert events == expected


@pytest.mark.anyio
@pytest.mark.parametrize("awaitable_close", [False, True])
@pytest.mark.parametrize(
    ("with_tx", "fail_rollback", "fail_close"),
    [
        (False, False, False),
        (True, False, False),
        (True, True, False),
        (False, False, True),
        (True, False, True),
    ],
)
async def test_async_bigquery_driver_close_invokes_and_awaits_client_close(
    awaitable_close: bool,
    with_tx: bool,
    fail_rollback: bool,
    fail_close: bool,
) -> None:
    """Verify AsyncBigQueryDriver.close handles sync and async client.close and clears state in finally."""
    events: list[tuple[str, bool]] = []
    driver_ref: list[AsyncBigQueryDriver] = []

    class _SpyAsyncBigQueryClient:
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
            _ = (
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
            if sql == "BEGIN TRANSACTION":
                return {"session_id": "spy-session-async", "columns": [], "rows": []}
            if sql == "ROLLBACK TRANSACTION":
                events.append(("rollback", driver_ref[0]._client is not None))
                if fail_rollback:
                    msg = "rollback boom"
                    raise RuntimeError(msg)
            return {"columns": [], "rows": []}

        async def dry_run(self, sql: str, params_json: str | None = None) -> dict[str, Any]:
            _ = (sql, params_json)
            return {}

        async def read_table_arrow(
            self,
            project_id: str,
            dataset_id: str,
            table_id: str,
            selected_fields: Sequence[str] | None = None,
            row_restriction: str | None = None,
            max_streams: int = 4,
            compression: str = "LZ4_FRAME",
        ) -> Any:
            _ = (project_id, dataset_id, table_id, selected_fields, row_restriction, max_streams, compression)
            raise NotImplementedError

        async def append_arrow(
            self,
            parent_table: str,
            arrow_ipc_bytes: bytes,
            mode: str = "_default",
        ) -> dict[str, Any]:
            _ = (parent_table, arrow_ipc_bytes, mode)
            return {}

        def cursor(self, **kwargs: Any) -> Any:
            _ = kwargs
            client = self

            class _SpyAsyncCursor:
                arraysize: int = 1
                description: tuple[tuple[Any, ...], ...] | None = None
                rowcount: int = 0
                lastrowid: int | None = None
                query_metadata: dict[str, Any] | None = None

                async def execute(self, sql: str, params: Any = ()) -> Any:
                    _ = params
                    res = await client.query(sql)
                    self.query_metadata = res
                    return self

                async def fetchall(self) -> list[tuple[Any, ...]]:
                    return []

                async def close(self) -> None:
                    return None

            return _SpyAsyncCursor()

        def close(self) -> Any:
            if awaitable_close:

                async def _finish() -> None:
                    events.append(("close", driver_ref[0]._client is not None))
                    if fail_close:
                        msg = "close boom"
                        raise RuntimeError(msg)

                return _finish()
            events.append(("close", driver_ref[0]._client is not None))
            if fail_close:
                msg = "close boom"
                raise RuntimeError(msg)
            return None

    spy: Any = _SpyAsyncBigQueryClient()
    driver = AsyncBigQueryDriver(BigQueryConfig(project_id="test-proj"), client=spy)
    driver_ref.append(driver)
    if with_tx:
        await driver.begin()

    if fail_close:
        with pytest.raises(RuntimeError, match="close boom"):
            await driver.close()
    else:
        await driver.close()

    expected: list[tuple[str, bool]] = []
    if with_tx:
        expected.append(("rollback", True))
    expected.append(("close", True))
    assert events == expected
    assert driver._client is None
    assert driver._closed is True
    assert driver._in_transaction is False
    assert driver.session_id is None

    await driver.close()
    assert events == expected


@pytest.mark.parametrize("method_name", ["query", "dry_run", "read_table_arrow", "append_arrow"])
def test_sync_bigquery_native_client_close_is_idempotent_and_rejects_operations(method_name: str) -> None:
    """Verify SyncBigQueryClient.close is idempotent and subsequent operations raise BigQueryError."""
    client = SyncBigQueryClient(
        project_id="p1", rest_endpoint="http://127.0.0.1:9", storage_endpoint="http://127.0.0.1:9"
    )
    client.close()
    client.close()

    with pytest.raises(BigQueryError) as excinfo:
        if method_name == "query":
            client.query("SELECT 1")
        elif method_name == "dry_run":
            client.dry_run("SELECT 1")
        elif method_name == "read_table_arrow":
            client.read_table_arrow("p1", "ds", "tbl")
        else:
            client.append_arrow("projects/p1/datasets/ds/tables/tbl", b"")
    assert excinfo.value.code == 3
    assert "BigQuery client is closed" in str(excinfo.value)


@pytest.mark.anyio
@pytest.mark.parametrize("method_name", ["query", "dry_run", "read_table_arrow", "append_arrow"])
async def test_async_bigquery_native_client_close_is_idempotent_and_rejects_operations(method_name: str) -> None:
    """Verify AsyncBigQueryClient.close is idempotent and subsequent operations raise BigQueryError."""
    client = AsyncBigQueryClient(
        project_id="p1", rest_endpoint="http://127.0.0.1:9", storage_endpoint="http://127.0.0.1:9"
    )
    client.close()
    client.close()

    with pytest.raises(BigQueryError) as excinfo:
        if method_name == "query":
            await client.query("SELECT 1")
        elif method_name == "dry_run":
            await client.dry_run("SELECT 1")
        elif method_name == "read_table_arrow":
            await client.read_table_arrow("p1", "ds", "tbl")
        else:
            await client.append_arrow("projects/p1/datasets/ds/tables/tbl", b"")
    assert excinfo.value.code == 3
    assert "BigQuery client is closed" in str(excinfo.value)


class _CursorBigQueryRestServer:
    """Local HTTP/1.1 BigQuery REST v2 server tailored for native cursor contract tests."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.job_counter: int = 0
        self.in_flight_started = threading.Event()
        self.release_in_flight = threading.Event()
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                _ = (format, args)

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
                body = json.loads(raw_body) if raw_body else {}
                parent.requests.append({"method": "POST", "path": self.path, "body": body})

                if self.path.endswith("/jobs"):
                    query_cfg = body.get("configuration", {}).get("query", {})
                    sql = str(query_cfg.get("query", ""))
                    self._handle_sql(sql, body, query_cfg)
                    return

                if self.path.endswith("/queries"):
                    sql = str(body.get("query", ""))
                    self._handle_sql(sql, body, body)
                    return

                self._send_json(404, {"error": {"code": 404, "message": "Not found"}})

            def _handle_sql(self, sql: str, body: dict[str, Any], query_cfg: dict[str, Any]) -> None:
                upper = sql.strip().upper()
                if upper.startswith("SELEC "):
                    self._send_json(
                        400,
                        {
                            "error": {
                                "code": 400,
                                "message": 'Syntax error: Unexpected identifier "SELEC"',
                                "status": "INVALID_ARGUMENT",
                                "errors": [{"reason": "invalidQuery"}],
                            }
                        },
                    )
                    return

                if "FAIL_SECOND" in upper and len(parent.requests) >= 2:
                    self._send_json(
                        400,
                        {
                            "error": {
                                "code": 400,
                                "message": "Simulated second-statement failure",
                                "status": "INVALID_ARGUMENT",
                            }
                        },
                    )
                    return

                if "HOLD_IN_FLIGHT" in upper:
                    parent.in_flight_started.set()
                    parent.release_in_flight.wait(timeout=5.0)

                parent.job_counter += 1
                job_id = f"bq-cursor-job-{parent.job_counter}"
                conn_props = query_cfg.get("connectionProperties") or body.get("connectionProperties") or []
                session_id = None
                for prop in conn_props:
                    if prop.get("key") == "session_id":
                        session_id = prop.get("value")
                if "ASSIGN_SESSION" in upper:
                    session_id = "bq-reply-session-99"

                resp: dict[str, Any] = {
                    "jobComplete": True,
                    "jobReference": {"projectId": "test-proj", "jobId": job_id, "location": "US"},
                    "totalBytesProcessed": "2048",
                    "totalBytesBilled": "10485760",
                    "cacheHit": False,
                }
                if session_id:
                    resp["sessionInfo"] = {"sessionId": session_id}

                if "UNNEST([])" in upper:
                    resp["schema"] = {
                        "fields": [{"name": "a", "type": "INT64", "mode": "REQUIRED"}],
                    }
                    resp["rows"] = []
                    resp["totalRows"] = "0"
                elif upper.startswith("UPDATE "):
                    resp["schema"] = {"fields": []}
                    resp["rows"] = []
                    resp["totalRows"] = "0"
                    resp["numDmlAffectedRows"] = "2"
                elif upper.startswith("CREATE TABLE"):
                    resp["schema"] = {"fields": []}
                    resp["rows"] = []
                    resp["totalRows"] = "0"
                elif "SELECT 1 AS A, 'X' AS B" in upper:
                    resp["schema"] = {
                        "fields": [
                            {"name": "a", "type": "INT64", "mode": "REQUIRED"},
                            {"name": "b", "type": "STRING", "mode": "NULLABLE"},
                        ],
                    }
                    resp["rows"] = [{"f": [{"v": "1"}, {"v": "x"}]}]
                    resp["totalRows"] = "1"
                else:
                    resp["schema"] = {
                        "fields": [
                            {"name": "id", "type": "INT64", "mode": "REQUIRED"},
                            {"name": "val", "type": "STRING", "mode": "NULLABLE"},
                        ],
                    }
                    resp["rows"] = [
                        {"f": [{"v": "1"}, {"v": "one"}]},
                        {"f": [{"v": "2"}, {"v": "two"}]},
                        {"f": [{"v": "3"}, {"v": "three"}]},
                    ]
                    resp["totalRows"] = "3"

                self._send_json(200, resp)

            def _send_json(self, status: int, payload: dict[str, Any]) -> None:
                encoded = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self.rest_endpoint = f"http://127.0.0.1:{self._server.server_port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self.release_in_flight.set()
        self._server.shutdown()
        self._server.server_close()


def _extract_request_session_id(req: dict[str, Any]) -> str | None:
    """Return the session_id carried in a BigQuery REST query or job request body."""
    body = req["body"]
    props = body.get("connectionProperties") or body.get("configuration", {}).get("query", {}).get(
        "connectionProperties", []
    )
    for prop in props:
        if prop.get("key") == "session_id":
            return str(prop.get("value"))
    return None


def test_sync_cursor_select_dml_ddl_empty_and_metadata() -> None:
    """Verify SyncBigQueryCursor select, DML, DDL, empty set, fetch, metadata, and context manager."""
    server = _CursorBigQueryRestServer()
    try:
        client = SyncBigQueryClient(
            project_id="test-proj",
            rest_endpoint=server.rest_endpoint,
            storage_endpoint="http://127.0.0.1:9",
        )
        with client.cursor() as cur:
            assert isinstance(cur, SyncBigQueryCursor)
            initial_desc = cur.description
            assert initial_desc is None
            assert cur.rowcount == -1
            assert cur.lastrowid is None
            initial_meta = cur.query_metadata
            assert initial_meta is None
            assert cur.arraysize == 1
            assert cur.fetchone() is None
            assert cur.fetchmany() == []
            assert cur.fetchall() == []

            ret = cur.execute("SELECT 1 AS a, 'x' AS b")
            assert ret is cur
            select_desc = cur.description
            assert select_desc is not None
            assert [col[0] for col in select_desc] == ["a", "b"]
            assert select_desc[0] == ("a", "INT64", None, None, None, None, False)
            assert select_desc[1] == ("b", "STRING", None, None, None, None, True)
            assert cur.rowcount == 1
            assert cur.lastrowid is None
            assert cur.fetchall() == [(1, "x")]
            assert cur.fetchone() is None
            meta = cur.query_metadata
            assert meta is not None
            assert isinstance(meta["job_id"], str)
            assert meta["job_id"].startswith("bq-cursor-job-")

            cur.execute("SELECT id, val FROM items")
            cur.arraysize = 2
            assert cur.fetchmany(0) == []
            assert cur.fetchmany() == [(1, "one"), (2, "two")]
            assert cur.fetchone() == (3, "three")
            assert cur.fetchone() is None

            with pytest.raises(ProgrammingError):
                cur.arraysize = 0
            with pytest.raises(ProgrammingError):
                cur.fetchmany(-1)

            cur.execute("UPDATE items SET val = 'z' WHERE id IN (1, 2)")
            assert cur.rowcount == 2
            dml_desc = cur.description
            assert dml_desc is None
            assert cur.fetchall() == []

            cur.execute("CREATE TABLE items (id INT64)")
            assert cur.rowcount == -1
            ddl_desc = cur.description
            assert ddl_desc is None

            cur.execute("SELECT 1 AS a FROM UNNEST([]) AS x")
            empty_desc = cur.description
            assert empty_desc is not None
            assert [col[0] for col in empty_desc] == ["a"]
            assert cur.fetchall() == []

        cur.close()
        with pytest.raises(InterfaceError):
            cur.execute("SELECT 1 AS a, 'x' AS b")
        with pytest.raises(InterfaceError):
            cur.fetchone()
    finally:
        server.close()


def test_sync_cursor_session_params_encoder_options_errors_and_executemany() -> None:
    """Verify SyncBigQueryCursor session reuse, parameters, encoder, labels, limits, errors, and executemany."""
    server = _CursorBigQueryRestServer()
    try:
        client = SyncBigQueryClient(
            project_id="test-proj",
            rest_endpoint=server.rest_endpoint,
            storage_endpoint="http://127.0.0.1:9",
        )

        with pytest.raises(NotSupportedError):
            client.cursor(lazy_begin=True)
        assert server.requests == []

        sess_cur = client.cursor(session_id="sess-initial-1")
        sess_cur.execute("SELECT 1 AS a, 'x' AS b")
        sess_cur.execute("SELECT 1 AS a, 'x' AS b")
        assert _extract_request_session_id(server.requests[-2]) == "sess-initial-1"
        assert _extract_request_session_id(server.requests[-1]) == "sess-initial-1"

        reply_sess_cur = client.cursor()
        reply_sess_cur.execute("SELECT ASSIGN_SESSION")
        reply_sess_cur.execute("SELECT 1 AS a, 'x' AS b")
        assert _extract_request_session_id(server.requests[-1]) == "bq-reply-session-99"

        encoded_json = json.dumps([{"name": "a", "parameterType": {"type": "INT64"}, "parameterValue": {"value": "7"}}])
        param_cur = client.cursor()
        param_cur.execute("SELECT @a", encoded_json)
        last_body = server.requests[-1]["body"]
        assert last_body.get("queryParameters") == [
            {"name": "a", "parameterType": {"type": "INT64"}, "parameterValue": {"value": "7"}}
        ]

        with pytest.raises(InterfaceError, match="BigQuery cursor needs encoded parameters or a param_encoder"):
            param_cur.execute("SELECT @a", {"a": 1})

        calls: list[tuple[str, Any]] = []

        def encoder(sql: str, params: Any) -> tuple[str, str | None]:
            calls.append((sql, params))
            return (
                sql,
                json.dumps(
                    [{"name": "a", "parameterType": {"type": "INT64"}, "parameterValue": {"value": str(params["a"])}}]
                ),
            )

        enc_cur = client.cursor(
            labels=MappingProxyType({"team": "data"}),
            maximum_bytes_billed=1048576,
            param_encoder=encoder,
        )
        enc_cur.execute("SELECT @a", {"a": 1})
        assert calls == [("SELECT @a", {"a": 1})]
        job_req = server.requests[-1]["body"]
        assert job_req.get("labels") == {"team": "data"}
        assert str(job_req.get("maximumBytesBilled")) == "1048576"
        assert job_req.get("dryRun", False) is False

        def failing_encoder(_sql: str, _params: Any) -> tuple[str, str | None]:
            raise RuntimeError("encoder boom")

        fail_enc_cur = client.cursor(param_encoder=failing_encoder)
        with pytest.raises(RuntimeError, match="encoder boom"):
            fail_enc_cur.execute("SELECT @a", {"a": 1})

        err_cur = client.cursor()
        err_cur.execute("SELECT 1 AS a, 'x' AS b")
        assert err_cur.description is not None
        with pytest.raises(BigQueryError) as excinfo:
            err_cur.execute("SELEC 1")
        assert excinfo.value.code == 3
        assert err_cur.description is None
        assert err_cur.rowcount == -1
        assert err_cur.query_metadata is None

        many_cur = client.cursor(param_encoder=encoder)
        many_cur.executemany("UPDATE items SET a = @a", [{"a": 1}, {"a": 2}, {"a": 3}])
        assert many_cur.rowcount == 6
        assert many_cur.description is None

        server.requests.clear()
        with pytest.raises(BigQueryError) as many_exc:
            many_cur.executemany("UPDATE FAIL_SECOND SET a = @a", [{"a": 1}, {"a": 2}, {"a": 3}])
        assert many_exc.value.code == 3
        assert len(server.requests) == 2
        assert many_cur.rowcount == -1
        assert many_cur.description is None
    finally:
        server.close()


@pytest.mark.anyio
async def test_async_cursor_select_dml_ddl_empty_and_metadata() -> None:
    """Verify AsyncBigQueryCursor select, DML, DDL, empty set, fetch, metadata, and close."""
    server = _CursorBigQueryRestServer()
    try:
        client = AsyncBigQueryClient(
            project_id="test-proj",
            rest_endpoint=server.rest_endpoint,
            storage_endpoint="http://127.0.0.1:9",
        )
        cur = client.cursor()
        assert isinstance(cur, AsyncBigQueryCursor)
        initial_desc = cur.description
        assert initial_desc is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None
        initial_meta = cur.query_metadata
        assert initial_meta is None
        assert cur.arraysize == 1
        assert await cur.fetchone() is None
        assert await cur.fetchmany() == []
        assert await cur.fetchall() == []

        ret = await cur.execute("SELECT 1 AS a, 'x' AS b")
        assert ret is cur
        select_desc = cur.description
        assert select_desc is not None
        assert [col[0] for col in select_desc] == ["a", "b"]
        assert cur.rowcount == 1
        assert cur.lastrowid is None
        assert await cur.fetchall() == [(1, "x")]
        assert await cur.fetchone() is None
        meta = cur.query_metadata
        assert meta is not None
        assert isinstance(meta["job_id"], str)

        await cur.execute("SELECT id, val FROM items")
        cur.arraysize = 2
        assert await cur.fetchmany(0) == []
        assert await cur.fetchmany() == [(1, "one"), (2, "two")]
        assert await cur.fetchone() == (3, "three")

        with pytest.raises(ProgrammingError):
            cur.arraysize = 0
        with pytest.raises(ProgrammingError):
            await cur.fetchmany(-1)

        await cur.execute("UPDATE items SET val = 'z' WHERE id IN (1, 2)")
        assert cur.rowcount == 2
        dml_desc = cur.description
        assert dml_desc is None

        await cur.execute("CREATE TABLE items (id INT64)")
        assert cur.rowcount == -1
        ddl_desc = cur.description
        assert ddl_desc is None

        await cur.execute("SELECT 1 AS a FROM UNNEST([]) AS x")
        empty_desc = cur.description
        assert empty_desc is not None
        assert [col[0] for col in empty_desc] == ["a"]
        assert await cur.fetchall() == []

        await cur.close()
        await cur.close()
        with pytest.raises(InterfaceError):
            await cur.execute("SELECT 1 AS a, 'x' AS b")
        with pytest.raises(InterfaceError):
            await cur.fetchone()
    finally:
        server.close()


@pytest.mark.anyio
async def test_async_cursor_fetch_returns_completed_awaitable() -> None:
    """Verify AsyncBigQueryCursor.fetchall returns a completed awaitable with buffered rows."""
    server = _CursorBigQueryRestServer()
    try:
        client = AsyncBigQueryClient(
            project_id="test-proj",
            rest_endpoint=server.rest_endpoint,
            storage_endpoint="http://127.0.0.1:9",
        )
        cur = client.cursor()
        await cur.execute("SELECT 1 AS a, 'x' AS b")
        expected = await cur.fetchall()
        assert expected

        await cur.execute("SELECT 1 AS a, 'x' AS b")
        step = cur.fetchall().__await__()
        with pytest.raises(StopIteration) as exc_info:
            next(step)
        assert exc_info.value.value == expected
    finally:
        server.close()


@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "close"])
def test_async_cursor_ready_methods_complete_without_loop(method: str) -> None:
    """Verify AsyncBigQueryCursor ready methods finish on the first __await__ step without a running loop."""
    server = _CursorBigQueryRestServer()
    try:
        client = AsyncBigQueryClient(
            project_id="test-proj",
            rest_endpoint=server.rest_endpoint,
            storage_endpoint="http://127.0.0.1:9",
        )
        cur = client.cursor()

        async def _execute() -> None:
            await cur.execute("SELECT 1 AS a, 'x' AS b")

        asyncio.run(_execute())
        step = getattr(cur, method)().__await__()
        with pytest.raises(StopIteration):
            next(step)
    finally:
        server.close()


@pytest.mark.anyio
async def test_async_cursor_state_errors_raise_at_call() -> None:
    """Verify AsyncBigQueryCursor state errors are raised when calling fetch methods before awaiting."""
    server = _CursorBigQueryRestServer()
    try:
        client = AsyncBigQueryClient(
            project_id="test-proj",
            rest_endpoint=server.rest_endpoint,
            storage_endpoint="http://127.0.0.1:9",
        )
        cur = client.cursor()
        with pytest.raises(ProgrammingError):
            cur.fetchmany(-1)
        await cur.close()
        with pytest.raises(InterfaceError):
            cur.fetchone()
        with pytest.raises(InterfaceError):
            cur.fetchmany()
        with pytest.raises(InterfaceError):
            cur.fetchall()
    finally:
        server.close()


@pytest.mark.anyio
@pytest.mark.parametrize("params", [(), [], {}])
async def test_async_cursor_empty_container_matches_no_parameters(
    params: tuple[Any, ...] | list[Any] | dict[str, Any],
) -> None:
    """Verify empty exact parameter containers bypass param_encoder and match no-parameter execution."""
    server = _CursorBigQueryRestServer()
    try:
        client = AsyncBigQueryClient(
            project_id="test-proj",
            rest_endpoint=server.rest_endpoint,
            storage_endpoint="http://127.0.0.1:9",
        )
        cur = client.cursor()
        await cur.execute("SELECT 1 AS a, 'x' AS b")
        expected = await cur.fetchall()

        await cur.execute("SELECT 1 AS a, 'x' AS b", params)
        assert await cur.fetchall() == expected
    finally:
        server.close()


@pytest.mark.anyio
async def test_async_cursor_session_params_encoder_options_errors_and_concurrency() -> None:
    """Verify AsyncBigQueryCursor session reuse, parameters, encoder, labels, limits, errors, executemany, and concurrency."""
    server = _CursorBigQueryRestServer()
    try:
        client = AsyncBigQueryClient(
            project_id="test-proj",
            rest_endpoint=server.rest_endpoint,
            storage_endpoint="http://127.0.0.1:9",
        )

        with pytest.raises(NotSupportedError):
            client.cursor(lazy_begin=True)
        assert server.requests == []

        sess_cur = client.cursor(session_id="async-sess-1")
        await sess_cur.execute("SELECT 1 AS a, 'x' AS b")
        await sess_cur.execute("SELECT 1 AS a, 'x' AS b")
        assert _extract_request_session_id(server.requests[-2]) == "async-sess-1"
        assert _extract_request_session_id(server.requests[-1]) == "async-sess-1"

        reply_sess_cur = client.cursor()
        await reply_sess_cur.execute("SELECT ASSIGN_SESSION")
        await reply_sess_cur.execute("SELECT 1 AS a, 'x' AS b")
        assert _extract_request_session_id(server.requests[-1]) == "bq-reply-session-99"

        encoded_json = json.dumps([{"name": "a", "parameterType": {"type": "INT64"}, "parameterValue": {"value": "9"}}])
        param_cur = client.cursor()
        await param_cur.execute("SELECT @a", encoded_json)
        assert server.requests[-1]["body"].get("queryParameters") == [
            {"name": "a", "parameterType": {"type": "INT64"}, "parameterValue": {"value": "9"}}
        ]

        with pytest.raises(InterfaceError, match="BigQuery cursor needs encoded parameters or a param_encoder"):
            await param_cur.execute("SELECT @a", {"a": 1})

        calls: list[tuple[str, Any]] = []

        def encoder(sql: str, params: Any) -> tuple[str, str | None]:
            calls.append((sql, params))
            return (
                sql,
                json.dumps(
                    [{"name": "a", "parameterType": {"type": "INT64"}, "parameterValue": {"value": str(params["a"])}}]
                ),
            )

        enc_cur = client.cursor(
            labels={"env": "test"},
            maximum_bytes_billed=2097152,
            param_encoder=encoder,
        )
        await enc_cur.execute("SELECT @a", {"a": 1})
        assert calls == [("SELECT @a", {"a": 1})]
        job_req = server.requests[-1]["body"]
        assert job_req.get("labels") == {"env": "test"}
        assert str(job_req.get("maximumBytesBilled")) == "2097152"
        assert job_req.get("dryRun", False) is False

        err_cur = client.cursor()
        await err_cur.execute("SELECT 1 AS a, 'x' AS b")
        with pytest.raises(BigQueryError) as excinfo:
            await err_cur.execute("SELEC 1")
        assert excinfo.value.code == 3
        assert err_cur.description is None
        assert err_cur.rowcount == -1
        assert err_cur.query_metadata is None

        many_cur = client.cursor(param_encoder=encoder)
        await many_cur.executemany("UPDATE items SET a = @a", [{"a": 1}, {"a": 2}])
        assert many_cur.rowcount == 4
        assert many_cur.description is None

        conc_cur = client.cursor()
        first_awaitable = conc_cur.execute("SELECT HOLD_IN_FLIGHT")
        while not server.in_flight_started.is_set():
            await asyncio.sleep(0.01)
        with pytest.raises(InterfaceError, match="cursor is already executing"):
            conc_cur.execute("SELECT 1 AS a, 'x' AS b")
        server.release_in_flight.set()
        await first_awaitable
    finally:
        server.close()


class _DriverCursorMockServer:
    """Local HTTP/1.1 BigQuery REST server exercising Sync/AsyncBigQueryDriver native cursor paths."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.job_counter: int = 0
        self.session_counter: int = 0
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                _ = (format, args)

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
                body = json.loads(raw_body) if raw_body else {}
                parent.requests.append({"method": "POST", "path": self.path, "body": body})

                query_cfg = body.get("configuration", {}).get("query", {})
                sql = str(query_cfg.get("query") or body.get("query") or "")
                upper = sql.strip().upper()

                if upper.startswith("SELEC "):
                    self._send_json(
                        400,
                        {
                            "error": {
                                "code": 400,
                                "message": 'Syntax error: Unexpected identifier "SELEC"',
                                "status": "INVALID_ARGUMENT",
                                "errors": [{"reason": "invalidQuery"}],
                            }
                        },
                    )
                    return

                parent.job_counter += 1
                job_id = f"bq-drv-job-{parent.job_counter}"
                create_session = bool(query_cfg.get("createSession") or body.get("createSession"))
                conn_props = query_cfg.get("connectionProperties") or body.get("connectionProperties") or []
                session_id = None
                for prop in conn_props:
                    if prop.get("key") == "session_id":
                        session_id = prop.get("value")
                if create_session and not session_id:
                    parent.session_counter += 1
                    session_id = f"bq-drv-session-{parent.session_counter}"

                resp: dict[str, Any] = {
                    "jobComplete": True,
                    "jobReference": {"projectId": "test-proj", "jobId": job_id, "location": "US"},
                    "totalBytesProcessed": "4096",
                    "totalBytesBilled": "10485760",
                    "cacheHit": False,
                }
                if session_id:
                    resp["sessionInfo"] = {"sessionId": session_id}

                if upper.startswith(("BEGIN TRANSACTION", "COMMIT TRANSACTION", "ROLLBACK TRANSACTION")):
                    resp["schema"] = {"fields": []}
                    resp["rows"] = []
                    resp["totalRows"] = "0"
                elif upper.startswith("UPDATE "):
                    resp["schema"] = {"fields": []}
                    resp["rows"] = []
                    resp["totalRows"] = "0"
                    resp["numDmlAffectedRows"] = "2"
                elif upper.startswith("CREATE TABLE"):
                    resp["schema"] = {"fields": []}
                    resp["rows"] = []
                    resp["totalRows"] = "0"
                elif "SELECT 1 AS A, 2 AS B" in upper:
                    resp["schema"] = {
                        "fields": [
                            {"name": "a", "type": "INT64", "mode": "REQUIRED"},
                            {"name": "b", "type": "INT64", "mode": "REQUIRED"},
                        ],
                    }
                    resp["rows"] = [{"f": [{"v": "1"}, {"v": "2"}]}]
                    resp["totalRows"] = "1"
                elif "SELECT @A AS A" in upper:
                    params = query_cfg.get("queryParameters") or body.get("queryParameters") or []
                    val = "0"
                    for p in params:
                        if p.get("name") == "a":
                            val = str(p.get("parameterValue", {}).get("value", "0"))
                    resp["schema"] = {
                        "fields": [{"name": "a", "type": "INT64", "mode": "REQUIRED"}],
                    }
                    resp["rows"] = [{"f": [{"v": val}]}]
                    resp["totalRows"] = "1"
                elif "UNION ALL SELECT 2" in upper:
                    resp["schema"] = {
                        "fields": [{"name": "f0_", "type": "INT64", "mode": "REQUIRED"}],
                    }
                    resp["rows"] = [{"f": [{"v": "1"}]}, {"f": [{"v": "2"}]}]
                    resp["totalRows"] = "2"
                elif "SELECT 1 AS A" in upper:
                    resp["schema"] = {
                        "fields": [{"name": "a", "type": "INT64", "mode": "REQUIRED"}],
                    }
                    resp["rows"] = [{"f": [{"v": "1"}]}]
                    resp["totalRows"] = "1"
                else:
                    resp["schema"] = {"fields": []}
                    resp["rows"] = []
                    resp["totalRows"] = "0"

                self._send_json(200, resp)

            def _send_json(self, status: int, payload: dict[str, Any]) -> None:
                encoded = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self.rest_endpoint = f"http://127.0.0.1:{self._server.server_port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def test_sync_bigquery_driver_native_cursor_matrix() -> None:
    """Verify SyncBigQueryDriver native cursor query, DML, metadata, session, options, and error matrix."""
    server = _DriverCursorMockServer()
    try:
        cfg = BigQueryConfig(project_id="test-proj", rest_endpoint=server.rest_endpoint)
        with SyncBigQueryDriver(cfg) as driver:
            with pytest.raises(NotSupportedError):
                driver.cursor(lazy_begin=True)

            assert driver.select("SELECT 1 AS a") == [{"a": 1}]
            meta = driver.last_query_metadata
            assert meta["job_id"] == "bq-drv-job-1"
            assert set(meta.keys()) == {
                "job_id",
                "session_id",
                "total_rows",
                "total_bytes_processed",
                "total_bytes_billed",
                "slot_ms",
                "cache_hit",
                "num_dml_affected_rows",
            }

            rows, names = driver.execute_with_description("SELECT 1 AS a, 2 AS b")
            assert rows == [(1, 2)]
            assert isinstance(rows[0], tuple)
            assert names == ("a", "b")

            assert driver.execute("UPDATE t SET a = 1") == 2
            assert driver.execute("CREATE TABLE t (a INT64)") == 0
            assert driver.execute("SELECT 1 UNION ALL SELECT 2") == 2
            assert driver.select("SELECT @a AS a", {"a": 5}) == [{"a": 5}]
            assert driver.select_one("SELECT 1 AS a") == {"a": 1}
            assert driver.select_one_or_none("SELECT 1 AS a") == {"a": 1}
            assert driver.query("SELECT 1 AS a") == [{"a": 1}]
            assert driver.query_one("SELECT 1 AS a") == {"a": 1}
            assert driver.select_value("SELECT 1 AS a") == 1
            assert driver.execute_script("SELECT 1 AS a") == [{"a": 1}]

            driver.execute("SELECT 1 AS a", job_labels={"team": "x"}, maximum_bytes_billed=10)
            opt_body = server.requests[-1]["body"]
            assert opt_body.get("labels") == {"team": "x"}
            assert str(opt_body.get("maximumBytesBilled")) == "10"

            assert driver.in_transaction is False
            driver.begin()
            assert driver.in_transaction is True
            session_id = driver.session_id
            assert session_id == "bq-drv-session-1"
            req_start = len(server.requests)
            driver.execute("UPDATE t SET a = 2")
            driver.commit()
            assert driver.in_transaction is False
            assert _extract_request_session_id(server.requests[req_start]) == session_id
            assert _extract_request_session_id(server.requests[req_start + 1]) == session_id

            driver.begin()
            assert driver.in_transaction is True
            driver.rollback()
            assert driver.in_transaction is False
            assert _extract_request_session_id(server.requests[-2]) == session_id
            assert _extract_request_session_id(server.requests[-1]) == session_id

            with pytest.raises(BigQueryError) as excinfo:
                driver.execute("SELEC 1")
            assert excinfo.value.code == 3
    finally:
        server.close()


@pytest.mark.anyio
async def test_async_bigquery_driver_native_cursor_matrix() -> None:
    """Verify AsyncBigQueryDriver native cursor query, DML, metadata, session, options, and error matrix."""
    server = _DriverCursorMockServer()
    try:
        cfg = BigQueryConfig(project_id="test-proj", rest_endpoint=server.rest_endpoint)
        async with AsyncBigQueryDriver(cfg) as driver:
            with pytest.raises(NotSupportedError):
                driver.cursor(lazy_begin=True)

            assert await driver.select("SELECT 1 AS a") == [{"a": 1}]
            meta = driver.last_query_metadata
            assert meta["job_id"] == "bq-drv-job-1"
            assert set(meta.keys()) == {
                "job_id",
                "session_id",
                "total_rows",
                "total_bytes_processed",
                "total_bytes_billed",
                "slot_ms",
                "cache_hit",
                "num_dml_affected_rows",
            }

            rows, names = await driver.execute_with_description("SELECT 1 AS a, 2 AS b")
            assert rows == [(1, 2)]
            assert isinstance(rows[0], tuple)
            assert names == ("a", "b")

            assert await driver.execute("UPDATE t SET a = 1") == 2
            assert await driver.execute("CREATE TABLE t (a INT64)") == 0
            assert await driver.execute("SELECT 1 UNION ALL SELECT 2") == 2
            assert await driver.select("SELECT @a AS a", {"a": 5}) == [{"a": 5}]
            assert await driver.select_one("SELECT 1 AS a") == {"a": 1}
            assert await driver.select_one_or_none("SELECT 1 AS a") == {"a": 1}
            assert await driver.query("SELECT 1 AS a") == [{"a": 1}]
            assert await driver.query_one("SELECT 1 AS a") == {"a": 1}
            assert await driver.select_value("SELECT 1 AS a") == 1
            assert await driver.execute_script("SELECT 1 AS a") == [{"a": 1}]

            await driver.execute("SELECT 1 AS a", job_labels={"team": "x"}, maximum_bytes_billed=10)
            opt_body = server.requests[-1]["body"]
            assert opt_body.get("labels") == {"team": "x"}
            assert str(opt_body.get("maximumBytesBilled")) == "10"

            assert driver.in_transaction is False
            await driver.begin()
            assert driver.in_transaction is True
            session_id = driver.session_id
            assert session_id == "bq-drv-session-1"
            req_start = len(server.requests)
            await driver.execute("UPDATE t SET a = 2")
            await driver.commit()
            assert driver.in_transaction is False
            assert _extract_request_session_id(server.requests[req_start]) == session_id
            assert _extract_request_session_id(server.requests[req_start + 1]) == session_id

            await driver.begin()
            assert driver.in_transaction is True
            await driver.rollback()
            assert driver.in_transaction is False
            assert _extract_request_session_id(server.requests[-2]) == session_id
            assert _extract_request_session_id(server.requests[-1]) == session_id

            with pytest.raises(BigQueryError) as excinfo:
                await driver.execute("SELEC 1")
            assert excinfo.value.code == 3
    finally:
        server.close()
