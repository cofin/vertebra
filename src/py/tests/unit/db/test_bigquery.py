"""Unit tests for pure-Rust Google Cloud BigQuery drivers, Arrow Storage APIs, Object Tables, and DataFusion."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from vertebra._vertebra import BigQueryClient, BigQueryError
from vertebra.db import (
    ArrowCursor,
    AsyncBigQueryDriver,
    BigQueryConfig,
    BigQueryDryRunResult,
    DatabaseRegistry,
    DataFusionEngine,
    ObjectRef,
    ObjectTableConfig,
    SyncBigQueryDriver,
)
from vertebra.db.bigquery.core import BigQueryCore
from vertebra.embeddings import BigQueryEmbedder, EmbeddingError
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.storage import ObjectStore, SigningConfig
from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError, VertebraError


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
    assert fp[0] == "bigquery"
    assert fp[1] == "analytics-proj"
    assert fp[2] == "events_ds"
    sync_drv = reg.get_sync_driver("bq")
    assert isinstance(sync_drv, SyncBigQueryDriver)
    assert sync_drv.project_id == "analytics-proj"
    assert sync_drv.dataset_id == "events_ds"


def test_sync_bigquery_driver_queries_dry_run_and_transactions() -> None:
    """Verify SyncBigQueryDriver over the pure-Rust BigQuery REST v2 wire engine."""
    server = _MockBigQueryRestServer()
    try:
        cfg = BigQueryConfig(
            project_id="test-proj",
            dataset_id="analytics",
            rest_endpoint=f"http://{server.host_port}",
            labels={"Env Name!": "Prod-01"},
        )
        with SyncBigQueryDriver(cfg) as driver:
            rows = driver.select("SELECT id, name, score FROM metrics WHERE id >= @min_id", {"min_id": 1})
            assert rows == [
                {"id": 1, "name": "alpha", "score": 9.5},
                {"id": 2, "name": "beta", "score": 4.25},
            ]

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
async def test_async_bigquery_driver_arrow_object_store_and_object_tables() -> None:
    """Verify AsyncBigQueryDriver, ArrowCursor, multicloud ObjectStore stage/load, and ObjectRef presigning."""
    server = _MockBigQueryRestServer()
    try:
        cfg = BigQueryConfig(
            project_id="test-proj",
            dataset_id="analytics",
            rest_endpoint=f"http://{server.host_port}",
        )
        store = ObjectStore(
            "memory://",
            signing=SigningConfig(
                provider="hmac",
                hmac_secret="test-bq-signing-secret",
                endpoint="https://storage.example.com",
            ),
        )
        backend = ObjectStoreBackend("memory://warehouse", store=store)

        async with AsyncBigQueryDriver(cfg) as driver:
            cursor = await driver.fetch_arrow("SELECT id, name, score FROM metrics")
            assert isinstance(cursor, ArrowCursor)
            assert cursor.row_count == 2
            assert cursor.to_pylist() == [
                {"id": 1, "name": "alpha", "score": 9.5},
                {"id": 2, "name": "beta", "score": 4.25},
            ]

            exported_count = await driver.export_to_object_store(
                "SELECT id, name, score FROM metrics",
                "memory://warehouse/exports/metrics.parquet",
                store=backend,
                format="parquet",
            )
            assert exported_count == 2

            loaded_count = await driver.load_from_object_store(
                "analytics.metrics",
                "memory://warehouse/exports/metrics.parquet",
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
            assert "CREATE EXTERNAL TABLE IF NOT EXISTS analytics.object_table_docs" in ddl
            assert "WITH CONNECTION `us.gcs-conn`" in ddl
            assert "WITH MAX STALENESS INTERVAL 1 HOUR" in ddl
            assert "object_metadata = 'SIMPLE'" in ddl

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
async def test_bigquery_embedder_and_datafusion_registration() -> None:
    """Verify BigQueryEmbedder ML.GENERATE_EMBEDDING and DataFusionEngine.register_bigquery_query."""
    server = _MockBigQueryRestServer()
    try:
        cfg = BigQueryConfig(
            project_id="test-proj",
            dataset_id="analytics",
            rest_endpoint=f"http://{server.host_port}",
        )
        sync_driver = SyncBigQueryDriver(cfg)
        async_driver = AsyncBigQueryDriver(cfg)

        embedder_sync = BigQueryEmbedder(
            sync_driver,
            model="analytics.text_embedding_model",
            dimensions=3,
            normalize=False,
        )
        vec = embedder_sync.embed_query_sync("hello world")
        assert vec.to_list() == [0.25, 0.5, 0.75]

        embedder_async = BigQueryEmbedder(
            async_driver,
            model="analytics.text_embedding_model",
            dimensions=3,
            normalize=False,
        )
        vecs = await embedder_async.embed_many(["doc one", "doc two"])
        assert len(vecs.to_list()) == 2
        assert vecs.to_list()[0] == [0.25, 0.5, 0.75]

        engine = DataFusionEngine()
        engine.register_bigquery_query(
            "bq_metrics",
            sync_driver,
            "SELECT id, name, score FROM metrics",
        )
        rows = engine.execute_dicts("SELECT id, name FROM bq_metrics WHERE id = 1")
        assert rows == [{"id": 1, "name": "alpha"}]
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
    with pytest.raises(ValueError, match="model_endpoint"):
        BigQueryEmbedder(object(), model="analytics.model`; DROP TABLE x; --").build_generate_embedding_sql()
    embedder = BigQueryEmbedder(object(), model="analytics-proj.analytics.text_embedding_model")
    assert "MODEL `analytics-proj.analytics.text_embedding_model`" in embedder.build_generate_embedding_sql()
    with pytest.raises(ValueError, match="base_table"):
        embedder.build_vector_search_sql("analytics.docs) --")
    with pytest.raises(ValueError, match="embedding_column"):
        embedder.build_vector_search_sql("analytics.docs", embedding_column="emb' OR '1")
    with pytest.raises(ValueError, match="distance_type"):
        embedder.build_vector_search_sql("analytics.docs", distance_type="MANHATTAN")


@pytest.mark.parametrize("use_ai_function", [False, True], ids=["ml-generate", "ai-generate"])
def test_bigquery_embedder_generate_sql_orders_rows_by_input_index(use_ai_function: bool) -> None:
    """The generated SQL carries the input offset through the model and orders the output by it."""
    sql = BigQueryEmbedder(
        object(), model="analytics.text_embedding_model", use_ai_function=use_ai_function
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
    """A short result or an empty vector raises EmbeddingError."""
    embedder = BigQueryEmbedder(
        _StubBigQuerySelectDriver(rows), model="analytics.text_embedding_model", dimensions=2, normalize=False
    )
    with pytest.raises(EmbeddingError):
        await embedder.embed_many(["alpha", "beta"])


def test_bigquery_client_invalid_params_raise_typed_error() -> None:
    """Sync BigQuery binding raises BigQueryError with INVALID_ARGUMENT for malformed parameters."""
    client = BigQueryClient(project_id="p1", rest_endpoint="http://127.0.0.1:9")
    with pytest.raises(BigQueryError) as excinfo:
        client.query("SELECT 1", "not json")
    assert excinfo.value.code == 3
    assert excinfo.value.code_name == "INVALID_ARGUMENT"
    assert isinstance(excinfo.value, VertebraError)


@pytest.mark.anyio
async def test_bigquery_client_invalid_params_raise_typed_error_async() -> None:
    """Async BigQuery binding raises the same BigQueryError as the sync binding."""
    client = BigQueryClient(project_id="p1", rest_endpoint="http://127.0.0.1:9")
    with pytest.raises(BigQueryError) as excinfo:
        await client.query_async("SELECT 1", "not json")
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
