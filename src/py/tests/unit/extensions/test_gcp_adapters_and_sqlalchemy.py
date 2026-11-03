"""Unit tests for GCP framework adapters, FastPath streaming, governed MCP/A2A tools, and secondary SQLAlchemy +vtb dialects."""

from __future__ import annotations

from typing import Any

import pytest

from tests.unit.db.test_spanner import FakeSpannerClient
from vertebra._vertebra import ArrowCursor as RustArrowCursor
from vertebra._vertebra import Request, Response
from vertebra.agent.runtime import ConfirmationGate
from vertebra.db import (
    ArrowCursor,
    BigQueryConfig,
    BigQueryDryRunResult,
    DatabaseRegistry,
    GCPDatabaseToolPolicy,
    MySQLConfig,
    PostgreSQLConfig,
    SpannerConfig,
    SyncBigQueryDriver,
    SyncMySQLDriver,
    SyncPostgreSQLDriver,
    SyncSpannerDriver,
    is_mutating_sql,
    provide_bigquery_driver,
    provide_spanner_driver,
    register_gcp_database_tools,
)
from vertebra.extensions.alchemy import (
    create_async_alchemy_config,
    create_sync_alchemy_config,
    normalize_alchemy_url,
)
from vertebra.extensions.django.db.backends.bigquery.base import (
    DatabaseWrapper as BigQueryDjangoDatabaseWrapper,
)
from vertebra.extensions.django.db.backends.spanner.base import (
    DatabaseWrapper as SpannerDjangoDatabaseWrapper,
)
from vertebra.extensions.django.fastpath import (
    CompiledRoute as DjangoCompiledRoute,
)
from vertebra.extensions.django.fastpath import (
    dispatch_django_direct,
)
from vertebra.extensions.fastapi import (
    provide_bigquery_driver as fastapi_provide_bigquery,
)
from vertebra.extensions.fastapi import (
    provide_spanner_driver as fastapi_provide_spanner,
)
from vertebra.extensions.fastapi.fastpath import (
    CompiledRoute as FastAPICompiledRoute,
)
from vertebra.extensions.fastapi.fastpath import (
    dispatch_handler_direct as dispatch_fastapi_direct,
)
from vertebra.extensions.flask import (
    provide_bigquery_driver as flask_provide_bigquery,
)
from vertebra.extensions.flask import (
    provide_spanner_driver as flask_provide_spanner,
)
from vertebra.extensions.flask.fastpath import (
    CompiledRoute as FlaskCompiledRoute,
)
from vertebra.extensions.flask.fastpath import (
    dispatch_wsgi_direct,
)
from vertebra.extensions.litestar import (
    provide_bigquery_driver as litestar_provide_bigquery,
)
from vertebra.extensions.litestar import (
    provide_spanner_driver as litestar_provide_spanner,
)
from vertebra.extensions.litestar.fastpath import (
    _DirectSignaturePlan,
    _execute_direct_signature_plan,
)
from vertebra.extensions.sqlalchemy import (
    ALL_DIALECT_SCHEMES,
    DIALECT_DESCRIPTORS,
    BigQueryAlembicImpl,
    BigQueryAsyncDialect,
    BigQueryDialect,
    BinaryJSON,
    CreateVectorIndexOp,
    SpangresAsyncDialect,
    SpangresDialect,
    SpannerAlembicImpl,
    SpannerAsyncDialect,
    SpannerDialect,
    Vector,
)
from vertebra.extensions.starlette import (
    provide_bigquery_driver as starlette_provide_bigquery,
)
from vertebra.extensions.starlette import (
    provide_spanner_driver as starlette_provide_spanner,
)
from vertebra.extensions.starlette.fastpath import (
    CompiledRoute as StarletteCompiledRoute,
)
from vertebra.extensions.starlette.fastpath import (
    dispatch_handler_direct as dispatch_starlette_direct,
)
from vertebra.framework.security import PermissionDeniedError
from vertebra.server._dispatch import _coerce_response
from vertebra.server._fastpath import (
    ARROW_STREAM_MEDIA_TYPE,
    serialize_fastpath_special_result,
)
from vertebra.storage import ObjectStore, StreamingStorageResponse


@pytest.mark.anyio
async def test_fastpath_zero_copy_arrow_cursor_and_streaming_storage() -> None:
    """Verify FastPath zero-copy ArrowCursor and StreamingStorageResponse across all framework adapters."""
    ipc_payload = b"ARROW1\x00\x00test-arrow-ipc-payload"
    cursor = ArrowCursor(
        ipc_payload,
        rows=[(1, "alpha"), (2, "beta")],
        columns=["id", "name"],
    )
    serialized = serialize_fastpath_special_result(cursor)
    assert serialized is not None
    status, headers, body = serialized
    assert status == 200
    assert (b"content-type", ARROW_STREAM_MEDIA_TYPE) in headers
    assert body == ipc_payload

    rust_cursor = RustArrowCursor.write_ipc_to_shm("/vtb-test-fastpath", ipc_payload, 2)
    rust_serialized = serialize_fastpath_special_result(rust_cursor)
    assert rust_serialized is not None
    assert rust_serialized[2] == ipc_payload

    coerced = _coerce_response(cursor, 1, Response)
    assert coerced.status == 200
    assert bytes(coerced.body) == ipc_payload

    direct_resp = StreamingStorageResponse(
        content_type="application/parquet",
        etag="etag-123",
        content_range="bytes 0-15/16",
        data=b"PAR1-binary-data",
    )
    direct_tuple = serialize_fastpath_special_result(direct_resp)
    assert direct_tuple is not None
    assert direct_tuple[0] == 206
    assert (b"content-type", b"application/parquet") in direct_tuple[1]
    assert (b"etag", b"etag-123") in direct_tuple[1]
    assert (b"content-range", b"bytes 0-15/16") in direct_tuple[1]
    assert direct_tuple[2] == b"PAR1-binary-data"

    signed_resp = StreamingStorageResponse(
        content_type="application/octet-stream",
        etag="etag-signed",
        signed_url="https://storage.googleapis.com/bucket/object?X-Goog-Signature=abc",
    )
    signed_tuple = serialize_fastpath_special_result(signed_resp)
    assert signed_tuple is not None
    assert signed_tuple[0] == 307
    assert (
        b"location",
        b"https://storage.googleapis.com/bucket/object?X-Goog-Signature=abc",
    ) in signed_tuple[1]
    assert (
        b"x-vertebra-signed-url",
        b"https://storage.googleapis.com/bucket/object?X-Goog-Signature=abc",
    ) in signed_tuple[1]

    mem_store = ObjectStore.from_memory()
    mem_store.put("reports/q1.bin", b"0123456789abcdef")
    store_resp = StreamingStorageResponse(
        store=mem_store,
        key="reports/q1.bin",
        total_size=16,
        content_type="application/octet-stream",
        etag="etag-q1",
        range_start=0,
        range_end=3,
    )
    store_tuple = serialize_fastpath_special_result(store_resp)
    assert store_tuple is not None
    assert store_tuple[0] == 206
    assert store_tuple[2] == b"0123"

    ls_plan = _DirectSignaturePlan(
        fn=lambda: cursor,
        is_async=False,
        status_code=200,
        media_type="application/json",
        path_param_names=(),
        query_params=(),
        body_param=None,
        body_type=None,
        json_headers=[(b"content-type", b"application/json")],
        text_headers=[(b"content-type", b"text/plain")],
        raw_bytes_headers=[(b"content-type", b"application/octet-stream")],
    )
    ls_out = await _execute_direct_signature_plan(
        ls_plan,
        scope={},
        path_params={},
        receive=None,
        body=b"",
    )
    assert ls_out is not None
    assert ls_out[2] == ipc_payload

    fa_compiled = FastAPICompiledRoute(
        handler_id=1,
        method="GET",
        path_template="/parquet",
        route=None,
        endpoint=lambda: direct_resp,
        is_async=False,
        status_code=200,
        has_middleware=False,
        has_custom_exception_handlers=False,
        dependant_plan=None,
        request_param_name=None,
        path_param_names=(),
        query_params=(),
        body_param_name=None,
        body_type=None,
        body_is_msgspec=False,
        body_is_pydantic=False,
        response_serializer=None,
    )
    fa_out = await dispatch_fastapi_direct(
        fa_compiled,
        scope={},
        path_params={},
        body=b"",
        receive=None,
        send=None,
    )
    assert fa_out is not None
    assert fa_out[2] == b"PAR1-binary-data"

    st_compiled = StarletteCompiledRoute(
        handler_id=2,
        method="GET",
        path_template="/arrow",
        route=None,
        endpoint=lambda _req: cursor,
        is_async=False,
        has_middleware=False,
    )
    st_out = await dispatch_starlette_direct(
        st_compiled,
        scope={
            "type": "http",
            "method": "GET",
            "path": "/arrow",
            "headers": [],
            "query_string": b"",
        },
        body=b"",
        receive=None,
        send=None,
    )
    assert st_out is not None
    assert st_out[2] == ipc_payload

    fl_compiled = FlaskCompiledRoute(
        handler_id=3,
        endpoint="arrow_view",
        rule="/arrow",
        view_func=lambda: cursor,
        is_async=False,
        status_code=200,
    )
    fl_resp = dispatch_wsgi_direct(fl_compiled, {}, {})
    assert fl_resp is not None
    assert bytes(fl_resp.body) == ipc_payload

    dj_compiled = DjangoCompiledRoute(
        handler_id=4,
        name="storage_view",
        pattern="/storage",
        callback=lambda: direct_resp,
        is_async=False,
        status_code=200,
        accepts_request=False,
    )
    dj_resp = dispatch_django_direct(
        dj_compiled,
        Request.from_parts(method="GET", path="/storage", body=b""),
        {},
    )
    assert dj_resp is not None
    assert bytes(dj_resp.body) == b"PAR1-binary-data"


def test_django_backends_and_shared_provider_fingerprinting() -> None:
    """Verify Django BigQuery and Spanner DatabaseWrappers and multi-framework provider sharing."""
    registry = DatabaseRegistry(broker=DatabaseRegistry.default_broker())

    bq_wrapper = BigQueryDjangoDatabaseWrapper(
        {
            "NAME": "analytics_ds",
            "PROJECT": "analytics-proj",
            "LOCATION": "US",
            "MAXIMUM_BYTES_BILLED": 50_000_000,
        }
    )
    assert bq_wrapper.vendor == "bigquery"
    assert bq_wrapper.display_name == "BigQuery (Vertebra)"
    assert bq_wrapper.driver_scheme == "bigquery"
    bq_drv = bq_wrapper.native_driver
    assert isinstance(bq_drv, SyncBigQueryDriver)
    assert bq_drv.config.project_id == "analytics-proj"
    assert bq_drv.config.dataset_id == "analytics_ds"
    assert bq_drv.config.maximum_bytes_billed == 50_000_000

    spanner_wrapper = SpannerDjangoDatabaseWrapper(
        {
            "PROJECT": "core-proj",
            "INSTANCE": "prod-inst",
            "NAME": "ledger-db",
            "DIALECT": "POSTGRESQL",
        }
    )
    assert spanner_wrapper.vendor == "spanner"
    assert spanner_wrapper.display_name == "Cloud Spanner (Vertebra)"
    assert spanner_wrapper.driver_scheme == "spanner"
    sp_drv = spanner_wrapper.native_driver
    assert isinstance(sp_drv, SyncSpannerDriver)
    assert sp_drv.config.project_id == "core-proj"
    assert sp_drv.config.instance_id == "prod-inst"
    assert sp_drv.config.database_id == "ledger-db"
    assert sp_drv.config.dialect == "spangres"

    bq_cfg = BigQueryConfig(
        project_id="analytics-proj",
        dataset_id="analytics_ds",
        rest_endpoint="http://localhost:9050",
    )
    sp_cfg = SpannerConfig(
        project_id="core-proj",
        instance_id="prod-inst",
        database_id="ledger-db",
        endpoint="http://localhost:9010",
    )

    d1 = provide_bigquery_driver(bq_cfg, sync=True, registry=registry)
    d2 = litestar_provide_bigquery(bq_cfg, sync=True, registry=registry)
    d3 = fastapi_provide_bigquery(bq_cfg, sync=True, registry=registry)
    d4 = starlette_provide_bigquery(bq_cfg, sync=True, registry=registry)
    d5 = flask_provide_bigquery(bq_cfg, sync=True, registry=registry)
    assert d1 is d2 is d3 is d4 is d5

    s1 = provide_spanner_driver(sp_cfg, sync=True, registry=registry)
    s2 = litestar_provide_spanner(sp_cfg, sync=True, registry=registry)
    s3 = fastapi_provide_spanner(sp_cfg, sync=True, registry=registry)
    s4 = starlette_provide_spanner(sp_cfg, sync=True, registry=registry)
    s5 = flask_provide_spanner(sp_cfg, sync=True, registry=registry)
    assert s1 is s2 is s3 is s4 is s5


class _StubBigQueryDriver(SyncBigQueryDriver):
    """Deterministic stub SyncBigQueryDriver for governed MCP tool unit tests."""

    def dry_run(
        self,
        sql: str,
        parameters: Any = None,
    ) -> BigQueryDryRunResult:
        """Return a deterministic dry-run estimate based on SQL text."""
        _ = parameters
        processed = 5_000 if "LARGE_SCAN" in sql else 256
        return BigQueryDryRunResult(
            total_bytes_processed=processed,
            total_bytes_billed=max(processed, 1024),
            cache_hit=False,
            referenced_tables=("analytics-proj.warehouse.events",),
            schema=({"name": "val", "type": "INT64"},),
        )

    def select(
        self,
        sql: str,
        parameters: Any = None,
        *,
        schema_type: Any = None,
    ) -> list[Any]:
        """Return deterministic rows for SELECT and Object Table inspection queries."""
        _ = (parameters, schema_type)
        if "uri, content_type" in sql:
            return [
                {
                    "uri": "gs://warehouse/invoices/inv-001.pdf",
                    "content_type": "application/pdf",
                    "size": 4096,
                    "md5_hash": "deadbeef",
                    "updated": "2026-11-02T00:00:00Z",
                }
            ]
        return [{"val": 1}]

    def execute_dml(
        self,
        sql: str,
        parameters: Any = None,
    ) -> int:
        """Return deterministic affected row count for confirmed DML."""
        _ = (sql, parameters)
        return 3


@pytest.mark.anyio
async def test_governed_mcp_and_a2a_gcp_database_tools() -> None:
    """Verify governed BigQuery and Spanner MCP tools, cost ceilings, and ConfirmationGate enforcement."""
    gate = ConfirmationGate()
    bq_driver = _StubBigQueryDriver(
        BigQueryConfig(
            project_id="analytics-proj",
            dataset_id="warehouse",
            rest_endpoint="http://localhost:9050",
            maximum_bytes_billed=1_000,
        )
    )
    spanner_driver = SyncSpannerDriver(
        SpannerConfig(
            project_id="core-proj",
            instance_id="prod-inst",
            database_id="orders-db",
            endpoint="http://localhost:9010",
        ),
        client=FakeSpannerClient(),
    )
    spanner_driver.update_ddl(["CREATE TABLE orders (id INT64 NOT NULL, status STRING(32)) PRIMARY KEY (id)"])
    policy = GCPDatabaseToolPolicy(
        maximum_bytes_billed=1_000,
        enforce_dry_run_limit=True,
        read_only_staleness_seconds=5.0,
    )
    assert policy.protocol_version == "2026-07-28"
    assert policy.a2a_version == "1.0"

    tools = register_gcp_database_tools(
        bigquery_driver=bq_driver,
        spanner_driver=spanner_driver,
        maximum_bytes_billed=1_000,
        enforce_dry_run_limit=True,
        read_only_staleness_seconds=5.0,
        confirmation_gate=gate,
    )
    assert list(tools.keys()) == [
        "bigquery_dry_run",
        "bigquery_query",
        "bigquery_object_table_inspect",
        "spanner_query",
        "spanner_schema_inspect",
    ]

    assert is_mutating_sql("SELECT * FROM users") is False
    assert is_mutating_sql("WITH cte AS (SELECT 1) SELECT * FROM cte") is False
    assert is_mutating_sql("INSERT INTO users (id) VALUES (1)") is True
    assert is_mutating_sql("DROP TABLE users") is True

    dry_run_res = await tools["bigquery_dry_run"].fn(sql="SELECT 1")
    assert dry_run_res["statement_type"] == "SELECT"
    assert dry_run_res["within_budget"] is True

    select_res = await tools["bigquery_query"].fn(sql="SELECT 1")
    assert select_res["mode"] == "query"

    with pytest.raises(PermissionDeniedError, match="maximum_bytes_billed"):
        await tools["bigquery_query"].fn(sql="SELECT 1", max_bytes_billed=0)

    with pytest.raises(PermissionDeniedError, match="exceeds maximum_bytes_billed"):
        await tools["bigquery_query"].fn(sql="SELECT * FROM LARGE_SCAN")

    obj_inspect = await tools["bigquery_object_table_inspect"].fn(
        table_name="warehouse.invoices_obj",
        connection_id="us.gcs-conn",
    )
    assert obj_inspect["row_count"] == 1
    assert "CREATE EXTERNAL TABLE" in obj_inspect["ddl"]

    mutate_sql = "DELETE FROM orders WHERE order_id = 42"
    with pytest.raises(PermissionDeniedError, match="ConfirmationGate") as exc_info:
        await tools["bigquery_query"].fn(sql=mutate_sql)
    err_text = str(exc_info.value)
    token = err_text.split("ticket=", 1)[1].rstrip(")")

    gate.approve(token)
    confirmed_bq = await tools["bigquery_query"].fn(
        sql=mutate_sql,
        confirmation_token=token,
    )
    assert confirmed_bq["mode"] == "dml"

    sp_select = await tools["spanner_query"].fn(sql="SELECT 1")
    assert sp_select["mode"] == "read_only_snapshot"

    with pytest.raises(PermissionDeniedError, match="ConfirmationGate") as sp_exc:
        await tools["spanner_query"].fn(sql="UPDATE orders SET status = 'PAID' WHERE id = 1")
    sp_token = str(sp_exc.value).split("ticket=", 1)[1].rstrip(")")
    gate.approve(sp_token)
    sp_confirmed = await tools["spanner_query"].fn(
        sql="UPDATE orders SET status = 'PAID' WHERE id = 1",
        confirmation_token=sp_token,
    )
    assert sp_confirmed["mode"] == "dml"

    schema_res = await tools["spanner_schema_inspect"].fn(table_name="orders")
    assert schema_res["table_name"] == "orders"


def test_sqlalchemy_vtb_dialects_types_and_alembic_batching() -> None:
    """Verify SQLAlchemy 2.1 +vtb dialects, Vector/BinaryJSON types, CreateVectorIndexOp, and SpannerAlembicImpl."""
    for scheme in (
        "bigquery+vtb",
        "bigquery+vertebra",
        "bigquery+vtb_async",
        "bigquery+vertebra_async",
        "spanner+vtb",
        "spanner+vertebra",
        "spanner+vtb_async",
        "spanner+vertebra_async",
        "spangres+vtb",
        "spangres+vertebra",
        "spangres+vtb_async",
        "spangres+vertebra_async",
    ):
        assert scheme in ALL_DIALECT_SCHEMES
        assert scheme in DIALECT_DESCRIPTORS

    assert BigQueryDialect.paramstyle == "named"
    assert BigQueryAsyncDialect.is_async is True
    merge_sql = BigQueryDialect.compile_merge_upsert(
        "analytics.events",
        "analytics.staging_events",
        on_keys=("event_id",),
        update_columns=("payload", "updated_at"),
    )
    assert "MERGE INTO analytics.events AS T USING analytics.staging_events AS S" in merge_sql
    assert "T.event_id = S.event_id" in merge_sql
    assert "WHEN MATCHED THEN UPDATE SET T.payload = S.payload" in merge_sql

    assert SpannerDialect.paramstyle == "named"
    assert SpannerAsyncDialect.is_async is True
    upsert_sql = SpannerDialect.compile_insert_or_update(
        "Singers",
        ("SingerId", "FirstName", "LastName"),
    )
    assert (
        upsert_sql
        == "INSERT OR UPDATE INTO Singers (SingerId, FirstName, LastName) VALUES (@SingerId, @FirstName, @LastName)"
    )
    interleave_sql = SpannerDialect.compile_interleave_clause(
        "Singers",
        on_delete="CASCADE",
    )
    assert interleave_sql == ", INTERLEAVE IN PARENT Singers ON DELETE CASCADE"

    assert SpangresDialect.paramstyle == "pyformat"
    assert SpangresAsyncDialect.is_async is True

    vec32 = Vector(dimensions=768, precision="float32", column_name="embedding")
    vec64 = Vector(dimensions=768, precision="float64", column_name="embedding")
    assert vec32.get_col_spec(dialect_name="bigquery") == "ARRAY<FLOAT64>"
    assert vec32.get_col_spec(dialect_name="spanner") == "ARRAY<FLOAT32>(vector_length=>768)"
    assert vec64.get_col_spec(dialect_name="spanner") == "ARRAY<FLOAT64>(vector_length=>768)"
    assert vec32.get_col_spec(dialect_name="spangres") == "VECTOR(768)"

    cos_expr = vec32.cosine_distance([0.1, 0.2], param_name="q")
    assert cos_expr.compile_sql(dialect_name="bigquery") == "COSINE_DISTANCE(embedding, @q)"
    assert cos_expr.compile_sql(dialect_name="spanner") == "COSINE_DISTANCE(embedding, @q)"

    approx_expr = vec32.approx_cosine_distance(
        [0.1, 0.2],
        param_name="q",
        num_leaves_to_search=10,
    )
    assert (
        approx_expr.compile_sql(dialect_name="spanner")
        == "APPROX_COSINE_DISTANCE(embedding, @q, options => JSON '{\"num_leaves_to_search\": 10}')"
    )

    bjson = BinaryJSON()
    assert bjson.get_col_spec(dialect_name="bigquery") == "JSON"
    assert bjson.get_col_spec(dialect_name="spanner") == "JSON"
    assert bjson.get_col_spec(dialect_name="spangres") == "JSONB"

    spanner_idx = CreateVectorIndexOp(
        index_name="idx_docs_embedding",
        table_name="Documents",
        column_name="embedding",
        metric="cosine",
        storing=("title", "tenant_id"),
    )
    spanner_idx_sql = spanner_idx.to_sql(dialect_name="spanner")
    assert (
        spanner_idx_sql
        == "CREATE VECTOR INDEX idx_docs_embedding ON Documents (embedding) STORING (title, tenant_id) WHERE embedding IS NOT NULL OPTIONS (tree_type = 'SCANN', distance_type = 'COSINE')"
    )

    bq_idx = CreateVectorIndexOp(
        index_name="idx_events_embedding",
        table_name="analytics.events",
        column_name="embedding",
        metric="cosine",
    )
    bq_idx_sql = bq_idx.to_sql(dialect_name="bigquery")
    assert (
        bq_idx_sql
        == "CREATE VECTOR INDEX idx_events_embedding ON analytics.events(embedding) OPTIONS(index_type = 'IVF', distance_type = 'COSINE')"
    )

    bq_alembic = BigQueryAlembicImpl()
    assert bq_alembic.transactional_ddl is False

    sp_alembic = SpannerAlembicImpl()
    sp_alembic.buffer_ddl("CREATE TABLE Singers (SingerId INT64 NOT NULL) PRIMARY KEY (SingerId);")
    sp_alembic.buffer_ddl(
        "CREATE TABLE Albums (SingerId INT64 NOT NULL, AlbumId INT64 NOT NULL) PRIMARY KEY (SingerId, AlbumId), INTERLEAVE IN PARENT Singers ON DELETE CASCADE;"
    )
    assert len(sp_alembic.buffered_ddl) == 2

    sp_driver = SyncSpannerDriver(
        SpannerConfig(
            project_id="core-proj",
            instance_id="prod-inst",
            database_id="music-db",
            endpoint="http://localhost:9010",
        ),
        client=FakeSpannerClient(),
    )
    lro_result = sp_alembic.flush_ddl_batch(sp_driver)
    assert isinstance(lro_result, str)
    assert lro_result
    assert sp_alembic.buffered_ddl == ()

    assert normalize_alchemy_url("bigquery://proj/dataset", is_async=True) == "bigquery+vtb_async://proj/dataset"
    assert normalize_alchemy_url("bigquery://proj/dataset", is_async=False) == "bigquery+vtb://proj/dataset"
    assert normalize_alchemy_url("spanner://proj/inst/db", is_async=True) == "spanner+vtb_async://proj/inst/db"
    assert normalize_alchemy_url("spangres://proj/inst/db", is_async=False) == "spangres+vtb://proj/inst/db"

    async_cfg = create_async_alchemy_config("bigquery://proj/dataset")
    sync_cfg = create_sync_alchemy_config("spanner://proj/inst/db")
    assert async_cfg.connection_string == "bigquery+vtb_async://proj/dataset"
    assert sync_cfg.connection_string == "spanner+vtb://proj/inst/db"


@pytest.mark.anyio
async def test_direct_tcp_iam_auth_for_postgres_and_mysql() -> None:
    """Verify direct TCP/TLS (:5432 and :3306) IAM token password resolution without Auth Proxy socket dialers."""
    pg_cfg = PostgreSQLConfig(
        dsn="postgresql://iam_sa_user@10.128.0.10:5432/appdb",
        enable_iam_auth=True,
        iam_auth_options={"access_token": "oauth2-direct-pg-5432-token"},
    )
    assert pg_cfg.to_cloud_dialer_config() is None
    assert pg_cfg.resolve_iam_password_sync() == "oauth2-direct-pg-5432-token"
    assert await pg_cfg.resolve_iam_password_async() == "oauth2-direct-pg-5432-token"
    pg_driver = SyncPostgreSQLDriver(pg_cfg)
    assert pg_driver.dialer is None

    mysql_cfg = MySQLConfig(
        dsn="mysql://iam_sa_user@10.128.0.11:3306/appdb",
        enable_iam_auth=True,
        iam_auth_options={"access_token": "oauth2-direct-mysql-3306-token"},
    )
    assert mysql_cfg.to_cloud_dialer_config() is None
    assert mysql_cfg.resolve_iam_password_sync() == "oauth2-direct-mysql-3306-token"
    assert await mysql_cfg.resolve_iam_password_async() == "oauth2-direct-mysql-3306-token"
    dsn_params = mysql_cfg.parse_dsn_params()
    assert dsn_params["host"] == "10.128.0.11"
    assert dsn_params["port"] == 3306
    assert dsn_params["password"] == "oauth2-direct-mysql-3306-token"
    mysql_driver = SyncMySQLDriver(mysql_cfg, connect_factory=lambda: None)
    assert mysql_driver.dialer is None
