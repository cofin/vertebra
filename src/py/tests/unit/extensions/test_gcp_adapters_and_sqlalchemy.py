"""Unit tests for GCP framework adapters, FastPath streaming, governed MCP/A2A tools, and secondary SQLAlchemy +vtb dialects."""

from __future__ import annotations

import ast
import asyncio
import importlib
import threading
from pathlib import Path
from typing import Any

import pytest

import vertebra._bigquery as bigquery_native
import vertebra._spanner as spanner_native
import vertebra.agent as vertebra_agent
import vertebra.db as vertebra_db
import vertebra.extensions.fastapi as fastapi_ext
import vertebra.extensions.flask as flask_ext
import vertebra.extensions.litestar as litestar_ext
import vertebra.extensions.starlette as starlette_ext
from tests.unit.db.test_spanner import FakeSpannerClient
from vertebra._vertebra import ArrowCursor as RustArrowCursor
from vertebra._vertebra import Request, Response
from vertebra.agent.core import PermissionDeniedError, Tool
from vertebra.agent.mcp.registry import MCPRegistry
from vertebra.agent.runtime import ConfirmationGate
from vertebra.agent.tools import build_tools
from vertebra.db import (
    ArrowCursor,
    DatabaseRegistry,
    provide_driver,
)
from vertebra.db.adapters.bigquery import (
    AsyncBigQueryDriver,
    BigQueryConfig,
    BigQueryToolProvider,
    SyncBigQueryDriver,
)
from vertebra.db.adapters.mysql import MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.postgres import PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.spanner import AsyncSpannerDriver, SpannerConfig, SpannerToolProvider, SyncSpannerDriver
from vertebra.db.driver import DatabaseError
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
    provide_driver as fastapi_provide_driver,
)
from vertebra.extensions.fastapi.fastpath import (
    CompiledRoute as FastAPICompiledRoute,
)
from vertebra.extensions.fastapi.fastpath import (
    dispatch_handler_direct as dispatch_fastapi_direct,
)
from vertebra.extensions.flask import (
    provide_driver as flask_provide_driver,
)
from vertebra.extensions.flask.fastpath import (
    CompiledRoute as FlaskCompiledRoute,
)
from vertebra.extensions.flask.fastpath import (
    dispatch_wsgi_direct,
)
from vertebra.extensions.litestar import (
    provide_driver as litestar_provide_driver,
)
from vertebra.extensions.litestar.fastpath import (
    _DirectSignaturePlan,
    _execute_direct_signature_plan,
)
from vertebra.extensions.sqlalchemy import (
    ALL_DIALECT_SCHEMES,
    DIALECT_DESCRIPTORS,
    AsyncBigQueryDialect,
    AsyncSpangresDialect,
    AsyncSpannerDialect,
    BigQueryAlembicImpl,
    BinaryJSON,
    CreateVectorIndexOp,
    SpannerAlembicImpl,
    SyncBigQueryDialect,
    SyncSpangresDialect,
    SyncSpannerDialect,
    Vector,
)
from vertebra.extensions.starlette import (
    provide_driver as starlette_provide_driver,
)
from vertebra.extensions.starlette.fastpath import (
    CompiledRoute as StarletteCompiledRoute,
)
from vertebra.extensions.starlette.fastpath import (
    dispatch_handler_direct as dispatch_starlette_direct,
)
from vertebra.server._dispatch import _coerce_response
from vertebra.server._fastpath import (
    ARROW_STREAM_MEDIA_TYPE,
    serialize_fastpath_special_result,
)
from vertebra.storage import StreamingStorageResponse, SyncObjectStore
from vertebra.typing import ToolPolicyError, ToolProvider


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

    mem_store = SyncObjectStore.from_memory()
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

    d1 = provide_driver(bq_cfg, sync=True, registry=registry)
    d2 = litestar_provide_driver(bq_cfg, sync=True, registry=registry)
    d3 = fastapi_provide_driver(bq_cfg, sync=True, registry=registry)
    d4 = starlette_provide_driver(bq_cfg, sync=True, registry=registry)
    d5 = flask_provide_driver(bq_cfg, sync=True, registry=registry)
    assert d1 is d2 is d3 is d4 is d5

    s1 = provide_driver(sp_cfg, sync=True, registry=registry)
    s2 = litestar_provide_driver(sp_cfg, sync=True, registry=registry)
    s3 = fastapi_provide_driver(sp_cfg, sync=True, registry=registry)
    s4 = starlette_provide_driver(sp_cfg, sync=True, registry=registry)
    s5 = flask_provide_driver(sp_cfg, sync=True, registry=registry)
    assert s1 is s2 is s3 is s4 is s5


@pytest.mark.parametrize("ext_mod", [vertebra_db, litestar_ext, fastapi_ext, starlette_ext, flask_ext])
def test_legacy_gcp_provider_helpers_removed(ext_mod: Any) -> None:
    """Engine-specific provide_*_driver helpers remain absent across db and host-framework extensions."""
    assert not hasattr(ext_mod, "provide_bigquery_driver")
    assert not hasattr(ext_mod, "provide_spanner_driver")


class _BigQueryToolClient:
    """BigQuery wire port recording requests made by the real adapter."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, str, int]] = []

    def dry_run(self, sql: str, parameters: Any = None) -> dict[str, Any]:
        """Return wire-format cost statistics without executing a job."""
        self.requests.append(("dry_run", sql, threading.get_ident()))
        processed = 5_000 if "LARGE_SCAN" in sql else 256
        return {
            "total_bytes_processed": processed,
            "total_bytes_billed": max(processed, 1024),
            "referenced_tables": ["analytics-proj.warehouse.events"],
            "schema": [{"name": "val", "type": "INT64"}],
        }

    def query(self, sql: str, parameters: Any = None, *options: Any) -> dict[str, Any]:
        """Return tabular wire data or a DML affected-row count."""
        self.requests.append(("query", sql, threading.get_ident()))
        if "FAIL_QUERY" in sql:
            raise RuntimeError("BigQuery query failed")
        if "uri, content_type" in sql:
            return {
                "columns": ["uri", "content_type", "size"],
                "rows": [["gs://warehouse/invoices/inv-001.pdf", "application/pdf", 4096]],
            }
        if "DELETE" in sql:
            return {"num_dml_affected_rows": 3}
        return {"columns": ["val"], "rows": [[1]], "total_rows": 1}


class _AsyncBigQueryToolClient:
    """Async BigQuery wire port sharing the same request contract."""

    def __init__(self, client: _BigQueryToolClient) -> None:
        self.client = client

    async def dry_run(self, sql: str, parameters: Any = None) -> dict[str, Any]:
        """Return async dry-run wire statistics."""
        return self.client.dry_run(sql, parameters)

    async def query(self, sql: str, parameters: Any = None, *options: Any) -> dict[str, Any]:
        """Return async query wire data."""
        return self.client.query(sql, parameters, *options)


class _SpannerToolClient(FakeSpannerClient):
    """Spanner wire port recording read-only snapshot bounds and queries."""

    def __init__(self) -> None:
        super().__init__()
        self.queries: list[tuple[str, str | None, str, int, int]] = []

    def query(
        self, sql: str, params_json: str | None = None, staleness_mode: str = "strong", staleness_ms: int = 0
    ) -> dict[str, Any]:
        """Record snapshot options before executing the SQL on the wire double."""
        self.queries.append((sql, params_json, staleness_mode, staleness_ms, threading.get_ident()))
        return super().query(sql, params_json, staleness_mode, staleness_ms)


class _AsyncSpannerToolClient:
    """Async Spanner wire port backed by the same deterministic database."""

    def __init__(self, client: _SpannerToolClient) -> None:
        self.client = client

    async def query(
        self, sql: str, params_json: str | None = None, staleness_mode: str = "strong", staleness_ms: int = 0
    ) -> dict[str, Any]:
        """Execute an async read-only request."""
        return self.client.query(sql, params_json, staleness_mode, staleness_ms)

    async def execute_dml(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> int:
        """Execute confirmed DML."""
        return self.client.execute_dml(sql, params_json, request_tag, transaction_tag)

    async def update_ddl(self, statements: Any) -> str:
        """Apply confirmed DDL."""
        return self.client.update_ddl(statements)


@pytest.mark.anyio
async def test_governed_mcp_and_a2a_gcp_database_tools() -> None:
    """Verify governed BigQuery and Spanner MCP tools, cost ceilings, and ConfirmationGate enforcement."""
    gate = ConfirmationGate()
    bq_driver = SyncBigQueryDriver(
        BigQueryConfig(
            project_id="analytics-proj",
            dataset_id="warehouse",
            rest_endpoint="http://localhost:9050",
            maximum_bytes_billed=1_000,
        ),
        client=_BigQueryToolClient(),
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
    tools = build_tools(BigQueryToolProvider(bq_driver, maximum_bytes_billed=1_000), confirmation_gate=gate)
    tools.update(
        build_tools(SpannerToolProvider(spanner_driver, read_only_staleness_seconds=5.0), confirmation_gate=gate)
    )
    assert list(tools.keys()) == [
        "bigquery_dry_run",
        "bigquery_query",
        "bigquery_object_table_inspect",
        "spanner_query",
        "spanner_schema_inspect",
    ]

    dry_run_res = await tools["bigquery_dry_run"].invoke({"sql": "SELECT 1"})
    assert dry_run_res["statement_type"] == "SELECT"
    assert dry_run_res["within_budget"] is True

    select_res = await tools["bigquery_query"].invoke({"sql": "SELECT 1"})
    assert select_res["mode"] == "query"

    with pytest.raises(PermissionDeniedError, match="maximum_bytes_billed"):
        await tools["bigquery_query"].invoke({"sql": "SELECT 1", "max_bytes_billed": 0})

    with pytest.raises(PermissionDeniedError, match="exceeds maximum_bytes_billed"):
        await tools["bigquery_query"].invoke({"sql": "SELECT * FROM LARGE_SCAN"})

    obj_inspect = await tools["bigquery_object_table_inspect"].invoke(
        {"table_name": "warehouse.invoices_obj", "connection_id": "us.gcs-conn"}
    )
    assert obj_inspect["row_count"] == 1
    assert "CREATE EXTERNAL TABLE" in obj_inspect["ddl"]

    mutate_sql = "DELETE FROM orders WHERE order_id = 42"
    with pytest.raises(PermissionDeniedError, match="ConfirmationGate") as exc_info:
        await tools["bigquery_query"].invoke({"sql": mutate_sql})
    err_text = str(exc_info.value)
    token = err_text.split("ticket=", 1)[1].rstrip(")")

    gate.approve(token)
    confirmed_bq = await tools["bigquery_query"].invoke({"sql": mutate_sql, "confirmation_token": token})
    assert confirmed_bq["mode"] == "dml"
    assert confirmed_bq["affected_rows"] == 3

    sp_select = await tools["spanner_query"].invoke({"sql": "SELECT 1"})
    assert sp_select["mode"] == "read_only_snapshot"

    with pytest.raises(PermissionDeniedError, match="ConfirmationGate") as sp_exc:
        await tools["spanner_query"].invoke({"sql": "UPDATE orders SET status = 'PAID' WHERE id = 1"})
    sp_token = str(sp_exc.value).split("ticket=", 1)[1].rstrip(")")
    gate.approve(sp_token)
    sp_confirmed = await tools["spanner_query"].invoke(
        {"sql": "UPDATE orders SET status = 'PAID' WHERE id = 1", "confirmation_token": sp_token}
    )
    assert sp_confirmed["mode"] == "dml"

    schema_res = await tools["spanner_schema_inspect"].invoke({"table_name": "orders"})
    assert schema_res["table_name"] == "orders"


@pytest.fixture
def gcp_database_tools() -> dict[str, Any]:
    """Governed GCP database tools backed by the stub BigQuery driver and the fake Spanner client."""
    bq_driver = SyncBigQueryDriver(
        BigQueryConfig(
            project_id="analytics-proj",
            dataset_id="warehouse",
            rest_endpoint="http://localhost:9050",
            maximum_bytes_billed=1_000,
        ),
        client=_BigQueryToolClient(),
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
    return build_tools(BigQueryToolProvider(bq_driver, maximum_bytes_billed=1_000)) | build_tools(
        SpannerToolProvider(spanner_driver)
    )


@pytest.mark.parametrize(
    ("provider_cls", "op_name"),
    [(BigQueryToolProvider, "bigquery_query"), (SpannerToolProvider, "spanner_query")],
)
@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT * FROM users", False),
        ("WITH cte AS (SELECT 1) SELECT * FROM cte", True),
        ("WITH cte AS (SELECT 1) DELETE FROM users", True),
        ("EXPLAIN SELECT * FROM users", True),
        ("SELECT 1;", False),
        ("/* audit */ SELECT 1", False),
        ("INSERT INTO users (id) VALUES (1)", True),
        ("DROP TABLE users", True),
        ("EXECUTE IMMEDIATE 'DELETE FROM users WHERE TRUE'", True),
        ("CALL warehouse.cleanup()", True),
        ("GRANT `roles/bigquery.dataViewer` ON TABLE warehouse.t TO 'user:a@example.com'", True),
        ("REVOKE SELECT ON TABLE orders FROM ROLE analyst", True),
        ("EXPORT DATA OPTIONS (uri = 'gs://bucket/*.csv', format = 'CSV') AS SELECT * FROM t", True),
        ("LOAD DATA INTO warehouse.t FROM FILES (format = 'CSV', uris = ['gs://bucket/a.csv'])", True),
    ],
)
def test_provider_classifies_confirmation(
    provider_cls: type[BigQueryToolProvider | SpannerToolProvider],
    op_name: str,
    sql: str,
    expected: bool,
) -> None:
    """Providers classify simple reads separately from executable statements needing approval."""
    assert bool(provider_cls().confirmation_reason(op_name, {"sql": sql})) is expected


@pytest.mark.parametrize(
    ("provider_cls", "op_name"),
    [
        (BigQueryToolProvider, "bigquery_query"),
        (BigQueryToolProvider, "bigquery_dry_run"),
        (SpannerToolProvider, "spanner_query"),
    ],
)
@pytest.mark.parametrize(
    "arguments",
    [
        {},
        {"sql": None},
        {"sql": 42},
        {"sql": ""},
        {"sql": "   \n\t "},
        {"sql": "/* comment only */"},
        {"sql": "-- line comment\n"},
    ],
)
def test_provider_confirmation_reason_ignores_missing_or_blank_sql(
    provider_cls: type[BigQueryToolProvider | SpannerToolProvider],
    op_name: str,
    arguments: dict[str, Any],
) -> None:
    """Missing, non-string, blank, or comment-only SQL does not trigger a spurious confirmation prompt."""
    assert provider_cls().confirmation_reason(op_name, arguments) is None


@pytest.mark.parametrize(
    ("provider_cls", "op_name"),
    [(BigQueryToolProvider, "bigquery_query"), (SpannerToolProvider, "spanner_query")],
)
@pytest.mark.parametrize(
    "sql",
    ["SELECT 1; DELETE FROM orders", "SELECT 1; -- done", "SELECT 'a;b' AS v"],
    ids=["second-statement", "trailing-comment", "semicolon-in-literal"],
)
def test_provider_rejects_non_trailing_semicolons(
    provider_cls: type[BigQueryToolProvider | SpannerToolProvider],
    op_name: str,
    sql: str,
) -> None:
    """Any semicolon before the trailing terminator is rejected, so no second statement can hide."""
    with pytest.raises(PermissionError, match="Multi-statement SQL is not allowed"):
        provider_cls().confirmation_reason(op_name, {"sql": sql})


@pytest.mark.anyio
@pytest.mark.parametrize(
    "table_name",
    [
        "warehouse.invoices_obj LIMIT 1; DROP TABLE x",
        "warehouse.`obj`",
        "1warehouse.obj",
        "bad-dataset.invoices_obj",
    ],
)
async def test_bigquery_object_table_inspect_rejects_invalid_table_name(
    gcp_database_tools: dict[str, Any],
    table_name: str,
) -> None:
    """bigquery_object_table_inspect rejects table names that are not valid BigQuery qualified identifiers."""
    with pytest.raises(ValueError, match="table_name"):
        await gcp_database_tools["bigquery_object_table_inspect"].invoke({"table_name": table_name})


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("table_name", "expected_quoted"),
    [
        ("analytics-proj.warehouse.invoices_obj", "`analytics-proj`.`warehouse`.`invoices_obj`"),
        ("warehouse.invoices-obj", "`warehouse`.`invoices-obj`"),
    ],
)
async def test_bigquery_object_table_inspect_accepts_hyphenated_project_and_table_names(
    gcp_database_tools: dict[str, Any],
    table_name: str,
    expected_quoted: str,
) -> None:
    """Hyphenated project and table IDs are backtick-quoted in generated Object Table SQL and DDL."""
    result = await gcp_database_tools["bigquery_object_table_inspect"].invoke({"table_name": table_name})
    assert result["row_count"] == 1
    assert f"CREATE EXTERNAL TABLE IF NOT EXISTS {expected_quoted}" in result["ddl"]


@pytest.mark.anyio
async def test_bigquery_object_table_inspect_rejects_invalid_connection_id(
    gcp_database_tools: dict[str, Any],
) -> None:
    """bigquery_object_table_inspect rejects connection identifiers containing backticks or SQL syntax."""
    with pytest.raises(ValueError, match="connection_id"):
        await gcp_database_tools["bigquery_object_table_inspect"].invoke(
            {
                "table_name": "warehouse.invoices_obj",
                "connection_id": "us.gcs-conn`; DROP TABLE x",
            }
        )


@pytest.mark.anyio
async def test_spanner_query_rejects_multiple_statements(gcp_database_tools: dict[str, Any]) -> None:
    """spanner_query rejects a SELECT followed by a second statement."""
    with pytest.raises(PermissionDeniedError, match="Multi-statement SQL is not allowed"):
        await gcp_database_tools["spanner_query"].invoke({"sql": "SELECT 1; DELETE FROM orders WHERE id = 1"})


@pytest.mark.anyio
@pytest.mark.parametrize(
    "sql",
    ["GRANT SELECT ON TABLE orders TO ROLE analyst", "CALL cancel_query('12345')"],
)
async def test_spanner_query_requires_confirmation_for_privilege_and_procedure_calls(
    gcp_database_tools: dict[str, Any],
    sql: str,
) -> None:
    """GRANT and CALL statements are gated by the ConfirmationGate."""
    with pytest.raises(PermissionDeniedError, match="ConfirmationGate"):
        await gcp_database_tools["spanner_query"].invoke({"sql": sql})


@pytest.mark.parametrize(
    "scheme",
    [
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
    ],
)
def test_gcp_sqlalchemy_dialect_schemes_registered(scheme: str) -> None:
    """All sync and async BigQuery, Spanner, and Spangres SQLAlchemy schemes are registered."""
    assert scheme in ALL_DIALECT_SCHEMES
    assert scheme in DIALECT_DESCRIPTORS


def test_sqlalchemy_vtb_dialects_types_and_alembic_batching() -> None:
    """Verify SQLAlchemy 2.1 +vtb dialects, Vector/BinaryJSON types, CreateVectorIndexOp, and SpannerAlembicImpl."""

    assert SyncBigQueryDialect.paramstyle == "named"
    assert AsyncBigQueryDialect.is_async is True
    merge_sql = SyncBigQueryDialect.compile_merge_upsert(
        "analytics.events",
        "analytics.staging_events",
        on_keys=("event_id",),
        update_columns=("payload", "updated_at"),
    )
    assert "MERGE INTO analytics.events AS T USING analytics.staging_events AS S" in merge_sql
    assert "T.event_id = S.event_id" in merge_sql
    assert "WHEN MATCHED THEN UPDATE SET T.payload = S.payload" in merge_sql

    assert SyncSpannerDialect.paramstyle == "named"
    assert AsyncSpannerDialect.is_async is True
    upsert_sql = SyncSpannerDialect.compile_insert_or_update(
        "Singers",
        ("SingerId", "FirstName", "LastName"),
    )
    assert (
        upsert_sql
        == "INSERT OR UPDATE INTO Singers (SingerId, FirstName, LastName) VALUES (@SingerId, @FirstName, @LastName)"
    )
    interleave_sql = SyncSpannerDialect.compile_interleave_clause(
        "Singers",
        on_delete="CASCADE",
    )
    assert interleave_sql == ", INTERLEAVE IN PARENT Singers ON DELETE CASCADE"

    assert SyncSpangresDialect.paramstyle == "pyformat"
    assert AsyncSpangresDialect.is_async is True

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
    lro_result = sp_alembic.flush_ddl_batch_sync(sp_driver)
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


@pytest.mark.parametrize(
    "legacy_symbol",
    [
        "GCPDatabaseToolPolicy",
        "build_bigquery_tools",
        "build_spanner_tools",
        "is_mutating_sql",
        "register_gcp_database_tools",
    ],
)
def test_legacy_agent_gcp_exports_removed(legacy_symbol: str) -> None:
    """Legacy GCP-specific symbols remain absent from vertebra.agent and vertebra.agent.tools."""
    assert not hasattr(vertebra_agent, legacy_symbol)
    assert not hasattr(vertebra_agent.tools, legacy_symbol)


@pytest.mark.parametrize(
    "module_name",
    ["vertebra.db.mcp_tools", "vertebra.agent.tools.bigquery", "vertebra.agent.tools.spanner"],
)
def test_legacy_gcp_tool_modules_removed(module_name: str) -> None:
    """Removed database MCP tool modules raise ModuleNotFoundError."""
    with pytest.raises(ModuleNotFoundError, match=module_name):
        importlib.import_module(module_name)


def test_agent_tool_exports_and_removed_database_module() -> None:
    """Agent package exports resolve to the tools package without adapter imports."""
    assert vertebra_agent.build_tools is build_tools
    source = Path(vertebra_agent.tools.__file__).read_text()
    imports = [node for node in ast.walk(ast.parse(source)) if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert not any("vertebra.db.adapters" in ast.unparse(node) for node in imports)


@pytest.mark.anyio
@pytest.mark.parametrize("engine", ["bigquery", "spanner"])
@pytest.mark.parametrize("target_kind", ["alias", "config"])
async def test_agent_tools_resolve_registry_at_invocation(
    monkeypatch: pytest.MonkeyPatch, engine: str, target_kind: str
) -> None:
    """Real registry resolution stays lazy and uses native adapter client ports."""
    registry = DatabaseRegistry()
    bq_client = _BigQueryToolClient()
    sp_client = _SpannerToolClient()
    constructors: list[dict[str, Any]] = []
    resolutions: list[Any] = []

    def resolve(config: Any, *, registry: Any) -> Any:
        resolutions.append(config)
        return provide_driver(config, registry=registry)

    monkeypatch.setattr(importlib.import_module(f"vertebra.db.adapters.{engine}.tools"), "provide_driver", resolve)

    def make_bq_client(**options: Any) -> _AsyncBigQueryToolClient:
        constructors.append(options)
        return _AsyncBigQueryToolClient(bq_client)

    def make_sp_client(**options: Any) -> _AsyncSpannerToolClient:
        constructors.append(options)
        return _AsyncSpannerToolClient(sp_client)

    monkeypatch.setattr(bigquery_native, "AsyncBigQueryClient", make_bq_client)
    monkeypatch.setattr(spanner_native, "AsyncSpannerClient", make_sp_client)
    config = (
        BigQueryConfig(project_id="analytics-proj", dataset_id="warehouse")
        if engine == "bigquery"
        else SpannerConfig(project_id="core-proj", instance_id="prod-inst", database_id="orders-db")
    )
    provider_type = BigQueryToolProvider if engine == "bigquery" else SpannerToolProvider
    provider = provider_type(config="analytics" if target_kind == "alias" else config, registry=registry)
    assert isinstance(provider, ToolProvider)
    assert provider.operations()
    tools = build_tools(provider)
    with pytest.raises(PermissionDeniedError):
        await tools[f"{engine}_query"].invoke({"sql": "DELETE FROM orders"})
    with pytest.raises(vertebra_agent.ToolArgumentError):
        await tools[f"{engine}_query"].invoke({"sql": 42})
    assert constructors == []
    assert resolutions == []
    assert bq_client.requests == []
    assert sp_client.queries == []
    registry.register("analytics", config)
    result = await tools[f"{engine}_query"].invoke({"sql": "SELECT 1"})
    assert result["row_count"] == 1
    assert result["rows"] == ([{"val": 1}] if engine == "bigquery" else [{"1": 1}])
    assert len(constructors) == 1
    assert resolutions == ["analytics" if target_kind == "alias" else config]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_agent_tools_dispatch_and_snapshot_scope(asynchronous: bool) -> None:
    """Sync calls leave the event loop and Spanner schema reads stay in bounded snapshots."""
    bq_client = _BigQueryToolClient()
    sp_client = _SpannerToolClient()
    bq_config = BigQueryConfig(project_id="analytics-proj", dataset_id="warehouse")
    sp_config = SpannerConfig(project_id="core-proj", instance_id="prod-inst", database_id="orders-db")
    bq_driver = (
        AsyncBigQueryDriver(bq_config, client=_AsyncBigQueryToolClient(bq_client))
        if asynchronous
        else SyncBigQueryDriver(bq_config, client=bq_client)
    )
    sp_driver = (
        AsyncSpannerDriver(sp_config, client=_AsyncSpannerToolClient(sp_client))
        if asynchronous
        else SyncSpannerDriver(sp_config, client=sp_client)
    )
    bq_tools = build_tools(BigQueryToolProvider(bq_driver))
    sp_tools = build_tools(SpannerToolProvider(sp_driver, read_only_staleness_seconds=2.5))
    await bq_tools["bigquery_query"].invoke({"sql": "SELECT 1"})
    await sp_tools["spanner_query"].invoke({"sql": "SELECT 1", "staleness_seconds": 1.25})
    result = await sp_tools["spanner_schema_inspect"].invoke({"table_name": "orders"})
    assert result == {"table_name": "orders", "tables": [], "columns": []}
    assert len(sp_client.queries) == 3
    assert sp_client.queries[0][2:4] == ("exact_staleness", 1250)
    tables_query, cols_query = sp_client.queries[1:]
    assert "FROM information_schema.tables" in tables_query[0]
    assert "WHERE table_name = @table_name" in tables_query[0]
    assert "orders" not in tables_query[0]
    assert tables_query[1] is not None and "orders" in tables_query[1]
    assert tables_query[2:4] == ("exact_staleness", 2500)
    assert (tables_query[4] == threading.get_ident()) is asynchronous
    assert "FROM information_schema.columns" in cols_query[0]
    assert "WHERE table_name = @table_name" in cols_query[0]
    assert "orders" not in cols_query[0]
    assert cols_query[1] is not None and "orders" in cols_query[1]
    assert cols_query[2:4] == ("exact_staleness", 2500)
    assert (cols_query[4] == threading.get_ident()) is asynchronous
    assert all((request[2] == threading.get_ident()) is asynchronous for request in bq_client.requests)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_spanner_schema_inspect_spangres_dialect_uses_positional_binds(asynchronous: bool) -> None:
    """Spanner schema inspection under dialect='spangres' filters table_schema='public' and binds $1."""
    sp_client = _SpannerToolClient()
    sp_config = SpannerConfig(
        project_id="core-proj",
        instance_id="prod-inst",
        database_id="orders-db",
        dialect="spangres",
    )
    sp_driver = (
        AsyncSpannerDriver(sp_config, client=_AsyncSpannerToolClient(sp_client))
        if asynchronous
        else SyncSpannerDriver(sp_config, client=sp_client)
    )
    sp_tools = build_tools(SpannerToolProvider(sp_driver, read_only_staleness_seconds=1.5))

    filtered = await sp_tools["spanner_schema_inspect"].invoke({"table_name": "orders"})
    assert filtered["table_name"] == "orders"
    tables_q, cols_q = sp_client.queries
    assert "WHERE table_schema = 'public' AND table_name = $1" in tables_q[0]
    assert "@table_name" not in tables_q[0]
    assert tables_q[1] is not None and '"orders"' in tables_q[1]
    assert "WHERE table_schema = 'public' AND table_name = $1" in cols_q[0]
    assert "@table_name" not in cols_q[0]

    sp_client.queries.clear()
    unfiltered = await sp_tools["spanner_schema_inspect"].invoke({})
    assert unfiltered["table_name"] is None
    all_tables_q, all_cols_q = sp_client.queries
    assert "WHERE table_schema = 'public'" in all_tables_q[0]
    assert "$1" not in all_tables_q[0]
    assert all_tables_q[1] is None
    assert "WHERE table_schema = 'public'" in all_cols_q[0]
    assert all_cols_q[1] is None


@pytest.mark.anyio
async def test_agent_tools_register_single_engine_without_default_config() -> None:
    """Single-engine tools register on a real MCP registry without resolving the other driver."""
    registry = MCPRegistry()
    driver = SyncBigQueryDriver(BigQueryConfig(project_id="analytics-proj"), client=_BigQueryToolClient())
    tools = build_tools(BigQueryToolProvider(driver, registry=DatabaseRegistry()))
    dry_run_tool, query_tool, inspect_tool = tools.values()
    registry.register_tool(dry_run_tool)
    registry.register_tool(query_tool)
    registry.register_tool(inspect_tool)
    assert all(isinstance(tool, Tool) for tool in tools.values())
    assert {tool.name for tool in registry.list_tools()} == set(tools)
    assert (await tools["bigquery_query"].invoke({"sql": "SELECT 1"}))["rows"] == [{"val": 1}]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_agent_tool_guards_and_driver_errors(asynchronous: bool) -> None:
    """Governance blocks wire execution and adapter failures reach the tool caller."""
    client = _BigQueryToolClient()
    config = BigQueryConfig(project_id="analytics-proj")
    driver = (
        AsyncBigQueryDriver(config, client=_AsyncBigQueryToolClient(client))
        if asynchronous
        else SyncBigQueryDriver(config, client=client)
    )
    gate = ConfirmationGate()
    tools = build_tools(BigQueryToolProvider(driver, maximum_bytes_billed=1_000), confirmation_gate=gate)
    with pytest.raises(PermissionDeniedError, match="exceeds maximum_bytes_billed"):
        await tools["bigquery_query"].invoke({"sql": "SELECT * FROM LARGE_SCAN"})
    assert [request[0] for request in client.requests] == ["dry_run"]
    with pytest.raises(PermissionDeniedError, match="ConfirmationGate") as exc:
        await tools["bigquery_query"].invoke({"sql": "DELETE FROM orders"})
    assert [request[0] for request in client.requests] == ["dry_run"]
    gate.approve(str(exc.value).split("ticket=", 1)[1].rstrip(")"))
    result = await tools["bigquery_query"].invoke(
        {"sql": "DELETE FROM orders", "confirmation_token": str(exc.value).split("ticket=", 1)[1].rstrip(")")}
    )
    assert result["affected_rows"] == 3
    with pytest.raises(RuntimeError, match="BigQuery query failed"):
        await tools["bigquery_query"].invoke({"sql": "SELECT * FROM FAIL_QUERY"})
    client.requests.clear()
    tools = build_tools(BigQueryToolProvider(driver, enforce_dry_run_limit=False))
    result = await tools["bigquery_query"].invoke({"sql": "SELECT 1"})
    assert result["estimated_bytes_processed"] == 0
    assert [request[0] for request in client.requests] == ["query"]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_spanner_tools_confirm_ddl_and_dml(asynchronous: bool) -> None:
    """Confirmed Spanner operations change the wire-port database through the real adapter."""
    client = _SpannerToolClient()
    config = SpannerConfig(project_id="core-proj", instance_id="prod-inst", database_id="orders-db")
    driver = (
        AsyncSpannerDriver(config, client=_AsyncSpannerToolClient(client))
        if asynchronous
        else SyncSpannerDriver(config, client=client)
    )
    gate = ConfirmationGate()
    tools = build_tools(SpannerToolProvider(driver), confirmation_gate=gate)

    ddl_sql = "CREATE TABLE orders (id INT64 NOT NULL, status STRING(32)) PRIMARY KEY (id)"
    with pytest.raises(PermissionDeniedError, match="ConfirmationGate") as ddl_exc:
        await tools["spanner_query"].invoke({"sql": ddl_sql})
    ddl_token = str(ddl_exc.value).split("ticket=", 1)[1].rstrip(")")
    gate.approve(ddl_token)
    ddl_result = await tools["spanner_query"].invoke({"sql": ddl_sql, "confirmation_token": ddl_token})
    assert ddl_result["mode"] == "ddl"
    assert ddl_result["operation"] == "operations/ddl-1"

    dml_sql = "INSERT INTO orders (id, status) VALUES (1, 'PAID')"
    with pytest.raises(PermissionDeniedError, match="ConfirmationGate") as dml_exc:
        await tools["spanner_query"].invoke({"sql": dml_sql})
    dml_token = str(dml_exc.value).split("ticket=", 1)[1].rstrip(")")
    gate.approve(dml_token)
    dml_result = await tools["spanner_query"].invoke({"sql": dml_sql, "confirmation_token": dml_token})
    assert dml_result["mode"] == "dml"
    assert dml_result["affected_rows"] == 1

    result = await tools["spanner_query"].invoke({"sql": "SELECT id, status FROM orders"})
    assert result["rows"] == [{"id": 1, "status": "PAID"}]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("engine", ["bigquery", "spanner"])
async def test_provider_approved_read_cte_retains_native_query_routing(
    asynchronous: bool,
    engine: str,
) -> None:
    """Ambiguous CTE SQL requires approval and keeps the native row-returning path."""
    if engine == "bigquery":
        bq_client = _BigQueryToolClient()
        bq_config = BigQueryConfig(project_id="analytics-proj")
        bq_driver = (
            AsyncBigQueryDriver(bq_config, client=_AsyncBigQueryToolClient(bq_client))
            if asynchronous
            else SyncBigQueryDriver(bq_config, client=bq_client)
        )
        provider: BigQueryToolProvider | SpannerToolProvider = BigQueryToolProvider(
            bq_driver, enforce_dry_run_limit=False
        )
        name = "bigquery_query"
        client_calls: list[Any] = bq_client.requests
    else:
        sp_client = _SpannerToolClient()
        sp_config = SpannerConfig(project_id="core-proj", instance_id="prod-inst", database_id="orders-db")
        sp_driver = (
            AsyncSpannerDriver(sp_config, client=_AsyncSpannerToolClient(sp_client))
            if asynchronous
            else SyncSpannerDriver(sp_config, client=sp_client)
        )
        provider = SpannerToolProvider(sp_driver)
        name = "spanner_query"
        client_calls = sp_client.queries

    gate = ConfirmationGate()
    tool = build_tools(provider, confirmation_gate=gate)[name]
    sql = "WITH record AS (SELECT 1 AS val) SELECT val FROM record"
    with pytest.raises(PermissionDeniedError) as exc:
        await tool.invoke({"sql": sql})
    assert client_calls == []
    token = str(exc.value).split("ticket=", 1)[1].rstrip(")")
    gate.approve(token)
    result = await tool.invoke({"sql": sql, "confirmation_token": token})
    assert result["mode"] in {"query", "read_only_snapshot"}
    assert "rows" in result
    assert len(client_calls) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "failure_factory",
    [
        lambda: DatabaseError("database"),
        lambda: PermissionError("wire permission"),
        lambda: RuntimeError("wire"),
        asyncio.CancelledError,
        lambda: ToolPolicyError("explicit policy"),
    ],
    ids=["database-error", "permission-error", "runtime-error", "cancelled-error", "tool-policy-error"],
)
async def test_native_provider_preserves_driver_exception_identity(
    asynchronous: bool,
    failure_factory: Any,
) -> None:
    """Only explicit adapter policy refusals translate across the generic consumer."""

    class FailingClient(_BigQueryToolClient):
        def __init__(self, failure: BaseException) -> None:
            super().__init__()
            self.failure = failure

        def query(self, sql: str, parameters: Any = None, *options: Any) -> dict[str, Any]:
            raise self.failure

    failure = failure_factory()
    client = FailingClient(failure)
    config = BigQueryConfig(project_id="analytics-proj")
    driver = (
        AsyncBigQueryDriver(config, client=_AsyncBigQueryToolClient(client))
        if asynchronous
        else SyncBigQueryDriver(config, client=client)
    )
    tool = build_tools(BigQueryToolProvider(driver, enforce_dry_run_limit=False))["bigquery_query"]
    expected = PermissionDeniedError if isinstance(failure, ToolPolicyError) else type(failure)
    with pytest.raises(expected) as exc:
        await tool.invoke({"sql": "SELECT 1"})
    if isinstance(failure, ToolPolicyError):
        assert exc.value.__cause__ is failure
    else:
        assert exc.value is failure
