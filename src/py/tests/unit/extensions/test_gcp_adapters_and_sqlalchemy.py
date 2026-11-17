"""Unit tests for GCP framework adapters, FastPath streaming, governed MCP/A2A tools, and secondary SQLAlchemy +vtb dialects."""

from __future__ import annotations

import ast
import asyncio
import importlib
import importlib.util
import io
import json
import sys
import threading
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from alembic.ddl.impl import DefaultImpl
from django.db.backends.base.base import BaseDatabaseWrapper
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, insert, select
from sqlalchemy import exc as sa_exc
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import CreateTable

import vertebra._bigquery as bigquery_native
import vertebra._spanner as spanner_native
import vertebra.agent as vertebra_agent
import vertebra.db as vertebra_db
import vertebra.extensions.fastapi as fastapi_ext
import vertebra.extensions.flask as flask_ext
import vertebra.extensions.litestar as litestar_ext
import vertebra.extensions.starlette as starlette_ext
from tests.unit.db.test_spanner import FakeAsyncReadWriteTransaction, FakeSpannerClient
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
from vertebra.db.dbapi import AsyncDBAPIConnection, SyncDBAPIConnection
from vertebra.db.driver import DatabaseError
from vertebra.extensions.alchemy import (
    create_async_alchemy_config,
    create_sync_alchemy_config,
    normalize_alchemy_url,
)
from vertebra.extensions.broker.transport import AsyncSHMBrokerTransport, BrokerConfig, SyncSHMBrokerTransport
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
    _execute_direct_signature_plan,
)
from vertebra.extensions.sqlalchemy import (
    ALL_DIALECT_SCHEMES,
    DIALECT_DESCRIPTORS,
    BigQueryAlembicImpl,
    CreateVectorIndexOp,
    MsgspecJSON,
    SpangresAlembicImpl,
    SpannerAlembicImpl,
    Vector,
)
from vertebra.extensions.sqlalchemy import dialects as sqlalchemy_dialects_mod
from vertebra.extensions.sqlalchemy.pool import AsyncBrokerPool, BrokerPool
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
from vertebra.server._fastpath_core import DirectPlan
from vertebra.storage import StreamingStorageResponse, SyncObjectStore
from vertebra.typing import (
    ImproperConfigurationError,
    MissingDependencyError,
    ToolPolicyError,
    ToolProvider,
    reset_dependency_cache,
)

_HAS_SQLALCHEMY_GCP = (
    importlib.util.find_spec("sqlalchemy_bigquery") is not None
    and importlib.util.find_spec("google.cloud.sqlalchemy_spanner") is not None
)
if _HAS_SQLALCHEMY_GCP:
    from vertebra.extensions.sqlalchemy import (
        AsyncBigQueryDialect as AsyncBigQueryDialect,
    )
    from vertebra.extensions.sqlalchemy import (
        AsyncSpangresDialect as AsyncSpangresDialect,
    )
    from vertebra.extensions.sqlalchemy import (
        AsyncSpannerDialect as AsyncSpannerDialect,
    )
    from vertebra.extensions.sqlalchemy import (
        SyncBigQueryDialect as SyncBigQueryDialect,
    )
    from vertebra.extensions.sqlalchemy import (
        SyncSpangresDialect as SyncSpangresDialect,
    )
    from vertebra.extensions.sqlalchemy import (
        SyncSpannerDialect as SyncSpannerDialect,
    )
else:
    AsyncBigQueryDialect = cast("Any", None)
    AsyncSpangresDialect = cast("Any", None)
    AsyncSpannerDialect = cast("Any", None)
    SyncBigQueryDialect = cast("Any", None)
    SyncSpangresDialect = cast("Any", None)
    SyncSpannerDialect = cast("Any", None)


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
    assert (b"etag", b'"etag-123"') in direct_tuple[1]
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

    ls_plan = DirectPlan(
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


def test_django_backends_and_shared_provider_fingerprinting(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify Django BigQuery and Spanner DatabaseWrappers and multi-framework provider sharing."""
    registry = DatabaseRegistry(broker=DatabaseRegistry.default_broker())

    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("vertebra.extensions.django.db.backends.bigquery")

    monkeypatch.setitem(sys.modules, "django_spanner", None)
    monkeypatch.setitem(sys.modules, "django_spanner.base", None)
    reset_dependency_cache("django_spanner.base")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            SpannerDjangoDatabaseWrapper(
                {
                    "PROJECT": "core-proj",
                    "INSTANCE": "prod-inst",
                    "NAME": "ledger-db",
                    "DIALECT": "POSTGRESQL",
                }
            )
        assert "django-google-spanner" in str(exc_info.value)
        assert "django-spanner" in exc_info.value.hint
    finally:
        reset_dependency_cache("django_spanner.base")

    monkeypatch.setattr(
        "vertebra.extensions.django.db.backends.spanner.base.ensure_module",
        lambda *args, **kwargs: None,
    )
    spanner_wrapper = SpannerDjangoDatabaseWrapper(
        {
            "PROJECT": "core-proj",
            "INSTANCE": "prod-inst",
            "NAME": "ledger-db",
            "DIALECT": "POSTGRESQL",
        }
    )
    assert isinstance(spanner_wrapper, BaseDatabaseWrapper)
    assert spanner_wrapper.vendor == "spanner"
    assert spanner_wrapper.display_name == "Cloud Spanner (Vertebra)"
    assert spanner_wrapper.driver_scheme == "spanner"
    assert spanner_wrapper.Database.paramstyle == "named"
    conn_params = spanner_wrapper.get_connection_params()
    cfg = conn_params["config"]
    assert cfg.project_id == "core-proj"
    assert cfg.instance_id == "prod-inst"
    assert cfg.database_id == "ledger-db"
    assert cfg.dialect == "spangres"
    conn = spanner_wrapper.get_new_connection(conn_params)
    assert isinstance(conn.driver, SyncSpannerDriver)

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


class _ClosableFakeSpannerClient(FakeSpannerClient):
    """FakeSpannerClient implementing SyncSpannerClientProtocol.close."""

    __slots__ = ()

    def close(self) -> None:
        return None


class _BigQueryToolClient:
    """BigQuery wire port recording requests made by the real adapter."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, str, int]] = []

    def dry_run(self, sql: str, params_json: str | None = None) -> dict[str, Any]:
        """Return wire-format cost statistics without executing a job."""
        _ = params_json
        self.requests.append(("dry_run", sql, threading.get_ident()))
        processed = 5_000 if "LARGE_SCAN" in sql else 256
        return {
            "total_bytes_processed": processed,
            "total_bytes_billed": max(processed, 1024),
            "referenced_tables": ["analytics-proj.warehouse.events"],
            "schema": [{"name": "val", "type": "INT64"}],
        }

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
        """Return tabular wire data or a DML affected-row count."""
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
        """Unsupported stub satisfying SyncBigQueryClientProtocol."""
        _ = (project_id, dataset_id, table_id, selected_fields, row_restriction, max_streams, compression)
        raise NotImplementedError

    def append_arrow(
        self,
        parent_table: str,
        arrow_ipc_bytes: bytes,
        mode: str = "_default",
    ) -> dict[str, Any]:
        """Unsupported stub satisfying SyncBigQueryClientProtocol."""
        _ = (parent_table, arrow_ipc_bytes, mode)
        raise NotImplementedError

    def close(self) -> None:
        """Close the wire double."""
        return None


class _AsyncBigQueryToolClient:
    """Async BigQuery wire port sharing the same request contract."""

    def __init__(self, client: _BigQueryToolClient) -> None:
        self.client = client

    async def dry_run(self, sql: str, params_json: str | None = None) -> dict[str, Any]:
        """Return async dry-run wire statistics."""
        return self.client.dry_run(sql, params_json)

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
        """Return async query wire data."""
        return self.client.query(
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
        """Unsupported stub satisfying AsyncBigQueryClientProtocol."""
        return self.client.read_table_arrow(
            project_id, dataset_id, table_id, selected_fields, row_restriction, max_streams, compression
        )

    async def append_arrow(
        self,
        parent_table: str,
        arrow_ipc_bytes: bytes,
        mode: str = "_default",
    ) -> dict[str, Any]:
        """Unsupported stub satisfying AsyncBigQueryClientProtocol."""
        return self.client.append_arrow(parent_table, arrow_ipc_bytes, mode)

    def close(self) -> None:
        """Close the async wire double."""
        self.client.close()


class _SpannerToolClient(_ClosableFakeSpannerClient):
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

    async def execute_batch_dml(
        self,
        statements_json: str,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> list[int]:
        """Execute batch DML."""
        return self.client.execute_batch_dml(statements_json, request_tag, transaction_tag)

    async def execute_partitioned_dml(
        self,
        sql: str,
        params_json: str | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> int:
        """Execute Partitioned DML."""
        return self.client.execute_partitioned_dml(sql, params_json, exclude_txn_from_change_streams)

    async def apply_mutations(
        self,
        mutations_json: str,
        transaction_tag: str | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> dict[str, Any]:
        """Apply mutations."""
        return self.client.apply_mutations(
            mutations_json,
            transaction_tag,
            exclude_txn_from_change_streams,
            max_commit_delay_ms,
        )

    async def batch_write(
        self,
        mutation_groups_json: str,
        exclude_txn_from_change_streams: bool = False,
    ) -> list[dict[str, Any]]:
        """Commit mutation groups."""
        return self.client.batch_write(mutation_groups_json, exclude_txn_from_change_streams)

    async def update_ddl(self, statements: Sequence[str]) -> str:
        """Apply confirmed DDL."""
        return self.client.update_ddl(statements)

    async def partitioned_query(
        self,
        sql: str,
        params_json: str | None = None,
        max_partitions: int = 4,
        data_boost_enabled: bool = False,
        staleness_mode: str = "strong",
        staleness_ms: int = 0,
    ) -> dict[str, Any]:
        """Execute a partitioned query."""
        return self.client.partitioned_query(
            sql,
            params_json,
            max_partitions,
            data_boost_enabled,
            staleness_mode,
            staleness_ms,
        )

    async def begin_read_write_transaction(
        self,
        transaction_tag: str | None = None,
        previous_transaction_id: bytes | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> FakeAsyncReadWriteTransaction:
        """Open an async interactive transaction."""
        sync_tx = self.client.begin_read_write_transaction(
            transaction_tag,
            previous_transaction_id,
            exclude_txn_from_change_streams,
            max_commit_delay_ms,
        )
        return FakeAsyncReadWriteTransaction(sync_tx)

    def close(self) -> None:
        """Close the async wire double."""
        self.client.close()


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
        client=_ClosableFakeSpannerClient(),
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
        client=_ClosableFakeSpannerClient(),
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
        ("# audit\nSELECT 1", False),
        ("# audit\nDELETE FROM users", True),
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
        {"sql": "# hash comment\n"},
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
        "warehouse.`obj",
        "warehouse.`obj; DROP TABLE x`",
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
    ("table_name", "expected_quoted", "expected_uris"),
    [
        (
            "analytics-proj.warehouse.invoices_obj",
            "`analytics-proj`.`warehouse`.`invoices_obj`",
            ["gs://analytics-proj/warehouse/invoices_obj/*"],
        ),
        (
            "warehouse.invoices-obj",
            "`warehouse`.`invoices-obj`",
            ["gs://warehouse/invoices-obj/*"],
        ),
        (
            "warehouse.`obj`",
            "`warehouse`.`obj`",
            ["gs://warehouse/obj/*"],
        ),
        (
            "google.com:corp-proj.warehouse.invoices-obj",
            "`google.com:corp-proj`.`warehouse`.`invoices-obj`",
            ["gs://google.com:corp-proj/warehouse/invoices-obj/*"],
        ),
        (
            "projects/analytics-proj/datasets/warehouse/tables/invoices-obj",
            "`analytics-proj`.`warehouse`.`invoices-obj`",
            ["gs://analytics-proj/warehouse/invoices-obj/*"],
        ),
    ],
)
async def test_bigquery_object_table_inspect_accepts_hyphenated_project_and_table_names(
    gcp_database_tools: dict[str, Any],
    table_name: str,
    expected_quoted: str,
    expected_uris: list[str],
) -> None:
    """Supported BigQuery table identifiers are normalized and backtick-quoted in Object Table SQL and DDL."""
    result = await gcp_database_tools["bigquery_object_table_inspect"].invoke({"table_name": table_name})
    assert result["row_count"] == 1
    assert result["uris"] == expected_uris
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


@pytest.mark.skipif(not _HAS_SQLALCHEMY_GCP, reason="sqlalchemy GCP dialects require Python < 3.14")
def test_sqlalchemy_vtb_dialects_types_and_alembic_batching() -> None:
    """Verify SQLAlchemy 2.1 +vtb dialects, Vector/MsgspecJSON types, CreateVectorIndexOp, and SpannerAlembicImpl."""

    assert SyncBigQueryDialect.paramstyle == "named"
    assert AsyncBigQueryDialect.is_async is True

    assert SyncSpannerDialect.paramstyle == "named"
    assert AsyncSpannerDialect.is_async is True

    assert SyncSpangresDialect.paramstyle == "numeric_dollar"
    assert AsyncSpangresDialect.is_async is True

    vec32 = Vector(dimensions=768, precision="float32", column_name="embedding")
    vec64 = Vector(dimensions=768, precision="float64", column_name="embedding")
    assert vec32.get_col_spec(dialect_name="bigquery") == "ARRAY<FLOAT64>"
    assert vec32.get_col_spec(dialect_name="spanner") == "ARRAY<FLOAT32>(vector_length=>768)"
    assert vec64.get_col_spec(dialect_name="spanner") == "ARRAY<FLOAT64>(vector_length=>768)"
    assert vec32.get_col_spec(dialect_name="spangres") == "VECTOR(768)"

    tbl = Table("Documents", MetaData(), Column("embedding", vec32))
    bq_sql = str(
        select(tbl).where(tbl.c.embedding.cosine_distance([0.1, 0.2]) < 0.5).compile(dialect=SyncBigQueryDialect())
    )
    assert "COSINE_DISTANCE(`Documents`.`embedding`," in bq_sql

    sp_dialect: Any = SyncSpannerDialect()
    sp_sql = str(select(tbl).where(tbl.c.embedding.cosine_distance([0.1, 0.2]) < 0.5).compile(dialect=sp_dialect))
    assert "COSINE_DISTANCE(`Documents`.embedding," in sp_sql

    approx_sql = str(
        select(tbl)
        .order_by(tbl.c.embedding.approx_cosine_distance([0.1, 0.2], num_leaves_to_search=10))
        .compile(dialect=sp_dialect)
    )
    assert "APPROX_COSINE_DISTANCE(`Documents`.embedding," in approx_sql
    assert "options => JSON '{\"num_leaves_to_search\":10}'" in approx_sql

    mjson = MsgspecJSON()
    assert mjson.get_col_spec(dialect_name="bigquery") == "JSON"
    assert mjson.get_col_spec(dialect_name="spanner") == "JSON"
    assert mjson.get_col_spec(dialect_name="spangres") == "JSONB"

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

    assert DefaultImpl.get_by_dialect(SyncBigQueryDialect()) is BigQueryAlembicImpl
    assert BigQueryAlembicImpl.transactional_ddl is False
    assert DefaultImpl.get_by_dialect(sp_dialect) is SpannerAlembicImpl
    spangres_dialect: Any = cast("Any", SyncSpangresDialect)()
    assert DefaultImpl.get_by_dialect(spangres_dialect) is SpangresAlembicImpl

    assert normalize_alchemy_url("bigquery://proj/dataset", is_async=True) == "bigquery+vtb_async://proj/dataset"
    assert normalize_alchemy_url("bigquery://proj/dataset", is_async=False) == "bigquery+vtb://proj/dataset"
    assert normalize_alchemy_url("spanner://proj/inst/db", is_async=True) == "spanner+vtb_async://proj/inst/db"
    assert normalize_alchemy_url("spangres://proj/inst/db", is_async=False) == "spangres+vtb://proj/inst/db"

    async_cfg = create_async_alchemy_config("bigquery://proj/dataset")
    sync_cfg = create_sync_alchemy_config("spanner://proj/inst/db")
    assert async_cfg.connection_string == "bigquery+vtb_async://proj/dataset"
    assert sync_cfg.connection_string == "spanner+vtb://proj/inst/db"


def test_spanner_alembic_impl_batches_ddl_in_exec_and_offline_sql() -> None:
    """Verify SpannerAlembicImpl._exec batches online DDL until emit_commit/DML and emits offline SQL."""
    batched_calls: list[list[str]] = []
    executed_dml: list[Any] = []

    class _RecordingSpannerClient(_ClosableFakeSpannerClient):
        def update_ddl(self, statements: Sequence[str] | str) -> str:
            stmt_list = [statements] if isinstance(statements, str) else list(statements)
            batched_calls.append(stmt_list)
            return super().update_ddl(stmt_list)

    sp_driver = SyncSpannerDriver(
        SpannerConfig(
            project_id="core-proj",
            instance_id="prod-inst",
            database_id="music-db",
            endpoint="http://localhost:9010",
        ),
        client=_RecordingSpannerClient(),
    )

    class _FakeSAConnection:
        def __init__(self, driver_obj: Any) -> None:
            self.connection = SimpleNamespace(dbapi_connection=SimpleNamespace(driver=driver_obj))

        def execution_options(self, **_kw: Any) -> _FakeSAConnection:
            return self

        def execute(self, construct: Any, params: Any = None) -> None:
            executed_dml.append((str(construct), params))

    sp_dialect: Any = SyncSpannerDialect()
    fake_conn: Any = _FakeSAConnection(sp_driver)
    online_impl = SpannerAlembicImpl(sp_dialect, fake_conn, False, False, None, {})

    meta = MetaData()
    singers = Table("Singers", meta, Column("SingerId", Integer, primary_key=True))
    albums = Table("Albums", meta, Column("AlbumId", Integer, primary_key=True), Column("Title", String(64)))
    version_tbl = online_impl.version_table_impl(
        version_table="alembic_version",
        version_table_schema=None,
        version_table_pk=True,
    )

    online_impl._exec(CreateTable(singers))
    online_impl._exec(CreateTable(albums))
    assert len(online_impl._ddl_buffer) == 2
    assert batched_calls == []

    online_impl.emit_commit()
    assert not online_impl._ddl_buffer
    assert len(batched_calls) == 1
    assert len(batched_calls[0]) == 2
    assert "CREATE TABLE" in batched_calls[0][0]
    assert "Singers" in batched_calls[0][0]
    assert "Albums" in batched_calls[0][1]

    online_impl._exec(CreateTable(singers))
    assert len(online_impl._ddl_buffer) == 1
    online_impl._exec(insert(version_tbl).values(version_num="rev001"))
    assert not online_impl._ddl_buffer
    assert len(batched_calls) == 2
    assert len(executed_dml) == 1
    assert "alembic_version" in executed_dml[0][0]

    sql_buf = io.StringIO()
    offline_impl = SpannerAlembicImpl(sp_dialect, None, True, False, sql_buf, {})
    offline_impl._exec(CreateTable(singers))
    offline_impl.emit_commit()
    offline_sql = sql_buf.getvalue()
    assert not offline_impl._ddl_buffer
    assert "CREATE TABLE" in offline_sql
    assert "Singers" in offline_sql
    assert ";" in offline_sql
    assert "COMMIT;" in offline_sql


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
    mysql_driver = SyncMySQLDriver(mysql_cfg)
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
    config: BigQueryConfig | SpannerConfig
    provider: ToolProvider
    if engine == "bigquery":
        bq_cfg = BigQueryConfig(project_id="analytics-proj", dataset_id="warehouse")
        config = bq_cfg
        provider = BigQueryToolProvider(
            config="analytics" if target_kind == "alias" else bq_cfg,
            registry=registry,
        )
    else:
        sp_cfg = SpannerConfig(project_id="core-proj", instance_id="prod-inst", database_id="orders-db")
        config = sp_cfg
        provider = SpannerToolProvider(
            config="analytics" if target_kind == "alias" else sp_cfg,
            registry=registry,
        )
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


@pytest.mark.skipif(not _HAS_SQLALCHEMY_GCP, reason="sqlalchemy GCP dialects require Python < 3.14")
@pytest.mark.anyio
async def test_gcp_alchemy_configs_use_broker_pool_and_stripped_dialects_module() -> None:
    """Verify BigQuery/Spanner configs wire BrokerPool/AsyncBrokerPool and dialects expose PEP 249 interfaces."""
    assert not hasattr(sqlalchemy_dialects_mod, "SyncDBAPIConnection")
    assert not hasattr(sqlalchemy_dialects_mod, "AsyncDBAPIConnection")
    assert SyncBigQueryDialect.import_dbapi().paramstyle == "named"
    assert AsyncSpannerDialect.import_dbapi().paramstyle == "named"

    sync_broker = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    async_broker = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    bq_engine = None
    sp_engine = None
    try:
        bq_cfg = create_sync_alchemy_config("bigquery://analytics-proj/ds", broker=sync_broker)
        sp_cfg = create_async_alchemy_config("spanner://core-proj/prod-inst/orders", broker=async_broker)
        assert isinstance(bq_cfg.engine_config.poolclass, type)
        assert issubclass(bq_cfg.engine_config.poolclass, BrokerPool)
        assert isinstance(sp_cfg.engine_config.poolclass, type)
        assert issubclass(sp_cfg.engine_config.poolclass, AsyncBrokerPool)
        assert isinstance(bq_cfg.engine_config.connect_args, dict)
        assert bq_cfg.engine_config.connect_args["broker"] is sync_broker
        assert isinstance(sp_cfg.engine_config.connect_args, dict)
        assert sp_cfg.engine_config.connect_args["broker"] is async_broker
        bq_engine = bq_cfg.get_engine()
        sp_engine = sp_cfg.get_engine()
        assert isinstance(bq_engine.pool, BrokerPool)
        assert isinstance(sp_engine.pool, AsyncBrokerPool)
        assert SyncBigQueryDialect().create_connect_args(bq_engine.url) == (
            [],
            {"url": "bigquery+vtb://analytics-proj/ds"},
        )
        assert AsyncSpannerDialect().create_connect_args(sp_engine.url) == (
            [],
            {"url": "spanner+vtb_async://core-proj/prod-inst/orders"},
        )
    finally:
        if bq_engine is not None:
            bq_engine.dispose()
        if sp_engine is not None:
            await sp_engine.dispose()
        sync_broker.close()
        await async_broker.close()


class _ProtocolFakeGCPDriver:
    """Protocol-fake synchronous SQL driver for BigQuery, Spanner, and Spangres SQLAlchemy tests."""

    def __init__(self, *, version_string: str = "PostgreSQL 14.1 on spangres") -> None:
        self.queries: list[tuple[str, Any]] = []
        self.version_string = version_string
        self.rows: list[tuple[Any, ...]] = [(1, "alpha")]

    def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], Any]:
        self.queries.append((sql, parameters))
        lowered = sql.lower()
        if "pg_catalog.version" in lowered:
            return [(self.version_string,)], (("version", None, None, None, None, None, None),)
        if "current_schema" in lowered:
            return [("public",)], (("current_schema", None, None, None, None, None, None),)
        if "show transaction isolation level" in lowered:
            return [("serializable",)], (("transaction_isolation", None, None, None, None, None, None),)
        if "select 1" in lowered:
            return [(1,)], (("col_0", None, None, None, None, None, None),)
        return list(self.rows), (
            ("id", None, None, None, None, None, None),
            ("name", None, None, None, None, None, None),
        )

    def execute(self, sql: str, parameters: Any = ()) -> int:
        self.queries.append((sql, parameters))
        return 1

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


class _AsyncProtocolFakeGCPDriver:
    """Protocol-fake asynchronous SQL driver wrapping _ProtocolFakeGCPDriver."""

    def __init__(self, *, version_string: str = "PostgreSQL 14.1 on spangres") -> None:
        self.sync_delegate = _ProtocolFakeGCPDriver(version_string=version_string)

    @property
    def queries(self) -> list[tuple[str, Any]]:
        return self.sync_delegate.queries

    async def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], Any]:
        return self.sync_delegate.execute_with_description(sql, parameters)

    async def execute(self, sql: str, parameters: Any = ()) -> int:
        return self.sync_delegate.execute(sql, parameters)

    async def commit(self) -> None:
        self.sync_delegate.commit()

    async def rollback(self) -> None:
        self.sync_delegate.rollback()

    async def close(self) -> None:
        self.sync_delegate.close()


class _GCPProtocolError(Exception):
    """Test error carrying optional SQLSTATE, vendor code, or gRPC StatusCode-like code."""

    def __init__(self, message: str, *, sqlstate: str | None = None, code: Any = None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.code = code


class _FakeGrpcStatusCode:
    """Stub mimicking grpc.StatusCode with a .name attribute."""

    def __init__(self, name: str) -> None:
        self.name = name


class _BrokenStrGCPError(Exception):
    """Exception whose __str__ fails to verify is_disconnect never raises."""

    def __str__(self) -> str:
        raise RuntimeError("broken string conversion")


@pytest.mark.skipif(not _HAS_SQLALCHEMY_GCP, reason="sqlalchemy GCP dialects require Python < 3.14")
@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_bigquery_sqlalchemy21_compilation_and_execution(asynchronous: bool) -> None:
    """Verify BigQuery SQLAlchemy 2.1 compilation, identifier quoting, JSON serializers, and execution."""
    events = Table(
        "user-events",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("name", String(64)),
    )

    if asynchronous:
        async_driver = _AsyncProtocolFakeGCPDriver()
        async_engine = create_async_engine(
            "bigquery+vtb_async://analytics-proj/warehouse",
            json_serializer=json.dumps,
            json_deserializer=json.loads,
            async_creator=lambda: AsyncDBAPIConnection(
                "bigquery+vtb_async://analytics-proj/warehouse",
                driver=async_driver,
            ),
        )
        try:
            assert isinstance(async_engine.dialect, AsyncBigQueryDialect)
            assert async_engine.dialect._json_serializer is json.dumps
            assert async_engine.dialect._json_deserializer is json.loads
            assert async_engine.dialect.identifier_preparer.quote_identifier("user-events") == "`user-events`"
            compiled = str(select(events).where(events.c.id == 1).compile(dialect=async_engine.dialect))
            assert "`user-events`" in compiled
            async with async_engine.connect() as aconn:
                rows = (await aconn.execute(select(events).where(events.c.id == 1))).fetchall()
                assert rows == [(1, "alpha")]
                assert async_engine.dialect.server_version_info is None
                ping_ok = await aconn.run_sync(
                    lambda sc: (
                        sc.connection.dbapi_connection is not None
                        and async_engine.dialect.do_ping(sc.connection.dbapi_connection)
                    )
                )
                assert ping_ok is True
                await aconn.execute(insert(events).values(id=2, name="beta"))
            assert any("INSERT INTO `user-events`" in sql for sql, _ in async_driver.queries)
        finally:
            await async_engine.dispose()
        return

    sync_driver = _ProtocolFakeGCPDriver()
    sync_engine = create_engine(
        "bigquery+vtb://analytics-proj/warehouse",
        json_serializer=json.dumps,
        json_deserializer=json.loads,
        creator=lambda: SyncDBAPIConnection(
            "bigquery+vtb://analytics-proj/warehouse",
            driver=sync_driver,
        ),
    )
    try:
        assert isinstance(sync_engine.dialect, SyncBigQueryDialect)
        assert sync_engine.dialect._json_serializer is json.dumps
        assert sync_engine.dialect._json_deserializer is json.loads
        assert sync_engine.dialect.identifier_preparer.quote_identifier("user-events") == "`user-events`"
        compiled = str(select(events).where(events.c.id == 1).compile(dialect=sync_engine.dialect))
        assert "`user-events`" in compiled
        with sync_engine.connect() as conn:
            rows = conn.execute(select(events).where(events.c.id == 1)).fetchall()
            assert rows == [(1, "alpha")]
            assert sync_engine.dialect.server_version_info is None
            dbapi_conn = conn.connection.dbapi_connection
            assert dbapi_conn is not None
            assert sync_engine.dialect.do_ping(dbapi_conn) is True
            conn.execute(insert(events).values(id=2, name="beta"))
        assert any("INSERT INTO `user-events`" in sql for sql, _ in sync_driver.queries)
    finally:
        sync_engine.dispose()


@pytest.mark.skipif(not _HAS_SQLALCHEMY_GCP, reason="sqlalchemy GCP dialects require Python < 3.14")
@pytest.mark.parametrize(
    ("scheme", "dialect_cls", "expected_broker_base"),
    [
        ("bigquery+vtb", SyncBigQueryDialect, BrokerPool),
        ("bigquery+vtb_async", AsyncBigQueryDialect, AsyncBrokerPool),
        ("spanner+vtb", SyncSpannerDialect, BrokerPool),
        ("spanner+vtb_async", AsyncSpannerDialect, AsyncBrokerPool),
        ("spangres+vtb", SyncSpangresDialect, BrokerPool),
        ("spangres+vtb_async", AsyncSpangresDialect, AsyncBrokerPool),
    ],
)
def test_bigquery_broker_query_selects_broker_pool_or_null_pool(
    monkeypatch: pytest.MonkeyPatch,
    scheme: str,
    dialect_cls: type[Any],
    expected_broker_base: type[BrokerPool],
) -> None:
    """Verify GCP dialects default to NullPool and select BrokerPool/AsyncBrokerPool on ?broker=true."""
    base_url = f"{scheme}://analytics-proj/warehouse"
    assert dialect_cls.get_pool_class(make_url(base_url)) is NullPool
    assert dialect_cls.get_pool_class(make_url(f"{base_url}?shm_broker=true")) is NullPool

    fake_broker = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    try:
        monkeypatch.setattr(DatabaseRegistry, "default_broker", classmethod(lambda cls: fake_broker))
        for query_param in ("broker=true", "broker_pool=true"):
            bound_cls = dialect_cls.get_pool_class(make_url(f"{base_url}?{query_param}"))
            assert issubclass(bound_cls, expected_broker_base)
            assert bound_cls._bound_broker is fake_broker

        monkeypatch.setattr(DatabaseRegistry, "default_broker", classmethod(lambda cls: None))
        with pytest.raises(ImproperConfigurationError, match="default broker"):
            dialect_cls.get_pool_class(make_url(f"{base_url}?broker=true"))
    finally:
        fake_broker.close()


@pytest.mark.skipif(not _HAS_SQLALCHEMY_GCP, reason="sqlalchemy GCP dialects require Python < 3.14")
@pytest.mark.anyio
@pytest.mark.parametrize(
    ("scheme_prefix", "asynchronous"),
    [
        ("spanner", False),
        ("spanner", True),
        ("spangres", False),
        ("spangres", True),
    ],
)
async def test_spanner_and_spangres_sqlalchemy21_execution_and_pool(
    scheme_prefix: str,
    asynchronous: bool,
) -> None:
    """Verify Spanner and Spangres SQLAlchemy 2.1 engines execute queries, ping, and use NullPool by default."""
    items = Table(
        "singers",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("name", String(64)),
    )
    scheme = f"{scheme_prefix}+vtb_async" if asynchronous else f"{scheme_prefix}+vtb"
    dsn = f"{scheme}://core-proj/prod-inst/music-db"

    if asynchronous:
        async_driver = _AsyncProtocolFakeGCPDriver()
        async_engine = create_async_engine(
            dsn,
            async_creator=lambda: AsyncDBAPIConnection(dsn, driver=async_driver),
        )
        try:
            assert isinstance(async_engine.pool, NullPool)
            async with async_engine.connect() as aconn:
                rows = (await aconn.execute(select(items).where(items.c.id == 1))).fetchall()
                assert rows == [(1, "alpha")]
                ping_ok = await aconn.run_sync(
                    lambda sc: (
                        sc.connection.dbapi_connection is not None
                        and async_engine.dialect.do_ping(sc.connection.dbapi_connection)
                    )
                )
                assert ping_ok is True
                await aconn.execute(insert(items).values(id=2, name="beta"))
            assert any("INSERT INTO singers" in sql for sql, _ in async_driver.queries)
        finally:
            await async_engine.dispose()
        return

    sync_driver = _ProtocolFakeGCPDriver()
    sync_engine = create_engine(
        dsn,
        creator=lambda: SyncDBAPIConnection(dsn, driver=sync_driver),
    )
    try:
        assert isinstance(sync_engine.pool, NullPool)
        with sync_engine.connect() as conn:
            rows = conn.execute(select(items).where(items.c.id == 1)).fetchall()
            assert rows == [(1, "alpha")]
            dbapi_conn = conn.connection.dbapi_connection
            assert dbapi_conn is not None
            assert sync_engine.dialect.do_ping(dbapi_conn) is True
            assert sync_engine.dialect.get_isolation_level(dbapi_conn) in {"AUTOCOMMIT", "SERIALIZABLE"}
            conn.execute(insert(items).values(id=2, name="beta"))
        assert any("INSERT INTO singers" in sql for sql, _ in sync_driver.queries)
    finally:
        sync_engine.dispose()


@pytest.mark.skipif(not _HAS_SQLALCHEMY_GCP, reason="sqlalchemy GCP dialects require Python < 3.14")
@pytest.mark.anyio
async def test_spangres_rejects_twophase_and_spanner_is_disconnect() -> None:
    """Verify Spangres rejects two-phase commit loudly without emitting PREPARE TRANSACTION."""
    assert SyncSpangresDialect.supports_twophase is False
    assert AsyncSpangresDialect.supports_twophase is False

    sync_driver = _ProtocolFakeGCPDriver()
    sync_engine = create_engine(
        "spangres+vtb://core-proj/prod-inst/music-db",
        creator=lambda: SyncDBAPIConnection(
            "spangres+vtb://core-proj/prod-inst/music-db",
            driver=sync_driver,
        ),
    )
    try:
        with sync_engine.connect() as conn, pytest.raises(sa_exc.NotSupportedError, match="two-phase"):
            conn.begin_twophase()
        with sync_engine.connect() as conn:
            with pytest.raises(sa_exc.NotSupportedError, match="two-phase"):
                conn.recover_twophase()
            for method_call in (
                lambda: sync_engine.dialect.do_begin_twophase(conn, "xid-1"),
                lambda: sync_engine.dialect.do_prepare_twophase(conn, "xid-1"),
                lambda: sync_engine.dialect.do_commit_twophase(conn, "xid-1"),
                lambda: sync_engine.dialect.do_rollback_twophase(conn, "xid-1"),
                lambda: sync_engine.dialect.do_recover_twophase(conn),
            ):
                with pytest.raises(sa_exc.NotSupportedError, match="two-phase"):
                    method_call()
        assert not any("PREPARE TRANSACTION" in sql.upper() for sql, _ in sync_driver.queries)
    finally:
        sync_engine.dispose()

    async_driver = _AsyncProtocolFakeGCPDriver()
    async_engine = create_async_engine(
        "spangres+vtb_async://core-proj/prod-inst/music-db",
        async_creator=lambda: AsyncDBAPIConnection(
            "spangres+vtb_async://core-proj/prod-inst/music-db",
            driver=async_driver,
        ),
    )
    try:
        async with async_engine.connect() as aconn:
            with pytest.raises(sa_exc.NotSupportedError, match="two-phase"):
                await aconn.run_sync(lambda sc: sc.begin_twophase())
            with pytest.raises(sa_exc.NotSupportedError, match="two-phase"):
                await aconn.run_sync(lambda sc: sc.recover_twophase())
        assert not any("PREPARE TRANSACTION" in sql.upper() for sql, _ in async_driver.queries)
    finally:
        await async_engine.dispose()


@pytest.mark.skipif(not _HAS_SQLALCHEMY_GCP, reason="sqlalchemy GCP dialects require Python < 3.14")
@pytest.mark.parametrize(
    "dialect_cls",
    [
        SyncBigQueryDialect,
        AsyncBigQueryDialect,
        SyncSpannerDialect,
        AsyncSpannerDialect,
        SyncSpangresDialect,
        AsyncSpangresDialect,
    ],
)
@pytest.mark.parametrize(
    ("exc_factory", "closed", "expected"),
    [
        (lambda: _GCPProtocolError("socket lost", sqlstate="08006"), False, True),
        (lambda: _GCPProtocolError("comm link", sqlstate="08S01"), False, True),
        (lambda: _GCPProtocolError("admin shutdown", sqlstate="57P01"), False, True),
        (lambda: _GCPProtocolError("endpoint unavailable", code=14), False, True),
        (lambda: _GCPProtocolError("rpc failed", code=_FakeGrpcStatusCode("UNAVAILABLE")), False, True),
        (lambda: _GCPProtocolError("503 UNAVAILABLE: channel closed"), False, True),
        (lambda: _GCPProtocolError("connection reset by peer"), False, True),
        (lambda: _GCPProtocolError("generic query error"), True, True),
        (lambda: _GCPProtocolError("aborted", sqlstate="40001"), False, False),
        (lambda: _GCPProtocolError("duplicate key", sqlstate="23505"), False, False),
        (lambda: type("SerializationFailure", (Exception,), {})("concurrent update"), False, False),
        (_BrokenStrGCPError, False, False),
    ],
)
def test_gcp_dialects_is_disconnect_classification(
    dialect_cls: type[Any],
    exc_factory: Any,
    closed: bool,
    expected: bool,
) -> None:
    """Verify BigQuery, Spanner, and Spangres dialects classify 08*, gRPC UNAVAILABLE, and non-disconnects."""

    class _ConnStub:
        def __init__(self, is_closed: bool) -> None:
            self.closed = is_closed

    dialect = dialect_cls()
    assert dialect.is_disconnect(exc_factory(), _ConnStub(closed)) is expected


@pytest.mark.skipif(not _HAS_SQLALCHEMY_GCP, reason="sqlalchemy GCP dialects require Python < 3.14")
@pytest.mark.parametrize(
    ("dialect_module_name", "missing_dep"),
    [
        ("vertebra.extensions.sqlalchemy.dialects.bigquery", "sqlalchemy_bigquery"),
        ("vertebra.extensions.sqlalchemy.dialects.spanner", "google.cloud.sqlalchemy_spanner"),
    ],
)
def test_bigquery_and_spanner_missing_dependency_raises(
    monkeypatch: pytest.MonkeyPatch,
    dialect_module_name: str,
    missing_dep: str,
) -> None:
    """Verify importing BigQuery or Spanner dialects without required base packages raises MissingDependencyError."""
    dialect_mod = importlib.import_module(dialect_module_name)
    monkeypatch.setitem(sys.modules, missing_dep, None)
    reset_dependency_cache(missing_dep)
    try:
        with pytest.raises(MissingDependencyError, match=r"sqlalchemy-gcp"):
            importlib.reload(dialect_mod)
    finally:
        monkeypatch.undo()
        reset_dependency_cache(missing_dep)
        importlib.reload(dialect_mod)
