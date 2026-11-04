"""Synchronous and asynchronous pure-Rust Google Cloud BigQuery drivers."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager, suppress
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self

import msgspec

from vertebra._vertebra import (
    LeaseBroker,
)
from vertebra._vertebra import (
    StatementCache as _RustStatementCache,
)
from vertebra.db.adapters.bigquery.config import (
    BIGQUERY_DIALECT_TAG,
    BigQueryConfig,
    BigQueryDryRunResult,
    ObjectRef,
    ObjectTableConfig,
)
from vertebra.db.adapters.bigquery.core import BigQueryCore
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS, ArrowCursor
from vertebra.db.driver._async import AsyncDatabaseDriver
from vertebra.db.driver._sync import SyncDatabaseDriver
from vertebra.telemetry.db_propagation import SqlTracePropagator
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.schema import to_value_type

if TYPE_CHECKING:
    from vertebra._bigquery import AsyncBigQueryClient, SyncBigQueryClient

__all__ = (
    "AsyncBigQueryDriver",
    "SyncBigQueryDriver",
)


class SyncBigQueryDriver(SyncDatabaseDriver[BigQueryConfig]):
    """Synchronous pure-Rust Google Cloud BigQuery driver."""

    dialect: ClassVar[str] = "bigquery"
    dialect_tag: ClassVar[int] = BIGQUERY_DIALECT_TAG

    __slots__ = (
        "_client",
        "_in_transaction",
        "_last_query_metadata",
        "_propagator",
        "_session_id",
        "_statement_cache",
    )

    def __init__(
        self,
        config: BigQueryConfig | str | None = None,
        *,
        broker: LeaseBroker | None = None,
        statement_cache: _RustStatementCache | None = None,
        propagator: SqlTracePropagator | None = None,
        client: SyncBigQueryClient | None = None,
        **overrides: Any,
    ) -> None:
        if isinstance(config, str):
            resolved = BigQueryConfig.from_dsn(config, **overrides)
        elif isinstance(config, BigQueryConfig):
            resolved = msgspec.structs.replace(config, **overrides) if overrides else config
        else:
            dsn = overrides.pop("dsn", None)
            if isinstance(dsn, str) and dsn.startswith(("bigquery://", "bq://")):
                resolved = BigQueryConfig.from_dsn(dsn, **overrides)
            else:
                resolved = BigQueryConfig(**overrides)

        resolved_broker = broker or resolved.broker or LeaseBroker(max(1, resolved.pool_size * 4))
        super().__init__(resolved, broker=resolved_broker)
        self._statement_cache: _RustStatementCache | None = (
            statement_cache
            if statement_cache is not None
            else (_RustStatementCache(max(16, resolved.statement_cache_capacity)) if resolved.use_shm_cache else None)
        )
        self._propagator: SqlTracePropagator = propagator or SqlTracePropagator()
        self._client: SyncBigQueryClient | None = client
        self._session_id: str | None = None
        self._in_transaction: bool = False
        self._last_query_metadata: dict[str, Any] = {}

    @property
    def project_id(self) -> str:
        """Return the effective Google Cloud project ID."""
        return self.config.effective_project_id()

    @property
    def dataset_id(self) -> str | None:
        """Return the effective default BigQuery dataset ID."""
        return self.config.effective_dataset_id()

    @property
    def session_id(self) -> str | None:
        """Return the active BigQuery multi-statement session ID, if any."""
        return self._session_id

    @property
    def in_transaction(self) -> bool:
        """Return whether a session-backed transaction is currently active."""
        return self._in_transaction

    @property
    def last_query_metadata(self) -> dict[str, Any]:
        """Return execution statistics from the most recent query job."""
        return dict(self._last_query_metadata)

    def __enter__(self) -> Self:
        """Connect and enter the synchronous driver context."""
        return self.connect()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Roll back any active transaction and close the driver upon exiting the context."""
        if self._in_transaction:
            with suppress(Exception):
                self.rollback()
        self.close()

    def connect(self) -> Self:
        """Initialize the underlying Rust `SyncBigQueryClient` channel pool."""
        if self._client is None:
            self._client = BigQueryCore.create_sync_rust_client(self.config)
        self._closed = False
        return self

    def close(self) -> None:
        """Release the active session and Rust client handle."""
        self._session_id = None
        self._in_transaction = False
        self._client = None
        self._closed = True

    def execute(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        job_labels: Mapping[str, str] | None = None,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML, DDL, or query statement and return affected or produced row count."""
        _ = kwargs
        raw = self._run_query_sync(
            sql,
            parameters,
            job_labels=job_labels,
            destination_table=destination_table,
            write_disposition=write_disposition,
            maximum_bytes_billed=maximum_bytes_billed,
        )
        affected = raw.get("num_dml_affected_rows")
        if affected is not None:
            return int(affected)
        return int(raw.get("total_rows", len(raw.get("rows", ()))))

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query and return `(tuple_rows, column_names)`."""
        _ = kwargs
        raw = self._run_query_sync(sql, parameters)
        cols = tuple(str(c) for c in raw.get("columns", ()))
        rows = [tuple(r) for r in raw.get("rows", ())]
        return rows, cols

    def execute_script(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a GoogleSQL procedural script or multi-statement batch."""
        raw = self._run_query_sync(sql, parameters)
        return BigQueryCore.rows_to_dicts(raw.get("columns", ()), raw.get("rows", ()))

    def select(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and return rows as dictionaries or `schema_type` instances."""
        _ = kwargs
        raw = self._run_query_sync(sql, parameters)
        return BigQueryCore.hydrate_query_rows(
            raw.get("columns", ()),
            raw.get("rows", ()),
            schema_type=schema_type,
        )

    def select_one(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query and return the first row or `None`."""
        rows = self.select(sql, parameters, schema_type=schema_type, **kwargs)
        return rows[0] if rows else None

    def select_value(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first scalar column of the first row."""
        _ = kwargs
        raw = self._run_query_sync(sql, parameters)
        rows = raw.get("rows", ())
        if not rows or not rows[0]:
            return None
        val = rows[0][0]
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

    def dry_run(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> BigQueryDryRunResult:
        """Validate `sql` via `jobs.insert(dryRun=True)` and return byte and cost estimates."""
        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(sql, parameters)
        raw = client.dry_run(rewritten_sql, params_json)
        return BigQueryCore.build_dry_run_result(raw)

    def fetch_arrow(
        self,
        sql_or_table: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        selected_fields: Sequence[str] | None = None,
        row_restriction: str | None = None,
        max_streams: int | None = None,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Read a table or query into a zero-copy `VTBARROW` shared-memory `ArrowCursor`."""
        _ = (threshold_rows, kwargs)
        client = self._ensure_client()
        streams = max_streams or self.config.max_read_streams
        table_parts = BigQueryCore.try_parse_table_reference(
            sql_or_table,
            default_project=self.project_id,
            default_dataset=self.dataset_id,
        )
        if table_parts is not None and not parameters:
            project_id, dataset_id, table_id = table_parts
            lease = self._acquire_pid_lease()
            try:
                raw_cursor = client.read_table_arrow(
                    project_id,
                    dataset_id,
                    table_id,
                    list(selected_fields) if selected_fields else None,
                    row_restriction,
                    streams,
                    self.config.arrow_compression,
                )
                return ArrowCursor(raw_cursor)
            except Exception as exc:
                if not BigQueryCore.is_storage_api_fallback_error(exc):
                    raise
            finally:
                self._release_pid_lease(lease)

        select_sql = BigQueryCore.build_table_or_query_sql(
            sql_or_table,
            selected_fields=selected_fields,
            row_restriction=row_restriction,
        )
        raw = self._run_query_sync(select_sql, parameters)
        return BigQueryCore.query_result_to_arrow_cursor(raw)

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query or table read and return a zero-copy `ArrowCursor`."""
        return self.fetch_arrow(sql, parameters, threshold_rows=threshold_rows, **kwargs)

    def insert_arrow(
        self,
        table_name: str,
        data: ArrowCursor | bytes | Any,
        *,
        columns: Sequence[str] | None = None,
        mode: Literal["_default", "COMMITTED", "PENDING", "BUFFERED"] = "_default",
        **kwargs: Any,
    ) -> int:
        """Append an Arrow IPC stream, `ArrowCursor`, or `pyarrow` table via Storage Write API v1."""
        _ = (columns, kwargs)
        client = self._ensure_client()
        ipc_bytes = BigQueryCore.coerce_arrow_ipc_bytes(data)
        if not ipc_bytes:
            return 0
        parent_table = BigQueryCore.qualify_table_resource(
            table_name,
            default_project=self.project_id,
            default_dataset=self.dataset_id,
        )
        lease = self._acquire_pid_lease()
        try:
            res = client.append_arrow(parent_table, ipc_bytes, mode)
            return int(res.get("appended_rows", 0))
        except Exception as exc:
            if BigQueryCore.is_storage_api_fallback_error(exc):
                return self._fallback_insert_arrow_via_rest(table_name, ipc_bytes)
            raise
        finally:
            self._release_pid_lease(lease)

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        mode: Literal["_default", "COMMITTED", "PENDING", "BUFFERED"] = "_default",
        **kwargs: Any,
    ) -> int:
        """Bulk-load an Arrow payload into `table_name` via BigQuery Storage Write API v1."""
        return self.insert_arrow(table_name, data, columns=columns, mode=mode, **kwargs)

    def begin(self) -> str:
        """Start a BigQuery multi-statement transaction session and return its `session_id`."""
        client = self._ensure_client()
        create_session = self._session_id is None
        res = client.query(
            "BEGIN TRANSACTION",
            None,
            self._session_id,
            create_session,
        )
        sid = res.get("session_id") or self._session_id
        if sid:
            self._session_id = str(sid)
        self._in_transaction = True
        return self._session_id or ""

    def commit(self) -> None:
        """Commit the active BigQuery session transaction."""
        if not self._in_transaction:
            return
        client = self._ensure_client()
        try:
            client.query("COMMIT TRANSACTION", None, self._session_id, False)
        finally:
            self._in_transaction = False

    def rollback(self) -> None:
        """Roll back the active BigQuery session transaction."""
        if not self._in_transaction:
            return
        client = self._ensure_client()
        try:
            client.query("ROLLBACK TRANSACTION", None, self._session_id, False)
        finally:
            self._in_transaction = False

    @contextmanager
    def transaction(self) -> Iterator[Self]:
        """Context manager executing statements inside a BigQuery `BEGIN ... COMMIT` session."""
        self.begin()
        try:
            yield self
        except Exception:
            self.rollback()
            raise
        else:
            self.commit()

    def export_to_object_store(
        self,
        sql_or_table: str,
        destination_uri: str,
        *,
        store: Any | None = None,
        format: Literal["parquet", "csv", "json", "avro"] = "parquet",
        compression: str = "ZSTD",
        overwrite: bool = True,
    ) -> int:
        """Export a BigQuery table or query to `gs://` via `EXPORT DATA` or multicloud `ObjectStore`."""
        if destination_uri.startswith("gs://") and store is None:
            export_sql = BigQueryCore.build_export_data_sql(
                sql_or_table,
                destination_uri=destination_uri,
                format=format,
                compression=compression,
                overwrite=overwrite,
            )
            return self.execute(export_sql)

        cursor = self.fetch_arrow(sql_or_table)
        BigQueryCore.write_arrow_cursor_to_object_store_sync(
            cursor,
            destination_uri=destination_uri,
            store=store,
            format=format,
            compression=compression,
        )
        return cursor.row_count

    def load_from_object_store(
        self,
        table: str,
        source_uris: str | Sequence[str],
        *,
        store: Any | None = None,
        format: Literal["parquet", "csv", "json", "avro", "orc"] = "parquet",
        write_disposition: Literal["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"] = "WRITE_APPEND",
    ) -> int:
        """Load files from `gs://` via `LOAD DATA INTO` or stream from multicloud `ObjectStore`."""
        uris = [source_uris] if isinstance(source_uris, str) else list(source_uris)
        if not uris:
            return 0
        if all(u.startswith("gs://") for u in uris) and store is None:
            load_sql = BigQueryCore.build_load_data_sql(
                table,
                uris=uris,
                format=format,
                write_disposition=write_disposition,
            )
            return self.execute(load_sql)

        total_rows = 0
        for uri in uris:
            cursor = BigQueryCore.read_arrow_cursor_from_object_store_sync(
                uri,
                store=store,
                format=format,
            )
            total_rows += self.insert_arrow(table, cursor)
        return total_rows

    def create_object_table(
        self,
        config: ObjectTableConfig,
        *,
        if_not_exists: bool = True,
    ) -> None:
        """Create a BigQuery External Object Table from `config`."""
        ddl = config.create_object_table_sql(if_not_exists=if_not_exists)
        self.execute(ddl)

    def list_object_table_refs(
        self,
        config: ObjectTableConfig,
        *,
        store: Any | None = None,
        where: str | None = None,
        limit: int | None = None,
        presign: bool = True,
        expires_in_secs: int = 3600,
    ) -> list[ObjectRef]:
        """Query a BigQuery Object Table and hydrate zero-RTT presigned `ObjectRef` entries."""
        sql = BigQueryCore.build_object_table_select_sql(config.table_name, where=where, limit=limit)
        try:
            rows = self.select(sql)
        except Exception:
            if store is not None:
                rows = BigQueryCore.list_objects_from_store_fallback_sync(config, store, limit=limit)
            else:
                raise
        return config.hydrate_object_refs_sync(
            rows,
            store=store if presign else None,
            expires_in_secs=expires_in_secs,
        )

    def _ensure_client(self) -> SyncBigQueryClient:
        if self._client is None:
            ensure_module("vertebra._bigquery", package="vertebra-core[bigquery]", extra="bigquery")
            self._client = BigQueryCore.create_sync_rust_client(self.config)
        return self._client

    def _acquire_pid_lease(self) -> Any:
        acquire_fn = getattr(self._broker, "acquire", None)
        if callable(acquire_fn):
            with suppress(Exception):
                return acquire_fn(os.getpid(), timeout_ms=5000)
        return None

    def _release_pid_lease(self, lease: Any) -> None:
        if lease is None or self._broker is None:
            return
        release_fn = getattr(self._broker, "release", None)
        if callable(release_fn):
            with suppress(Exception):
                release_fn(lease.slot_index, lease.generation)

    def _prepare_sql_and_params(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None,
    ) -> tuple[str, str | None]:
        rewritten = sql
        if self._statement_cache is not None:
            raw_bytes = sql.encode("utf-8")
            plan = self._statement_cache.lookup(BIGQUERY_DIALECT_TAG, raw_bytes)
            if plan is not None and plan.compiled_sql:
                rewritten = plan.compiled_sql.decode("utf-8", errors="replace")
            else:
                param_count = len(parameters) if parameters is not None else 0
                self._statement_cache.insert(
                    BIGQUERY_DIALECT_TAG,
                    raw_bytes,
                    param_count,
                    raw_bytes,
                    b"",
                )
        params_json = BigQueryCore.encode_parameters_json(parameters)
        return rewritten, params_json

    def _run_query_sync(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        job_labels: Mapping[str, str] | None = None,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
    ) -> dict[str, Any]:
        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(sql, parameters)
        annotated_sql = BigQueryCore.annotate_sql_with_trace(rewritten_sql, self._propagator)
        merged_labels = BigQueryCore.sanitize_labels(self.config.labels, job_labels)
        raw = client.query(
            annotated_sql,
            params_json,
            self._session_id,
            False,
            destination_table,
            write_disposition,
            maximum_bytes_billed or self.config.maximum_bytes_billed,
            self.config.job_timeout_ms,
            self.config.priority,
            merged_labels or None,
        )
        if raw.get("session_id"):
            self._session_id = str(raw["session_id"])
        self._last_query_metadata = {
            "job_id": raw.get("job_id"),
            "session_id": self._session_id,
            "total_rows": raw.get("total_rows", 0),
            "total_bytes_processed": raw.get("total_bytes_processed", 0),
            "total_bytes_billed": raw.get("total_bytes_billed", 0),
            "slot_ms": raw.get("slot_ms", 0),
            "cache_hit": raw.get("cache_hit", False),
            "num_dml_affected_rows": raw.get("num_dml_affected_rows"),
        }
        BigQueryCore.record_finops_metrics(self.project_id, self.config.location, raw)
        return dict(raw)

    def _fallback_insert_arrow_via_rest(
        self,
        table: str,
        ipc_bytes: bytes,
    ) -> int:
        rows = BigQueryCore.arrow_ipc_bytes_to_dicts(ipc_bytes)
        if not rows:
            return 0
        cols = list(rows[0].keys())
        col_list = ", ".join(cols)
        placeholders = ", ".join(f"@{c}" for c in cols)
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
        return self.execute_many(sql, rows)


class AsyncBigQueryDriver(AsyncDatabaseDriver[BigQueryConfig]):
    """Asynchronous pure-Rust Google Cloud BigQuery driver."""

    dialect: ClassVar[str] = "bigquery"
    dialect_tag: ClassVar[int] = BIGQUERY_DIALECT_TAG

    __slots__ = (
        "_client",
        "_in_transaction",
        "_last_query_metadata",
        "_propagator",
        "_session_id",
        "_statement_cache",
    )

    def __init__(
        self,
        config: BigQueryConfig | str | None = None,
        *,
        broker: LeaseBroker | None = None,
        statement_cache: _RustStatementCache | None = None,
        propagator: SqlTracePropagator | None = None,
        client: AsyncBigQueryClient | None = None,
        **overrides: Any,
    ) -> None:
        if isinstance(config, str):
            resolved = BigQueryConfig.from_dsn(config, **overrides)
        elif isinstance(config, BigQueryConfig):
            resolved = msgspec.structs.replace(config, **overrides) if overrides else config
        else:
            dsn = overrides.pop("dsn", None)
            if isinstance(dsn, str) and dsn.startswith(("bigquery://", "bq://")):
                resolved = BigQueryConfig.from_dsn(dsn, **overrides)
            else:
                resolved = BigQueryConfig(**overrides)

        resolved_broker = broker or resolved.broker or LeaseBroker(max(1, resolved.pool_size * 4))
        super().__init__(resolved, broker=resolved_broker)
        self._statement_cache: _RustStatementCache | None = (
            statement_cache
            if statement_cache is not None
            else (_RustStatementCache(max(16, resolved.statement_cache_capacity)) if resolved.use_shm_cache else None)
        )
        self._propagator: SqlTracePropagator = propagator or SqlTracePropagator()
        self._client: AsyncBigQueryClient | None = client
        self._session_id: str | None = None
        self._in_transaction: bool = False
        self._last_query_metadata: dict[str, Any] = {}

    @property
    def project_id(self) -> str:
        """Return the effective Google Cloud project ID."""
        return self.config.effective_project_id()

    @property
    def dataset_id(self) -> str | None:
        """Return the effective default BigQuery dataset ID."""
        return self.config.effective_dataset_id()

    @property
    def session_id(self) -> str | None:
        """Return the active BigQuery multi-statement session ID, if any."""
        return self._session_id

    @property
    def in_transaction(self) -> bool:
        """Return whether a session-backed transaction is currently active."""
        return self._in_transaction

    @property
    def last_query_metadata(self) -> dict[str, Any]:
        """Return execution statistics from the most recent query job."""
        return dict(self._last_query_metadata)

    async def __aenter__(self) -> Self:
        """Connect and enter the asynchronous driver context."""
        return await self.connect()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Roll back any active transaction and close the driver upon exiting the context."""
        if self._in_transaction:
            with suppress(Exception):
                await self.rollback()
        await self.close()

    async def connect(self) -> Self:
        """Initialize the underlying Rust `AsyncBigQueryClient` channel pool."""
        if self._client is None:
            self._client = BigQueryCore.create_async_rust_client(self.config)
        self._closed = False
        return self

    async def close(self) -> None:
        """Release the active session and Rust client handle."""
        self._session_id = None
        self._in_transaction = False
        self._client = None
        self._closed = True

    async def execute(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        job_labels: Mapping[str, str] | None = None,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML, DDL, or query statement and return affected or produced row count."""
        _ = kwargs
        raw = await self._run_query_async(
            sql,
            parameters,
            job_labels=job_labels,
            destination_table=destination_table,
            write_disposition=write_disposition,
            maximum_bytes_billed=maximum_bytes_billed,
        )
        affected = raw.get("num_dml_affected_rows")
        if affected is not None:
            return int(affected)
        return int(raw.get("total_rows", len(raw.get("rows", ()))))

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query and return `(tuple_rows, column_names)`."""
        _ = kwargs
        raw = await self._run_query_async(sql, parameters)
        cols = tuple(str(c) for c in raw.get("columns", ()))
        rows = [tuple(r) for r in raw.get("rows", ())]
        return rows, cols

    async def execute_script(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a GoogleSQL procedural script or multi-statement batch."""
        raw = await self._run_query_async(sql, parameters)
        return BigQueryCore.rows_to_dicts(raw.get("columns", ()), raw.get("rows", ()))

    async def select(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and return rows as dictionaries or `schema_type` instances."""
        _ = kwargs
        raw = await self._run_query_async(sql, parameters)
        return BigQueryCore.hydrate_query_rows(
            raw.get("columns", ()),
            raw.get("rows", ()),
            schema_type=schema_type,
        )

    async def select_one(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query and return the first row or `None`."""
        rows = await self.select(sql, parameters, schema_type=schema_type, **kwargs)
        return rows[0] if rows else None

    async def select_value(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first scalar column of the first row."""
        _ = kwargs
        raw = await self._run_query_async(sql, parameters)
        rows = raw.get("rows", ())
        if not rows or not rows[0]:
            return None
        val = rows[0][0]
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

    async def dry_run(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> BigQueryDryRunResult:
        """Validate `sql` via `jobs.insert(dryRun=True)` and return byte and cost estimates."""
        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(sql, parameters)
        raw = await client.dry_run(rewritten_sql, params_json)
        return BigQueryCore.build_dry_run_result(raw)

    async def fetch_arrow(
        self,
        sql_or_table: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        selected_fields: Sequence[str] | None = None,
        row_restriction: str | None = None,
        max_streams: int | None = None,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Asynchronously read a table or query into a zero-copy `VTBARROW` `ArrowCursor`."""
        _ = (threshold_rows, kwargs)
        client = self._ensure_client()
        streams = max_streams or self.config.max_read_streams
        table_parts = BigQueryCore.try_parse_table_reference(
            sql_or_table,
            default_project=self.project_id,
            default_dataset=self.dataset_id,
        )
        if table_parts is not None and not parameters:
            project_id, dataset_id, table_id = table_parts
            lease = self._acquire_pid_lease()
            try:
                raw_cursor = await client.read_table_arrow(
                    project_id,
                    dataset_id,
                    table_id,
                    list(selected_fields) if selected_fields else None,
                    row_restriction,
                    streams,
                    self.config.arrow_compression,
                )
                return ArrowCursor(raw_cursor)
            except Exception as exc:
                if not BigQueryCore.is_storage_api_fallback_error(exc):
                    raise
            finally:
                self._release_pid_lease(lease)

        select_sql = BigQueryCore.build_table_or_query_sql(
            sql_or_table,
            selected_fields=selected_fields,
            row_restriction=row_restriction,
        )
        raw = await self._run_query_async(select_sql, parameters)
        return BigQueryCore.query_result_to_arrow_cursor(raw)

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query or table read and return a zero-copy `ArrowCursor`."""
        return await self.fetch_arrow(sql, parameters, threshold_rows=threshold_rows, **kwargs)

    async def insert_arrow(
        self,
        table_name: str,
        data: ArrowCursor | bytes | Any,
        *,
        columns: Sequence[str] | None = None,
        mode: Literal["_default", "COMMITTED", "PENDING", "BUFFERED"] = "_default",
        **kwargs: Any,
    ) -> int:
        """Asynchronously append an Arrow IPC stream, `ArrowCursor`, or `pyarrow` table."""
        _ = (columns, kwargs)
        client = self._ensure_client()
        ipc_bytes = BigQueryCore.coerce_arrow_ipc_bytes(data)
        if not ipc_bytes:
            return 0
        parent_table = BigQueryCore.qualify_table_resource(
            table_name,
            default_project=self.project_id,
            default_dataset=self.dataset_id,
        )
        lease = self._acquire_pid_lease()
        try:
            res = await client.append_arrow(parent_table, ipc_bytes, mode)
            return int(res.get("appended_rows", 0))
        except Exception as exc:
            if BigQueryCore.is_storage_api_fallback_error(exc):
                return await self._fallback_insert_arrow_via_rest(table_name, ipc_bytes)
            raise
        finally:
            self._release_pid_lease(lease)

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        mode: Literal["_default", "COMMITTED", "PENDING", "BUFFERED"] = "_default",
        **kwargs: Any,
    ) -> int:
        """Asynchronously bulk-load an Arrow payload into `table_name`."""
        return await self.insert_arrow(table_name, data, columns=columns, mode=mode, **kwargs)

    async def begin(self) -> str:
        """Start a BigQuery multi-statement transaction session and return its `session_id`."""
        client = self._ensure_client()
        create_session = self._session_id is None
        res = await client.query(
            "BEGIN TRANSACTION",
            None,
            self._session_id,
            create_session,
        )
        sid = res.get("session_id") or self._session_id
        if sid:
            self._session_id = str(sid)
        self._in_transaction = True
        return self._session_id or ""

    async def commit(self) -> None:
        """Commit the active BigQuery session transaction."""
        if not self._in_transaction:
            return
        client = self._ensure_client()
        try:
            await client.query("COMMIT TRANSACTION", None, self._session_id, False)
        finally:
            self._in_transaction = False

    async def rollback(self) -> None:
        """Roll back the active BigQuery session transaction."""
        if not self._in_transaction:
            return
        client = self._ensure_client()
        try:
            await client.query("ROLLBACK TRANSACTION", None, self._session_id, False)
        finally:
            self._in_transaction = False

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Self]:
        """Async context manager executing statements inside a BigQuery `BEGIN ... COMMIT` session."""
        await self.begin()
        try:
            yield self
        except Exception:
            await self.rollback()
            raise
        else:
            await self.commit()

    async def export_to_object_store(
        self,
        sql_or_table: str,
        destination_uri: str,
        *,
        store: Any | None = None,
        format: Literal["parquet", "csv", "json", "avro"] = "parquet",
        compression: str = "ZSTD",
        overwrite: bool = True,
    ) -> int:
        """Asynchronously export a table or query to `gs://` or multicloud `ObjectStore`."""
        if destination_uri.startswith("gs://") and store is None:
            export_sql = BigQueryCore.build_export_data_sql(
                sql_or_table,
                destination_uri=destination_uri,
                format=format,
                compression=compression,
                overwrite=overwrite,
            )
            return await self.execute(export_sql)

        cursor = await self.fetch_arrow(sql_or_table)
        await BigQueryCore.write_arrow_cursor_to_object_store_async(
            cursor,
            destination_uri=destination_uri,
            store=store,
            format=format,
            compression=compression,
        )
        return cursor.row_count

    async def load_from_object_store(
        self,
        table: str,
        source_uris: str | Sequence[str],
        *,
        store: Any | None = None,
        format: Literal["parquet", "csv", "json", "avro", "orc"] = "parquet",
        write_disposition: Literal["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"] = "WRITE_APPEND",
    ) -> int:
        """Asynchronously load files from `gs://` or stream from multicloud `ObjectStore`."""
        uris = [source_uris] if isinstance(source_uris, str) else list(source_uris)
        if not uris:
            return 0
        if all(u.startswith("gs://") for u in uris) and store is None:
            load_sql = BigQueryCore.build_load_data_sql(
                table,
                uris=uris,
                format=format,
                write_disposition=write_disposition,
            )
            return await self.execute(load_sql)

        total_rows = 0
        for uri in uris:
            cursor = await BigQueryCore.read_arrow_cursor_from_object_store_async(
                uri,
                store=store,
                format=format,
            )
            total_rows += await self.insert_arrow(table, cursor)
        return total_rows

    async def create_object_table(
        self,
        config: ObjectTableConfig,
        *,
        if_not_exists: bool = True,
    ) -> None:
        """Asynchronously create a BigQuery External Object Table from `config`."""
        ddl = config.create_object_table_sql(if_not_exists=if_not_exists)
        await self.execute(ddl)

    async def list_object_table_refs(
        self,
        config: ObjectTableConfig,
        *,
        store: Any | None = None,
        where: str | None = None,
        limit: int | None = None,
        presign: bool = True,
        expires_in_secs: int = 3600,
    ) -> list[ObjectRef]:
        """Asynchronously query a BigQuery Object Table and hydrate presigned `ObjectRef` entries."""
        sql = BigQueryCore.build_object_table_select_sql(config.table_name, where=where, limit=limit)
        try:
            rows = await self.select(sql)
        except Exception:
            if store is not None:
                rows = await BigQueryCore.list_objects_from_store_fallback_async(config, store, limit=limit)
            else:
                raise
        return await config.hydrate_object_refs_async(
            rows,
            store=store if presign else None,
            expires_in_secs=expires_in_secs,
        )

    def _ensure_client(self) -> AsyncBigQueryClient:
        if self._client is None:
            ensure_module("vertebra._bigquery", package="vertebra-core[bigquery]", extra="bigquery")
            self._client = BigQueryCore.create_async_rust_client(self.config)
        return self._client

    def _acquire_pid_lease(self) -> Any:
        acquire_fn = getattr(self._broker, "acquire", None)
        if callable(acquire_fn):
            with suppress(Exception):
                return acquire_fn(os.getpid(), timeout_ms=5000)
        return None

    def _release_pid_lease(self, lease: Any) -> None:
        if lease is None or self._broker is None:
            return
        release_fn = getattr(self._broker, "release", None)
        if callable(release_fn):
            with suppress(Exception):
                release_fn(lease.slot_index, lease.generation)

    def _prepare_sql_and_params(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None,
    ) -> tuple[str, str | None]:
        rewritten = sql
        if self._statement_cache is not None:
            raw_bytes = sql.encode("utf-8")
            plan = self._statement_cache.lookup(BIGQUERY_DIALECT_TAG, raw_bytes)
            if plan is not None and plan.compiled_sql:
                rewritten = plan.compiled_sql.decode("utf-8", errors="replace")
            else:
                param_count = len(parameters) if parameters is not None else 0
                self._statement_cache.insert(
                    BIGQUERY_DIALECT_TAG,
                    raw_bytes,
                    param_count,
                    raw_bytes,
                    b"",
                )
        params_json = BigQueryCore.encode_parameters_json(parameters)
        return rewritten, params_json

    async def _run_query_async(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        job_labels: Mapping[str, str] | None = None,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
    ) -> dict[str, Any]:
        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(sql, parameters)
        annotated_sql = BigQueryCore.annotate_sql_with_trace(rewritten_sql, self._propagator)
        merged_labels = BigQueryCore.sanitize_labels(self.config.labels, job_labels)
        raw = await client.query(
            annotated_sql,
            params_json,
            self._session_id,
            False,
            destination_table,
            write_disposition,
            maximum_bytes_billed or self.config.maximum_bytes_billed,
            self.config.job_timeout_ms,
            self.config.priority,
            merged_labels or None,
        )
        if raw.get("session_id"):
            self._session_id = str(raw["session_id"])
        self._last_query_metadata = {
            "job_id": raw.get("job_id"),
            "session_id": self._session_id,
            "total_rows": raw.get("total_rows", 0),
            "total_bytes_processed": raw.get("total_bytes_processed", 0),
            "total_bytes_billed": raw.get("total_bytes_billed", 0),
            "slot_ms": raw.get("slot_ms", 0),
            "cache_hit": raw.get("cache_hit", False),
            "num_dml_affected_rows": raw.get("num_dml_affected_rows"),
        }
        BigQueryCore.record_finops_metrics(self.project_id, self.config.location, raw)
        return dict(raw)

    async def _fallback_insert_arrow_via_rest(
        self,
        table: str,
        ipc_bytes: bytes,
    ) -> int:
        rows = BigQueryCore.arrow_ipc_bytes_to_dicts(ipc_bytes)
        if not rows:
            return 0
        cols = list(rows[0].keys())
        col_list = ", ".join(cols)
        placeholders = ", ".join(f"@{c}" for c in cols)
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
        return await self.execute_many(sql, rows)
