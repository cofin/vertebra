"""BigQuery synchronous and asynchronous SQLSpec drivers over pure-Rust Vertebra drivers."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from sqlspec.core import SQL, ArrowResult, ParameterStyle, StackResult, StatementConfig, StatementStack
from sqlspec.driver import ExecutionResult
from sqlspec.storage import StorageBridgeJob

from vertebra.db.adapters.bigquery import BigQueryCore, ObjectRef, ObjectTableConfig
from vertebra.db.driver import NotSupportedError
from vertebra.extensions.sqlspec._driver import (
    BaseAsyncDriver,
    BaseSyncDriver,
)
from vertebra.extensions.sqlspec.adapters.bigquery.core import (
    compile_bigquery_statement,
    default_statement_config,
    encode_typed_bigquery_many,
    encode_typed_bigquery_parameters,
    is_compound_googlesql_script,
    map_bigquery_exception,
)
from vertebra.extensions.sqlspec.adapters.bigquery.data_dictionary import (
    BigQueryAsyncDataDictionary,
    BigQuerySyncDataDictionary,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_pyarrow

if TYPE_CHECKING:
    from sqlspec.storage import StorageDestination, StorageFormat, StorageTelemetry

    from vertebra.extensions.sqlspec.adapters.bigquery._typing import BigQueryDriverFeatures

__all__ = (
    "BigQueryAsyncDriver",
    "BigQuerySyncDriver",
)

_NATIVE_EXPORT_FORMATS: Final[dict[str, str]] = {
    "avro": "AVRO",
    "csv": "CSV",
    "json": "JSON",
    "jsonl": "JSON",
    "ndjson": "JSON",
    "parquet": "PARQUET",
}
_CONNECTION_NAME_PARTS: Final[int] = 3


def _reject_tenant_id(tenant_id: Any) -> None:
    if tenant_id is not None:
        msg = "Tenant session enforcement is unsupported for BigQuery"
        raise NotSupportedError(msg)


def _record_cursor_metadata(connection: Any, cursor: Any) -> None:
    """Propagate cursor query metadata to the native BigQuery driver when available."""
    metadata = getattr(cursor, "query_metadata", None)
    if metadata is None:
        return
    after_query = getattr(connection, "_after_query", None)
    if callable(after_query):
        after_query(metadata)


def _extract_arrow_ipc_bytes(driver_instance: Any, source: Any) -> tuple[bytes, int]:
    """Serialize an ``ArrowResult``, ``ArrowCursor``, PyArrow table, or bytes buffer into IPC bytes."""
    if isinstance(source, ArrowResult):
        data = source.data
        row_count = int(source.num_rows)
        return bytes(BigQueryCore.coerce_arrow_ipc_bytes(data)), row_count
    if isinstance(source, (bytes, bytearray, memoryview)):
        raw = bytes(source)
        return raw, 0
    _ = ensure_pyarrow()
    table = driver_instance._coerce_arrow_table(source)
    row_count = int(getattr(table, "num_rows", 0) or 0)
    return bytes(BigQueryCore.coerce_arrow_ipc_bytes(table)), row_count


def _arrow_cursor_to_pyarrow(arrow_cursor: Any, return_format: str) -> tuple[Any, int]:
    """Convert a native ``ArrowCursor`` or PyArrow table into the requested SQLSpec Arrow payload."""
    pa = ensure_pyarrow()
    table = arrow_cursor.to_arrow_table() if hasattr(arrow_cursor, "to_arrow_table") else arrow_cursor
    if not isinstance(table, pa.Table):
        if hasattr(arrow_cursor, "to_arrow_ipc"):
            ipc_bytes = arrow_cursor.to_arrow_ipc()
            with pa.ipc.open_stream(ipc_bytes) as reader:
                table = reader.read_all()
        else:
            table = pa.Table.from_pylist(list(arrow_cursor or ()))
    num_rows = int(table.num_rows)
    if return_format == "reader":
        return pa.RecordBatchReader.from_batches(table.schema, table.to_batches()), num_rows
    if return_format == "batches":
        return table.to_batches(), num_rows
    if return_format == "batch":
        batches = table.to_batches()
        return batches[0] if batches else table.slice(0, 0), num_rows
    return table, num_rows


def _uses_local_bigquery_endpoint(connection: Any) -> bool:
    """Return whether `connection` targets a local HTTP or loopback BigQuery emulator endpoint."""
    cfg = getattr(connection, "config", None)
    endpoint = getattr(cfg, "rest_endpoint", None)
    if not isinstance(endpoint, str) or not endpoint:
        return False
    lowered = endpoint.lower()
    return lowered.startswith("http://") or "localhost" in lowered or "127.0.0.1" in lowered


def _resolve_native_export_format(format_hint: StorageFormat | None) -> str | None:
    """Resolve `format_hint` to a BigQuery ``EXPORT DATA`` format string, or `None` if unsupported."""
    if format_hint is None:
        return "PARQUET"
    return _NATIVE_EXPORT_FORMATS.get(str(format_hint).strip().lower())


def _build_native_export_sql(
    sql: str,
    uri: str,
    export_format: str,
    connection_id: str | None,
) -> str:
    """Build a GoogleSQL ``EXPORT DATA`` statement without modifying bound query parameters."""
    if sql.lstrip().upper().startswith("EXPORT DATA"):
        return sql
    if not uri.isprintable() or any(ch in uri for ch in ("'", '"', "\\", "?", "#")):
        msg = "BigQuery export URI contains unsafe characters."
        raise ImproperConfigurationError(msg)
    connection_clause = ""
    if connection_id is not None:
        parts = str(connection_id).split(".")
        if len(parts) != _CONNECTION_NAME_PARTS or any(
            not part or not all(ch.isascii() and (ch.isalnum() or ch in "-_") for ch in part) for part in parts
        ):
            msg = "native_export_connection must be a project.location.connection identifier"
            raise ImproperConfigurationError(msg)
        connection_clause = f" WITH CONNECTION `{connection_id}`"
    options = f"uri='{uri}', format='{export_format}', overwrite=true"
    if export_format == "CSV":
        options += ", header=true"
    return f"EXPORT DATA{connection_clause} OPTIONS ({options}) AS {sql}"


def _prepare_native_export(
    driver: Any,
    statement: Any,
    destination: StorageDestination,
    parameters: tuple[Any, ...],
    *,
    statement_config: StatementConfig | None,
    format_hint: StorageFormat | None,
    kwargs: dict[str, Any],
) -> tuple[str, str, Any, dict[str, Any]] | None:
    """Return compiled native ``EXPORT DATA`` tuple when remote BigQuery export applies, else ``None``."""
    if not driver.driver_features.get("enable_native_storage", True):
        return None
    if getattr(driver, "storage_pipeline_factory", None) is not None:
        return None
    if _uses_local_bigquery_endpoint(driver.connection):
        return None
    export_format = _resolve_native_export_format(format_hint)
    if export_format is None:
        return None
    native_uri = str(destination)
    if native_uri.startswith("alias://"):
        native_uri = driver._storage_pipeline().resolve_destination(destination).uri
    scheme = native_uri.partition("://")[0].lower()
    export_conn = driver.driver_features.get("native_export_connection")
    if scheme not in {"gs", "gcs"} and not (scheme in {"s3", "azure"} and export_conn is not None):
        return None
    if scheme == "gcs":
        native_uri = "gs://" + native_uri[len("gcs://") :]
    cfg = statement_config or driver.statement_config
    prepared = driver.prepare_statement(statement, parameters, statement_config=cfg, kwargs=kwargs)
    sql, params = compile_bigquery_statement(prepared)
    export_sql = _build_native_export_sql(
        sql,
        native_uri,
        export_format,
        str(export_conn) if export_conn is not None else None,
    )
    telemetry_payload: dict[str, Any] = {
        "destination": native_uri,
        "format": format_hint or "parquet",
        "extra": {"native_export": True},
    }
    return export_sql, native_uri, params, telemetry_payload


class BigQuerySyncDriver(BaseSyncDriver):
    """BigQuery synchronous SQLSpec driver over ``SyncBigQueryDriver`` and ``BigQueryConnectionShim``."""

    __slots__ = ("_data_dictionary",)

    dialect: str = "bigquery"
    _execution_parameter_style: ClassVar[ParameterStyle | None] = ParameterStyle.NAMED_AT

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: BigQueryDriverFeatures | dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
        tenant_predicate: Any = None,
    ) -> None:
        if tenant_predicate is not None:
            msg = "BigQuery does not support tenant_predicate scoping"
            raise NotSupportedError(msg)
        super().__init__(
            connection,
            statement_config or default_statement_config(),
            driver_features=cast("dict[str, Any] | None", driver_features),
            observability=observability,
            shared_pool=shared_pool,
        )
        self._data_dictionary: BigQuerySyncDataDictionary | None = None

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default BigQuery ``StatementConfig`` targeting ``@name`` GoogleSQL binds."""
        return default_statement_config()

    @property
    def data_dictionary(self) -> BigQuerySyncDataDictionary:
        """Return the BigQuery synchronous data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = BigQuerySyncDataDictionary()
        return self._data_dictionary

    def _cache_hit_owner(self) -> type[BaseSyncDriver]:
        """Return `BigQuerySyncDriver` because it overrides `with_cursor` and `resolve_last_inserted_id`."""
        return BigQuerySyncDriver

    def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
        """Return the metadata-recording callables the cache-hit executor calls, or `None` to register no executor."""
        if not callable(getattr(self.native_driver, "cursor", None)):
            return None
        return self._cached_fetch_rows, self._cached_run_dml

    def _cached_fetch_rows(self, sql: str, params: Any = ()) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Run a cached row-returning statement on one cursor, record its metadata, and return `(rows, names)`."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params)
            _record_cursor_metadata(self.connection, cursor)
            description = cursor.description
            if description is None:
                return [], ()
            return cursor.fetchall(), tuple(str(column[0]) for column in description)
        finally:
            cursor.close()

    def _cached_run_dml(self, sql: str, params: Any = ()) -> tuple[int, Any]:
        """Run a cached statement without rows on one cursor, record its metadata, and return `(rowcount, None)`."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params)
            _record_cursor_metadata(self.connection, cursor)
            return BaseSyncDriver._normalize_rowcount(cursor.rowcount), None
        finally:
            cursor.close()

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_bigquery_exception(exc)

    def prepare_statement(
        self,
        statement: Any,
        parameters: Any = (),
        *,
        statement_config: StatementConfig | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> SQL:
        """Prepare a BigQuery SQL statement after rejecting unsupported ``tenant_id`` kwargs."""
        if kwargs and kwargs.get("tenant_id") is not None:
            _reject_tenant_id(kwargs.get("tenant_id"))
        return super().prepare_statement(statement, parameters, statement_config=statement_config, kwargs=kwargs)

    @contextmanager
    def with_cursor(self, connection: Any) -> Iterator[Any]:
        """Yield a native BigQuery cursor, closing it on exit."""
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def resolve_last_inserted_id(self, cursor: Any) -> int | str | None:
        """Return `None` so cached `INSERT` paths stay aligned with BigQuery `dispatch_execute`."""
        _ = cursor
        return None

    def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a compiled BigQuery statement through the native cursor."""
        sql, params = compile_bigquery_statement(statement)
        self._cache_statement(statement)
        cursor.execute(sql, params)
        _record_cursor_metadata(self.connection, cursor)
        description = cursor.description
        if description is not None:
            rows = cursor.fetchall()
            return self.create_execution_result(
                cursor,
                selected_data=rows,
                column_names=[str(col[0]) for col in description],
                data_row_count=len(rows),
                is_select_result=True,
                row_format="tuple",
            )
        return self.create_execution_result(cursor, rowcount_override=max(cursor.rowcount, 0))

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a parameterized batch against BigQuery through the native cursor."""
        sql, prepared = statement.compile()
        param_sets = encode_typed_bigquery_many(prepared)
        if not param_sets:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        cursor.executemany(sql, param_sets)
        _record_cursor_metadata(self.connection, cursor)
        return self.create_execution_result(
            cursor,
            rowcount_override=max(cursor.rowcount, 0),
            is_many_result=True,
        )

    def dispatch_execute_script(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute BigQuery scripts without splitting compound ``BEGIN ... END`` procedural blocks."""
        raw_sql = statement.raw_sql
        params = encode_typed_bigquery_parameters(statement.parameters)
        if isinstance(params, (dict, list, tuple)) and len(params) == 0:
            params = None
        if is_compound_googlesql_script(raw_sql):
            cursor.execute(raw_sql, params)
            _record_cursor_metadata(self.connection, cursor)
            return self.create_execution_result(
                cursor,
                statement_count=1,
                successful_statements=1,
                is_script_result=True,
            )
        statements = self.split_script_statements(raw_sql, statement.statement_config)
        for chunk in statements:
            cursor.execute(chunk, None)
            _record_cursor_metadata(self.connection, cursor)
        return self.create_execution_result(
            cursor,
            statement_count=len(statements),
            successful_statements=len(statements),
            is_script_result=True,
        )

    def select_to_arrow(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: StatementConfig | None = None,
        return_format: str = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any | None = None,
        **kwargs: Any,
    ) -> ArrowResult:
        """Execute a BigQuery query and return an ``ArrowResult`` via Storage Read API when available."""
        _ = ensure_pyarrow()
        native_arrow_fn = getattr(self.connection, "select_to_arrow", None)
        if callable(native_arrow_fn):
            stmt_kwargs = dict(kwargs)
            native_kwargs: dict[str, Any] = {}
            for key in ("threshold_rows", "selected_fields", "row_restriction", "max_streams"):
                val = stmt_kwargs.pop(key, None)
                if val is not None:
                    native_kwargs[key] = val
            prepared = self.prepare_statement(
                statement,
                parameters,
                statement_config=statement_config or self.statement_config,
                kwargs=stmt_kwargs,
            )
            sql, params = compile_bigquery_statement(prepared)
            exc_handler = self.handle_database_exceptions()
            with exc_handler:
                arrow_cursor = native_arrow_fn(sql, params, **native_kwargs)
                arrow_data, num_rows = _arrow_cursor_to_pyarrow(arrow_cursor, return_format)
                return ArrowResult(statement=prepared, data=arrow_data, rows_affected=num_rows)
            self._check_pending_exception(exc_handler)
        return super().select_to_arrow(
            statement,
            *parameters,
            statement_config=statement_config,
            return_format=cast("Any", return_format),
            native_only=native_only,
            batch_size=batch_size,
            arrow_schema=arrow_schema,
            **kwargs,
        )

    def select_to_storage(
        self,
        statement: SQL | str,
        destination: StorageDestination,
        /,
        *parameters: Any,
        statement_config: StatementConfig | None = None,
        partitioner: dict[str, object] | None = None,
        format_hint: StorageFormat | None = None,
        telemetry: StorageTelemetry | None = None,
        **kwargs: Any,
    ) -> StorageBridgeJob:
        """Export eligible remote queries via native ``EXPORT DATA`` or stream Arrow batches to storage."""
        native_export = _prepare_native_export(
            self,
            statement,
            destination,
            parameters,
            statement_config=statement_config,
            format_hint=format_hint,
            kwargs=dict(kwargs),
        )
        if native_export is not None:
            export_sql, _, params, native_telemetry = native_export
            exc_handler = self.handle_database_exceptions()
            with exc_handler:
                self.connection.execute(export_sql, params)
                self._attach_partition_telemetry(cast("Any", native_telemetry), partitioner)
                return self._storage_job(cast("Any", native_telemetry), telemetry, status="completed")
            self._check_pending_exception(exc_handler)
            return self._storage_job({"destination": str(destination)}, telemetry, status="failed")
        arrow_result = self._select_for_storage(statement, *parameters, statement_config=statement_config, **kwargs)
        sync_pipeline = self._storage_pipeline()
        telemetry_payload = self._write_storage_result(
            arrow_result,
            destination,
            format_hint=format_hint,
            pipeline=sync_pipeline,
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    def load_from_arrow(
        self,
        table: str,
        source: Any,
        *,
        partitioner: dict[str, object] | None = None,
        overwrite: bool = False,
        telemetry: Any | None = None,
        stage_table: bool | str | None = None,
        conflict_keys: Sequence[str] | None = None,
        on_conflict: str = "update",
        exclude_update_columns: Sequence[str] | None = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        type_casts: Mapping[str, str] | None = None,
    ) -> StorageBridgeJob:
        """Load Arrow records into BigQuery using atomic Storage Write API ``PENDING`` commits."""
        _ = (
            partitioner,
            stage_table,
            conflict_keys,
            on_conflict,
            exclude_update_columns,
            drop_stage_table,
            drop_indexes,
            concurrent_indexes,
            type_casts,
        )
        _ = ensure_pyarrow()
        payload, inferred_rows = _extract_arrow_ipc_bytes(self, source)
        load_fn = getattr(self.connection, "load_from_arrow", None)
        if not callable(load_fn):
            return super().load_from_arrow(
                table,
                source,
                partitioner=partitioner,
                overwrite=overwrite,
                telemetry=telemetry,
            )
        write_disposition = "WRITE_TRUNCATE" if overwrite else "WRITE_APPEND"
        exc_handler = self.handle_database_exceptions()
        with exc_handler:
            loaded = load_fn(
                table,
                payload,
                mode="PENDING",
                write_disposition=write_disposition,
            )
            rows_processed = int(loaded) if isinstance(loaded, int) else inferred_rows
            return self._storage_job(
                {
                    "destination": table,
                    "rows_processed": rows_processed,
                    "bytes_processed": len(payload),
                    "format": "arrow_ipc",
                    "extra": {
                        "mode": "PENDING",
                        "write_disposition": write_disposition,
                    },
                },
                telemetry,
                status="completed",
            )
        self._check_pending_exception(exc_handler)
        return self._storage_job({"destination": table}, telemetry, status="failed")

    def dry_run(
        self,
        statement: str | SQL,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        tenant_id: Any = None,
    ) -> Any:
        """Estimate query bytes processed and cacheability via native BigQuery dry run."""
        _reject_tenant_id(tenant_id)
        sql_obj = (
            statement
            if isinstance(statement, SQL)
            else SQL(statement, parameters, statement_config=self.statement_config)
        )
        sql, params = compile_bigquery_statement(sql_obj)
        exc_handler = self.handle_database_exceptions()
        with exc_handler:
            return self.connection.dry_run(sql, params)
        self._check_pending_exception(exc_handler)
        return None

    def execute_stack(
        self,
        stack: StatementStack,
        *,
        continue_on_error: bool = False,
        tenant_id: Any = None,
    ) -> tuple[StackResult, ...]:
        """Execute a ``StatementStack`` after rejecting ``tenant_id`` scoping."""
        _reject_tenant_id(tenant_id)
        return super().execute_stack(stack, continue_on_error=continue_on_error)

    def create_object_table(self, config: ObjectTableConfig, *, if_not_exists: bool = True) -> None:
        """Create a BigQuery Object Table referencing unstructured Cloud Storage objects."""
        self.connection.create_object_table(config, if_not_exists=if_not_exists)

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
        """List BigQuery Object Table entries preserving native ``ObjectRef`` metadata."""
        return list(
            self.connection.list_object_table_refs(
                config,
                store=store,
                where=where,
                limit=limit,
                presign=presign,
                expires_in_secs=expires_in_secs,
            )
        )

    def begin(self) -> None:
        """Begin a BigQuery multi-statement session transaction."""
        self.connection.begin()

    def commit(self) -> None:
        """Commit the active BigQuery multi-statement session transaction."""
        self.connection.commit()

    def rollback(self) -> None:
        """Rollback the active BigQuery multi-statement session transaction."""
        self.connection.rollback()

    def create_savepoint(self, name: str) -> None:
        """Raise ``NotImplementedError`` because BigQuery sessions do not support savepoints."""
        _ = name
        msg = "BigQuery does not support SQL savepoints"
        raise NotImplementedError(msg)


class BigQueryAsyncDriver(BaseAsyncDriver):
    """BigQuery asynchronous SQLSpec driver over ``AsyncBigQueryDriver``."""

    __slots__ = ("_data_dictionary",)

    dialect: str = "bigquery"
    _execution_parameter_style: ClassVar[ParameterStyle | None] = ParameterStyle.NAMED_AT

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: BigQueryDriverFeatures | dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
        tenant_predicate: Any = None,
    ) -> None:
        if tenant_predicate is not None:
            msg = "BigQuery does not support tenant_predicate scoping"
            raise NotSupportedError(msg)
        super().__init__(
            connection,
            statement_config or default_statement_config(),
            driver_features=cast("dict[str, Any] | None", driver_features),
            observability=observability,
            shared_pool=shared_pool,
        )
        self._data_dictionary: BigQueryAsyncDataDictionary | None = None

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default BigQuery ``StatementConfig`` targeting ``@name`` GoogleSQL binds."""
        return default_statement_config()

    @property
    def data_dictionary(self) -> BigQueryAsyncDataDictionary:
        """Return the BigQuery asynchronous data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = BigQueryAsyncDataDictionary()
        return self._data_dictionary

    def _cache_hit_owner(self) -> type[BaseAsyncDriver]:
        """Return `BigQueryAsyncDriver` because it overrides `with_cursor` and `resolve_last_inserted_id`."""
        return BigQueryAsyncDriver

    def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
        """Return the metadata-recording callables the cache-hit executor calls, or `None` to register no executor."""
        if not callable(getattr(self.native_driver, "cursor", None)):
            return None
        return self._cached_fetch_rows, self._cached_run_dml

    async def _cached_fetch_rows(self, sql: str, params: Any = ()) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Run a cached row-returning statement on one cursor, record its metadata, and return `(rows, names)`."""
        cursor = self.connection.cursor()
        try:
            await cursor.execute(sql, params)
            _record_cursor_metadata(self.connection, cursor)
            description = cursor.description
            if description is None:
                return [], ()
            return await cursor.fetchall(), tuple(str(column[0]) for column in description)
        finally:
            await cursor.close()

    async def _cached_run_dml(self, sql: str, params: Any = ()) -> tuple[int, Any]:
        """Run a cached statement without rows on one cursor, record its metadata, and return `(rowcount, None)`."""
        cursor = self.connection.cursor()
        try:
            await cursor.execute(sql, params)
            _record_cursor_metadata(self.connection, cursor)
            return BaseSyncDriver._normalize_rowcount(cursor.rowcount), None
        finally:
            await cursor.close()

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_bigquery_exception(exc)

    def prepare_statement(
        self,
        statement: Any,
        parameters: Any = (),
        *,
        statement_config: StatementConfig | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> SQL:
        """Prepare a BigQuery SQL statement after rejecting unsupported ``tenant_id`` kwargs."""
        if kwargs and kwargs.get("tenant_id") is not None:
            _reject_tenant_id(kwargs.get("tenant_id"))
        return super().prepare_statement(statement, parameters, statement_config=statement_config, kwargs=kwargs)

    @asynccontextmanager
    async def with_cursor(self, connection: Any) -> AsyncIterator[Any]:
        """Yield an async native BigQuery cursor, closing it on exit."""
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    def resolve_last_inserted_id(self, cursor: Any) -> int | str | None:
        """Return `None` so async cached `INSERT` paths stay aligned with BigQuery `dispatch_execute`."""
        _ = cursor
        return None

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a compiled BigQuery statement asynchronously through the native cursor."""
        sql, params = compile_bigquery_statement(statement)
        self._cache_statement(statement)
        await cursor.execute(sql, params)
        _record_cursor_metadata(self.connection, cursor)
        description = cursor.description
        if description is not None:
            rows = await cursor.fetchall()
            return self.create_execution_result(
                cursor,
                selected_data=rows,
                column_names=[str(col[0]) for col in description],
                data_row_count=len(rows),
                is_select_result=True,
                row_format="tuple",
            )
        return self.create_execution_result(cursor, rowcount_override=max(cursor.rowcount, 0))

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a parameterized batch asynchronously against BigQuery through the native cursor."""
        sql, prepared = statement.compile()
        param_sets = encode_typed_bigquery_many(prepared)
        if not param_sets:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        await cursor.executemany(sql, param_sets)
        _record_cursor_metadata(self.connection, cursor)
        return self.create_execution_result(
            cursor,
            rowcount_override=max(cursor.rowcount, 0),
            is_many_result=True,
        )

    async def dispatch_execute_script(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute BigQuery procedural scripts asynchronously without breaking ``BEGIN ... END`` blocks."""
        raw_sql = statement.raw_sql
        params = encode_typed_bigquery_parameters(statement.parameters)
        if isinstance(params, (dict, list, tuple)) and len(params) == 0:
            params = None
        if is_compound_googlesql_script(raw_sql):
            await cursor.execute(raw_sql, params)
            _record_cursor_metadata(self.connection, cursor)
            return self.create_execution_result(
                cursor,
                statement_count=1,
                successful_statements=1,
                is_script_result=True,
            )
        statements = self.split_script_statements(raw_sql, statement.statement_config)
        for chunk in statements:
            await cursor.execute(chunk, None)
            _record_cursor_metadata(self.connection, cursor)
        return self.create_execution_result(
            cursor,
            statement_count=len(statements),
            successful_statements=len(statements),
            is_script_result=True,
        )

    async def select_to_arrow(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: StatementConfig | None = None,
        return_format: str = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any | None = None,
        **kwargs: Any,
    ) -> ArrowResult:
        """Execute a BigQuery query asynchronously and return an ``ArrowResult``."""
        _ = ensure_pyarrow()
        native_arrow_fn = getattr(self.connection, "select_to_arrow", None)
        if callable(native_arrow_fn):
            stmt_kwargs = dict(kwargs)
            native_kwargs: dict[str, Any] = {}
            for key in ("threshold_rows", "selected_fields", "row_restriction", "max_streams"):
                val = stmt_kwargs.pop(key, None)
                if val is not None:
                    native_kwargs[key] = val
            prepared = self.prepare_statement(
                statement,
                parameters,
                statement_config=statement_config or self.statement_config,
                kwargs=stmt_kwargs,
            )
            sql, params = compile_bigquery_statement(prepared)
            exc_handler = self.handle_database_exceptions()
            async with exc_handler:
                arrow_cursor = await cast("Any", native_arrow_fn)(sql, params, **native_kwargs)
                arrow_data, num_rows = _arrow_cursor_to_pyarrow(arrow_cursor, return_format)
                return ArrowResult(statement=prepared, data=arrow_data, rows_affected=num_rows)
            self._check_pending_exception(exc_handler)
        return await super().select_to_arrow(
            statement,
            *parameters,
            statement_config=statement_config,
            return_format=cast("Any", return_format),
            native_only=native_only,
            batch_size=batch_size,
            arrow_schema=arrow_schema,
            **kwargs,
        )

    async def select_to_storage(
        self,
        statement: SQL | str,
        destination: StorageDestination,
        /,
        *parameters: Any,
        statement_config: StatementConfig | None = None,
        partitioner: dict[str, object] | None = None,
        format_hint: StorageFormat | None = None,
        telemetry: StorageTelemetry | None = None,
        **kwargs: Any,
    ) -> StorageBridgeJob:
        """Asynchronously export via native ``EXPORT DATA`` or stream Arrow batches to storage."""
        native_export = _prepare_native_export(
            self,
            statement,
            destination,
            parameters,
            statement_config=statement_config,
            format_hint=format_hint,
            kwargs=dict(kwargs),
        )
        if native_export is not None:
            export_sql, _, params, native_telemetry = native_export
            exc_handler = self.handle_database_exceptions()
            async with exc_handler:
                await self.connection.execute(export_sql, params)
                self._attach_partition_telemetry(cast("Any", native_telemetry), partitioner)
                return self._storage_job(cast("Any", native_telemetry), telemetry, status="completed")
            self._check_pending_exception(exc_handler)
            return self._storage_job({"destination": str(destination)}, telemetry, status="failed")
        arrow_result = await self._select_for_storage(
            statement, *parameters, statement_config=statement_config, **kwargs
        )
        async_pipeline = self._storage_pipeline()
        telemetry_payload = await self._write_storage_result(
            arrow_result,
            destination,
            format_hint=format_hint,
            pipeline=async_pipeline,
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    async def load_from_arrow(
        self,
        table: str,
        source: Any,
        *,
        partitioner: dict[str, object] | None = None,
        overwrite: bool = False,
        telemetry: Any | None = None,
        stage_table: bool | str | None = None,
        conflict_keys: Sequence[str] | None = None,
        on_conflict: str = "update",
        exclude_update_columns: Sequence[str] | None = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        type_casts: Mapping[str, str] | None = None,
    ) -> StorageBridgeJob:
        """Load Arrow records into BigQuery asynchronously using Storage Write API ``PENDING`` commits."""
        _ = (
            partitioner,
            stage_table,
            conflict_keys,
            on_conflict,
            exclude_update_columns,
            drop_stage_table,
            drop_indexes,
            concurrent_indexes,
            type_casts,
        )
        _ = ensure_pyarrow()
        payload, inferred_rows = _extract_arrow_ipc_bytes(self, source)
        load_fn = getattr(self.connection, "load_from_arrow", None)
        if not callable(load_fn):
            return await super().load_from_arrow(
                table,
                source,
                partitioner=partitioner,
                overwrite=overwrite,
                telemetry=telemetry,
            )
        write_disposition = "WRITE_TRUNCATE" if overwrite else "WRITE_APPEND"
        exc_handler = self.handle_database_exceptions()
        async with exc_handler:
            loaded = await cast("Any", load_fn)(
                table,
                payload,
                mode="PENDING",
                write_disposition=write_disposition,
            )
            rows_processed = int(loaded) if isinstance(loaded, int) else inferred_rows
            return self._storage_job(
                {
                    "destination": table,
                    "rows_processed": rows_processed,
                    "bytes_processed": len(payload),
                    "format": "arrow_ipc",
                    "extra": {
                        "mode": "PENDING",
                        "write_disposition": write_disposition,
                    },
                },
                telemetry,
                status="completed",
            )
        self._check_pending_exception(exc_handler)
        return self._storage_job({"destination": table}, telemetry, status="failed")

    async def execute_stack(
        self,
        stack: StatementStack,
        *,
        continue_on_error: bool = False,
        tenant_id: Any = None,
    ) -> tuple[StackResult, ...]:
        """Asynchronously execute a ``StatementStack`` after rejecting ``tenant_id`` scoping."""
        _reject_tenant_id(tenant_id)
        return await super().execute_stack(stack, continue_on_error=continue_on_error)

    async def dry_run(
        self,
        statement: str | SQL,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        tenant_id: Any = None,
    ) -> Any:
        """Estimate query bytes processed asynchronously via native BigQuery dry run."""
        _reject_tenant_id(tenant_id)
        sql_obj = (
            statement
            if isinstance(statement, SQL)
            else SQL(statement, parameters, statement_config=self.statement_config)
        )
        sql, params = compile_bigquery_statement(sql_obj)
        exc_handler = self.handle_database_exceptions()
        async with exc_handler:
            return await self.connection.dry_run(sql, params)
        self._check_pending_exception(exc_handler)
        return None

    async def create_object_table(self, config: ObjectTableConfig, *, if_not_exists: bool = True) -> None:
        """Create a BigQuery Object Table asynchronously referencing Cloud Storage objects."""
        await self.connection.create_object_table(config, if_not_exists=if_not_exists)

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
        """List BigQuery Object Table entries asynchronously preserving native ``ObjectRef`` metadata."""
        return list(
            await self.connection.list_object_table_refs(
                config,
                store=store,
                where=where,
                limit=limit,
                presign=presign,
                expires_in_secs=expires_in_secs,
            )
        )

    async def begin(self) -> None:
        """Begin an async BigQuery multi-statement session transaction."""
        await self.connection.begin()

    async def commit(self) -> None:
        """Commit the active async BigQuery multi-statement session transaction."""
        await self.connection.commit()

    async def rollback(self) -> None:
        """Rollback the active async BigQuery multi-statement session transaction."""
        await self.connection.rollback()

    async def create_savepoint(self, name: str) -> None:
        """Raise ``NotImplementedError`` because BigQuery sessions do not support savepoints."""
        _ = name
        msg = "BigQuery does not support SQL savepoints"
        raise NotImplementedError(msg)
