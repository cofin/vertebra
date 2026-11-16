"""BigQuery synchronous and asynchronous SQLSpec drivers over pure-Rust Vertebra drivers."""

from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, cast

import pyarrow as pa
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

__all__ = (
    "BigQueryAsyncDriver",
    "BigQuerySyncDriver",
)


def _reject_tenant_id(tenant_id: Any) -> None:
    if tenant_id is not None:
        msg = "Tenant session enforcement is unsupported for BigQuery"
        raise NotSupportedError(msg)


def _normalize_row_dicts(
    raw_rows: Any,
    column_names: Sequence[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Convert driver result rows and optional column names into uniform dictionaries."""
    rows_list = list(raw_rows) if raw_rows is not None else []
    names = list(column_names) if column_names else []
    if not rows_list:
        return [], names
    first = rows_list[0]
    if isinstance(first, Mapping):
        if not names:
            names = [str(key) for key in first]
        return [dict(row) for row in rows_list], names
    if isinstance(first, (tuple, list)):
        if not names:
            names = [f"col_{idx}" for idx in range(len(first))]
        dict_rows = [{names[idx]: val for idx, val in enumerate(row)} for row in rows_list]
        return dict_rows, names
    if not names:
        names = ["value"]
    return [{names[0]: row} for row in rows_list], names


def _extract_arrow_ipc_bytes(driver_instance: Any, source: Any) -> tuple[bytes, int]:
    """Serialize an ``ArrowResult``, ``ArrowCursor``, PyArrow table, or bytes buffer into IPC bytes."""
    if isinstance(source, ArrowResult):
        data = source.data
        row_count = int(source.num_rows)
        return bytes(BigQueryCore.coerce_arrow_ipc_bytes(data)), row_count
    if isinstance(source, (bytes, bytearray, memoryview)):
        raw = bytes(source)
        return raw, 0
    table = driver_instance._coerce_arrow_table(source)
    row_count = int(getattr(table, "num_rows", 0) or 0)
    return bytes(BigQueryCore.coerce_arrow_ipc_bytes(table)), row_count


def _arrow_cursor_to_pyarrow(arrow_cursor: Any, return_format: str) -> tuple[Any, int]:
    """Convert a native ``ArrowCursor`` or PyArrow table into the requested SQLSpec Arrow payload."""
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


def _execute_sync_select(cursor: Any, sql: str, params: Any) -> tuple[list[dict[str, Any]], list[str]]:
    """Execute a synchronous BigQuery row-returning query preserving zero-row column metadata."""
    if callable(getattr(cursor, "execute_with_description", None)):
        raw_rows, col_names = cursor.execute_with_description(sql, params)
        return _normalize_row_dicts(raw_rows, col_names)
    if callable(getattr(cursor, "select", None)):
        raw_rows = cursor.select(sql, params)
        col_names = getattr(cursor, "last_column_names", None)
        return _normalize_row_dicts(raw_rows, col_names)
    if callable(getattr(cursor, "query", None)):
        job = cursor.query(sql, params)
        raw_rows = job.result() if hasattr(job, "result") else list(job)
        return _normalize_row_dicts(raw_rows, None)
    cursor.execute(sql, params)
    description = getattr(cursor, "description", None)
    col_names = [str(col[0]) for col in description] if description else None
    raw_rows = cursor.fetchall() if callable(getattr(cursor, "fetchall", None)) else []
    return _normalize_row_dicts(raw_rows, col_names)


async def _execute_async_select(cursor: Any, sql: str, params: Any) -> tuple[list[dict[str, Any]], list[str]]:
    """Execute an asynchronous BigQuery row-returning query preserving zero-row column metadata."""
    if callable(getattr(cursor, "execute_with_description", None)):
        raw_rows, col_names = await cursor.execute_with_description(sql, params)
        return _normalize_row_dicts(raw_rows, col_names)
    if callable(getattr(cursor, "select", None)):
        raw_rows = await cursor.select(sql, params)
        col_names = getattr(cursor, "last_column_names", None)
        return _normalize_row_dicts(raw_rows, col_names)
    await cursor.execute(sql, params)
    description = getattr(cursor, "description", None)
    col_names = [str(col[0]) for col in description] if description else None
    fetchall = getattr(cursor, "fetchall", None)
    raw_rows = await fetchall() if callable(fetchall) else []
    return _normalize_row_dicts(raw_rows, col_names)


class BigQuerySyncDriver(BaseSyncDriver):
    """BigQuery synchronous SQLSpec driver over ``SyncBigQueryDriver`` and ``BigQueryConnectionShim``."""

    dialect = "bigquery"
    _execution_parameter_style: ClassVar[ParameterStyle] = ParameterStyle.NAMED_AT

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
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
            driver_features=driver_features,
            observability=observability,
            shared_pool=shared_pool,
        )
        self.dialect = "bigquery"
        self._data_dictionary = BigQuerySyncDataDictionary()

    @property
    def data_dictionary(self) -> BigQuerySyncDataDictionary:
        """Return the BigQuery synchronous data dictionary."""
        return self._data_dictionary

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

    def _compile_for_native(self, statement: SQL) -> tuple[str, Any]:
        sql, params = super()._compile_for_native(statement)
        return sql, encode_typed_bigquery_parameters(params)

    def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a compiled BigQuery statement preserving zero-row column metadata."""
        sql, params = self._compile_for_native(statement)
        if statement.returns_rows():
            dict_rows, column_names = _execute_sync_select(cursor, sql, params)
            return self.create_execution_result(
                cursor,
                selected_data=dict_rows,
                column_names=column_names,
                data_row_count=len(dict_rows),
                is_select_result=True,
            )
        affected = cursor.execute(sql, params)
        rowcount = int(affected) if isinstance(affected, int) else 0
        return self.create_execution_result(cursor, rowcount_override=rowcount)

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a parameterized batch against BigQuery."""
        sql, _ = self._compile_for_native(statement)
        param_sets = encode_typed_bigquery_many(statement.parameters)
        if not param_sets:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        execute_many_fn = getattr(cursor, "execute_many", None)
        if callable(execute_many_fn):
            affected = execute_many_fn(sql, param_sets)
            return self.create_execution_result(
                cursor,
                rowcount_override=int(affected) if isinstance(affected, int) else len(param_sets),
                is_many_result=True,
            )
        total_affected = 0
        for param_set in param_sets:
            res = cursor.execute(sql, param_set)
            total_affected += int(res) if isinstance(res, int) else 1
        return self.create_execution_result(cursor, rowcount_override=total_affected, is_many_result=True)

    def dispatch_execute_script(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute BigQuery scripts without splitting compound ``BEGIN ... END`` procedural blocks."""
        raw_sql = statement.raw_sql
        params = encode_typed_bigquery_parameters(statement.parameters)
        execute_script_fn = getattr(cursor, "execute_script", None)
        if is_compound_googlesql_script(raw_sql):
            if callable(execute_script_fn):
                execute_script_fn(raw_sql, params)
            else:
                cursor.execute(raw_sql, params)
            return self.create_execution_result(
                cursor,
                statement_count=1,
                successful_statements=1,
                is_script_result=True,
            )
        if callable(execute_script_fn):
            script_results = execute_script_fn(raw_sql, params)
            count = len(script_results) if isinstance(script_results, Sequence) else 1
            return self.create_execution_result(
                cursor,
                statement_count=max(count, 1),
                successful_statements=max(count, 1),
                is_script_result=True,
            )
        statements = self.split_script_statements(raw_sql, statement.statement_config)
        for chunk in statements:
            cursor.execute(chunk, None)
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
            if not prepared.is_processed:
                prepared.compile()
            sql, params = self._compile_for_native(prepared)
            arrow_cursor = native_arrow_fn(sql, params, **native_kwargs)
            arrow_data, num_rows = _arrow_cursor_to_pyarrow(arrow_cursor, return_format)
            return ArrowResult(statement=prepared, data=arrow_data, rows_affected=num_rows)
        return super().select_to_arrow(
            statement,
            *parameters,
            statement_config=statement_config,
            return_format=cast(Any, return_format),
            native_only=native_only,
            batch_size=batch_size,
            arrow_schema=arrow_schema,
            **kwargs,
        )

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
        sql, params = self._compile_for_native(sql_obj)
        return self.connection.dry_run(sql, params)

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

    dialect = "bigquery"
    _execution_parameter_style: ClassVar[ParameterStyle] = ParameterStyle.NAMED_AT

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
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
            driver_features=driver_features,
            observability=observability,
            shared_pool=shared_pool,
        )
        self.dialect = "bigquery"
        self._data_dictionary = BigQueryAsyncDataDictionary()

    @property
    def data_dictionary(self) -> BigQueryAsyncDataDictionary:
        """Return the BigQuery asynchronous data dictionary."""
        return self._data_dictionary

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

    def _compile_for_native(self, statement: SQL) -> tuple[str, Any]:
        sql, params = super()._compile_for_native(statement)
        return sql, encode_typed_bigquery_parameters(params)

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a compiled BigQuery statement asynchronously preserving zero-row column metadata."""
        sql, params = self._compile_for_native(statement)
        if statement.returns_rows():
            dict_rows, column_names = await _execute_async_select(cursor, sql, params)
            return self.create_execution_result(
                cursor,
                selected_data=dict_rows,
                column_names=column_names,
                data_row_count=len(dict_rows),
                is_select_result=True,
            )
        affected = await cursor.execute(sql, params)
        rowcount = int(affected) if isinstance(affected, int) else 0
        return self.create_execution_result(cursor, rowcount_override=rowcount)

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a parameterized batch asynchronously against BigQuery."""
        sql, _ = self._compile_for_native(statement)
        param_sets = encode_typed_bigquery_many(statement.parameters)
        if not param_sets:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        execute_many_fn = getattr(cursor, "execute_many", None)
        if callable(execute_many_fn):
            affected = await execute_many_fn(sql, param_sets)
            return self.create_execution_result(
                cursor,
                rowcount_override=int(affected) if isinstance(affected, int) else len(param_sets),
                is_many_result=True,
            )
        total_affected = 0
        for param_set in param_sets:
            res = await cursor.execute(sql, param_set)
            total_affected += int(res) if isinstance(res, int) else 1
        return self.create_execution_result(cursor, rowcount_override=total_affected, is_many_result=True)

    async def dispatch_execute_script(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute BigQuery procedural scripts asynchronously without breaking ``BEGIN ... END`` blocks."""
        raw_sql = statement.raw_sql
        params = encode_typed_bigquery_parameters(statement.parameters)
        execute_script_fn = getattr(cursor, "execute_script", None)
        if is_compound_googlesql_script(raw_sql):
            if callable(execute_script_fn):
                await execute_script_fn(raw_sql, params)
            else:
                await cursor.execute(raw_sql, params)
            return self.create_execution_result(
                cursor,
                statement_count=1,
                successful_statements=1,
                is_script_result=True,
            )
        if callable(execute_script_fn):
            script_results = await execute_script_fn(raw_sql, params)
            count = len(script_results) if isinstance(script_results, Sequence) else 1
            return self.create_execution_result(
                cursor,
                statement_count=max(count, 1),
                successful_statements=max(count, 1),
                is_script_result=True,
            )
        statements = self.split_script_statements(raw_sql, statement.statement_config)
        for chunk in statements:
            await cursor.execute(chunk, None)
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
            if not prepared.is_processed:
                prepared.compile()
            sql, params = self._compile_for_native(prepared)
            arrow_cursor = await native_arrow_fn(sql, params, **native_kwargs)
            arrow_data, num_rows = _arrow_cursor_to_pyarrow(arrow_cursor, return_format)
            return ArrowResult(statement=prepared, data=arrow_data, rows_affected=num_rows)
        return await super().select_to_arrow(
            statement,
            *parameters,
            statement_config=statement_config,
            return_format=cast(Any, return_format),
            native_only=native_only,
            batch_size=batch_size,
            arrow_schema=arrow_schema,
            **kwargs,
        )

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
        loaded = await load_fn(
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
        sql, params = self._compile_for_native(sql_obj)
        return await self.connection.dry_run(sql, params)

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
