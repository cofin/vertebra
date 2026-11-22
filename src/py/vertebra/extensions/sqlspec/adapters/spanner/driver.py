"""Cloud Spanner synchronous and asynchronous SQLSpec drivers over pure-Rust Vertebra drivers."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

from sqlspec.core import SQL, ArrowResult, ParameterStyle, StackResult, StatementConfig, StatementStack
from sqlspec.driver import ExecutionResult
from sqlspec.storage import StorageBridgeJob

from vertebra.db.adapters.spanner import (
    Mutation,
    MutationGroup,
    StalenessBound,
)
from vertebra.db.adapters.spanner.core import SpannerCore
from vertebra.db.driver import NotSupportedError
from vertebra.extensions.sqlspec._driver import (
    BaseAsyncDriver,
    BaseSyncDriver,
)
from vertebra.extensions.sqlspec.adapters.spanner.core import (
    compile_spanner_statement,
    default_statement_config,
    encode_spanner_batch_parameters,
    map_spanner_exception,
    normalize_spanner_dialect,
    sqlspec_dialect_name,
)
from vertebra.extensions.sqlspec.adapters.spanner.data_dictionary import (
    SpannerAsyncDataDictionary,
    SpannerSyncDataDictionary,
)
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_pyarrow
from vertebra.utils.serializers import decode_json

if TYPE_CHECKING:
    from sqlspec.storage import StorageDestination, StorageFormat, StorageTelemetry

    from vertebra.extensions.sqlspec.adapters.spanner._typing import SpannerDriverFeatures

__all__ = (
    "SpannerAsyncDriver",
    "SpannerSyncDriver",
)

_DDL_PREFIXES: tuple[str, ...] = ("CREATE ", "ALTER ", "DROP ")


def _reject_tenant_isolation(
    tenant_predicate: Any,
    tenant_id: Any,
    driver_features: Mapping[str, Any] | None,
) -> None:
    if tenant_predicate is not None or tenant_id is not None:
        msg = "Tenant session enforcement is unsupported for Cloud Spanner"
        raise NotSupportedError(msg)
    if driver_features and (
        driver_features.get("tenant_predicate") is not None or driver_features.get("tenant_id") is not None
    ):
        msg = "Tenant session enforcement is unsupported for Cloud Spanner"
        raise NotSupportedError(msg)


def _infer_spanner_dialect(
    connection: Any, statement_config: StatementConfig | None
) -> Literal["googlesql", "spangres"]:
    candidates = (
        getattr(getattr(connection, "config", None), "dialect", None),
        getattr(connection, "spanner_dialect", None),
        getattr(connection, "dialect", None),
        getattr(statement_config, "dialect", None) if statement_config is not None else None,
    )
    for item in candidates:
        if item and normalize_spanner_dialect(item) == "spangres":
            return "spangres"
    return "googlesql"


def _arrow_cursor_to_pyarrow(arrow_cursor: Any, return_format: str) -> tuple[Any, int]:
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


def _is_ddl_sql(sql: str) -> bool:
    return sql.lstrip().upper().startswith(_DDL_PREFIXES)


def _is_read_sql(sql: str) -> bool:
    text = sql.lstrip()
    while text:
        if text.startswith("--"):
            newline = text.find("\n")
            text = "" if newline < 0 else text[newline + 1 :].lstrip()
            continue
        if text.startswith("/*"):
            end = text.find("*/")
            text = "" if end < 0 else text[end + 2 :].lstrip()
            continue
        if text.startswith("@{"):
            depth = 1
            idx = 2
            while idx < len(text) and depth > 0:
                ch = text[idx]
                if ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                idx += 1
            text = text[idx:].lstrip()
            continue
        break
    upper = text.upper()
    return upper.startswith(("SELECT", "WITH", "GRAPH"))


def _decode_compiled_params(params_json: str | None) -> Any:
    if params_json is None:
        return None
    return decode_json(params_json)


class _ThreadedAsyncCursor:
    """Async adapter offloading a synchronous Spanner cursor onto worker threads."""

    __slots__ = ("_cursor",)

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    @property
    def description(self) -> Any:
        return self._cursor.description

    @property
    def rowcount(self) -> int:
        return int(self._cursor.rowcount)

    async def execute(self, sql: str, params: Any = None) -> Any:
        return await asyncio.to_thread(self._cursor.execute, sql, params)

    async def executemany(self, sql: str, seq_of_params: Any) -> Any:
        return await asyncio.to_thread(self._cursor.executemany, sql, seq_of_params)

    async def fetchall(self) -> list[Any]:
        return list(await asyncio.to_thread(self._cursor.fetchall))

    async def close(self) -> None:
        await asyncio.to_thread(self._cursor.close)


class _DriverDialectDescriptor:
    """Slot-compatible descriptor exposing class-level default dialect and per-instance overrides."""

    __slots__ = ("_default",)

    def __init__(self, default: str) -> None:
        self._default = default

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> str:
        _ = owner
        if instance is None:
            return self._default
        value = getattr(instance, "_dialect_value", None)
        return str(value) if value is not None else self._default

    def __set__(self, instance: Any, value: str) -> None:
        instance._dialect_value = str(value)


class SpannerSyncDriver(BaseSyncDriver):
    """Cloud Spanner synchronous SQLSpec driver supporting GoogleSQL and Spangres."""

    __slots__ = (
        "_active_snapshot",
        "_data_dictionary",
        "_dialect_value",
        "_in_transaction",
        "_read_only",
        "_session_managed_tx",
        "_spanner_dialect",
        "_staleness",
        "_storage_backend",
    )

    dialect: Any = _DriverDialectDescriptor("spanner")
    _execution_parameter_style: ClassVar[ParameterStyle | None] = ParameterStyle.NAMED_AT
    _arrow_execution_options: ClassVar[tuple[str, ...]] = (
        *BaseSyncDriver._arrow_execution_options,
        "data_boost",
        "max_partitions",
        "staleness",
    )

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build the default GoogleSQL Spanner ``StatementConfig``."""
        return default_statement_config()

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: SpannerDriverFeatures | dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
        tenant_predicate: Any = None,
        tenant_id: Any = None,
        read_only: bool = False,
        in_transaction: bool = False,
        staleness: StalenessBound | None = None,
        active_snapshot: Any = None,
        storage_backend: ObjectStoreBackend | None = None,
    ) -> None:
        _reject_tenant_isolation(tenant_predicate, tenant_id, driver_features)
        inferred = _infer_spanner_dialect(connection, statement_config)
        resolved_stmt = statement_config or default_statement_config(inferred)
        if normalize_spanner_dialect(resolved_stmt.dialect) != inferred:
            resolved_stmt = default_statement_config(inferred)
        super().__init__(
            connection,
            resolved_stmt,
            driver_features=cast("dict[str, Any] | None", driver_features),
            observability=observability,
            shared_pool=shared_pool,
        )
        self._spanner_dialect: Literal["googlesql", "spangres"] = inferred
        self._dialect_value: str = sqlspec_dialect_name(inferred)
        self._read_only: bool = bool(read_only)
        self._in_transaction: bool = bool(in_transaction)
        self._session_managed_tx: bool = bool(in_transaction)
        self._staleness: StalenessBound | None = staleness
        self._active_snapshot: Any = active_snapshot
        self._storage_backend: ObjectStoreBackend | None = storage_backend
        self._data_dictionary = SpannerSyncDataDictionary(dialect=self.dialect)

    @property
    def core_driver(self) -> Any:
        """Return the underlying synchronous Vertebra driver connection."""
        return self.connection

    @property
    def spanner_dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the internal Spanner dialect literal for native Rust client calls."""
        return self._spanner_dialect

    @property
    def is_read_only(self) -> bool:
        """Return whether this session is bound to a lock-free ``ReadOnly`` snapshot."""
        return self._read_only

    @property
    def in_transaction(self) -> bool:
        """Return whether this session is bound to an active ``ReadWrite`` transaction."""
        return self._in_transaction

    @property
    def staleness(self) -> StalenessBound | None:
        """Return the ``StalenessBound`` configured on this snapshot session, if any."""
        return self._staleness

    @property
    def data_dictionary(self) -> SpannerSyncDataDictionary:
        """Return the Spanner synchronous data dictionary."""
        return self._data_dictionary

    def _cache_hit_owner(self) -> type[BaseSyncDriver]:
        """Return `SpannerSyncDriver` because it overrides `with_cursor` and `resolve_last_inserted_id`."""
        return SpannerSyncDriver

    def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
        """Return the session-aware callables the cache-hit executor calls, or `None` to register no executor."""
        if not callable(getattr(self.native_driver, "cursor", None)):
            return None
        return self._cached_fetch_rows, self._cached_run_dml

    def _cached_fetch_rows(self, sql: str, params: Any = ()) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Run a cached row-returning statement on a session-configured cursor and return `(rows, names)`."""
        if self._read_only and not _is_read_sql(sql):
            self._require_writable("DML/DDL")
        cursor = self.connection.cursor(**self._cursor_kwargs())
        try:
            cursor.execute(sql, params)
            description = cursor.description
            if description is None:
                return [], ()
            return cursor.fetchall(), tuple(column[0] for column in description)
        finally:
            cursor.close()

    def _cached_run_dml(self, sql: str, params: Any = ()) -> tuple[int, Any]:
        """Run a cached statement without rows on a session-configured cursor and return `(rowcount, None)`."""
        if self._read_only and not _is_read_sql(sql):
            self._require_writable("DML/DDL")
        cursor = self.connection.cursor(**self._cursor_kwargs())
        try:
            cursor.execute(sql, params)
            return BaseSyncDriver._normalize_rowcount(cursor.rowcount), None
        finally:
            cursor.close()

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_spanner_exception(exc)

    def _cursor_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        staleness = (
            self._staleness if self._staleness is not None else getattr(self._active_snapshot, "staleness", None)
        )
        if staleness is not None:
            kwargs["staleness"] = staleness
        features = self.driver_features or {}
        request_tag = features.get("request_tag")
        if request_tag is not None:
            kwargs["request_tag"] = request_tag
        transaction_tag = features.get("transaction_tag")
        if transaction_tag is not None:
            kwargs["transaction_tag"] = transaction_tag
        return kwargs

    @contextmanager
    def with_cursor(self, connection: Any) -> Iterator[Any]:
        """Yield a native Spanner cursor configured with session staleness and tags, closing on exit."""
        cursor = connection.cursor(**self._cursor_kwargs())
        try:
            yield cursor
        finally:
            cursor.close()

    def _require_writable(self, action: str) -> None:
        if self._read_only:
            msg = f"Cannot execute {action} inside a read-only Spanner snapshot session"
            raise ImproperConfigurationError(msg)

    def resolve_last_inserted_id(self, cursor: Any) -> int | str | None:
        """Return `None` so cached `INSERT` paths stay aligned with Spanner `dispatch_execute`."""
        _ = cursor
        return None

    def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a compiled query, DML, or DDL statement through the native cursor."""
        sql, params_json = compile_spanner_statement(statement, dialect=self.dialect)
        if self._read_only and not _is_read_sql(sql):
            self._require_writable("DML/DDL")
        self._cache_statement(statement)
        cursor.execute(sql, params_json)
        description = cursor.description
        if description is not None:
            rows = cursor.fetchall()
            return self.create_execution_result(
                cursor,
                selected_data=rows,
                column_names=[column[0] for column in description],
                data_row_count=len(rows),
                is_select_result=True,
                row_format="tuple",
            )
        return self.create_execution_result(cursor, rowcount_override=max(cursor.rowcount, 0))

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a parameterized batch via the native cursor ``executemany`` path."""
        self._require_writable("ExecuteBatchDml")
        sql, prepared = statement.compile()
        param_sets = encode_spanner_batch_parameters(prepared, dialect=self._spanner_dialect)
        if not param_sets:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        json_sets = [
            SpannerCore.encode_parameters_json(param_set, dialect=self._spanner_dialect) for param_set in param_sets
        ]
        cursor.executemany(sql, json_sets)
        return self.create_execution_result(
            cursor,
            rowcount_override=max(cursor.rowcount, 0),
            is_many_result=True,
        )

    def dispatch_execute_script(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a script of Spanner DDL and/or DML statements."""
        statements = self.split_script_statements(statement.raw_sql, statement.statement_config)
        if not statements:
            return self.create_execution_result(
                cursor, statement_count=0, successful_statements=0, is_script_result=True
            )
        ddl_batch = [stmt for stmt in statements if _is_ddl_sql(stmt)]
        if ddl_batch or any(not _is_read_sql(stmt) for stmt in statements):
            self._require_writable("script DML/DDL")
        update_ddl_fn = getattr(self.connection, "update_ddl", None)
        if ddl_batch and len(ddl_batch) == len(statements):
            if callable(update_ddl_fn):
                update_ddl_fn(ddl_batch)
            else:
                for stmt in ddl_batch:
                    cursor.execute(stmt, None)
            return self.create_execution_result(
                cursor,
                statement_count=len(statements),
                successful_statements=len(statements),
                is_script_result=True,
            )
        for stmt in statements:
            if _is_ddl_sql(stmt) and callable(update_ddl_fn):
                update_ddl_fn([stmt])
            else:
                cursor.execute(stmt, None)
        return self.create_execution_result(
            cursor,
            statement_count=len(statements),
            successful_statements=len(statements),
            is_script_result=True,
        )

    def execute_stack(
        self,
        stack: StatementStack,
        *,
        continue_on_error: bool = False,
    ) -> tuple[StackResult, ...]:
        """Execute a ``StatementStack``, coalescing pure DML stacks into ``ExecuteBatchDml``."""
        if not isinstance(stack, StatementStack):
            msg = "execute_stack expects a StatementStack instance"
            raise TypeError(msg)
        if not stack:
            msg = "Cannot execute an empty StatementStack"
            raise ValueError(msg)
        operations = stack.operations
        batch_fn = getattr(self.connection, "execute_batch_dml", None)
        if (
            not continue_on_error
            and not self._read_only
            and callable(batch_fn)
            and all(op.method == "execute" for op in operations)
        ):
            compiled_batch: list[tuple[str, Any]] = []
            all_dml = True
            for op in operations:
                op_kwargs = dict(op.keyword_arguments or {})
                stmt_cfg = op_kwargs.pop("statement_config", None)
                prepared_stmt = self.prepare_statement(
                    op.statement,
                    op.arguments,
                    statement_config=stmt_cfg or self.statement_config,
                    kwargs=op_kwargs,
                )
                if prepared_stmt.returns_rows() or _is_ddl_sql(prepared_stmt.raw_sql):
                    all_dml = False
                    break
                sql, params_json = compile_spanner_statement(prepared_stmt, dialect=self.dialect)
                compiled_batch.append((sql, _decode_compiled_params(params_json)))
            if all_dml and compiled_batch:
                exc_handler = self.handle_database_exceptions()
                with exc_handler:
                    counts = cast("Sequence[Any]", batch_fn(compiled_batch))
                    return tuple(StackResult(rows_affected=int(count)) for count in counts)
                self._check_pending_exception(exc_handler)
        return super().execute_stack(stack, continue_on_error=continue_on_error)

    def execute_partitioned_dml(
        self,
        statement: str | SQL,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        exclude_txn_from_change_streams: bool = False,
    ) -> int:
        """Execute a large-scale Partitioned DML statement across database splits."""
        self._require_writable("Partitioned DML")
        sql_obj = (
            statement
            if isinstance(statement, SQL)
            else SQL(statement, parameters, statement_config=self.statement_config)
        )
        sql, params_json = compile_spanner_statement(sql_obj, dialect=self.dialect)
        params = _decode_compiled_params(params_json)
        pdml_kwargs: dict[str, Any] = {}
        if exclude_txn_from_change_streams:
            pdml_kwargs["exclude_txn_from_change_streams"] = True
        exc_handler = self.handle_database_exceptions()
        with exc_handler:
            return int(self.connection.execute_partitioned_dml(sql, params, **pdml_kwargs))
        self._check_pending_exception(exc_handler)
        return 0

    def execute_batch_dml(
        self,
        statements: Sequence[tuple[str, Mapping[str, Any] | Sequence[Any] | None]],
        *,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> list[int]:
        """Execute multiple DML statements in a single ``ExecuteBatchDml`` RPC."""
        self._require_writable("ExecuteBatchDml")
        compiled: list[tuple[str, Any]] = []
        for sql, params in statements:
            compiled_sql, params_json = compile_spanner_statement(
                SQL(sql, params, statement_config=self.statement_config),
                dialect=self.dialect,
            )
            compiled.append((compiled_sql, _decode_compiled_params(params_json)))
        batch_kwargs: dict[str, Any] = {}
        if request_tag is not None:
            batch_kwargs["request_tag"] = request_tag
        if transaction_tag is not None:
            batch_kwargs["transaction_tag"] = transaction_tag
        exc_handler = self.handle_database_exceptions()
        with exc_handler:
            return [int(c) for c in self.connection.execute_batch_dml(compiled, **batch_kwargs)]
        self._check_pending_exception(exc_handler)
        return []

    def snapshot(self, staleness: StalenessBound | None = None, *, multi_use: bool = True) -> Any:
        """Open a lock-free ReadOnly ``SyncSpannerSnapshot``."""
        return self.connection.snapshot(staleness, multi_use=multi_use)

    def run_in_transaction(
        self,
        fn: Any,
        *,
        max_attempts: int = 5,
        transaction_tag: str | None = None,
        max_commit_delay_ms: int | None = None,
    ) -> Any:
        """Execute ``fn(tx)`` inside an atomic ReadWrite transaction with wound-wait retry."""
        self._require_writable("ReadWrite transaction")
        exc_handler = self.handle_database_exceptions()
        with exc_handler:
            return self.connection.run_in_transaction(
                fn,
                max_attempts=max_attempts,
                transaction_tag=transaction_tag,
                max_commit_delay_ms=max_commit_delay_ms,
            )
        self._check_pending_exception(exc_handler)
        return None

    def apply_mutations(
        self,
        mutations: Sequence[Mutation | Mapping[str, Any]],
        *,
        transaction_tag: str | None = None,
        max_commit_delay_ms: int | None = None,
    ) -> dict[str, Any]:
        """Commit buffered Cloud Spanner ``Mutation`` objects atomically."""
        self._require_writable("mutations")
        exc_handler = self.handle_database_exceptions()
        with exc_handler:
            return dict(
                self.connection.apply_mutations(
                    mutations,
                    transaction_tag=transaction_tag,
                    max_commit_delay_ms=max_commit_delay_ms,
                )
            )
        self._check_pending_exception(exc_handler)
        return {}

    def batch_write(
        self,
        mutation_groups: Sequence[MutationGroup | Sequence[Mutation | Mapping[str, Any]]],
        *,
        exclude_txn_from_change_streams: bool = False,
    ) -> list[dict[str, Any]]:
        """Commit independent ``MutationGroup`` batches via ``BatchWrite``."""
        self._require_writable("BatchWrite")
        exc_handler = self.handle_database_exceptions()
        with exc_handler:
            return list(
                self.connection.batch_write(
                    mutation_groups,
                    exclude_txn_from_change_streams=exclude_txn_from_change_streams,
                )
            )
        self._check_pending_exception(exc_handler)
        return []

    def update_ddl(self, statements: Sequence[str] | str) -> str:
        """Apply Spanner schema DDL statements via DatabaseAdmin."""
        self._require_writable("DDL")
        exc_handler = self.handle_database_exceptions()
        with exc_handler:
            return str(self.connection.update_ddl(statements))
        self._check_pending_exception(exc_handler)
        return ""

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
        """Execute a parallel ``PartitionQuery`` scan into a SQLSpec ``ArrowResult``."""
        _ = ensure_pyarrow()
        native_arrow_fn = getattr(self.connection, "select_to_arrow", None)
        if callable(native_arrow_fn):
            stmt_kwargs = dict(kwargs)
            native_kwargs: dict[str, Any] = {}
            data_boost = stmt_kwargs.pop("data_boost", None)
            data_boost_enabled = stmt_kwargs.pop("data_boost_enabled", None)
            if data_boost is not None:
                native_kwargs["data_boost_enabled"] = bool(data_boost)
            elif data_boost_enabled is not None:
                native_kwargs["data_boost_enabled"] = bool(data_boost_enabled)
            max_partitions = stmt_kwargs.pop("max_partitions", None)
            if max_partitions is not None:
                native_kwargs["max_partitions"] = int(max_partitions)
            staleness = stmt_kwargs.pop("staleness", None)
            effective_staleness = staleness if staleness is not None else self._staleness
            if effective_staleness is not None:
                native_kwargs["staleness"] = effective_staleness
            prepared = self.prepare_statement(
                statement,
                parameters,
                statement_config=statement_config or self.statement_config,
                kwargs=stmt_kwargs,
            )
            sql, params_json = compile_spanner_statement(prepared, dialect=self.dialect)
            params = _decode_compiled_params(params_json)
            exc_handler = self.handle_database_exceptions()
            with exc_handler:
                cursor = native_arrow_fn(sql, params, **native_kwargs)
                arrow_data, num_rows = _arrow_cursor_to_pyarrow(cursor, return_format)
                return ArrowResult(statement=prepared, data=arrow_data, rows_affected=num_rows)
            self._check_pending_exception(exc_handler)
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
        """Stream a Spanner SELECT query directly into object storage via Arrow."""
        _ = ensure_pyarrow()
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
        """Load Arrow records into Cloud Spanner using chunked atomic mutations."""
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
        self._require_writable("load_from_arrow")
        _ = ensure_pyarrow()
        load_fn = getattr(self.connection, "load_from_arrow", None)
        if not callable(load_fn):
            return super().load_from_arrow(
                table,
                source,
                partitioner=partitioner,
                overwrite=overwrite,
                telemetry=telemetry,
            )
        exc_handler = self.handle_database_exceptions()
        with exc_handler:
            loaded = int(cast("Any", load_fn)(table, source, upsert=not overwrite))
            return self._storage_job(
                {
                    "destination": table,
                    "rows_processed": loaded,
                    "format": "arrow",
                    "extra": {"upsert": not overwrite},
                },
                telemetry,
                status="completed",
            )
        self._check_pending_exception(exc_handler)
        return self._storage_job({"destination": table}, telemetry, status="failed")

    def begin(self) -> None:
        """Begin an explicit interactive Spanner ``ReadWrite`` transaction."""
        self._require_writable("ReadWrite transaction")
        if self._session_managed_tx and self._in_transaction:
            return
        self.connection.begin()
        self._in_transaction = True

    def commit(self) -> None:
        """Commit the active interactive Spanner ``ReadWrite`` transaction."""
        if self._read_only or (self._session_managed_tx and self._in_transaction):
            return
        self.connection.commit()
        self._in_transaction = False

    def rollback(self) -> None:
        """Roll back the active interactive Spanner ``ReadWrite`` transaction."""
        if not self._read_only:
            self.connection.rollback()
            self._in_transaction = False

    def create_savepoint(self, name: str) -> None:
        """Raise ``NotSupportedError`` because Cloud Spanner does not support SQL savepoints."""
        _ = name
        msg = "Cloud Spanner does not support SQL savepoints"
        raise NotSupportedError(msg)


class SpannerAsyncDriver(BaseAsyncDriver):
    """Cloud Spanner asynchronous SQLSpec driver supporting GoogleSQL and Spangres."""

    __slots__ = (
        "_active_snapshot",
        "_data_dictionary",
        "_dialect_value",
        "_in_transaction",
        "_read_only",
        "_session_managed_tx",
        "_spanner_dialect",
        "_staleness",
        "_storage_backend",
    )

    dialect: Any = _DriverDialectDescriptor("spanner")
    _execution_parameter_style: ClassVar[ParameterStyle | None] = ParameterStyle.NAMED_AT
    _arrow_execution_options: ClassVar[tuple[str, ...]] = (
        *BaseAsyncDriver._arrow_execution_options,
        "data_boost",
        "max_partitions",
        "staleness",
    )

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build the default GoogleSQL Spanner ``StatementConfig``."""
        return default_statement_config()

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: SpannerDriverFeatures | dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
        tenant_predicate: Any = None,
        tenant_id: Any = None,
        read_only: bool = False,
        in_transaction: bool = False,
        staleness: StalenessBound | None = None,
        active_snapshot: Any = None,
        storage_backend: ObjectStoreBackend | None = None,
    ) -> None:
        _reject_tenant_isolation(tenant_predicate, tenant_id, driver_features)
        inferred = _infer_spanner_dialect(connection, statement_config)
        resolved_stmt = statement_config or default_statement_config(inferred)
        if normalize_spanner_dialect(resolved_stmt.dialect) != inferred:
            resolved_stmt = default_statement_config(inferred)
        super().__init__(
            connection,
            resolved_stmt,
            driver_features=cast("dict[str, Any] | None", driver_features),
            observability=observability,
            shared_pool=shared_pool,
        )
        self._spanner_dialect: Literal["googlesql", "spangres"] = inferred
        self._dialect_value: str = sqlspec_dialect_name(inferred)
        self._read_only: bool = bool(read_only)
        self._in_transaction: bool = bool(in_transaction)
        self._session_managed_tx: bool = bool(in_transaction)
        self._staleness: StalenessBound | None = staleness
        self._active_snapshot: Any = active_snapshot
        self._storage_backend: ObjectStoreBackend | None = storage_backend
        self._data_dictionary = SpannerAsyncDataDictionary(dialect=self.dialect)

    @property
    def core_driver(self) -> Any:
        """Return the underlying asynchronous Vertebra driver connection."""
        return self.connection

    @property
    def spanner_dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the internal Spanner dialect literal for native Rust client calls."""
        return self._spanner_dialect

    @property
    def is_read_only(self) -> bool:
        """Return whether this session is bound to a lock-free ``ReadOnly`` snapshot."""
        return self._read_only

    @property
    def in_transaction(self) -> bool:
        """Return whether this session is bound to an active ``ReadWrite`` transaction."""
        return self._in_transaction

    @property
    def staleness(self) -> StalenessBound | None:
        """Return the ``StalenessBound`` configured on this snapshot session, if any."""
        return self._staleness

    @property
    def data_dictionary(self) -> SpannerAsyncDataDictionary:
        """Return the Spanner asynchronous data dictionary."""
        return self._data_dictionary

    def _cache_hit_owner(self) -> type[BaseAsyncDriver]:
        """Return `SpannerAsyncDriver` because it overrides `with_cursor` and `resolve_last_inserted_id`."""
        return SpannerAsyncDriver

    def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
        """Return the session-aware callables the cache-hit executor calls, or `None` to register no executor."""
        if not callable(getattr(self.native_driver, "cursor", None)):
            return None
        return self._cached_fetch_rows, self._cached_run_dml

    async def _cached_fetch_rows(self, sql: str, params: Any = ()) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Run a cached row-returning statement on a session-configured cursor and return `(rows, names)`."""
        if self._read_only and not _is_read_sql(sql):
            self._require_writable("DML/DDL")
        cursor = self.connection.cursor(**self._cursor_kwargs())
        try:
            await cursor.execute(sql, params)
            description = cursor.description
            if description is None:
                return [], ()
            return await cursor.fetchall(), tuple(column[0] for column in description)
        finally:
            await cursor.close()

    async def _cached_run_dml(self, sql: str, params: Any = ()) -> tuple[int, Any]:
        """Run a cached statement without rows on a session-configured cursor and return `(rowcount, None)`."""
        if self._read_only and not _is_read_sql(sql):
            self._require_writable("DML/DDL")
        cursor = self.connection.cursor(**self._cursor_kwargs())
        try:
            await cursor.execute(sql, params)
            return BaseSyncDriver._normalize_rowcount(cursor.rowcount), None
        finally:
            await cursor.close()

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_spanner_exception(exc)

    def _cursor_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        staleness = (
            self._staleness if self._staleness is not None else getattr(self._active_snapshot, "staleness", None)
        )
        if staleness is not None:
            kwargs["staleness"] = staleness
        features = self.driver_features or {}
        request_tag = features.get("request_tag")
        if request_tag is not None:
            kwargs["request_tag"] = request_tag
        transaction_tag = features.get("transaction_tag")
        if transaction_tag is not None:
            kwargs["transaction_tag"] = transaction_tag
        return kwargs

    @asynccontextmanager
    async def with_cursor(self, connection: Any) -> AsyncIterator[Any]:
        """Yield an async native Spanner cursor configured with session staleness and tags, closing on exit."""
        cursor_fn = getattr(connection, "cursor", None)
        cursor: Any
        if callable(cursor_fn):
            cursor = cursor_fn(**self._cursor_kwargs())
        else:
            sync_conn = getattr(connection, "_sync", None)
            if sync_conn is None or not callable(getattr(sync_conn, "cursor", None)):
                msg = f"{type(connection).__name__} does not provide cursor()"
                raise AttributeError(msg)
            cursor = _ThreadedAsyncCursor(sync_conn.cursor(**self._cursor_kwargs()))
        try:
            yield cursor
        finally:
            await cursor.close()

    def _require_writable(self, action: str) -> None:
        if self._read_only:
            msg = f"Cannot execute {action} inside a read-only Spanner snapshot session"
            raise ImproperConfigurationError(msg)

    def resolve_last_inserted_id(self, cursor: Any) -> int | str | None:
        """Return `None` so async cached `INSERT` paths stay aligned with Spanner `dispatch_execute`."""
        _ = cursor
        return None

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Asynchronously execute a compiled query, DML, or DDL statement through the native cursor."""
        sql, params_json = compile_spanner_statement(statement, dialect=self.dialect)
        if self._read_only and not _is_read_sql(sql):
            self._require_writable("DML/DDL")
        self._cache_statement(statement)
        await cursor.execute(sql, params_json)
        description = cursor.description
        if description is not None:
            rows = await cursor.fetchall()
            return self.create_execution_result(
                cursor,
                selected_data=rows,
                column_names=[column[0] for column in description],
                data_row_count=len(rows),
                is_select_result=True,
                row_format="tuple",
            )
        return self.create_execution_result(cursor, rowcount_override=max(cursor.rowcount, 0))

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Asynchronously execute a parameterized batch via the native cursor ``executemany`` path."""
        self._require_writable("ExecuteBatchDml")
        sql, prepared = statement.compile()
        param_sets = encode_spanner_batch_parameters(prepared, dialect=self._spanner_dialect)
        if not param_sets:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        json_sets = [
            SpannerCore.encode_parameters_json(param_set, dialect=self._spanner_dialect) for param_set in param_sets
        ]
        await cursor.executemany(sql, json_sets)
        return self.create_execution_result(
            cursor,
            rowcount_override=max(cursor.rowcount, 0),
            is_many_result=True,
        )

    async def dispatch_execute_script(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Asynchronously execute a script of Spanner DDL and/or DML statements."""
        statements = self.split_script_statements(statement.raw_sql, statement.statement_config)
        if not statements:
            return self.create_execution_result(
                cursor, statement_count=0, successful_statements=0, is_script_result=True
            )
        ddl_batch = [stmt for stmt in statements if _is_ddl_sql(stmt)]
        if ddl_batch or any(not _is_read_sql(stmt) for stmt in statements):
            self._require_writable("script DML/DDL")
        update_ddl_fn = getattr(self.connection, "update_ddl", None)
        if ddl_batch and len(ddl_batch) == len(statements):
            if callable(update_ddl_fn):
                await cast("Any", update_ddl_fn)(ddl_batch)
            else:
                for stmt in ddl_batch:
                    await cursor.execute(stmt, None)
            return self.create_execution_result(
                cursor,
                statement_count=len(statements),
                successful_statements=len(statements),
                is_script_result=True,
            )
        for stmt in statements:
            if _is_ddl_sql(stmt) and callable(update_ddl_fn):
                await cast("Any", update_ddl_fn)([stmt])
            else:
                await cursor.execute(stmt, None)
        return self.create_execution_result(
            cursor,
            statement_count=len(statements),
            successful_statements=len(statements),
            is_script_result=True,
        )

    async def execute_stack(
        self,
        stack: StatementStack,
        *,
        continue_on_error: bool = False,
    ) -> tuple[StackResult, ...]:
        """Asynchronously execute a ``StatementStack``, coalescing pure DML stacks into ``ExecuteBatchDml``."""
        if not isinstance(stack, StatementStack):
            msg = "execute_stack expects a StatementStack instance"
            raise TypeError(msg)
        if not stack:
            msg = "Cannot execute an empty StatementStack"
            raise ValueError(msg)
        operations = stack.operations
        batch_fn = getattr(self.connection, "execute_batch_dml", None)
        if (
            not continue_on_error
            and not self._read_only
            and callable(batch_fn)
            and all(op.method == "execute" for op in operations)
        ):
            compiled_batch: list[tuple[str, Any]] = []
            all_dml = True
            for op in operations:
                op_kwargs = dict(op.keyword_arguments or {})
                stmt_cfg = op_kwargs.pop("statement_config", None)
                prepared_stmt = self.prepare_statement(
                    op.statement,
                    op.arguments,
                    statement_config=stmt_cfg or self.statement_config,
                    kwargs=op_kwargs,
                )
                if prepared_stmt.returns_rows() or _is_ddl_sql(prepared_stmt.raw_sql):
                    all_dml = False
                    break
                sql, params_json = compile_spanner_statement(prepared_stmt, dialect=self.dialect)
                compiled_batch.append((sql, _decode_compiled_params(params_json)))
            if all_dml and compiled_batch:
                exc_handler = self.handle_database_exceptions()
                async with exc_handler:
                    counts = await cast("Any", batch_fn)(compiled_batch)
                    return tuple(StackResult(rows_affected=int(count)) for count in counts)
                self._check_pending_exception(exc_handler)
        return await super().execute_stack(stack, continue_on_error=continue_on_error)

    async def execute_partitioned_dml(
        self,
        statement: str | SQL,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        exclude_txn_from_change_streams: bool = False,
    ) -> int:
        """Asynchronously execute a large-scale Partitioned DML statement across database splits."""
        self._require_writable("Partitioned DML")
        sql_obj = (
            statement
            if isinstance(statement, SQL)
            else SQL(statement, parameters, statement_config=self.statement_config)
        )
        sql, params_json = compile_spanner_statement(sql_obj, dialect=self.dialect)
        params = _decode_compiled_params(params_json)
        pdml_kwargs: dict[str, Any] = {}
        if exclude_txn_from_change_streams:
            pdml_kwargs["exclude_txn_from_change_streams"] = True
        exc_handler = self.handle_database_exceptions()
        async with exc_handler:
            return int(await self.connection.execute_partitioned_dml(sql, params, **pdml_kwargs))
        self._check_pending_exception(exc_handler)
        return 0

    async def execute_batch_dml(
        self,
        statements: Sequence[tuple[str, Mapping[str, Any] | Sequence[Any] | None]],
        *,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> list[int]:
        """Asynchronously execute multiple DML statements in a single ``ExecuteBatchDml`` RPC."""
        self._require_writable("ExecuteBatchDml")
        compiled: list[tuple[str, Any]] = []
        for sql, params in statements:
            compiled_sql, params_json = compile_spanner_statement(
                SQL(sql, params, statement_config=self.statement_config),
                dialect=self.dialect,
            )
            compiled.append((compiled_sql, _decode_compiled_params(params_json)))
        batch_kwargs: dict[str, Any] = {}
        if request_tag is not None:
            batch_kwargs["request_tag"] = request_tag
        if transaction_tag is not None:
            batch_kwargs["transaction_tag"] = transaction_tag
        exc_handler = self.handle_database_exceptions()
        async with exc_handler:
            return [int(c) for c in await self.connection.execute_batch_dml(compiled, **batch_kwargs)]
        self._check_pending_exception(exc_handler)
        return []

    def snapshot(self, staleness: StalenessBound | None = None, *, multi_use: bool = True) -> Any:
        """Open a lock-free ReadOnly ``AsyncSpannerSnapshot``."""
        return self.connection.snapshot(staleness, multi_use=multi_use)

    async def run_in_transaction(
        self,
        fn: Any,
        *,
        max_attempts: int = 5,
        transaction_tag: str | None = None,
        max_commit_delay_ms: int | None = None,
    ) -> Any:
        """Asynchronously execute ``fn(tx)`` inside an atomic ReadWrite transaction."""
        self._require_writable("ReadWrite transaction")
        exc_handler = self.handle_database_exceptions()
        async with exc_handler:
            return await self.connection.run_in_transaction(
                fn,
                max_attempts=max_attempts,
                transaction_tag=transaction_tag,
                max_commit_delay_ms=max_commit_delay_ms,
            )
        self._check_pending_exception(exc_handler)
        return None

    async def apply_mutations(
        self,
        mutations: Sequence[Mutation | Mapping[str, Any]],
        *,
        transaction_tag: str | None = None,
        max_commit_delay_ms: int | None = None,
    ) -> dict[str, Any]:
        """Asynchronously commit buffered Cloud Spanner ``Mutation`` objects atomically."""
        self._require_writable("mutations")
        exc_handler = self.handle_database_exceptions()
        async with exc_handler:
            return dict(
                await self.connection.apply_mutations(
                    mutations,
                    transaction_tag=transaction_tag,
                    max_commit_delay_ms=max_commit_delay_ms,
                )
            )
        self._check_pending_exception(exc_handler)
        return {}

    async def batch_write(
        self,
        mutation_groups: Sequence[MutationGroup | Sequence[Mutation | Mapping[str, Any]]],
        *,
        exclude_txn_from_change_streams: bool = False,
    ) -> list[dict[str, Any]]:
        """Asynchronously commit independent ``MutationGroup`` batches via ``BatchWrite``."""
        self._require_writable("BatchWrite")
        exc_handler = self.handle_database_exceptions()
        async with exc_handler:
            return list(
                await self.connection.batch_write(
                    mutation_groups,
                    exclude_txn_from_change_streams=exclude_txn_from_change_streams,
                )
            )
        self._check_pending_exception(exc_handler)
        return []

    async def update_ddl(self, statements: Sequence[str] | str) -> str:
        """Asynchronously apply Spanner schema DDL statements via DatabaseAdmin."""
        self._require_writable("DDL")
        exc_handler = self.handle_database_exceptions()
        async with exc_handler:
            return str(await self.connection.update_ddl(statements))
        self._check_pending_exception(exc_handler)
        return ""

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
        """Asynchronously execute a parallel ``PartitionQuery`` scan into a SQLSpec ``ArrowResult``."""
        _ = ensure_pyarrow()
        native_arrow_fn = getattr(self.connection, "select_to_arrow", None)
        if callable(native_arrow_fn):
            stmt_kwargs = dict(kwargs)
            native_kwargs: dict[str, Any] = {}
            data_boost = stmt_kwargs.pop("data_boost", None)
            data_boost_enabled = stmt_kwargs.pop("data_boost_enabled", None)
            if data_boost is not None:
                native_kwargs["data_boost_enabled"] = bool(data_boost)
            elif data_boost_enabled is not None:
                native_kwargs["data_boost_enabled"] = bool(data_boost_enabled)
            max_partitions = stmt_kwargs.pop("max_partitions", None)
            if max_partitions is not None:
                native_kwargs["max_partitions"] = int(max_partitions)
            staleness = stmt_kwargs.pop("staleness", None)
            effective_staleness = staleness if staleness is not None else self._staleness
            if effective_staleness is not None:
                native_kwargs["staleness"] = effective_staleness
            prepared = self.prepare_statement(
                statement,
                parameters,
                statement_config=statement_config or self.statement_config,
                kwargs=stmt_kwargs,
            )
            sql, params_json = compile_spanner_statement(prepared, dialect=self.dialect)
            params = _decode_compiled_params(params_json)
            exc_handler = self.handle_database_exceptions()
            async with exc_handler:
                cursor = await cast("Any", native_arrow_fn)(sql, params, **native_kwargs)
                arrow_data, num_rows = _arrow_cursor_to_pyarrow(cursor, return_format)
                return ArrowResult(statement=prepared, data=arrow_data, rows_affected=num_rows)
            self._check_pending_exception(exc_handler)
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
        """Asynchronously stream a Spanner SELECT query directly into object storage via Arrow."""
        _ = ensure_pyarrow()
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
        """Asynchronously load Arrow records into Cloud Spanner using chunked atomic mutations."""
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
        self._require_writable("load_from_arrow")
        _ = ensure_pyarrow()
        load_fn = getattr(self.connection, "load_from_arrow", None)
        if not callable(load_fn):
            return await super().load_from_arrow(
                table,
                source,
                partitioner=partitioner,
                overwrite=overwrite,
                telemetry=telemetry,
            )
        exc_handler = self.handle_database_exceptions()
        async with exc_handler:
            loaded = int(await cast("Any", load_fn)(table, source, upsert=not overwrite))
            return self._storage_job(
                {
                    "destination": table,
                    "rows_processed": loaded,
                    "format": "arrow",
                    "extra": {"upsert": not overwrite},
                },
                telemetry,
                status="completed",
            )
        self._check_pending_exception(exc_handler)
        return self._storage_job({"destination": table}, telemetry, status="failed")

    async def begin(self) -> None:
        """Begin an interactive async Spanner ``ReadWrite`` transaction."""
        self._require_writable("ReadWrite transaction")
        if self._session_managed_tx and self._in_transaction:
            return
        await self.connection.begin()
        self._in_transaction = True

    async def commit(self) -> None:
        """Commit the active interactive async Spanner ``ReadWrite`` transaction."""
        if self._read_only or (self._session_managed_tx and self._in_transaction):
            return
        await self.connection.commit()
        self._in_transaction = False

    async def rollback(self) -> None:
        """Roll back the active interactive async Spanner ``ReadWrite`` transaction."""
        if not self._read_only:
            await self.connection.rollback()
            self._in_transaction = False

    async def create_savepoint(self, name: str) -> None:
        """Raise ``NotSupportedError`` because Cloud Spanner does not support SQL savepoints."""
        _ = name
        msg = "Cloud Spanner does not support SQL savepoints"
        raise NotSupportedError(msg)
