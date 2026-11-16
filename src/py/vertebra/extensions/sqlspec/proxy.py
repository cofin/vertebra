"""Shared-memory proxy drivers and session context managers for SQLSpec configurations."""

from __future__ import annotations

import mmap
import os
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast, overload

import msgspec
from sqlspec.core import SQL, ArrowResult, SQLResult, StackResult
from sqlspec.core.stack import StatementStack
from sqlspec.core.statement import StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.driver._stream import AsyncRowStream, SyncRowStream
from sqlspec.exceptions import ImproperConfigurationError as SQLSpecImproperConfigurationError
from sqlspec.exceptions import StackExecutionError
from sqlspec.utils.schema import to_value_type

from vertebra.db.arrow import ArrowCursor, read_arrow_shm
from vertebra.extensions.broker import (
    AsyncSHMBrokerTransport,
    SyncBrokerLeaseSource,
    SyncSHMBrokerTransport,
)
from vertebra.extensions.broker.errors import LeaseQuarantinedError
from vertebra.extensions.sqlspec._driver import (
    DeferredAsyncExceptionHandler,
    DeferredSyncExceptionHandler,
    compile_many_transport,
    unwrap_typed_parameters,
)
from vertebra.extensions.sqlspec.provider import (
    BrokerRpcRequest,
    BrokerRpcResponse,
    SQLSpecAsyncProvider,
    SQLSpecSyncProvider,
    _BaseSQLSpecProvider,
    _coerce_pyarrow_table,
    _unlink_arrow_shm,
)
from vertebra.typing import ImproperConfigurationError, VertebraError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import (
    decode_msgpack,
    encode_msgpack_bytes,
)
from vertebra.utils.sync_tools import SyncPool
from vertebra.utils.type_guards import is_coroutine_callable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from types import TracebackType

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import Statement, StatementFilter
    from sqlspec.driver._common import ExecutionResult
    from sqlspec.typing import ArrowReturnFormat, SchemaT, StatementParameters, ValueT

__all__ = (
    "ProxyAsyncDriver",
    "ProxyAsyncSessionContext",
    "ProxySyncDriver",
    "ProxySyncSessionContext",
)


class ProxySyncDriver(SyncDriverAdapterBase):
    """Synchronous SQLSpec proxy driver executing queries through SQLSpecSyncProvider and SHM leases."""

    dialect: str = "sqlspec"

    def __init__(
        self,
        provider: SQLSpecSyncProvider | Callable[[BrokerRpcRequest], BrokerRpcResponse] | Callable[[bytes], bytes],
        broker: Any = None,
        *,
        pool_name: str = "default",
        worker_pid: int | None = None,
        slot_idx: int = 0,
        generation: int = 0,
        statement_config: Any = None,
        dialect: str | None = None,
        driver_features: dict[str, Any] | None = None,
    ) -> None:
        """Initialize ProxySyncDriver with a SQLSpecSyncProvider or RPC transport callable."""
        if isinstance(provider, _BaseSQLSpecProvider):
            self.provider: SQLSpecSyncProvider | None = provider
            self._transport_fn: Callable[[Any], Any] = provider.handle_request
            self._transport_mode: str | None = "rpc"
            resolved_broker = broker if broker is not None else provider.broker
            upstream_cfg = provider.resolve_upstream_optional(pool_name)
        elif callable(provider):
            self.provider = None
            self._transport_fn = provider
            self._transport_mode = None
            resolved_broker = broker
            upstream_cfg = None
        else:
            raise ImproperConfigurationError("ProxySyncDriver requires a SQLSpecSyncProvider or callable RPC transport")

        resolved_stmt_cfg = statement_config or getattr(upstream_cfg, "statement_config", None) or StatementConfig()
        resolved_feats = (
            driver_features if driver_features is not None else dict(getattr(upstream_cfg, "driver_features", {}) or {})
        )
        super().__init__(
            connection=self,
            statement_config=resolved_stmt_cfg,
            driver_features=resolved_feats,
        )
        resolved_dialect = (
            dialect
            or getattr(upstream_cfg, "dialect", None)
            or getattr(resolved_stmt_cfg, "dialect", None)
            or "sqlspec"
        )
        self.dialect = str(resolved_dialect)
        self.broker = resolved_broker
        self.pool_name = pool_name
        self.worker_pid = worker_pid if worker_pid is not None else os.getpid()
        self.slot_idx = slot_idx
        self.generation = generation
        self.last_response: BrokerRpcResponse | None = None
        self._broker_lease: Any = None
        self._leased: bool = generation > 0
        self._context_entered: bool = False
        self._in_tx: bool = False
        self._transaction_depth: int = 0
        self._request_seq: int = 0

    @property
    def in_transaction(self) -> bool:
        """Return True when the driver has an active transaction pinned to its lease."""
        return self._in_tx

    @property
    def is_leased(self) -> bool:
        """Return True when the driver holds an active broker lease."""
        return self._leased

    @property
    def data_dictionary(self) -> Any:
        """Return optional metadata dictionary (not exposed over RPC proxy)."""
        return None

    def acquire_lease(self) -> tuple[int, int]:
        """Acquire a connection lease and pin an upstream session in SQLSpecSyncProvider."""
        if self._leased:
            return self.slot_idx, self.generation

        slot_idx = self.slot_idx
        generation = 0
        if self.broker is not None:
            lease = self._acquire_broker_lease()
            self._broker_lease = lease
            slot_idx, generation = self._extract_lease_coords(lease)

        try:
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="acquire",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=slot_idx,
                    generation=generation,
                ),
                record_last=False,
            )
            self._raise_for_error(resp)
        except BaseException:
            failed_lease = self._broker_lease
            self._broker_lease = None
            if failed_lease is not None:
                failed_slot, failed_gen = self._extract_lease_coords(failed_lease)
                self._release_broker_lease(failed_lease, slot_idx=failed_slot, generation=failed_gen)
            raise
        self.slot_idx = resp.slot_idx
        self.generation = resp.generation
        self._leased = True
        self._in_tx = resp.in_tx
        return self.slot_idx, self.generation

    def release_lease(self) -> None:
        """Release the active connection lease and unpin the upstream session."""
        if not self._leased:
            return
        was_in_tx = self._in_tx
        self._in_tx = False
        self._transaction_depth = 0
        self._leased = False
        lease = self._broker_lease
        self._broker_lease = None
        try:
            rel_resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="release",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    in_tx=was_in_tx,
                ),
                record_last=False,
            )
            self._raise_for_error(rel_resp)
        except BaseException as exc:
            self._quarantine_broker_lease(lease)
            if isinstance(exc, Exception) and not isinstance(exc, LeaseQuarantinedError):
                raise LeaseQuarantinedError(self.slot_idx, self.generation, str(exc)) from exc
            raise
        self._release_broker_lease(lease)

    def close(self) -> None:
        """Close the proxy driver and release its held lease."""
        self._context_entered = False
        self.release_lease()

    def begin(self) -> None:
        """Begin a transaction pinned to the current connection lease."""
        self.acquire_lease()
        resp = self._send_rpc(
            BrokerRpcRequest(
                request_id=self._next_request_id(),
                op="begin",
                pool_name=self.pool_name,
                worker_pid=self.worker_pid,
                slot_idx=self.slot_idx,
                generation=self.generation,
                in_tx=self._in_tx,
            )
        )
        self._raise_for_error(resp)
        self._in_tx = True
        if self._transaction_depth == 0:
            self._transaction_depth = 1

    def commit(self) -> None:
        """Commit the active pinned transaction."""
        try:
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="commit",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            self._in_tx = False
            self._transaction_depth = 0
        finally:
            if not self._context_entered:
                self.release_lease()

    def rollback(self) -> None:
        """Roll back the active pinned transaction."""
        if not self._leased and not self._in_tx:
            self._transaction_depth = 0
            return
        try:
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="rollback",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            self._in_tx = False
            self._transaction_depth = 0
        finally:
            if not self._context_entered:
                self.release_lease()

    def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single SQL statement on the proxy driver."""
        with self._operation_lease():
            compiled_sql, compiled_params, prepared = self._compile_statement(statement)
            if prepared.returns_rows():
                resp = self._send_rpc(
                    BrokerRpcRequest(
                        request_id=self._next_request_id(),
                        op="select",
                        pool_name=self.pool_name,
                        worker_pid=self.worker_pid,
                        slot_idx=self.slot_idx,
                        generation=self.generation,
                        sql=compiled_sql,
                        params=compiled_params,
                        in_tx=self._in_tx,
                    )
                )
                self._raise_for_error(resp)
                dict_rows, columns = self._unpack_select_rows(resp)
                return self.create_execution_result(
                    cursor,
                    selected_data=dict_rows,
                    column_names=columns,
                    data_row_count=len(dict_rows),
                    is_select_result=True,
                )
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="execute",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            return self.create_execution_result(cursor, rowcount_override=int(resp.rowcount))

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a SQL statement with multiple parameter sets on the proxy driver."""
        params = getattr(statement, "parameters", ())
        rowcount = int(self.execute_many(statement, list(params)))
        return self.create_execution_result(cursor, rowcount_override=rowcount, is_many_result=True)

    @overload
    def select_stream(
        self,
        statement: SQL | Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT],
        statement_config: StatementConfig | None = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> SyncRowStream[SchemaT]: ...

    @overload
    def select_stream(
        self,
        statement: SQL | Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: None = None,
        statement_config: StatementConfig | None = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> SyncRowStream[dict[str, Any]]: ...

    def select_stream(
        self,
        statement: SQL | Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT] | None = None,
        statement_config: StatementConfig | None = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> SyncRowStream[SchemaT] | SyncRowStream[dict[str, Any]]:
        """Execute a query and stream rows in chunks, failing when native_only=True is requested."""
        if chunk_size < 1:
            msg = "chunk_size must be greater than or equal to 1"
            raise ValueError(msg)
        sql_statement = self.prepare_statement(
            statement, parameters, statement_config=statement_config or self.statement_config, kwargs=kwargs
        )
        stream = self.dispatch_select_stream(sql_statement, chunk_size)
        if stream is not None:
            return stream._with_schema_type(schema_type)
        if native_only:
            msg = (
                f"Adapter '{type(self).__name__}' does not support native row streaming. "
                "Use native_only=False to allow eager fallback (not bounded-memory), "
                "or switch to an adapter with native row streaming support."
            )
            raise SQLSpecImproperConfigurationError(msg)
        return SyncRowStream(_LazySelectSyncRowSource(self, sql_statement, chunk_size), schema_type=schema_type)

    def dispatch_select_stream(self, statement: SQL, chunk_size: int) -> SyncRowStream[dict[str, Any]] | None:
        """Return None so buffered streaming requires native_only=False fallback."""
        _ = (statement, chunk_size)
        return None

    def handle_database_exceptions(self) -> DeferredSyncExceptionHandler:
        """Return sync exception handler for proxy driver operations."""
        return DeferredSyncExceptionHandler()

    @contextmanager
    def with_cursor(self, connection: Any) -> Iterator[Any]:
        """Yield the active proxy connection as a cursor handle."""
        yield connection

    def execute(
        self,
        statement: SQL | Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a SQL statement synchronously and return the affected row count."""
        with self._operation_lease():
            compiled_sql, compiled_params, _ = self._compile_statement(
                statement,
                parameters,
                statement_config=statement_config,
                kwargs=kwargs,
            )
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="execute",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            return resp.rowcount

    def execute_many(
        self,
        statement: SQL | Statement | QueryBuilder,
        /,
        parameters: Sequence[StatementParameters],
        *filters: StatementParameters | StatementFilter,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a batch SQL statement across parameters and return affected row count."""
        with self._operation_lease():
            _prepared, compiled_sql, compiled_many = compile_many_transport(
                self,
                statement,
                parameters,
                filters,
                statement_config=statement_config,
                kwargs=kwargs,
            )
            normalized_many: list[list[Any] | dict[str, Any]] = [
                item if isinstance(item, dict) else list(item) for item in compiled_many
            ]
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="execute_many",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    many_params=normalized_many,
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            return resp.rowcount

    def execute_script(
        self,
        statement: str | SQL,
        /,
        *parameters: StatementParameters | StatementFilter,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> SQLResult:
        """Execute a multi-statement script on a single leased proxy session."""
        with self._operation_lease():
            return super().execute_script(statement, *parameters, statement_config=statement_config, **kwargs)

    @overload
    def select(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT],
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> list[SchemaT]: ...

    @overload
    def select(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]: ...

    def select(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT] | None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> list[SchemaT] | list[dict[str, Any]]:
        """Execute a SELECT statement and return rows, transparently reading spilled Arrow SHM."""
        with self._operation_lease():
            compiled_sql, compiled_params, _ = self._compile_statement(
                statement,
                parameters,
                statement_config=statement_config,
                kwargs=kwargs,
            )
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            if resp.is_arrow_shm:
                if schema_type is not None and schema_type is not dict:
                    return self._unpack_select_typed(resp, schema_type)
                dict_rows, _ = self._unpack_select_rows(resp)
                return dict_rows

            if schema_type is not None and schema_type is not dict:
                return self._convert_rows(self._select_inline_dicts(resp.rows, resp.columns), schema_type)
            return self._select_inline_dicts(resp.rows, resp.columns)

    @overload
    def select_one(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT],
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> SchemaT: ...

    @overload
    def select_one(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]: ...

    def select_one(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT] | None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> SchemaT | dict[str, Any]:
        """Execute a SELECT statement and return exactly one row."""
        with self._operation_lease():
            compiled_sql, compiled_params, _ = self._compile_statement(
                statement,
                parameters,
                statement_config=statement_config,
                kwargs=kwargs,
            )
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_one",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            dict_rows, _ = self._unpack_select_rows(resp)
            if not dict_rows:
                raise VertebraError("ValueError: No row found for select_one")
            return cast("SchemaT | dict[str, Any]", self._convert_single(dict_rows[0], schema_type))

    @overload
    def select_one_or_none(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT],
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> SchemaT | None: ...

    @overload
    def select_one_or_none(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None: ...

    def select_one_or_none(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT] | None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> SchemaT | dict[str, Any] | None:
        """Execute a SELECT statement and return at most one row or None."""
        with self._operation_lease():
            compiled_sql, compiled_params, _ = self._compile_statement(
                statement,
                parameters,
                statement_config=statement_config,
                kwargs=kwargs,
            )
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_one_or_none",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            dict_rows, _ = self._unpack_select_rows(resp)
            if not dict_rows:
                return None
            return cast("SchemaT | dict[str, Any]", self._convert_single(dict_rows[0], schema_type))

    @overload
    def select_value(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        value_type: type[ValueT],
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> ValueT: ...

    @overload
    def select_value(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        value_type: None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> Any: ...

    def select_value(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        value_type: type[ValueT] | None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> ValueT | Any:
        """Execute a SELECT statement and return the first column of the first row."""
        with self._operation_lease():
            compiled_sql, compiled_params, _ = self._compile_statement(
                statement,
                parameters,
                statement_config=statement_config,
                kwargs=kwargs,
            )
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_value",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            if value_type is not None and resp.scalar is not None:
                return to_value_type(resp.scalar, value_type)
            return resp.scalar

    @overload
    def select_value_or_none(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        value_type: type[ValueT],
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> ValueT | None: ...

    @overload
    def select_value_or_none(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        value_type: None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> Any: ...

    def select_value_or_none(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        value_type: type[ValueT] | None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> ValueT | Any | None:
        """Execute a SELECT statement and return a single scalar value or None."""
        with self._operation_lease():
            compiled_sql, compiled_params, _ = self._compile_statement(
                statement,
                parameters,
                statement_config=statement_config,
                kwargs=kwargs,
            )
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_value_or_none",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            if resp.scalar is None:
                return None
            if value_type is not None:
                return to_value_type(resp.scalar, value_type)
            return resp.scalar

    @overload
    def select_with_total(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT],
        statement_config: StatementConfig | None = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> tuple[list[SchemaT], int]: ...

    @overload
    def select_with_total(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: None = None,
        statement_config: StatementConfig | None = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> tuple[list[dict[str, Any]], int]: ...

    def select_with_total(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT] | None = None,
        statement_config: StatementConfig | None = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> tuple[list[SchemaT] | list[dict[str, Any]], int]:
        """Execute a SELECT statement and return both page rows and total matching row count."""
        effective_cfg = statement_config or self.statement_config
        sql_statement = self.prepare_statement(statement, parameters, statement_config=effective_cfg, kwargs=kwargs)
        with self._operation_lease():
            if count_with_window:
                rows = self.select(self._with_total_count(sql_statement), statement_config=effective_cfg)
                data, total = self._extract_total_from_rows(rows)
                if schema_type is not None:
                    return cast("list[SchemaT]", self._convert_rows(data, schema_type)), int(total)
                return data, int(total)

            total = int(self.select_value(self._count_query(sql_statement), statement_config=effective_cfg) or 0)
            if schema_type is not None:
                typed_data = cast(
                    "list[SchemaT]",
                    self.select(sql_statement, schema_type=cast("Any", schema_type), statement_config=effective_cfg),
                )
                return typed_data, total
            return self.select(sql_statement, statement_config=effective_cfg), total

    def execute_stack(
        self,
        stack: StatementStack,
        *,
        continue_on_error: bool = False,
    ) -> tuple[StackResult, ...]:
        """Execute a StatementStack sequentially over the active proxy session lease."""
        if not isinstance(stack, StatementStack):
            msg = "execute_stack expects a StatementStack instance"
            raise TypeError(msg)
        if not stack:
            msg = "Cannot execute an empty StatementStack"
            raise ValueError(msg)

        results: list[StackResult] = []
        single_transaction = not continue_on_error
        with self._operation_lease():
            caller_in_tx = self._in_tx
            started_tx = False
            started_op_tx = False
            try:
                if single_transaction and not caller_in_tx:
                    self._send_tx_rpc("begin", next_in_tx=True)
                    started_tx = True

                for index, operation in enumerate(stack.operations):
                    started_op_tx = False
                    if continue_on_error and not caller_in_tx:
                        self._send_tx_rpc("begin", next_in_tx=True)
                        started_op_tx = True
                    try:
                        op_res = self._execute_stack_operation_sync(operation)
                    except Exception as exc:
                        if started_op_tx:
                            started_op_tx = False
                            self._send_tx_rpc("rollback", next_in_tx=False)
                        stack_error = StackExecutionError(
                            index,
                            str(operation.statement),
                            exc,
                            adapter=type(self).__name__,
                            mode="continue-on-error" if continue_on_error else "fail-fast",
                        )
                        if started_tx and not continue_on_error:
                            started_tx = False
                            self._send_tx_rpc("rollback", next_in_tx=False)
                        if continue_on_error:
                            results.append(StackResult.from_error(stack_error))
                            continue
                        raise stack_error from exc
                    if started_op_tx:
                        started_op_tx = False
                        self._send_tx_rpc("commit", next_in_tx=False)
                    results.append(StackResult(result=op_res))

                if started_tx:
                    started_tx = False
                    self._send_tx_rpc("commit", next_in_tx=False)
            except BaseException:
                if started_op_tx:
                    self._send_tx_rpc("rollback", next_in_tx=False)
                if started_tx:
                    self._send_tx_rpc("rollback", next_in_tx=False)
                raise
        return tuple(results)

    def select_to_arrow(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        statement_config: StatementConfig | None = None,
        return_format: ArrowReturnFormat = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any = None,
        **kwargs: Any,
    ) -> ArrowResult:
        """Execute a SELECT statement and return a zero-copy ArrowResult from SharedArrowBuffer."""
        if native_only and import_optional("pyarrow") is None:
            msg = f"Adapter '{type(self).__name__}' requires pyarrow for native Arrow results when native_only=True."
            raise SQLSpecImproperConfigurationError(msg)
        effective_cfg = statement_config or self.statement_config
        with self._operation_lease():
            compiled_sql, compiled_params, prepared = self._compile_statement(
                statement,
                parameters,
                statement_config=effective_cfg,
                kwargs=kwargs,
            )
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_to_arrow",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            if native_only and not resp.is_arrow_shm:
                msg = f"Adapter '{type(self).__name__}' did not return native SHM Arrow results when native_only=True."
                raise SQLSpecImproperConfigurationError(msg)
            if resp.is_arrow_shm:
                try:
                    table = self._read_arrow_shm_table(
                        resp.arrow_shm_name,
                        resp.arrow_byte_len,
                        resp.rowcount,
                        resp.columns,
                    )
                finally:
                    self._release_arrow_shm(resp.arrow_shm_name)
            else:
                dict_rows = self._rows_to_dicts(resp.rows, resp.columns)
                table = self._coerce_inline_arrow_table(dict_rows, columns=resp.columns)
            return self._wrap_arrow_result(
                prepared,
                compiled_sql,
                table,
                statement_config=effective_cfg,
                return_format=return_format,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
            )

    @staticmethod
    def compile_with_broker(
        driver: SyncDriverAdapterBase | AsyncDriverAdapterBase,
        broker: Any,
        dialect: str,
        statement: Any,
        parameters: tuple[Any, ...] = (),
        *,
        statement_config: StatementConfig | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> tuple[str, list[Any] | dict[str, Any], SQL]:
        """Compile a SQL statement, QueryBuilder, filters, and kwargs via SQLSpec."""
        _ = (broker, dialect)
        effective_cfg = statement_config or driver.statement_config
        prepared = driver.prepare_statement(
            statement,
            parameters,
            statement_config=effective_cfg,
            kwargs=kwargs or {},
        )
        compiled_sql, compiled_params = prepared.compile()
        prepared_params = driver.prepare_driver_parameters(
            compiled_params,
            prepared.statement_config,
            prepared_statement=prepared,
        )
        unwrapped_params = unwrap_typed_parameters(prepared_params)
        rpc_params = ProxySyncDriver._coerce_rpc_params(unwrapped_params)
        return compiled_sql, rpc_params, prepared

    def __enter__(self) -> ProxySyncDriver:
        """Enter context manager and acquire a pinned broker lease."""
        self._context_entered = True
        self.acquire_lease()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context manager, rolling back any uncommitted transaction and releasing the lease."""
        _ = (exc_type, exc_val, exc_tb)
        self._context_entered = False
        self.release_lease()

    def _connection_in_transaction(self) -> bool:
        """Return True when the pinned proxy lease holds an active transaction."""
        return self._in_tx

    def _next_request_id(self) -> int:
        """Increment and return the next monotonically increasing request ID."""
        self._request_seq += 1
        return self._request_seq

    @contextmanager
    def _operation_lease(self) -> Iterator[None]:
        """Ensure a lease is held for an operation, releasing it afterwards when not context-pinned."""
        auto_acquired = not self._leased
        if auto_acquired:
            self.acquire_lease()
        try:
            yield
        finally:
            if auto_acquired and not self._context_entered and not self._in_tx:
                self.release_lease()

    def _compile_statement(
        self,
        statement: Any,
        parameters: tuple[Any, ...] = (),
        *,
        statement_config: StatementConfig | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> tuple[str, list[Any] | dict[str, Any], SQL]:
        """Compile statement, parameters, filters, and kwargs using SQLSpec and optional broker cache."""
        return self.compile_with_broker(
            self,
            self.broker,
            self.dialect,
            statement,
            parameters,
            statement_config=statement_config,
            kwargs=kwargs,
        )

    @staticmethod
    def _coerce_rpc_params(compiled_params: Any) -> list[Any] | dict[str, Any]:
        if compiled_params is None:
            return []
        if isinstance(compiled_params, Mapping):
            return dict(compiled_params)
        if isinstance(compiled_params, Sequence) and not isinstance(compiled_params, str | bytes | bytearray):
            return list(compiled_params)
        return [compiled_params]

    @staticmethod
    def _select_inline_dicts(rows: list[Any], columns: Sequence[str]) -> list[dict[str, Any]]:
        """Return inline dict rows directly when already normalized, converting list rows only when needed."""
        if rows and isinstance(rows[0], dict):
            return cast("list[dict[str, Any]]", rows)
        return ProxySyncDriver._rows_to_dicts(rows, columns)

    @staticmethod
    def _read_arrow_shm_cursor(
        shm_name: str,
        byte_len: int,
        row_count: int,
        columns: Sequence[str],
    ) -> ArrowCursor:
        """Open a VTBARROW SHM segment as an ArrowCursor without eager row materialization."""
        native_mod = import_optional("vertebra._vertebra")
        native_cls = getattr(native_mod, "ArrowCursor", None) if native_mod is not None else None
        if native_cls is not None:
            native_cur = native_cls(shm_name, byte_len, row_count)
            return ArrowCursor(native_cur, columns=columns, used_shm=True, row_count=row_count)
        shm_path = Path("/dev/shm") / shm_name.lstrip("/")
        if not shm_path.exists():
            msg = f"Shared Arrow buffer {shm_name!r} does not exist at {shm_path}"
            raise FileNotFoundError(msg)
        fd = os.open(shm_path, os.O_RDONLY)
        try:
            mm = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)
        finally:
            os.close(fd)
        return read_arrow_shm(mm)

    @classmethod
    def _read_arrow_shm_table(
        cls,
        shm_name: str,
        byte_len: int,
        row_count: int,
        columns: Sequence[str],
    ) -> Any:
        """Decode a VTBARROW SHM segment into an Arrow Table or ArrowCursor."""
        cursor = cls._read_arrow_shm_cursor(shm_name, byte_len, row_count, columns)
        pa = import_optional("pyarrow")
        if pa is not None:
            return cursor.to_arrow_table()
        return ArrowCursor(
            bytes(cursor.buffer_view()),
            rows=[tuple(r.get(c) for c in columns) for r in cursor.to_pylist()],
            columns=columns,
        )

    @staticmethod
    def _coerce_inline_arrow_table(
        dict_rows: list[dict[str, Any]],
        *,
        columns: Sequence[str] | None = None,
    ) -> Any:
        """Coerce inline RPC rows into a PyArrow Table when available or an ArrowCursor otherwise."""
        pa = import_optional("pyarrow")
        if pa is not None:
            return _coerce_pyarrow_table(pa, dict_rows, columns=columns)
        cols = tuple(columns) if columns else (tuple(dict_rows[0].keys()) if dict_rows else ())
        tuple_rows = [tuple(row.get(c) for c in cols) for row in dict_rows]
        return ArrowCursor(b"", rows=tuple_rows, columns=cols)

    @staticmethod
    def _hydrate_arrow_table(table: Any, rpc_columns: Sequence[str], schema_type: type[Any]) -> list[Any]:
        """Convert Arrow or ArrowCursor rows and validate them into `schema_type` via msgspec."""
        if isinstance(table, ArrowCursor):
            return ProxySyncDriver._convert_rows(table.to_pylist(), schema_type)
        raw_rows = getattr(table, "_rows", None)
        if raw_rows is not None and not isinstance(table, ArrowCursor):
            return ProxySyncDriver._convert_rows(
                ProxySyncDriver._select_inline_dicts(raw_rows, rpc_columns),
                schema_type,
            )
        if hasattr(table, "to_pylist"):
            return ProxySyncDriver._convert_rows(table.to_pylist(), schema_type)
        return ProxySyncDriver._convert_rows(list(table), schema_type)

    def _unpack_select_typed(self, resp: BrokerRpcResponse, schema_type: type[Any]) -> list[Any]:
        """Read spilled VTBARROW SHM and hydrate into `schema_type`."""
        try:
            table = self._read_arrow_shm_table(
                resp.arrow_shm_name,
                resp.arrow_byte_len,
                resp.rowcount,
                resp.columns,
            )
            return self._hydrate_arrow_table(table, resp.columns, schema_type)
        finally:
            self._release_arrow_shm(resp.arrow_shm_name)

    def _unpack_select_rows(self, resp: BrokerRpcResponse) -> tuple[list[dict[str, Any]], list[str]]:
        if resp.is_arrow_shm:
            try:
                cursor = self._read_arrow_shm_cursor(
                    resp.arrow_shm_name,
                    resp.arrow_byte_len,
                    resp.rowcount,
                    resp.columns,
                )
                dict_rows: list[dict[str, Any]] = cursor.to_pylist()
                columns = list(resp.columns) if resp.columns else list(cursor._columns)
                if not columns:
                    table = cursor.to_arrow_table()
                    if hasattr(table, "column_names"):
                        columns = list(table.column_names)
                return dict_rows, columns
            finally:
                self._release_arrow_shm(resp.arrow_shm_name)
        dict_rows = self._rows_to_dicts(resp.rows, resp.columns)
        columns = list(resp.columns) if resp.columns else (list(dict_rows[0].keys()) if dict_rows else [])
        return dict_rows, columns

    def _execute_stack_operation_sync(self, operation: Any) -> SQLResult | ArrowResult:
        kwargs = dict(operation.keyword_arguments) if operation.keyword_arguments else {}
        if operation.method == "execute":
            compiled_sql, compiled_params, prepared = self._compile_statement(
                operation.statement,
                operation.arguments,
                kwargs=kwargs,
            )
            is_query = prepared.returns_rows()
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select" if is_query else "execute",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            if is_query:
                dict_rows, columns = self._unpack_select_rows(resp)
                return SQLResult(
                    statement=prepared,
                    data=dict_rows,
                    rows_affected=len(dict_rows),
                    column_names=columns,
                    operation_type=prepared.operation_type,
                )
            return SQLResult(
                statement=prepared, rows_affected=int(resp.rowcount), operation_type=prepared.operation_type
            )
        if operation.method == "execute_many":
            if not operation.arguments:
                msg = "execute_many stack operation requires parameter sets"
                raise ValueError(msg)
            parameter_sets = operation.arguments[0]
            filters = operation.arguments[1:]
            rc = int(self.execute_many(operation.statement, parameter_sets, *filters, **kwargs))
            prepared = self.prepare_statement(operation.statement, filters, statement_config=self.statement_config)
            return SQLResult(statement=prepared, rows_affected=rc, operation_type=prepared.operation_type)
        if operation.method == "execute_script":
            return self.execute_script(operation.statement, *operation.arguments, **kwargs)
        if operation.method == "execute_arrow":
            return self.select_to_arrow(operation.statement, *operation.arguments, **kwargs)
        msg = f"Unsupported stack operation method: {operation.method}"
        raise ValueError(msg)

    @staticmethod
    def _wrap_arrow_result(
        statement: Any,
        compiled_sql: str,
        table: Any,
        *,
        statement_config: StatementConfig | None = None,
        return_format: ArrowReturnFormat = "table",
        batch_size: int | None = None,
        arrow_schema: Any = None,
    ) -> ArrowResult:
        """Wrap a PyArrow table in a sqlspec ``ArrowResult`` preserving schema, format, and row counts."""
        if return_format not in {"table", "batch", "batches", "reader"}:
            msg = f"Unsupported Arrow return_format: {return_format!r}"
            raise ValueError(msg)
        prepared = statement if isinstance(statement, SQL) else SQL(compiled_sql, statement_config=statement_config)
        if arrow_schema is not None and hasattr(table, "cast"):
            table = table.cast(arrow_schema)
        num_rows = getattr(table, "num_rows", len(table) if hasattr(table, "__len__") else 0)
        arrow_data: Any = table
        if return_format != "table":
            if not hasattr(table, "to_batches"):
                msg = f"Arrow return_format={return_format!r} requires pyarrow"
                raise SQLSpecImproperConfigurationError(msg)
            batches = table.to_batches(max_chunksize=batch_size)
            if return_format == "batch":
                if batches:
                    arrow_data = batches[0]
                else:
                    pa = import_optional("pyarrow")
                    if pa is not None:
                        arrow_data = pa.RecordBatch.from_arrays(
                            [pa.array([], type=field.type) for field in table.schema],
                            schema=table.schema,
                        )
            elif return_format == "batches":
                arrow_data = iter(batches)
            elif return_format == "reader":
                arrow_data = table.to_reader(max_chunksize=batch_size)
        return ArrowResult(statement=prepared, data=arrow_data, rows_affected=num_rows)

    def _send_rpc(self, request: BrokerRpcRequest, *, record_last: bool = True) -> BrokerRpcResponse:
        """Dispatch a BrokerRpcRequest over the configured provider or raw transport."""
        if self._transport_mode == "raw":
            res = self._transport_fn(encode_msgpack_bytes(request))
        elif self._transport_mode == "rpc":
            res = self._transport_fn(request)
        else:
            try:
                res = self._transport_fn(request)
                if isinstance(res, (BrokerRpcResponse, bytes, bytearray, memoryview)):
                    self._transport_mode = "rpc"
            except TypeError:
                res = self._transport_fn(encode_msgpack_bytes(request))
                if isinstance(res, (BrokerRpcResponse, bytes, bytearray, memoryview)):
                    self._transport_mode = "raw"

        if isinstance(res, (bytes, bytearray, memoryview)):
            resp = cast("BrokerRpcResponse", decode_msgpack(res, type=BrokerRpcResponse))
        elif isinstance(res, BrokerRpcResponse):
            resp = res
        else:
            msg = f"Unexpected RPC transport response type: {type(res).__name__}"
            raise TypeError(msg)
        if record_last:
            self.last_response = resp
        return resp

    def _send_tx_rpc(self, op: str, *, next_in_tx: bool) -> None:
        resp = self._send_rpc(
            BrokerRpcRequest(
                request_id=self._next_request_id(),
                op=op,
                pool_name=self.pool_name,
                worker_pid=self.worker_pid,
                slot_idx=self.slot_idx,
                generation=self.generation,
                in_tx=self._in_tx,
            )
        )
        self._raise_for_error(resp)
        self._in_tx = next_in_tx
        self._transaction_depth = 1 if next_in_tx else 0

    def _release_arrow_shm(self, shm_name: str) -> None:
        """Send arrow_release RPC and unlink the shared-memory Arrow segment."""
        if not shm_name:
            return
        try:
            self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="arrow_release",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    in_tx=self._in_tx,
                    arrow_shm_name=shm_name,
                ),
                record_last=False,
            )
        finally:
            _unlink_arrow_shm(shm_name)

    def _acquire_broker_lease(self) -> Any:
        """Acquire a lease from self.broker via typed broker contracts."""
        if self.provider is not None and self.broker is self.provider.broker:
            return None
        target = getattr(self.broker, "transport", self.broker)
        if isinstance(target, SyncSHMBrokerTransport):
            return target.acquire(bind_key=self.pool_name, worker_id=self.worker_pid)
        if isinstance(target, AsyncSHMBrokerTransport):
            return target.acquire_sync(bind_key=self.pool_name, worker_id=self.worker_pid)
        acquire_lease_fn = getattr(self.broker, "acquire_lease", None)
        if callable(acquire_lease_fn):
            return acquire_lease_fn(pool_name=self.pool_name, worker_pid=self.worker_pid)
        if isinstance(target, SyncBrokerLeaseSource):
            return target.acquire(bind_key=self.pool_name, worker_id=self.worker_pid)
        return None

    def _quarantine_broker_lease(self, lease: Any) -> None:
        """Quarantine a directly held broker lease if not already quarantined."""
        if lease is None or getattr(lease, "quarantined", False):
            return
        if self.broker is None:
            return
        target = getattr(self.broker, "transport", self.broker)
        quarantine_sync_fn = getattr(self.broker, "quarantine_sync", None) or getattr(target, "quarantine_sync", None)
        quarantine_fn = (
            quarantine_sync_fn or getattr(self.broker, "quarantine", None) or getattr(target, "quarantine", None)
        )
        if callable(quarantine_fn) and not is_coroutine_callable(quarantine_fn):
            try:
                quarantine_fn(lease)
            except RuntimeError as exc:
                if "invalid lease state for operation" not in str(exc):
                    raise

    def _release_broker_lease(
        self,
        lease: Any,
        *,
        slot_idx: int | None = None,
        generation: int | None = None,
    ) -> None:
        """Release a directly acquired broker lease."""
        if lease is None or getattr(lease, "quarantined", False):
            return
        lease_release_sync = getattr(lease, "release_sync", None)
        if callable(lease_release_sync):
            lease_release_sync()
            return
        if hasattr(lease, "release") and callable(lease.release) and not is_coroutine_callable(lease.release):
            lease.release()
            return
        if self.broker is None:
            return
        resolved_slot = self.slot_idx if slot_idx is None else slot_idx
        resolved_gen = self.generation if generation is None else generation
        release_lease_fn = getattr(self.broker, "release_lease", None)
        if callable(release_lease_fn):
            release_lease_fn(resolved_slot, resolved_gen, in_tx=False)
            return
        target = getattr(self.broker, "transport", self.broker)
        release_sync_fn = getattr(self.broker, "release_sync", None) or getattr(target, "release_sync", None)
        if callable(release_sync_fn):
            release_sync_fn(f"{resolved_slot}:{resolved_gen}")
            return
        release_fn = getattr(target, "release", None)
        if callable(release_fn) and not is_coroutine_callable(release_fn):
            release_fn(f"{resolved_slot}:{resolved_gen}")

    @staticmethod
    def _extract_lease_coords(lease: Any) -> tuple[int, int]:
        """Extract (slot_idx, generation) from a lease object or tuple."""
        if lease is None:
            return 0, 0
        if isinstance(lease, tuple) and len(lease) >= 2:
            return int(lease[0]), int(lease[1])
        slot_idx = int(getattr(lease, "slot_idx", getattr(lease, "slot_index", 0)))
        generation = int(getattr(lease, "generation", 1))
        return slot_idx, max(1, generation)

    @staticmethod
    def _rows_to_dicts(
        rows: Sequence[dict[str, Any] | tuple[Any, ...] | list[Any]],
        columns: Sequence[str],
    ) -> list[dict[str, Any]]:
        """Convert a sequence of dict or tuple rows into a list of dictionaries."""
        if not rows:
            return []
        first = rows[0]
        col_names = list(columns) if columns else [f"c{idx}" for idx in range(len(first))]
        return [dict(r) if isinstance(r, dict) else dict(zip(col_names, r, strict=False)) for r in rows]

    @staticmethod
    def _convert_rows(rows: list[dict[str, Any]], schema_type: type[Any] | None) -> list[Any]:
        """Convert dictionary rows into schema_type instances when schema_type is provided."""
        if schema_type is None or schema_type is dict or not rows:
            return rows
        return cast("list[Any]", msgspec.convert(rows, type=cast("Any", list)[schema_type]))

    @staticmethod
    def _convert_single(row: dict[str, Any], schema_type: type[Any] | None) -> Any:
        """Convert a single dictionary row into schema_type when provided."""
        if schema_type is None or schema_type is dict:
            return row
        return msgspec.convert(row, type=schema_type)

    @staticmethod
    def _raise_for_error(response: BrokerRpcResponse) -> None:
        """Raise typed exception when response.ok is False."""
        if not response.ok:
            if response.error_type == "LeaseQuarantinedError":
                raise LeaseQuarantinedError(response.slot_idx, response.generation, response.error_message)
            err_type = response.error_type or "BrokerRpcError"
            raise VertebraError(f"{err_type}: {response.error_message}")


class ProxyAsyncDriver(AsyncDriverAdapterBase):
    """Asynchronous SQLSpec proxy driver executing queries through SQLSpecAsyncProvider and SHM leases."""

    dialect: str = "sqlspec"

    def __init__(
        self,
        provider: SQLSpecAsyncProvider | Callable[[BrokerRpcRequest], Any] | Callable[[bytes], Any],
        broker: Any = None,
        *,
        pool_name: str = "default",
        worker_pid: int | None = None,
        slot_idx: int = 0,
        generation: int = 0,
        statement_config: Any = None,
        dialect: str | None = None,
        driver_features: dict[str, Any] | None = None,
    ) -> None:
        """Initialize ProxyAsyncDriver with a SQLSpecAsyncProvider or async RPC transport callable."""
        if isinstance(provider, _BaseSQLSpecProvider):
            self.provider: SQLSpecAsyncProvider | None = provider
            self._transport_fn: Callable[[Any], Any] = provider.handle_request
            self._transport_mode: str | None = "rpc"
            resolved_broker = broker if broker is not None else provider.broker
            upstream_cfg = provider.resolve_upstream_optional(pool_name)
        elif callable(provider):
            self.provider = None
            self._transport_fn = provider
            self._transport_mode = None
            resolved_broker = broker
            upstream_cfg = None
        else:
            raise ImproperConfigurationError(
                "ProxyAsyncDriver requires a SQLSpecAsyncProvider or callable RPC transport"
            )

        resolved_stmt_cfg = statement_config or getattr(upstream_cfg, "statement_config", None) or StatementConfig()
        resolved_feats = (
            driver_features if driver_features is not None else dict(getattr(upstream_cfg, "driver_features", {}) or {})
        )
        super().__init__(
            connection=self,
            statement_config=resolved_stmt_cfg,
            driver_features=resolved_feats,
        )
        resolved_dialect = (
            dialect
            or getattr(upstream_cfg, "dialect", None)
            or getattr(resolved_stmt_cfg, "dialect", None)
            or "sqlspec"
        )
        self.dialect = str(resolved_dialect)
        self.broker = resolved_broker
        self.pool_name = pool_name
        self.worker_pid = worker_pid if worker_pid is not None else os.getpid()
        self.slot_idx = slot_idx
        self.generation = generation
        self.last_response: BrokerRpcResponse | None = None
        self._broker_lease: Any = None
        self._leased: bool = generation > 0
        self._context_entered: bool = False
        self._in_tx: bool = False
        self._transaction_depth: int = 0
        self._request_seq: int = 0

    @property
    def in_transaction(self) -> bool:
        """Return True when the driver has an active transaction pinned to its lease."""
        return self._in_tx

    @property
    def is_leased(self) -> bool:
        """Return True when the driver holds an active broker lease."""
        return self._leased

    @property
    def data_dictionary(self) -> Any:
        """Return optional metadata dictionary (not exposed over RPC proxy)."""
        return None

    async def acquire_lease(self) -> tuple[int, int]:
        """Acquire a connection lease asynchronously and pin an upstream session."""
        if self._leased:
            return self.slot_idx, self.generation

        slot_idx = self.slot_idx
        generation = 0
        if self.broker is not None:
            lease = await self._acquire_broker_lease()
            self._broker_lease = lease
            slot_idx, generation = ProxySyncDriver._extract_lease_coords(lease)

        try:
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="acquire",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=slot_idx,
                    generation=generation,
                ),
                record_last=False,
            )
            ProxySyncDriver._raise_for_error(resp)
        except BaseException:
            failed_lease = self._broker_lease
            self._broker_lease = None
            if failed_lease is not None:
                failed_slot, failed_gen = ProxySyncDriver._extract_lease_coords(failed_lease)
                await self._release_broker_lease_async(
                    failed_lease,
                    slot_idx=failed_slot,
                    generation=failed_gen,
                )
            raise
        self.slot_idx = resp.slot_idx
        self.generation = resp.generation
        self._leased = True
        self._in_tx = resp.in_tx
        return self.slot_idx, self.generation

    async def release_lease(self) -> None:
        """Release the active connection lease asynchronously and unpin the upstream session."""
        if not self._leased:
            return
        was_in_tx = self._in_tx
        self._in_tx = False
        self._transaction_depth = 0
        self._leased = False
        lease = self._broker_lease
        self._broker_lease = None
        try:
            rel_resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="release",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    in_tx=was_in_tx,
                ),
                record_last=False,
            )
            ProxySyncDriver._raise_for_error(rel_resp)
        except BaseException as exc:
            await self._quarantine_broker_lease_async(lease)
            if isinstance(exc, Exception) and not isinstance(exc, LeaseQuarantinedError):
                raise LeaseQuarantinedError(self.slot_idx, self.generation, str(exc)) from exc
            raise
        await self._release_broker_lease_async(lease)

    async def close(self) -> None:
        """Close the async proxy driver and release its held lease."""
        self._context_entered = False
        await self.release_lease()

    async def begin(self) -> None:
        """Begin a transaction pinned to the current connection lease."""
        await self.acquire_lease()
        resp = await self._send_rpc(
            BrokerRpcRequest(
                request_id=self._next_request_id(),
                op="begin",
                pool_name=self.pool_name,
                worker_pid=self.worker_pid,
                slot_idx=self.slot_idx,
                generation=self.generation,
                in_tx=self._in_tx,
            )
        )
        ProxySyncDriver._raise_for_error(resp)
        self._in_tx = True
        if self._transaction_depth == 0:
            self._transaction_depth = 1

    async def commit(self) -> None:
        """Commit the active pinned transaction."""
        try:
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="commit",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            self._in_tx = False
            self._transaction_depth = 0
        finally:
            if not self._context_entered:
                await self.release_lease()

    async def rollback(self) -> None:
        """Roll back the active pinned transaction."""
        if not self._leased and not self._in_tx:
            self._transaction_depth = 0
            return
        try:
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="rollback",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            self._in_tx = False
            self._transaction_depth = 0
        finally:
            if not self._context_entered:
                await self.release_lease()

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single SQL statement asynchronously on the proxy driver."""
        async with self._operation_lease():
            compiled_sql, compiled_params, prepared = self._compile_statement(statement)
            if prepared.returns_rows():
                resp = await self._send_rpc(
                    BrokerRpcRequest(
                        request_id=self._next_request_id(),
                        op="select",
                        pool_name=self.pool_name,
                        worker_pid=self.worker_pid,
                        slot_idx=self.slot_idx,
                        generation=self.generation,
                        sql=compiled_sql,
                        params=compiled_params,
                        in_tx=self._in_tx,
                    )
                )
                ProxySyncDriver._raise_for_error(resp)
                dict_rows, columns = await self._unpack_select_rows(resp)
                return self.create_execution_result(
                    cursor,
                    selected_data=dict_rows,
                    column_names=columns,
                    data_row_count=len(dict_rows),
                    is_select_result=True,
                )
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="execute",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            return self.create_execution_result(cursor, rowcount_override=int(resp.rowcount))

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a SQL statement with multiple parameter sets asynchronously on the proxy driver."""
        params = getattr(statement, "parameters", ())
        rowcount = int(await self.execute_many(statement, list(params)))
        return self.create_execution_result(cursor, rowcount_override=rowcount, is_many_result=True)

    @overload
    def select_stream(
        self,
        statement: SQL | Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT],
        statement_config: StatementConfig | None = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> AsyncRowStream[SchemaT]: ...

    @overload
    def select_stream(
        self,
        statement: SQL | Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: None = None,
        statement_config: StatementConfig | None = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> AsyncRowStream[dict[str, Any]]: ...

    def select_stream(
        self,
        statement: SQL | Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT] | None = None,
        statement_config: StatementConfig | None = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> AsyncRowStream[SchemaT] | AsyncRowStream[dict[str, Any]]:
        """Execute a query and stream rows asynchronously in chunks, failing when native_only=True is requested."""
        if chunk_size < 1:
            msg = "chunk_size must be greater than or equal to 1"
            raise ValueError(msg)
        sql_statement = self.prepare_statement(
            statement, parameters, statement_config=statement_config or self.statement_config, kwargs=kwargs
        )
        stream = self.dispatch_select_stream(sql_statement, chunk_size)
        if stream is not None:
            return stream._with_schema_type(schema_type)
        if native_only:
            msg = (
                f"Adapter '{type(self).__name__}' does not support native row streaming. "
                "Use native_only=False to allow eager fallback (not bounded-memory), "
                "or switch to an adapter with native row streaming support."
            )
            raise SQLSpecImproperConfigurationError(msg)
        return AsyncRowStream(_LazySelectAsyncRowSource(self, sql_statement, chunk_size), schema_type=schema_type)

    def dispatch_select_stream(self, statement: SQL, chunk_size: int) -> AsyncRowStream[dict[str, Any]] | None:
        """Return None so async buffered streaming requires native_only=False fallback."""
        _ = (statement, chunk_size)
        return None

    def handle_database_exceptions(self) -> DeferredAsyncExceptionHandler:
        """Return async exception handler for proxy driver operations."""
        return DeferredAsyncExceptionHandler()

    @asynccontextmanager
    async def with_cursor(self, connection: Any) -> AsyncIterator[Any]:
        """Yield the active async proxy connection as a cursor handle."""
        yield connection

    async def execute(
        self,
        statement: SQL | Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a SQL statement asynchronously and return the affected row count."""
        async with self._operation_lease():
            compiled_sql, compiled_params, _ = self._compile_statement(
                statement,
                parameters,
                statement_config=statement_config,
                kwargs=kwargs,
            )
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="execute",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            return resp.rowcount

    async def execute_many(
        self,
        statement: SQL | Statement | QueryBuilder,
        /,
        parameters: Sequence[StatementParameters],
        *filters: StatementParameters | StatementFilter,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a batch SQL statement asynchronously across parameters and return affected row count."""
        async with self._operation_lease():
            _prepared, compiled_sql, compiled_many = compile_many_transport(
                self,
                statement,
                parameters,
                filters,
                statement_config=statement_config,
                kwargs=kwargs,
            )
            normalized_many: list[list[Any] | dict[str, Any]] = [
                item if isinstance(item, dict) else list(item) for item in compiled_many
            ]
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="execute_many",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    many_params=normalized_many,
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            return resp.rowcount

    async def execute_script(
        self,
        statement: str | SQL,
        /,
        *parameters: StatementParameters | StatementFilter,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> SQLResult:
        """Execute a multi-statement script asynchronously on a single leased proxy session."""
        async with self._operation_lease():
            return await super().execute_script(statement, *parameters, statement_config=statement_config, **kwargs)

    @overload
    async def select(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT],
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> list[SchemaT]: ...

    @overload
    async def select(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]: ...

    async def select(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT] | None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> list[SchemaT] | list[dict[str, Any]]:
        """Execute a SELECT statement asynchronously and return rows, transparently reading spilled Arrow SHM."""
        async with self._operation_lease():
            compiled_sql, compiled_params, _ = self._compile_statement(
                statement,
                parameters,
                statement_config=statement_config,
                kwargs=kwargs,
            )
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            if resp.is_arrow_shm:
                if schema_type is not None and schema_type is not dict:
                    return await self._unpack_select_typed(resp, schema_type)
                dict_rows, _ = await self._unpack_select_rows(resp)
                return dict_rows

            if schema_type is not None and schema_type is not dict:
                return ProxySyncDriver._convert_rows(
                    ProxySyncDriver._select_inline_dicts(resp.rows, resp.columns),
                    schema_type,
                )
            return ProxySyncDriver._select_inline_dicts(resp.rows, resp.columns)

    @overload
    async def select_one(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT],
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> SchemaT: ...

    @overload
    async def select_one(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]: ...

    async def select_one(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT] | None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> SchemaT | dict[str, Any]:
        """Execute a SELECT statement asynchronously and return exactly one row."""
        async with self._operation_lease():
            compiled_sql, compiled_params, _ = self._compile_statement(
                statement,
                parameters,
                statement_config=statement_config,
                kwargs=kwargs,
            )
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_one",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            dict_rows, _ = await self._unpack_select_rows(resp)
            if not dict_rows:
                raise VertebraError("ValueError: No row found for select_one")
            return cast("SchemaT | dict[str, Any]", ProxySyncDriver._convert_single(dict_rows[0], schema_type))

    @overload
    async def select_one_or_none(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT],
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> SchemaT | None: ...

    @overload
    async def select_one_or_none(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None: ...

    async def select_one_or_none(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT] | None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> SchemaT | dict[str, Any] | None:
        """Execute a SELECT statement asynchronously and return at most one row or None."""
        async with self._operation_lease():
            compiled_sql, compiled_params, _ = self._compile_statement(
                statement,
                parameters,
                statement_config=statement_config,
                kwargs=kwargs,
            )
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_one_or_none",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            dict_rows, _ = await self._unpack_select_rows(resp)
            if not dict_rows:
                return None
            return cast("SchemaT | dict[str, Any]", ProxySyncDriver._convert_single(dict_rows[0], schema_type))

    @overload
    async def select_value(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        value_type: type[ValueT],
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> ValueT: ...

    @overload
    async def select_value(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        value_type: None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> Any: ...

    async def select_value(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        value_type: type[ValueT] | None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> ValueT | Any:
        """Execute a SELECT statement asynchronously and return the first scalar column value."""
        async with self._operation_lease():
            compiled_sql, compiled_params, _ = self._compile_statement(
                statement,
                parameters,
                statement_config=statement_config,
                kwargs=kwargs,
            )
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_value",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            if value_type is not None and resp.scalar is not None:
                return to_value_type(resp.scalar, value_type)
            return resp.scalar

    @overload
    async def select_value_or_none(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        value_type: type[ValueT],
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> ValueT | None: ...

    @overload
    async def select_value_or_none(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        value_type: None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> Any: ...

    async def select_value_or_none(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        value_type: type[ValueT] | None = None,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> ValueT | Any | None:
        """Execute a SELECT statement asynchronously and return a single scalar value or None."""
        async with self._operation_lease():
            compiled_sql, compiled_params, _ = self._compile_statement(
                statement,
                parameters,
                statement_config=statement_config,
                kwargs=kwargs,
            )
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_value_or_none",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            if resp.scalar is None:
                return None
            if value_type is not None:
                return to_value_type(resp.scalar, value_type)
            return resp.scalar

    @overload
    async def select_with_total(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT],
        statement_config: StatementConfig | None = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> tuple[list[SchemaT], int]: ...

    @overload
    async def select_with_total(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: None = None,
        statement_config: StatementConfig | None = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> tuple[list[dict[str, Any]], int]: ...

    async def select_with_total(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        schema_type: type[SchemaT] | None = None,
        statement_config: StatementConfig | None = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> tuple[list[SchemaT] | list[dict[str, Any]], int]:
        """Execute an async SELECT statement and return page rows alongside total matching count."""
        effective_cfg = statement_config or self.statement_config
        sql_statement = self.prepare_statement(statement, parameters, statement_config=effective_cfg, kwargs=kwargs)
        async with self._operation_lease():
            if count_with_window:
                rows = await self.select(self._with_total_count(sql_statement), statement_config=effective_cfg)
                data, total = self._extract_total_from_rows(rows)
                if schema_type is not None:
                    return cast("list[SchemaT]", ProxySyncDriver._convert_rows(data, schema_type)), int(total)
                return data, int(total)

            total = int(await self.select_value(self._count_query(sql_statement), statement_config=effective_cfg) or 0)
            if schema_type is not None:
                typed_data = cast(
                    "list[SchemaT]",
                    await self.select(
                        sql_statement,
                        schema_type=cast("Any", schema_type),
                        statement_config=effective_cfg,
                    ),
                )
                return typed_data, total
            return await self.select(sql_statement, statement_config=effective_cfg), total

    async def execute_stack(
        self,
        stack: StatementStack,
        *,
        continue_on_error: bool = False,
    ) -> tuple[StackResult, ...]:
        """Execute a StatementStack asynchronously over the active proxy session lease."""
        if not isinstance(stack, StatementStack):
            msg = "execute_stack expects a StatementStack instance"
            raise TypeError(msg)
        if not stack:
            msg = "Cannot execute an empty StatementStack"
            raise ValueError(msg)

        results: list[StackResult] = []
        single_transaction = not continue_on_error
        async with self._operation_lease():
            caller_in_tx = self._in_tx
            started_tx = False
            started_op_tx = False
            try:
                if single_transaction and not caller_in_tx:
                    await self._send_tx_rpc_async("begin", next_in_tx=True)
                    started_tx = True

                for index, operation in enumerate(stack.operations):
                    started_op_tx = False
                    if continue_on_error and not caller_in_tx:
                        await self._send_tx_rpc_async("begin", next_in_tx=True)
                        started_op_tx = True
                    try:
                        op_res = await self._execute_stack_operation_async(operation)
                    except Exception as exc:
                        if started_op_tx:
                            started_op_tx = False
                            await self._send_tx_rpc_async("rollback", next_in_tx=False)
                        stack_error = StackExecutionError(
                            index,
                            str(operation.statement),
                            exc,
                            adapter=type(self).__name__,
                            mode="continue-on-error" if continue_on_error else "fail-fast",
                        )
                        if started_tx and not continue_on_error:
                            started_tx = False
                            await self._send_tx_rpc_async("rollback", next_in_tx=False)
                        if continue_on_error:
                            results.append(StackResult.from_error(stack_error))
                            continue
                        raise stack_error from exc
                    if started_op_tx:
                        started_op_tx = False
                        await self._send_tx_rpc_async("commit", next_in_tx=False)
                    results.append(StackResult(result=op_res))

                if started_tx:
                    started_tx = False
                    await self._send_tx_rpc_async("commit", next_in_tx=False)
            except BaseException:
                if started_op_tx:
                    await self._send_tx_rpc_async("rollback", next_in_tx=False)
                if started_tx:
                    await self._send_tx_rpc_async("rollback", next_in_tx=False)
                raise
        return tuple(results)

    async def select_to_arrow(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: StatementParameters | StatementFilter,
        statement_config: StatementConfig | None = None,
        return_format: ArrowReturnFormat = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any = None,
        **kwargs: Any,
    ) -> ArrowResult:
        """Execute a SELECT statement asynchronously and return a zero-copy ArrowResult."""
        if native_only and import_optional("pyarrow") is None:
            msg = f"Adapter '{type(self).__name__}' requires pyarrow for native Arrow results when native_only=True."
            raise SQLSpecImproperConfigurationError(msg)
        effective_cfg = statement_config or self.statement_config
        async with self._operation_lease():
            compiled_sql, compiled_params, prepared = self._compile_statement(
                statement,
                parameters,
                statement_config=effective_cfg,
                kwargs=kwargs,
            )
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_to_arrow",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            if native_only and not resp.is_arrow_shm:
                msg = f"Adapter '{type(self).__name__}' did not return native SHM Arrow results when native_only=True."
                raise SQLSpecImproperConfigurationError(msg)
            if resp.is_arrow_shm:
                try:
                    table = await SyncPool.default().submit_async(
                        ProxySyncDriver._read_arrow_shm_table,
                        resp.arrow_shm_name,
                        resp.arrow_byte_len,
                        resp.rowcount,
                        resp.columns,
                    )
                finally:
                    await self._release_arrow_shm(resp.arrow_shm_name)
            else:
                dict_rows = ProxySyncDriver._rows_to_dicts(resp.rows, resp.columns)
                table = await SyncPool.default().submit_async(
                    ProxySyncDriver._coerce_inline_arrow_table, dict_rows, columns=resp.columns
                )
            return ProxySyncDriver._wrap_arrow_result(
                prepared,
                compiled_sql,
                table,
                statement_config=effective_cfg,
                return_format=return_format,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
            )

    async def __aenter__(self) -> ProxyAsyncDriver:
        """Enter async context manager and acquire a pinned broker lease."""
        self._context_entered = True
        await self.acquire_lease()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit async context manager, rolling back any uncommitted transaction and releasing the lease."""
        _ = (exc_type, exc_val, exc_tb)
        self._context_entered = False
        await self.release_lease()

    def _connection_in_transaction(self) -> bool:
        """Return True when the pinned proxy lease holds an active transaction."""
        return self._in_tx

    def _next_request_id(self) -> int:
        """Increment and return the next monotonically increasing request ID."""
        self._request_seq += 1
        return self._request_seq

    @asynccontextmanager
    async def _operation_lease(self) -> AsyncIterator[None]:
        """Ensure a lease is held for an async operation, releasing it afterwards when not context-pinned."""
        auto_acquired = not self._leased
        if auto_acquired:
            await self.acquire_lease()
        try:
            yield
        finally:
            if auto_acquired and not self._context_entered and not self._in_tx:
                await self.release_lease()

    def _compile_statement(
        self,
        statement: Any,
        parameters: tuple[Any, ...] = (),
        *,
        statement_config: StatementConfig | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> tuple[str, list[Any] | dict[str, Any], SQL]:
        """Delegate statement compilation to ProxySyncDriver.compile_with_broker."""
        return ProxySyncDriver.compile_with_broker(
            self,
            self.broker,
            self.dialect,
            statement,
            parameters,
            statement_config=statement_config,
            kwargs=kwargs,
        )

    @staticmethod
    def _read_and_hydrate_arrow(
        shm_name: str,
        byte_len: int,
        rpc_columns: Sequence[str],
        schema_type: type[Any],
        row_count: int = 0,
    ) -> list[Any]:
        table = ProxySyncDriver._read_arrow_shm_table(shm_name, byte_len, row_count, rpc_columns)
        return ProxySyncDriver._hydrate_arrow_table(table, rpc_columns, schema_type)

    @staticmethod
    def _read_and_unpack_arrow_dicts(
        shm_name: str,
        byte_len: int,
        rpc_columns: Sequence[str],
        row_count: int = 0,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        cursor = ProxySyncDriver._read_arrow_shm_cursor(shm_name, byte_len, row_count, rpc_columns)
        dict_rows: list[dict[str, Any]] = cursor.to_pylist()
        columns = list(rpc_columns) if rpc_columns else list(cursor._columns)
        if not columns:
            table = cursor.to_arrow_table()
            if hasattr(table, "column_names"):
                columns = list(table.column_names)
        return dict_rows, columns

    async def _unpack_select_typed(self, resp: BrokerRpcResponse, schema_type: type[Any]) -> list[Any]:
        """Read spilled Arrow SHM off the loop and hydrate schema_type directly."""
        try:
            return await SyncPool.default().submit_async(
                self._read_and_hydrate_arrow,
                resp.arrow_shm_name,
                resp.arrow_byte_len,
                resp.columns,
                schema_type,
                resp.rowcount,
            )
        finally:
            await self._release_arrow_shm(resp.arrow_shm_name)

    async def _unpack_select_rows(self, resp: BrokerRpcResponse) -> tuple[list[dict[str, Any]], list[str]]:
        if resp.is_arrow_shm:
            try:
                return await SyncPool.default().submit_async(
                    self._read_and_unpack_arrow_dicts,
                    resp.arrow_shm_name,
                    resp.arrow_byte_len,
                    resp.columns,
                    resp.rowcount,
                )
            finally:
                await self._release_arrow_shm(resp.arrow_shm_name)
        dict_rows = ProxySyncDriver._rows_to_dicts(resp.rows, resp.columns)
        columns = list(resp.columns) if resp.columns else (list(dict_rows[0].keys()) if dict_rows else [])
        return dict_rows, columns

    async def _execute_stack_operation_async(self, operation: Any) -> SQLResult | ArrowResult:
        kwargs = dict(operation.keyword_arguments) if operation.keyword_arguments else {}
        if operation.method == "execute":
            compiled_sql, compiled_params, prepared = self._compile_statement(
                operation.statement,
                operation.arguments,
                kwargs=kwargs,
            )
            is_query = prepared.returns_rows()
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select" if is_query else "execute",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=compiled_params,
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            if is_query:
                dict_rows, columns = await self._unpack_select_rows(resp)
                return SQLResult(
                    statement=prepared,
                    data=dict_rows,
                    rows_affected=len(dict_rows),
                    column_names=columns,
                    operation_type=prepared.operation_type,
                )
            return SQLResult(
                statement=prepared, rows_affected=int(resp.rowcount), operation_type=prepared.operation_type
            )
        if operation.method == "execute_many":
            if not operation.arguments:
                msg = "execute_many stack operation requires parameter sets"
                raise ValueError(msg)
            parameter_sets = operation.arguments[0]
            filters = operation.arguments[1:]
            rc = int(await self.execute_many(operation.statement, parameter_sets, *filters, **kwargs))
            prepared = self.prepare_statement(operation.statement, filters, statement_config=self.statement_config)
            return SQLResult(statement=prepared, rows_affected=rc, operation_type=prepared.operation_type)
        if operation.method == "execute_script":
            return await self.execute_script(operation.statement, *operation.arguments, **kwargs)
        if operation.method == "execute_arrow":
            return await self.select_to_arrow(operation.statement, *operation.arguments, **kwargs)
        msg = f"Unsupported stack operation method: {operation.method}"
        raise ValueError(msg)

    async def _send_rpc(self, request: BrokerRpcRequest, *, record_last: bool = True) -> BrokerRpcResponse:
        """Dispatch a BrokerRpcRequest asynchronously over the configured provider or raw transport."""
        if self._transport_mode == "raw":
            res = self._transport_fn(encode_msgpack_bytes(request))
            if isinstance(res, Awaitable):
                res = await res
        elif self._transport_mode == "rpc":
            res = self._transport_fn(request)
            if isinstance(res, Awaitable):
                res = await res
        else:
            try:
                res = self._transport_fn(request)
                if isinstance(res, Awaitable):
                    res = await res
                if isinstance(res, (BrokerRpcResponse, bytes, bytearray, memoryview)):
                    self._transport_mode = "rpc"
            except TypeError:
                res = self._transport_fn(encode_msgpack_bytes(request))
                if isinstance(res, Awaitable):
                    res = await res
                if isinstance(res, (BrokerRpcResponse, bytes, bytearray, memoryview)):
                    self._transport_mode = "raw"

        if isinstance(res, (bytes, bytearray, memoryview)):
            resp = cast("BrokerRpcResponse", decode_msgpack(res, type=BrokerRpcResponse))
        elif isinstance(res, BrokerRpcResponse):
            resp = res
        else:
            msg = f"Unexpected RPC transport response type: {type(res).__name__}"
            raise TypeError(msg)
        if record_last:
            self.last_response = resp
        return resp

    async def _send_tx_rpc_async(self, op: str, *, next_in_tx: bool) -> None:
        resp = await self._send_rpc(
            BrokerRpcRequest(
                request_id=self._next_request_id(),
                op=op,
                pool_name=self.pool_name,
                worker_pid=self.worker_pid,
                slot_idx=self.slot_idx,
                generation=self.generation,
                in_tx=self._in_tx,
            )
        )
        ProxySyncDriver._raise_for_error(resp)
        self._in_tx = next_in_tx
        self._transaction_depth = 1 if next_in_tx else 0

    async def _release_arrow_shm(self, shm_name: str) -> None:
        """Send arrow_release RPC asynchronously and unlink the shared-memory Arrow segment."""
        if not shm_name:
            return
        try:
            await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="arrow_release",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    in_tx=self._in_tx,
                    arrow_shm_name=shm_name,
                ),
                record_last=False,
            )
        finally:
            await SyncPool.default().submit_async(_unlink_arrow_shm, shm_name)

    async def _acquire_broker_lease(self) -> Any:
        """Acquire a lease asynchronously from self.broker via typed broker contracts."""
        if self.provider is not None and self.broker is self.provider.broker:
            return None
        target = getattr(self.broker, "transport", self.broker)
        if isinstance(target, AsyncSHMBrokerTransport):
            return await target.acquire(bind_key=self.pool_name, worker_id=self.worker_pid)
        if isinstance(target, SyncSHMBrokerTransport):
            return await SyncPool.default().submit_async(
                target.acquire,
                bind_key=self.pool_name,
                worker_id=self.worker_pid,
            )
        acquire_kwargs = (
            (getattr(self.broker, "acquire_lease", None), {"pool_name": self.pool_name, "worker_pid": self.worker_pid}),
            (getattr(target, "acquire", None), {"bind_key": self.pool_name, "worker_id": self.worker_pid}),
        )
        for fn, call_kwargs in acquire_kwargs:
            if callable(fn):
                return await _BaseSQLSpecProvider._invoke_async_or_offload(fn, **call_kwargs)
        return None

    async def _quarantine_broker_lease_async(self, lease: Any) -> None:
        """Quarantine a directly held async broker lease if not already quarantined."""
        if lease is None or getattr(lease, "quarantined", False):
            return
        if self.broker is None:
            return
        target = getattr(self.broker, "transport", self.broker)
        quarantine_fn = getattr(self.broker, "quarantine", None) or getattr(target, "quarantine", None)
        if callable(quarantine_fn):
            try:
                await _BaseSQLSpecProvider._invoke_async_or_offload(quarantine_fn, lease)
            except RuntimeError as exc:
                if "invalid lease state for operation" not in str(exc):
                    raise

    async def _release_broker_lease_async(
        self,
        lease: Any,
        *,
        slot_idx: int | None = None,
        generation: int | None = None,
    ) -> None:
        """Release a directly acquired broker lease asynchronously."""
        if lease is None or getattr(lease, "quarantined", False):
            return
        if hasattr(lease, "release") and callable(lease.release):
            await _BaseSQLSpecProvider._invoke_async_or_offload(lease.release)
            return
        resolved_slot = self.slot_idx if slot_idx is None else slot_idx
        resolved_gen = self.generation if generation is None else generation
        release_lease_fn = getattr(self.broker, "release_lease", None)
        if callable(release_lease_fn):
            await _BaseSQLSpecProvider._invoke_async_or_offload(
                release_lease_fn, resolved_slot, resolved_gen, in_tx=False
            )
            return
        target = getattr(self.broker, "transport", self.broker)
        release_fn = getattr(target, "release", None)
        if callable(release_fn):
            await _BaseSQLSpecProvider._invoke_async_or_offload(release_fn, f"{resolved_slot}:{resolved_gen}")


class ProxySyncSessionContext:
    """Synchronous session context manager yielding a lease-pinned ProxySyncDriver."""

    __slots__ = ("_driver",)

    def __init__(
        self,
        provider: SQLSpecSyncProvider | Callable[[BrokerRpcRequest], BrokerRpcResponse],
        broker: Any = None,
        *,
        pool_name: str = "default",
        worker_pid: int | None = None,
        statement_config: Any = None,
        dialect: str | None = None,
    ) -> None:
        """Initialize ProxySyncSessionContext wrapping a ProxySyncDriver."""
        self._driver = ProxySyncDriver(
            provider=provider,
            broker=broker,
            pool_name=pool_name,
            worker_pid=worker_pid,
            statement_config=statement_config,
            dialect=dialect,
        )

    @property
    def driver(self) -> ProxySyncDriver:
        """Return the underlying ProxySyncDriver instance."""
        return self._driver

    def __enter__(self) -> ProxySyncDriver:
        """Acquire lease and return the active ProxySyncDriver."""
        return self._driver.__enter__()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Release the lease on context exit."""
        self._driver.__exit__(exc_type, exc_val, exc_tb)

    def __getattr__(self, name: str) -> Any:
        """Forward direct driver method calls to the underlying ProxySyncDriver."""
        return getattr(self._driver, name)


class ProxyAsyncSessionContext:
    """Asynchronous session context manager yielding a lease-pinned ProxyAsyncDriver."""

    __slots__ = ("_driver",)

    def __init__(
        self,
        provider: SQLSpecAsyncProvider | Callable[[BrokerRpcRequest], Any],
        broker: Any = None,
        *,
        pool_name: str = "default",
        worker_pid: int | None = None,
        statement_config: Any = None,
        dialect: str | None = None,
    ) -> None:
        """Initialize ProxyAsyncSessionContext wrapping a ProxyAsyncDriver."""
        self._driver = ProxyAsyncDriver(
            provider=provider,
            broker=broker,
            pool_name=pool_name,
            worker_pid=worker_pid,
            statement_config=statement_config,
            dialect=dialect,
        )

    @property
    def driver(self) -> ProxyAsyncDriver:
        """Return the underlying ProxyAsyncDriver instance."""
        return self._driver

    async def __aenter__(self) -> ProxyAsyncDriver:
        """Acquire lease asynchronously and return the active ProxyAsyncDriver."""
        return await self._driver.__aenter__()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Release the lease on async context exit."""
        await self._driver.__aexit__(exc_type, exc_val, exc_tb)

    def __getattr__(self, name: str) -> Any:
        """Forward direct driver method calls to the underlying ProxyAsyncDriver."""
        return getattr(self._driver, name)


class _LazySelectSyncRowSource:
    """Eager chunked sync row source backed by ProxySyncDriver.select()."""

    __slots__ = ("_chunk_size", "_driver", "_position", "_rows", "_statement")

    def __init__(self, driver: ProxySyncDriver, statement: SQL, chunk_size: int) -> None:
        self._driver = driver
        self._statement = statement
        self._chunk_size = chunk_size
        self._rows: list[dict[str, Any]] = []
        self._position = 0

    def start(self) -> None:
        """Materialize rows lazily when the stream starts."""
        self._rows = self._driver.select(self._statement)

    def fetch_chunk(self) -> list[dict[str, Any]]:
        """Yield the next chunk of buffered rows."""
        chunk = self._rows[self._position : self._position + self._chunk_size]
        self._position += len(chunk)
        return chunk

    def close(self, error: bool = False) -> None:
        """Clear buffered rows on stream close."""
        _ = error
        self._rows = []


class _LazySelectAsyncRowSource:
    """Eager chunked async row source backed by ProxyAsyncDriver.select()."""

    __slots__ = ("_chunk_size", "_driver", "_position", "_rows", "_statement")

    def __init__(self, driver: ProxyAsyncDriver, statement: SQL, chunk_size: int) -> None:
        self._driver = driver
        self._statement = statement
        self._chunk_size = chunk_size
        self._rows: list[dict[str, Any]] = []
        self._position = 0

    async def start(self) -> None:
        """Materialize rows lazily when the async stream starts."""
        self._rows = await self._driver.select(self._statement)

    async def fetch_chunk(self) -> list[dict[str, Any]]:
        """Yield the next chunk of buffered async rows."""
        chunk = self._rows[self._position : self._position + self._chunk_size]
        self._position += len(chunk)
        return chunk

    async def close(self, error: bool = False) -> None:
        """Clear buffered rows on async stream close."""
        _ = error
        self._rows = []
