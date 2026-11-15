"""Shared-memory proxy drivers and session context managers for SQLSpec configurations."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, cast, overload

import msgspec
from sqlspec.core import SQL, ArrowResult
from sqlspec.core.statement import StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

from vertebra.extensions.broker.errors import LeaseQuarantinedError
from vertebra.extensions.broker.transport import AsyncSHMBrokerTransport, SyncSHMBrokerTransport
from vertebra.extensions.sqlspec.adapters import (
    DeferredAsyncExceptionHandler,
    DeferredSyncExceptionHandler,
)
from vertebra.extensions.sqlspec.provider import (
    AsyncBrokerLeaseSource,
    BrokerRpcRequest,
    BrokerRpcResponse,
    SharedArrowBuffer,
    SQLSpecAsyncProvider,
    SQLSpecSyncProvider,
    SyncBrokerLeaseSource,
    _BaseSQLSpecProvider,
)
from vertebra.typing import ImproperConfigurationError, VertebraError
from vertebra.utils.serializers import (
    ZeroCopyCodecRegistry,
    decode_msgpack,
    encode_msgpack_bytes,
)

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
            resolved_broker = broker if broker is not None else provider.broker
            upstream_cfg = provider._resolve_upstream_optional(pool_name)
        elif callable(provider):
            self.provider = None
            self._transport_fn = provider
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
        self._request_seq: int = 0

    @property
    def in_transaction(self) -> bool:
        """Return True when the driver has an active transaction pinned to its lease."""
        return self._in_tx

    @property
    def is_leased(self) -> bool:
        """Return True when the driver holds an active broker lease."""
        return self._leased

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
            if isinstance(exc, LeaseQuarantinedError):
                raise
            raise LeaseQuarantinedError(self.slot_idx, self.generation, str(exc)) from exc
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
            self._in_tx = False
            self._raise_for_error(resp)
        finally:
            if not self._context_entered and not self._in_tx:
                self.release_lease()

    def rollback(self) -> None:
        """Roll back the active pinned transaction."""
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
            self._in_tx = False
            self._raise_for_error(resp)
        finally:
            if not self._context_entered and not self._in_tx:
                self.release_lease()

    @property
    def data_dictionary(self) -> Any:
        """Return optional metadata dictionary (not exposed over RPC proxy)."""
        return None

    def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single SQL statement on the proxy driver."""
        rowcount = int(self.execute(statement))
        return self.create_execution_result(cursor, rowcount_override=rowcount)

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a SQL statement with multiple parameter sets on the proxy driver."""
        params = getattr(statement, "parameters", ())
        rowcount = int(self.execute_many(statement, list(params)))
        return self.create_execution_result(cursor, rowcount_override=rowcount, is_many_result=True)

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
        _ = (statement_config, kwargs)
        with self._operation_lease():
            compiled_sql = self._compile_statement(statement)
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="execute",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=self._normalize_params(parameters),
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
        _ = (filters, statement_config, kwargs)
        with self._operation_lease():
            compiled_sql = self._compile_statement(statement)
            normalized_many: list[list[Any] | dict[str, Any]] = [
                list(item) if isinstance(item, tuple) else cast("dict[str, Any] | list[Any]", item)
                for item in parameters
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

    @overload
    def select(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: object,
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
        _ = (statement_config, kwargs)
        with self._operation_lease():
            compiled_sql = self._compile_statement(statement)
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=self._normalize_params(parameters),
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    table = SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                    dict_rows: list[dict[str, Any]] = table.to_pylist()
                    return self._convert_rows(dict_rows, schema_type)
                finally:
                    self._release_arrow_shm(resp.arrow_shm_name)

            if schema_type is not None and schema_type is not dict:
                return self._hydrate_raw_rows(resp.rows, resp.columns, schema_type)
            dict_rows = self._rows_to_dicts(resp.rows, resp.columns)
            return self._convert_rows(dict_rows, schema_type)

    @overload
    def select_one(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: object,
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
        _ = (statement_config, kwargs)
        with self._operation_lease():
            compiled_sql = self._compile_statement(statement)
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_one",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=self._normalize_params(parameters),
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    table = SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                    dict_rows = table.to_pylist()
                    if not dict_rows:
                        raise VertebraError("ValueError: No row found for select_one")
                    return cast("SchemaT | dict[str, Any]", self._convert_single(dict_rows[0], schema_type))
                finally:
                    self._release_arrow_shm(resp.arrow_shm_name)

            dict_rows = self._rows_to_dicts(resp.rows, resp.columns)
            if not dict_rows:
                raise VertebraError("ValueError: No row found for select_one")
            return cast("SchemaT | dict[str, Any]", self._convert_single(dict_rows[0], schema_type))

    @overload
    def select_one_or_none(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: object,
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
        _ = (statement_config, kwargs)
        with self._operation_lease():
            compiled_sql = self._compile_statement(statement)
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_one_or_none",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=self._normalize_params(parameters),
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    table = SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                    dict_rows = table.to_pylist()
                    if not dict_rows:
                        return None
                    return cast("SchemaT | dict[str, Any]", self._convert_single(dict_rows[0], schema_type))
                finally:
                    self._release_arrow_shm(resp.arrow_shm_name)

            dict_rows = self._rows_to_dicts(resp.rows, resp.columns)
            if not dict_rows:
                return None
            return cast("SchemaT | dict[str, Any]", self._convert_single(dict_rows[0], schema_type))

    @overload
    def select_value(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: object,
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
        _ = (value_type, statement_config, kwargs)
        with self._operation_lease():
            compiled_sql = self._compile_statement(statement)
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_value",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=self._normalize_params(parameters),
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            return resp.scalar

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
        _ = (return_format, native_only, batch_size, arrow_schema, kwargs)
        with self._operation_lease():
            compiled_sql = self._compile_statement(statement)
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_to_arrow",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=self._normalize_params(parameters),
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    table = SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                finally:
                    self._release_arrow_shm(resp.arrow_shm_name)
                return self._wrap_arrow_result(
                    statement,
                    compiled_sql,
                    table,
                    statement_config=statement_config or self.statement_config,
                )

            dict_rows = self._rows_to_dicts(resp.rows, resp.columns)
            table = SharedArrowBuffer.coerce_table(dict_rows, columns=resp.columns)
            return self._wrap_arrow_result(
                statement,
                compiled_sql,
                table,
                statement_config=statement_config or self.statement_config,
            )

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

    def _compile_statement(self, statement: Any) -> str:
        """Consult the broker statement cache when available and return compiled SQL."""
        sql = statement.sql if hasattr(statement, "sql") else str(statement)
        if self.broker is None:
            return str(sql)
        target = getattr(self.broker, "transport", self.broker)
        compile_fn = getattr(target, "get_or_compile_statement", None)
        if callable(compile_fn):
            dialect_name = self.dialect or "sqlspec"
            plan = compile_fn(dialect_name, sql, lambda raw: (raw, ()))
            compiled = getattr(plan, "compiled_sql", None)
            if isinstance(compiled, str):
                return compiled
        return str(sql)

    @staticmethod
    def _wrap_arrow_result(
        statement: Any,
        compiled_sql: str,
        table: Any,
        *,
        statement_config: StatementConfig | None = None,
    ) -> ArrowResult:
        """Wrap a PyArrow table in a sqlspec ``ArrowResult`` preserving row counts."""
        prepared = statement if isinstance(statement, SQL) else SQL(compiled_sql, statement_config=statement_config)
        num_rows = getattr(table, "num_rows", len(table) if hasattr(table, "__len__") else 0)
        return ArrowResult(statement=prepared, data=table, rows_affected=num_rows)

    def _send_rpc(self, request: BrokerRpcRequest, *, record_last: bool = True) -> BrokerRpcResponse:
        """Dispatch a BrokerRpcRequest over the configured provider or raw transport."""
        try:
            res = self._transport_fn(request)
        except TypeError:
            res = self._transport_fn(encode_msgpack_bytes(request))
        if isinstance(res, (bytes, bytearray, memoryview)):
            resp = cast("BrokerRpcResponse", decode_msgpack(res, type=BrokerRpcResponse))
        elif isinstance(res, BrokerRpcResponse):
            resp = res
        else:
            raw_req = encode_msgpack_bytes(request)
            raw_resp = self._transport_fn(raw_req)
            resp = cast("BrokerRpcResponse", decode_msgpack(raw_resp, type=BrokerRpcResponse))
        if record_last:
            self.last_response = resp
        return resp

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
            SharedArrowBuffer.unlink(shm_name)

    def _acquire_broker_lease(self) -> Any:
        """Acquire a lease from self.broker via typed broker contracts."""
        if self.provider is not None and self.broker is self.provider.broker:
            return None
        target = getattr(self.broker, "transport", self.broker)
        if isinstance(target, SyncSHMBrokerTransport):
            return target.acquire(bind_key=self.pool_name, worker_id=self.worker_pid)
        acquire_lease_fn = getattr(self.broker, "acquire_lease", None)
        if callable(acquire_lease_fn):
            return acquire_lease_fn(pool_name=self.pool_name, worker_pid=self.worker_pid)
        if isinstance(target, SyncBrokerLeaseSource):
            return target.acquire(pool_name=self.pool_name, worker_pid=self.worker_pid)
        return None

    def _quarantine_broker_lease(self, lease: Any) -> None:
        """Quarantine a directly held broker lease if not already quarantined."""
        if lease is None or getattr(lease, "quarantined", False):
            return
        if self.broker is None:
            return
        target = getattr(self.broker, "transport", self.broker)
        quarantine_fn = getattr(self.broker, "quarantine", None) or getattr(target, "quarantine", None)
        if callable(quarantine_fn):
            quarantine_fn(lease)

    def _release_broker_lease(self, lease: Any) -> None:
        """Release a directly acquired broker lease."""
        if lease is None or getattr(lease, "quarantined", False):
            return
        if hasattr(lease, "release") and callable(lease.release):
            lease.release()
            return
        release_lease_fn = getattr(self.broker, "release_lease", None)
        if callable(release_lease_fn):
            release_lease_fn(self.slot_idx, self.generation, in_tx=False)
            return
        target = getattr(self.broker, "transport", self.broker)
        release_fn = getattr(target, "release", None)
        if callable(release_fn):
            release_fn(f"{self.slot_idx}:{self.generation}")

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
    def _normalize_params(params: tuple[Any, ...]) -> list[Any] | dict[str, Any]:
        """Normalize positional varargs into a flat parameter list or dict."""
        if not params:
            return []
        if len(params) == 1 and isinstance(params[0], (tuple, list, dict)):
            first = params[0]
            if isinstance(first, (tuple, list)):
                return list(first)
            return dict(first)
        return list(params)

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
    def _hydrate_raw_rows(
        rows: Sequence[dict[str, Any] | tuple[Any, ...] | list[Any]],
        columns: Sequence[str],
        schema_type: type[Any],
    ) -> list[Any]:
        """Hydrate raw tuple/list/dict rows into schema_type via C-API vectorcall without intermediate dicts."""
        if not rows:
            return []
        plan = ZeroCopyCodecRegistry.compile(schema_type)
        first = rows[0]
        col_names = (
            tuple(columns)
            if columns
            else (tuple(first.keys()) if isinstance(first, dict) else tuple(f"c{idx}" for idx in range(len(first))))
        )
        if isinstance(first, dict):
            tuple_rows = [tuple(r.get(c) for c in col_names) for r in rows if isinstance(r, dict)]
            return plan.hydrate_rows(tuple_rows, col_names)
        return plan.hydrate_rows(rows, col_names)

    @staticmethod
    def _convert_rows(rows: list[dict[str, Any]], schema_type: type[Any] | None) -> list[Any]:
        """Convert dictionary rows into schema_type instances when schema_type is provided."""
        if schema_type is None or schema_type is dict:
            return rows
        return [msgspec.convert(row, type=schema_type) for row in rows]

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
            resolved_broker = broker if broker is not None else provider.broker
            upstream_cfg = provider._resolve_upstream_optional(pool_name)
        elif callable(provider):
            self.provider = None
            self._transport_fn = provider
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
        self._request_seq: int = 0

    @property
    def in_transaction(self) -> bool:
        """Return True when the driver has an active transaction pinned to its lease."""
        return self._in_tx

    @property
    def is_leased(self) -> bool:
        """Return True when the driver holds an active broker lease."""
        return self._leased

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
            if isinstance(exc, LeaseQuarantinedError):
                raise
            raise LeaseQuarantinedError(self.slot_idx, self.generation, str(exc)) from exc
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
            self._in_tx = False
            ProxySyncDriver._raise_for_error(resp)
        finally:
            if not self._context_entered and not self._in_tx:
                await self.release_lease()

    async def rollback(self) -> None:
        """Roll back the active pinned transaction."""
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
            self._in_tx = False
            ProxySyncDriver._raise_for_error(resp)
        finally:
            if not self._context_entered and not self._in_tx:
                await self.release_lease()

    @property
    def data_dictionary(self) -> Any:
        """Return optional metadata dictionary (not exposed over RPC proxy)."""
        return None

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single SQL statement asynchronously on the proxy driver."""
        rowcount = int(await self.execute(statement))
        return self.create_execution_result(cursor, rowcount_override=rowcount)

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a SQL statement with multiple parameter sets asynchronously on the proxy driver."""
        params = getattr(statement, "parameters", ())
        rowcount = int(await self.execute_many(statement, list(params)))
        return self.create_execution_result(cursor, rowcount_override=rowcount, is_many_result=True)

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
        _ = (statement_config, kwargs)
        async with self._operation_lease():
            compiled_sql = self._compile_statement(statement)
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="execute",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=ProxySyncDriver._normalize_params(parameters),
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
        _ = (filters, statement_config, kwargs)
        async with self._operation_lease():
            compiled_sql = self._compile_statement(statement)
            normalized_many: list[list[Any] | dict[str, Any]] = [
                list(item) if isinstance(item, tuple) else cast("dict[str, Any] | list[Any]", item)
                for item in parameters
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

    @overload
    async def select(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: object,
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
        _ = (statement_config, kwargs)
        async with self._operation_lease():
            compiled_sql = self._compile_statement(statement)
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=ProxySyncDriver._normalize_params(parameters),
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    table = SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                    dict_rows: list[dict[str, Any]] = table.to_pylist()
                    return ProxySyncDriver._convert_rows(dict_rows, schema_type)
                finally:
                    await self._release_arrow_shm(resp.arrow_shm_name)

            if schema_type is not None and schema_type is not dict:
                return ProxySyncDriver._hydrate_raw_rows(resp.rows, resp.columns, schema_type)
            dict_rows = ProxySyncDriver._rows_to_dicts(resp.rows, resp.columns)
            return ProxySyncDriver._convert_rows(dict_rows, schema_type)

    @overload
    async def select_one(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: object,
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
        _ = (statement_config, kwargs)
        async with self._operation_lease():
            compiled_sql = self._compile_statement(statement)
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_one",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=ProxySyncDriver._normalize_params(parameters),
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    table = SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                    dict_rows = table.to_pylist()
                    if not dict_rows:
                        raise VertebraError("ValueError: No row found for select_one")
                    return cast("SchemaT | dict[str, Any]", ProxySyncDriver._convert_single(dict_rows[0], schema_type))
                finally:
                    await self._release_arrow_shm(resp.arrow_shm_name)

            dict_rows = ProxySyncDriver._rows_to_dicts(resp.rows, resp.columns)
            if not dict_rows:
                raise VertebraError("ValueError: No row found for select_one")
            return cast("SchemaT | dict[str, Any]", ProxySyncDriver._convert_single(dict_rows[0], schema_type))

    @overload
    async def select_one_or_none(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: object,
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
        _ = (statement_config, kwargs)
        async with self._operation_lease():
            compiled_sql = self._compile_statement(statement)
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_one_or_none",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=ProxySyncDriver._normalize_params(parameters),
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    table = SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                    dict_rows = table.to_pylist()
                    if not dict_rows:
                        return None
                    return cast("SchemaT | dict[str, Any]", ProxySyncDriver._convert_single(dict_rows[0], schema_type))
                finally:
                    await self._release_arrow_shm(resp.arrow_shm_name)

            dict_rows = ProxySyncDriver._rows_to_dicts(resp.rows, resp.columns)
            if not dict_rows:
                return None
            return cast("SchemaT | dict[str, Any]", ProxySyncDriver._convert_single(dict_rows[0], schema_type))

    @overload
    async def select_value(
        self,
        statement: Statement | QueryBuilder,
        /,
        *parameters: object,
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
        _ = (value_type, statement_config, kwargs)
        async with self._operation_lease():
            compiled_sql = self._compile_statement(statement)
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_value",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=ProxySyncDriver._normalize_params(parameters),
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            return resp.scalar

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
        _ = (return_format, native_only, batch_size, arrow_schema, kwargs)
        async with self._operation_lease():
            compiled_sql = self._compile_statement(statement)
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_to_arrow",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=ProxySyncDriver._normalize_params(parameters),
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    table = SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                finally:
                    await self._release_arrow_shm(resp.arrow_shm_name)
                return ProxySyncDriver._wrap_arrow_result(
                    statement,
                    compiled_sql,
                    table,
                    statement_config=statement_config or self.statement_config,
                )

            dict_rows = ProxySyncDriver._rows_to_dicts(resp.rows, resp.columns)
            table = SharedArrowBuffer.coerce_table(dict_rows, columns=resp.columns)
            return ProxySyncDriver._wrap_arrow_result(
                statement,
                compiled_sql,
                table,
                statement_config=statement_config or self.statement_config,
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

    def _compile_statement(self, statement: Any) -> str:
        """Consult the broker statement cache when available and return compiled SQL."""
        sql = statement.sql if hasattr(statement, "sql") else str(statement)
        if self.broker is None:
            return str(sql)
        target = getattr(self.broker, "transport", self.broker)
        compile_fn = getattr(target, "get_or_compile_statement", None)
        if callable(compile_fn):
            dialect_name = self.dialect or "sqlspec"
            plan = compile_fn(dialect_name, sql, lambda raw: (raw, ()))
            compiled = getattr(plan, "compiled_sql", None)
            if isinstance(compiled, str):
                return compiled
        return str(sql)

    @staticmethod
    def _wrap_arrow_result(
        statement: Any,
        compiled_sql: str,
        table: Any,
        *,
        statement_config: StatementConfig | None = None,
    ) -> ArrowResult:
        """Wrap a PyArrow table in a sqlspec ``ArrowResult`` preserving row counts."""
        prepared = statement if isinstance(statement, SQL) else SQL(compiled_sql, statement_config=statement_config)
        num_rows = getattr(table, "num_rows", len(table) if hasattr(table, "__len__") else 0)
        return ArrowResult(statement=prepared, data=table, rows_affected=num_rows)

    async def _send_rpc(self, request: BrokerRpcRequest, *, record_last: bool = True) -> BrokerRpcResponse:
        """Dispatch a BrokerRpcRequest asynchronously over the configured provider or raw transport."""
        try:
            res = self._transport_fn(request)
            if isinstance(res, Awaitable):
                res = await res
        except TypeError:
            res = self._transport_fn(encode_msgpack_bytes(request))
            if isinstance(res, Awaitable):
                res = await res
        if isinstance(res, (bytes, bytearray, memoryview)):
            resp = cast("BrokerRpcResponse", decode_msgpack(res, type=BrokerRpcResponse))
        elif isinstance(res, BrokerRpcResponse):
            resp = res
        else:
            raw_req = encode_msgpack_bytes(request)
            raw_resp = self._transport_fn(raw_req)
            if isinstance(raw_resp, Awaitable):
                raw_resp = await raw_resp
            resp = cast("BrokerRpcResponse", decode_msgpack(raw_resp, type=BrokerRpcResponse))
        if record_last:
            self.last_response = resp
        return resp

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
            SharedArrowBuffer.unlink(shm_name)

    async def _acquire_broker_lease(self) -> Any:
        """Acquire a lease asynchronously from self.broker via typed broker contracts."""
        if self.provider is not None and self.broker is self.provider.broker:
            return None
        target = getattr(self.broker, "transport", self.broker)
        if isinstance(target, AsyncSHMBrokerTransport):
            return await target.acquire(bind_key=self.pool_name, worker_id=self.worker_pid)
        if isinstance(target, SyncSHMBrokerTransport):
            return target.acquire(bind_key=self.pool_name, worker_id=self.worker_pid)
        acquire_lease_fn = getattr(self.broker, "acquire_lease", None)
        if callable(acquire_lease_fn):
            res = acquire_lease_fn(pool_name=self.pool_name, worker_pid=self.worker_pid)
            return await res if isinstance(res, Awaitable) else res
        if isinstance(target, (AsyncBrokerLeaseSource, SyncBrokerLeaseSource)):
            res = target.acquire(pool_name=self.pool_name, worker_pid=self.worker_pid)
            return await res if isinstance(res, Awaitable) else res
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
            res = quarantine_fn(lease)
            if isinstance(res, Awaitable):
                await res

    async def _release_broker_lease_async(self, lease: Any) -> None:
        """Release a directly acquired broker lease asynchronously."""
        if lease is None or getattr(lease, "quarantined", False):
            return
        if hasattr(lease, "release") and callable(lease.release):
            res = lease.release()
            if isinstance(res, Awaitable):
                await res
            return
        release_lease_fn = getattr(self.broker, "release_lease", None)
        if callable(release_lease_fn):
            res = release_lease_fn(self.slot_idx, self.generation, in_tx=False)
            if isinstance(res, Awaitable):
                await res
            return
        target = getattr(self.broker, "transport", self.broker)
        release_fn = getattr(target, "release", None)
        if callable(release_fn):
            res = release_fn(f"{self.slot_idx}:{self.generation}")
            if isinstance(res, Awaitable):
                await res


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
