"""Oracle synchronous and asynchronous SQLSpec drivers over Vertebra native Oracle."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, cast

from sqlspec import SQLResult
from sqlspec.core import SQL, ParameterStyle, StatementConfig
from sqlspec.driver import validate_savepoint_name
from sqlspec.driver._common import cache_hit_hooks_unchanged
from sqlspec.exceptions import DialectNotSupportedError
from sqlspec.storage import StorageBridgeJob

from vertebra.db.adapters.oracle.core import OracleCore
from vertebra.db.rls import TenantPredicate
from vertebra.extensions.sqlspec._driver import BaseAsyncDriver, BaseSyncDriver
from vertebra.extensions.sqlspec.adapters.oracle._typing import (
    OracleAsyncSessionlessTransactionContext,
    OracleSyncSessionlessTransactionContext,
)
from vertebra.extensions.sqlspec.adapters.oracle.core import (
    apply_oracle_transaction_priority,
    build_oracle_statement_config,
    map_oracle_exception,
    validate_sessionless_execution_options,
    validate_sessionless_transaction_args,
)
from vertebra.extensions.sqlspec.adapters.oracle.data_dictionary import (
    OracleAsyncDataDictionary,
    OracleSyncDataDictionary,
)

if TYPE_CHECKING:
    from sqlspec.driver import ExecutionResult

__all__ = ("OracleAsyncDriver", "OracleSyncDriver")

_SESSIONLESS_UNSUPPORTED_MSG = (
    "Oracle 26ai sessionless transactions are not supported by the underlying native Oracle driver"
)


def _extract_tenant_context_args(connection: Any) -> tuple[str, str | None]:
    """Resolve Oracle namespace and package setter procedure name from native config."""
    cfg = getattr(connection, "config", None)
    namespace = getattr(cfg, "context_namespace", "USERENV") if cfg is not None else "USERENV"
    proc = getattr(cfg, "context_package_proc", None) if cfg is not None else None
    return str(namespace), str(proc) if proc is not None else None


def _execute_accepts_suspend_on_success(connection: Any) -> bool:
    """Return True when `connection.execute` explicitly declares a `suspend_on_success` parameter."""
    execute_fn = getattr(connection, "execute", None)
    if not callable(execute_fn):
        return False
    try:
        params = inspect.signature(execute_fn).parameters
    except (TypeError, ValueError):
        return False
    return "suspend_on_success" in params


def _normalize_sessionless_txid(txid: Any, fallback: bytes | None = None) -> bytes:
    """Coerce a native sessionless transaction identifier into `bytes`."""
    if isinstance(txid, (bytes, bytearray)):
        return bytes(txid)
    if isinstance(txid, str):
        return txid.encode("utf-8")
    if fallback is not None:
        return fallback
    return b""


class OracleSyncDriver(BaseSyncDriver):
    """Synchronous Oracle SQLSpec driver supporting `:name` and `:1` binds with `OracleCore` codecs."""

    __slots__ = ("_data_dictionary", "_sessionless_cleanup_error", "_sessionless_transaction_id")

    dialect: str = "oracle"
    _execution_parameter_style = ParameterStyle.NAMED_COLON

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize the synchronous Oracle SQLSpec driver."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=driver_features,
            observability=observability,
            shared_pool=shared_pool,
        )
        self._data_dictionary: OracleSyncDataDictionary | None = None
        self._sessionless_transaction_id: bytes | None = None
        self._sessionless_cleanup_error: BaseException | None = None

    @property
    def sessionless_transaction_id(self) -> bytes | None:
        """Return the active Oracle 26ai sessionless transaction identifier, if any."""
        return self._sessionless_transaction_id

    @property
    def data_dictionary(self) -> OracleSyncDataDictionary:
        """Return lazily instantiated synchronous Oracle data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = OracleSyncDataDictionary()
        return self._data_dictionary

    def set_transaction_priority(self, priority: Any) -> None:
        """Set Oracle 26ai transaction priority (`DEFAULT`, `LOW`, `MEDIUM`, `HIGH`)."""
        apply_oracle_transaction_priority(self.connection, priority)

    def begin_sessionless_transaction(
        self,
        transaction_id: str | bytes | None = None,
        *,
        timeout: int = 60,
        defer_round_trip: bool = True,
    ) -> bytes:
        """Begin an Oracle 26ai sessionless transaction on the underlying connection."""
        resolved = validate_sessionless_transaction_args(
            transaction_id,
            timeout=timeout,
            defer_round_trip=defer_round_trip,
        )
        method = getattr(self.connection, "begin_sessionless_transaction", None)
        if not callable(method):
            raise DialectNotSupportedError(_SESSIONLESS_UNSUPPORTED_MSG)
        try:
            txid = method(resolved, timeout=timeout, defer_round_trip=defer_round_trip)
        except Exception as exc:
            mapped = map_oracle_exception(exc)
            if mapped is not None:
                raise mapped from exc
            raise
        self._sessionless_transaction_id = _normalize_sessionless_txid(txid, resolved)
        return self._sessionless_transaction_id

    def resume_sessionless_transaction(
        self,
        transaction_id: str | bytes,
        *,
        timeout: int = 60,
        defer_round_trip: bool = True,
    ) -> bytes:
        """Resume a suspended Oracle 26ai sessionless transaction on this connection."""
        resolved = validate_sessionless_transaction_args(
            transaction_id,
            timeout=timeout,
            is_resume=True,
            defer_round_trip=defer_round_trip,
        )
        method = getattr(self.connection, "resume_sessionless_transaction", None)
        if not callable(method):
            raise DialectNotSupportedError(_SESSIONLESS_UNSUPPORTED_MSG)
        try:
            txid = method(resolved, timeout=timeout, defer_round_trip=defer_round_trip)
        except Exception as exc:
            mapped = map_oracle_exception(exc)
            if mapped is not None:
                raise mapped from exc
            raise
        self._sessionless_transaction_id = _normalize_sessionless_txid(txid, resolved)
        return self._sessionless_transaction_id

    def suspend_sessionless_transaction(self) -> None:
        """Suspend the active Oracle 26ai sessionless transaction for another pooled session."""
        method = getattr(self.connection, "suspend_sessionless_transaction", None)
        if not callable(method):
            raise DialectNotSupportedError(_SESSIONLESS_UNSUPPORTED_MSG)
        try:
            method()
        except BaseException as exc:
            mapped = map_oracle_exception(exc)
            pending = mapped if mapped is not None else exc
            if self._sessionless_transaction_id is not None:
                self._sessionless_cleanup_error = pending
            if mapped is not None:
                raise mapped from exc
            raise
        self._sessionless_transaction_id = None

    def sessionless_transaction(
        self,
        transaction_id: str | bytes | None = None,
        *,
        timeout: int = 60,
        defer_round_trip: bool = True,
        resume: bool = False,
        rollback_on_error: bool = True,
    ) -> OracleSyncSessionlessTransactionContext:
        """Scope a synchronous Oracle 26ai sessionless transaction and suspend on clean exit."""
        return OracleSyncSessionlessTransactionContext(
            self,
            transaction_id=transaction_id,
            timeout=timeout,
            defer_round_trip=defer_round_trip,
            resume=resume,
            rollback_on_error=rollback_on_error,
        )

    def commit(self) -> None:
        """Commit the active transaction and clear any attached sessionless transaction ID."""
        try:
            super().commit()
        except BaseException as exc:
            mapped = map_oracle_exception(exc)
            pending = mapped if mapped is not None else exc
            if self._sessionless_transaction_id is not None:
                self._sessionless_cleanup_error = pending
            if mapped is not None:
                raise mapped from exc
            raise
        self._sessionless_transaction_id = None

    def rollback(self) -> None:
        """Roll back the active transaction and clear any attached sessionless transaction ID."""
        try:
            super().rollback()
        except BaseException as exc:
            mapped = map_oracle_exception(exc)
            pending = mapped if mapped is not None else exc
            if self._sessionless_transaction_id is not None:
                self._sessionless_cleanup_error = pending
            if mapped is not None:
                raise mapped from exc
            raise
        self._sessionless_transaction_id = None

    def execute(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: StatementConfig | None = None,
        suspend_on_success: bool = False,
        **kwargs: Any,
    ) -> SQLResult:
        """Execute a statement with optional Oracle 26ai `suspend_on_success` sessionless detachment."""
        validate_sessionless_execution_options(suspend_on_success=suspend_on_success)
        if not suspend_on_success:
            return super().execute(statement, *parameters, statement_config=statement_config, **kwargs)
        accepts_native = _execute_accepts_suspend_on_success(self.connection)
        suspend_fn = getattr(self.connection, "suspend_sessionless_transaction", None)
        if not accepts_native and not callable(suspend_fn):
            raise DialectNotSupportedError(_SESSIONLESS_UNSUPPORTED_MSG)
        config = statement_config or self.statement_config
        args = dict(config.execution_args or {})
        args["oracle_suspend_on_success"] = True
        effective_config = config.replace(execution_args=args)
        prepared = self.prepare_statement(statement, parameters, statement_config=effective_config, kwargs=kwargs)
        prepared = prepared.copy(statement_config=effective_config)
        result = self.dispatch_statement_execution(prepared, self.connection)
        if not accepts_native and callable(suspend_fn):
            self.suspend_sessionless_transaction()
        else:
            self._sessionless_transaction_id = None
        return result

    @contextmanager
    def with_cursor(self, connection: Any) -> Iterator[Any]:
        """Enter `connection.checkout_cursor()` for native Oracle drivers."""
        with connection.checkout_cursor() as cursor:
            yield cursor

    def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single compiled Oracle statement on the native cursor or sessionless connection."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        exec_args = getattr(effective_config, "execution_args", None) or {}
        if (
            exec_args.get("oracle_suspend_on_success")
            and not statement.returns_rows()
            and _execute_accepts_suspend_on_success(cursor)
        ):
            sql, params = self._compiled_sql(statement, effective_config)
            raw = (
                cursor.execute(sql, params, suspend_on_success=True)
                if params is not None
                else cursor.execute(sql, suspend_on_success=True)
            )
            rowcount = len(raw) if isinstance(raw, list) else (int(raw) if raw is not None else 0)
            return self.create_execution_result(cursor, rowcount_override=rowcount)
        sql, params = self._compiled_sql(statement, effective_config)
        out_binds = exec_args.get("out_binds")
        if out_binds is not None:
            cursor.execute(sql, params or (), out_binds=out_binds)
        else:
            cursor.execute(sql, params or ())
        if hasattr(self.connection, "_last_out_binds"):
            self.connection._last_out_binds = cursor.out_binds
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
                last_inserted_id=cursor.lastrowid,
            )
        rowcount = cursor.rowcount
        return self.create_execution_result(
            cursor,
            rowcount_override=rowcount if rowcount > 0 else 0,
            last_inserted_id=cursor.lastrowid,
        )

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default Oracle `StatementConfig` targeting `:name` and `:1` binds."""
        return build_oracle_statement_config()

    def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
        """Return the cursor-checkout callables the cache-hit executor calls, or `None` to register no executor."""
        if not cache_hit_hooks_unchanged(type(self), OracleSyncDriver):
            return None
        if self.statement_config.execution_args:
            return None
        if not callable(getattr(self.native_driver, "checkout_cursor", None)):
            return None
        return self._cached_fetch_rows, self._cached_run_dml

    def _record_out_binds(self, cursor: Any) -> None:
        """Copy the cursor's out-bind values to the connection when it tracks them."""
        if hasattr(self.connection, "_last_out_binds"):
            self.connection._last_out_binds = cursor.out_binds

    def _cached_fetch_rows(self, sql: str, params: Any = ()) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Run a cached row-returning statement on one checked-out cursor and return `(rows, names)`."""
        with self.connection.checkout_cursor() as cursor:
            cursor.execute(sql, params)
            self._record_out_binds(cursor)
            description = cursor.description
            if description is None:
                return [], ()
            return cursor.fetchall(), tuple(column[0] for column in description)

    def _cached_run_dml(self, sql: str, params: Any = ()) -> tuple[int, Any]:
        """Run a cached statement without rows on one checked-out cursor and return `(rowcount, lastrowid)`."""
        with self.connection.checkout_cursor() as cursor:
            cursor.execute(sql, params)
            self._record_out_binds(cursor)
            rowcount = cursor.rowcount
            return (rowcount if rowcount > 0 else 0), cursor.lastrowid

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_oracle_exception(exc)

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
        columns: Sequence[str] | None = None,
        direct_path: bool | None = None,
        **kwargs: Any,
    ) -> StorageBridgeJob:
        """Bulk-load an Arrow payload into Oracle via direct-path array binding."""
        _ = (
            partitioner,
            overwrite,
            stage_table,
            conflict_keys,
            on_conflict,
            exclude_update_columns,
            drop_stage_table,
            drop_indexes,
            concurrent_indexes,
            type_casts,
        )
        rows_processed = 0
        with self.handle_database_exceptions() as exc_handler:
            rows_processed = int(
                self.connection.load_from_arrow(
                    table,
                    source,
                    columns=columns,
                    direct_path=direct_path,
                    **kwargs,
                )
            )
        self._check_pending_exception(exc_handler)
        return self._storage_job(
            {
                "destination": table,
                "rows_processed": rows_processed,
                "format": "arrow",
            },
            telemetry,
            status="completed",
        )

    def create_savepoint(self, name: str) -> None:
        """Create an Oracle transaction savepoint via native driver hook or `SAVEPOINT` SQL."""
        valid = validate_savepoint_name(name)
        savepoint_fn = getattr(self.connection, "savepoint", None)
        if callable(savepoint_fn):
            savepoint_fn(valid)
            return
        self.connection.execute(f"SAVEPOINT {valid}")

    def release_savepoint(self, name: str) -> None:
        """Validate Oracle savepoint name (`RELEASE SAVEPOINT` is a no-op on Oracle)."""
        valid = validate_savepoint_name(name)
        release_fn = getattr(self.connection, "release_savepoint", None)
        if callable(release_fn):
            release_fn(valid)

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to an Oracle savepoint via native driver hook or `ROLLBACK TO SAVEPOINT`."""
        valid = validate_savepoint_name(name)
        rollback_fn = getattr(self.connection, "rollback_to_savepoint", None)
        if callable(rollback_fn):
            rollback_fn(valid)
            return
        self.connection.execute(f"ROLLBACK TO SAVEPOINT {valid}")

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute batched Oracle DML directly on the native cursor."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        if not prepared:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        cursor.executemany(sql, prepared)
        return self.create_execution_result(
            cursor,
            rowcount_override=max(cursor.rowcount, 0),
            is_many_result=True,
        )

    def apply_tenant_predicate(self, predicate: TenantPredicate) -> str:
        """Bind Oracle session VPD context via `DBMS_SESSION` or package proc and return predicate SQL."""
        namespace, proc = _extract_tenant_context_args(self.connection)
        sql, params = OracleCore.build_session_context_sql(
            str(predicate.tenant_value),
            predicate.config,
            namespace=namespace,
            context_package_proc=proc,
        )
        self.connection.execute(sql, params)
        return predicate.to_sql_fragment(":tenant_id")


class OracleAsyncDriver(BaseAsyncDriver):
    """Asynchronous Oracle SQLSpec driver supporting `:name` and `:1` binds with `OracleCore` codecs."""

    __slots__ = ("_data_dictionary", "_sessionless_cleanup_error", "_sessionless_transaction_id")

    dialect: str = "oracle"
    _execution_parameter_style = ParameterStyle.NAMED_COLON

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize the asynchronous Oracle SQLSpec driver."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=driver_features,
            observability=observability,
            shared_pool=shared_pool,
        )
        self._data_dictionary: OracleAsyncDataDictionary | None = None
        self._sessionless_transaction_id: bytes | None = None
        self._sessionless_cleanup_error: BaseException | None = None

    @property
    def sessionless_transaction_id(self) -> bytes | None:
        """Return the active Oracle 26ai sessionless transaction identifier, if any."""
        return self._sessionless_transaction_id

    @property
    def data_dictionary(self) -> OracleAsyncDataDictionary:
        """Return lazily instantiated asynchronous Oracle data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = OracleAsyncDataDictionary()
        return self._data_dictionary

    def set_transaction_priority(self, priority: Any) -> None:
        """Set Oracle 26ai transaction priority (`DEFAULT`, `LOW`, `MEDIUM`, `HIGH`)."""
        apply_oracle_transaction_priority(self.connection, priority)

    async def begin_sessionless_transaction(
        self,
        transaction_id: str | bytes | None = None,
        *,
        timeout: int = 60,
        defer_round_trip: bool = True,
    ) -> bytes:
        """Asynchronously begin an Oracle 26ai sessionless transaction on the underlying connection."""
        resolved = validate_sessionless_transaction_args(
            transaction_id,
            timeout=timeout,
            defer_round_trip=defer_round_trip,
        )
        method = getattr(self.connection, "begin_sessionless_transaction", None)
        if not callable(method):
            raise DialectNotSupportedError(_SESSIONLESS_UNSUPPORTED_MSG)
        try:
            res = method(resolved, timeout=timeout, defer_round_trip=defer_round_trip)
            txid = await res if inspect.isawaitable(res) else res
        except Exception as exc:
            mapped = map_oracle_exception(exc)
            if mapped is not None:
                raise mapped from exc
            raise
        self._sessionless_transaction_id = _normalize_sessionless_txid(txid, resolved)
        return self._sessionless_transaction_id

    async def resume_sessionless_transaction(
        self,
        transaction_id: str | bytes,
        *,
        timeout: int = 60,
        defer_round_trip: bool = True,
    ) -> bytes:
        """Asynchronously resume a suspended Oracle 26ai sessionless transaction."""
        resolved = validate_sessionless_transaction_args(
            transaction_id,
            timeout=timeout,
            is_resume=True,
            defer_round_trip=defer_round_trip,
        )
        method = getattr(self.connection, "resume_sessionless_transaction", None)
        if not callable(method):
            raise DialectNotSupportedError(_SESSIONLESS_UNSUPPORTED_MSG)
        try:
            res = method(resolved, timeout=timeout, defer_round_trip=defer_round_trip)
            txid = await res if inspect.isawaitable(res) else res
        except Exception as exc:
            mapped = map_oracle_exception(exc)
            if mapped is not None:
                raise mapped from exc
            raise
        self._sessionless_transaction_id = _normalize_sessionless_txid(txid, resolved)
        return self._sessionless_transaction_id

    async def suspend_sessionless_transaction(self) -> None:
        """Asynchronously suspend the active Oracle 26ai sessionless transaction."""
        method = getattr(self.connection, "suspend_sessionless_transaction", None)
        if not callable(method):
            raise DialectNotSupportedError(_SESSIONLESS_UNSUPPORTED_MSG)
        try:
            res = method()
            if inspect.isawaitable(res):
                await res
        except BaseException as exc:
            mapped = map_oracle_exception(exc)
            pending = mapped if mapped is not None else exc
            if self._sessionless_transaction_id is not None:
                self._sessionless_cleanup_error = pending
            if mapped is not None:
                raise mapped from exc
            raise
        self._sessionless_transaction_id = None

    def sessionless_transaction(
        self,
        transaction_id: str | bytes | None = None,
        *,
        timeout: int = 60,
        defer_round_trip: bool = True,
        resume: bool = False,
        rollback_on_error: bool = True,
    ) -> OracleAsyncSessionlessTransactionContext:
        """Scope an asynchronous Oracle 26ai sessionless transaction and suspend on clean exit."""
        return OracleAsyncSessionlessTransactionContext(
            self,
            transaction_id=transaction_id,
            timeout=timeout,
            defer_round_trip=defer_round_trip,
            resume=resume,
            rollback_on_error=rollback_on_error,
        )

    async def commit(self) -> None:
        """Commit the active async transaction and clear any attached sessionless transaction ID."""
        try:
            await super().commit()
        except BaseException as exc:
            mapped = map_oracle_exception(exc)
            pending = mapped if mapped is not None else exc
            if self._sessionless_transaction_id is not None:
                self._sessionless_cleanup_error = pending
            if mapped is not None:
                raise mapped from exc
            raise
        self._sessionless_transaction_id = None

    async def rollback(self) -> None:
        """Roll back the active async transaction and clear any attached sessionless transaction ID."""
        try:
            await super().rollback()
        except BaseException as exc:
            mapped = map_oracle_exception(exc)
            pending = mapped if mapped is not None else exc
            if self._sessionless_transaction_id is not None:
                self._sessionless_cleanup_error = pending
            if mapped is not None:
                raise mapped from exc
            raise
        self._sessionless_transaction_id = None

    async def execute(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: StatementConfig | None = None,
        suspend_on_success: bool = False,
        **kwargs: Any,
    ) -> SQLResult:
        """Asynchronously execute a statement with optional Oracle 26ai `suspend_on_success`."""
        validate_sessionless_execution_options(suspend_on_success=suspend_on_success)
        if not suspend_on_success:
            return await super().execute(statement, *parameters, statement_config=statement_config, **kwargs)
        accepts_native = _execute_accepts_suspend_on_success(self.connection)
        suspend_fn = getattr(self.connection, "suspend_sessionless_transaction", None)
        if not accepts_native and not callable(suspend_fn):
            raise DialectNotSupportedError(_SESSIONLESS_UNSUPPORTED_MSG)
        config = statement_config or self.statement_config
        args = dict(config.execution_args or {})
        args["oracle_suspend_on_success"] = True
        effective_config = config.replace(execution_args=args)
        prepared = self.prepare_statement(statement, parameters, statement_config=effective_config, kwargs=kwargs)
        prepared = prepared.copy(statement_config=effective_config)
        result = await self.dispatch_statement_execution(prepared, self.connection)
        if not accepts_native and callable(suspend_fn):
            await self.suspend_sessionless_transaction()
        else:
            self._sessionless_transaction_id = None
        return result

    @asynccontextmanager
    async def with_cursor(self, connection: Any) -> AsyncIterator[Any]:
        """Enter `connection.checkout_cursor()` asynchronously for native Oracle drivers."""
        async with connection.checkout_cursor() as cursor:
            yield cursor

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Asynchronously execute a compiled Oracle statement on the native cursor or sessionless connection."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        exec_args = getattr(effective_config, "execution_args", None) or {}
        if (
            exec_args.get("oracle_suspend_on_success")
            and not statement.returns_rows()
            and _execute_accepts_suspend_on_success(cursor)
        ):
            sql, params = self._compiled_sql(statement, effective_config)
            raw = (
                await cursor.execute(sql, params, suspend_on_success=True)
                if params is not None
                else await cursor.execute(sql, suspend_on_success=True)
            )
            rowcount = len(raw) if isinstance(raw, list) else (int(raw) if raw is not None else 0)
            return self.create_execution_result(cursor, rowcount_override=rowcount)
        sql, params = self._compiled_sql(statement, effective_config)
        out_binds = exec_args.get("out_binds")
        if out_binds is not None:
            await cursor.execute(sql, params or (), out_binds=out_binds)
        else:
            await cursor.execute(sql, params or ())
        if hasattr(self.connection, "_last_out_binds"):
            self.connection._last_out_binds = cursor.out_binds
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
                last_inserted_id=cursor.lastrowid,
            )
        rowcount = cursor.rowcount
        return self.create_execution_result(
            cursor,
            rowcount_override=rowcount if rowcount > 0 else 0,
            last_inserted_id=cursor.lastrowid,
        )

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default Oracle `StatementConfig` targeting `:name` and `:1` binds."""
        return build_oracle_statement_config()

    def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
        """Return the cursor-checkout callables the cache-hit executor awaits, or `None` to register no executor."""
        if not cache_hit_hooks_unchanged(type(self), OracleAsyncDriver):
            return None
        if self.statement_config.execution_args:
            return None
        if not callable(getattr(self.native_driver, "checkout_cursor", None)):
            return None
        return self._cached_fetch_rows, self._cached_run_dml

    def _record_out_binds(self, cursor: Any) -> None:
        """Copy the cursor's out-bind values to the connection when it tracks them."""
        if hasattr(self.connection, "_last_out_binds"):
            self.connection._last_out_binds = cursor.out_binds

    async def _cached_fetch_rows(self, sql: str, params: Any = ()) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Run a cached row-returning statement on one checked-out async cursor and return `(rows, names)`."""
        async with self.connection.checkout_cursor() as cursor:
            await cursor.execute(sql, params)
            self._record_out_binds(cursor)
            description = cursor.description
            if description is None:
                return [], ()
            return await cursor.fetchall(), tuple(column[0] for column in description)

    async def _cached_run_dml(self, sql: str, params: Any = ()) -> tuple[int, Any]:
        """Run a cached statement without rows on one checked-out async cursor and return `(rowcount, lastrowid)`."""
        async with self.connection.checkout_cursor() as cursor:
            await cursor.execute(sql, params)
            self._record_out_binds(cursor)
            rowcount = cursor.rowcount
            return (rowcount if rowcount > 0 else 0), cursor.lastrowid

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_oracle_exception(exc)

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
        columns: Sequence[str] | None = None,
        direct_path: bool | None = None,
        **kwargs: Any,
    ) -> StorageBridgeJob:
        """Asynchronously bulk-load an Arrow payload into Oracle."""
        _ = (
            partitioner,
            overwrite,
            stage_table,
            conflict_keys,
            on_conflict,
            exclude_update_columns,
            drop_stage_table,
            drop_indexes,
            concurrent_indexes,
            type_casts,
        )
        rows_processed = 0
        async with self.handle_database_exceptions() as exc_handler:
            rows_processed = int(
                await self.connection.load_from_arrow(
                    table,
                    source,
                    columns=columns,
                    direct_path=direct_path,
                    **kwargs,
                )
            )
        self._check_pending_exception(exc_handler)
        return self._storage_job(
            {
                "destination": table,
                "rows_processed": rows_processed,
                "format": "arrow",
            },
            telemetry,
            status="completed",
        )

    async def create_savepoint(self, name: str) -> None:
        """Create an asynchronous Oracle transaction savepoint."""
        valid = validate_savepoint_name(name)
        savepoint_fn = getattr(self.connection, "savepoint", None)
        if callable(savepoint_fn):
            await cast("Any", savepoint_fn)(valid)
            return
        await self.connection.execute(f"SAVEPOINT {valid}")

    async def release_savepoint(self, name: str) -> None:
        """Validate Oracle savepoint name asynchronously."""
        valid = validate_savepoint_name(name)
        release_fn = getattr(self.connection, "release_savepoint", None)
        if callable(release_fn):
            await cast("Any", release_fn)(valid)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Asynchronously roll back to an Oracle savepoint."""
        valid = validate_savepoint_name(name)
        rollback_fn = getattr(self.connection, "rollback_to_savepoint", None)
        if callable(rollback_fn):
            await cast("Any", rollback_fn)(valid)
            return
        await self.connection.execute(f"ROLLBACK TO SAVEPOINT {valid}")

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Asynchronously execute batched Oracle DML directly on the native cursor."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        if not prepared:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        await cursor.executemany(sql, prepared)
        return self.create_execution_result(
            cursor,
            rowcount_override=max(cursor.rowcount, 0),
            is_many_result=True,
        )

    async def apply_tenant_predicate(self, predicate: TenantPredicate) -> str:
        """Asynchronously bind Oracle VPD context and return tenant predicate SQL."""
        namespace, proc = _extract_tenant_context_args(self.connection)
        sql, params = OracleCore.build_session_context_sql(
            str(predicate.tenant_value),
            predicate.config,
            namespace=namespace,
            context_package_proc=proc,
        )
        await self.connection.execute(sql, params)
        return predicate.to_sql_fragment(":tenant_id")
