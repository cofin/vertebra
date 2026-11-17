"""Oracle synchronous and asynchronous SQLSpec drivers over Vertebra native Oracle."""

from __future__ import annotations

import inspect
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

from sqlspec import SQLResult
from sqlspec.core import SQL, ParameterStyle, StatementConfig
from sqlspec.driver import validate_savepoint_name
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
    normalize_oracle_many_parameters,
    normalize_oracle_parameters,
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


def _extract_oracle_features(connection: Any) -> Any:
    """Resolve `OracleDriverFeatures` from the native Oracle connection configuration."""
    cfg = getattr(connection, "config", None)
    return getattr(cfg, "driver_features", None) if cfg is not None else None


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

    def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single compiled Oracle statement, forwarding `suspend_on_success` when supported."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        exec_args = getattr(effective_config, "execution_args", None) or {}
        if (
            exec_args.get("oracle_suspend_on_success")
            and not statement.returns_rows()
            and _execute_accepts_suspend_on_success(cursor)
        ):
            sql, params = self._compile_for_native(statement)
            raw = (
                cursor.execute(sql, params, suspend_on_success=True)
                if params is not None
                else cursor.execute(sql, suspend_on_success=True)
            )
            rowcount = len(raw) if isinstance(raw, list) else (int(raw) if raw is not None else 0)
            return self.create_execution_result(cursor, rowcount_override=rowcount)
        return super().dispatch_execute(cursor, statement)

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default Oracle `StatementConfig` targeting `:name` and `:1` binds."""
        return build_oracle_statement_config()

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_oracle_exception(exc)

    def _prepare_native_parameters(self, sql: str, params: Any, statement: SQL) -> tuple[str, Any]:
        _ = statement
        return sql, normalize_oracle_parameters(params, features=_extract_oracle_features(self.connection))

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

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> Any:
        """Execute batched Oracle DML across unwrapped and coerced parameter sets."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        param_sets = normalize_oracle_many_parameters(prepared, features=_extract_oracle_features(self.connection))
        if not param_sets:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        exec_many_fn = getattr(cursor, "execute_many", getattr(cursor, "executemany", None))
        if callable(exec_many_fn):
            rows_affected = exec_many_fn(sql, param_sets)
            return self.create_execution_result(
                cursor,
                rowcount_override=int(rows_affected) if isinstance(rows_affected, int) else len(param_sets),
                is_many_result=True,
            )
        return super().dispatch_execute_many(cursor, statement)

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

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Asynchronously execute a compiled Oracle statement, forwarding `suspend_on_success`."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        exec_args = getattr(effective_config, "execution_args", None) or {}
        if (
            exec_args.get("oracle_suspend_on_success")
            and not statement.returns_rows()
            and _execute_accepts_suspend_on_success(cursor)
        ):
            sql, params = self._compile_for_native(statement)
            raw = (
                await cursor.execute(sql, params, suspend_on_success=True)
                if params is not None
                else await cursor.execute(sql, suspend_on_success=True)
            )
            rowcount = len(raw) if isinstance(raw, list) else (int(raw) if raw is not None else 0)
            return self.create_execution_result(cursor, rowcount_override=rowcount)
        return await super().dispatch_execute(cursor, statement)

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default Oracle `StatementConfig` targeting `:name` and `:1` binds."""
        return build_oracle_statement_config()

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_oracle_exception(exc)

    def _prepare_native_parameters(self, sql: str, params: Any, statement: SQL) -> tuple[str, Any]:
        _ = statement
        return sql, normalize_oracle_parameters(params, features=_extract_oracle_features(self.connection))

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

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> Any:
        """Asynchronously execute batched Oracle DML across coerced parameter sets."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        param_sets = normalize_oracle_many_parameters(prepared, features=_extract_oracle_features(self.connection))
        if not param_sets:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        exec_many_fn = getattr(cursor, "execute_many", getattr(cursor, "executemany", None))
        if callable(exec_many_fn):
            rows_affected = await cast("Any", exec_many_fn)(sql, param_sets)
            return self.create_execution_result(
                cursor,
                rowcount_override=int(rows_affected) if isinstance(rows_affected, int) else len(param_sets),
                is_many_result=True,
            )
        return await super().dispatch_execute_many(cursor, statement)

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
