"""Oracle synchronous and asynchronous SQLSpec drivers over Vertebra native Oracle."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from sqlspec.core import SQL, ParameterStyle, StatementConfig
from sqlspec.driver import validate_savepoint_name
from sqlspec.storage import StorageBridgeJob

from vertebra.db.adapters.oracle.core import OracleCore
from vertebra.db.rls import TenantPredicate
from vertebra.extensions.sqlspec._driver import BaseAsyncDriver, BaseSyncDriver
from vertebra.extensions.sqlspec.adapters.oracle.core import (
    build_oracle_statement_config,
    map_oracle_exception,
    normalize_oracle_many_parameters,
    normalize_oracle_parameters,
)
from vertebra.extensions.sqlspec.adapters.oracle.data_dictionary import (
    OracleAsyncDataDictionary,
    OracleSyncDataDictionary,
)

__all__ = ("OracleAsyncDriver", "OracleSyncDriver")


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


class OracleSyncDriver(BaseSyncDriver):
    """Synchronous Oracle SQLSpec driver supporting `:name` and `:1` binds with `OracleCore` codecs."""

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

    @property
    def data_dictionary(self) -> OracleSyncDataDictionary:
        """Return lazily instantiated synchronous Oracle data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = OracleSyncDataDictionary()
        return self._data_dictionary

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

    @property
    def data_dictionary(self) -> OracleAsyncDataDictionary:
        """Return lazily instantiated asynchronous Oracle data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = OracleAsyncDataDictionary()
        return self._data_dictionary

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
            await savepoint_fn(valid)
            return
        await self.connection.execute(f"SAVEPOINT {valid}")

    async def release_savepoint(self, name: str) -> None:
        """Validate Oracle savepoint name asynchronously."""
        valid = validate_savepoint_name(name)
        release_fn = getattr(self.connection, "release_savepoint", None)
        if callable(release_fn):
            await release_fn(valid)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Asynchronously roll back to an Oracle savepoint."""
        valid = validate_savepoint_name(name)
        rollback_fn = getattr(self.connection, "rollback_to_savepoint", None)
        if callable(rollback_fn):
            await rollback_fn(valid)
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
            rows_affected = await exec_many_fn(sql, param_sets)
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
