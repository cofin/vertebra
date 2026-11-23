"""PostgreSQL synchronous and asynchronous SQLSpec drivers over Vertebra native Postgres."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from sqlspec.core import SQL, ParameterStyle, StatementConfig
from sqlspec.driver import validate_savepoint_name
from sqlspec.storage import StorageBridgeJob

from vertebra.db.adapters.postgres.core import PostgreSQLCore
from vertebra.db.rls import TenantPredicate
from vertebra.extensions.sqlspec._driver import BaseAsyncDriver, BaseSyncDriver
from vertebra.extensions.sqlspec.adapters.postgres.core import (
    build_postgres_statement_config,
    compile_postgres_numeric_transport,
    map_postgres_exception,
)
from vertebra.extensions.sqlspec.adapters.postgres.data_dictionary import (
    PostgresAsyncDataDictionary,
    PostgresSyncDataDictionary,
)

__all__ = ("PostgresAsyncDriver", "PostgresSyncDriver")


class _PostgresDriverMixin:
    """Shared pure-Python PostgreSQL driver hooks, numeric transport, and exception mapping."""

    __slots__ = ()

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default PostgreSQL `StatementConfig` targeting `postgres` `$1..$N` binds."""
        return build_postgres_statement_config()

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_postgres_exception(exc)

    def compile_numeric_transport(
        self,
        statement: Any,
        parameters: tuple[Any, ...] | list[Any] = (),
        statement_config: StatementConfig | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> tuple[Any, str, Any]:
        """Prepare and compile a statement to `$N` numeric binds preserving transformers and filters."""
        return compile_postgres_numeric_transport(
            cast("Any", self),
            statement,
            parameters,
            statement_config=statement_config,
            kwargs=kwargs,
        )


class PostgresSyncDriver(_PostgresDriverMixin, BaseSyncDriver):
    """Synchronous PostgreSQL SQLSpec driver emitting native `$1..$N` binds and Arrow COPY streams."""

    __slots__ = ("_data_dictionary", "_savepoint_tokens")

    dialect: str = "postgres"
    _execution_parameter_style = ParameterStyle.NUMERIC

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize the synchronous PostgreSQL SQLSpec driver."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=driver_features,
            observability=observability,
            shared_pool=shared_pool,
        )
        self._data_dictionary: PostgresSyncDataDictionary | None = None
        self._savepoint_tokens: dict[str, int] = {}

    @property
    def data_dictionary(self) -> PostgresSyncDataDictionary:
        """Return lazily instantiated synchronous PostgreSQL data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = PostgresSyncDataDictionary()
        return self._data_dictionary

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
        schema_name: str | None = None,
        **kwargs: Any,
    ) -> StorageBridgeJob:
        """Bulk-load an Arrow Table, RecordBatch, or IPC stream via PostgreSQL `COPY BINARY`."""
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
                    schema_name=schema_name,
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
        """Create a PostgreSQL transaction savepoint using native savepoint tokens or SQL."""
        valid = validate_savepoint_name(name)
        savepoint_fn = getattr(self.connection, "savepoint", None)
        if callable(savepoint_fn):
            token = savepoint_fn()
            if isinstance(token, int):
                self._savepoint_tokens[valid] = token
            return
        self.connection.execute(f"SAVEPOINT {valid}")

    def release_savepoint(self, name: str) -> None:
        """Release a PostgreSQL savepoint created via `create_savepoint`."""
        valid = validate_savepoint_name(name)
        token = self._savepoint_tokens.pop(valid, None)
        release_fn = getattr(self.connection, "release_savepoint", None)
        if token is not None and callable(release_fn):
            release_fn(token)
            return
        self.connection.execute(f"RELEASE SAVEPOINT {valid}")

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a PostgreSQL savepoint created via `create_savepoint`."""
        valid = validate_savepoint_name(name)
        token = self._savepoint_tokens.get(valid)
        rollback_fn = getattr(self.connection, "rollback_to_savepoint", None)
        if token is not None and callable(rollback_fn):
            rollback_fn(token)
            return
        self.connection.execute(f"ROLLBACK TO SAVEPOINT {valid}")

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> Any:
        """Execute batched `$1..$N` statements directly on the native cursor."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        if not prepared:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        cursor.executemany(sql, prepared)
        return self.create_execution_result(
            cursor,
            rowcount_override=BaseSyncDriver._normalize_rowcount(cursor.rowcount),
            is_many_result=True,
        )

    def apply_tenant_predicate(self, predicate: TenantPredicate) -> str:
        """Bind PostgreSQL session tenant GUC via `set_config($1, $2, $3)` and return predicate SQL."""
        sql, parameters = PostgreSQLCore.build_session_guc_sql(str(predicate.tenant_value), predicate.config)
        self.connection.execute(sql, parameters)
        return PostgreSQLCore.build_tenant_predicate_sql(predicate, param_index=1)


class PostgresAsyncDriver(_PostgresDriverMixin, BaseAsyncDriver):
    """Asynchronous PostgreSQL SQLSpec driver emitting native `$1..$N` binds and Arrow COPY streams."""

    __slots__ = ("_data_dictionary", "_savepoint_tokens")

    dialect: str = "postgres"
    _execution_parameter_style = ParameterStyle.NUMERIC

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize the asynchronous PostgreSQL SQLSpec driver."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=driver_features,
            observability=observability,
            shared_pool=shared_pool,
        )
        self._data_dictionary: PostgresAsyncDataDictionary | None = None
        self._savepoint_tokens: dict[str, int] = {}

    @property
    def data_dictionary(self) -> PostgresAsyncDataDictionary:
        """Return lazily instantiated asynchronous PostgreSQL data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = PostgresAsyncDataDictionary()
        return self._data_dictionary

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
        schema_name: str | None = None,
        **kwargs: Any,
    ) -> StorageBridgeJob:
        """Asynchronously bulk-load an Arrow payload via PostgreSQL `COPY BINARY`."""
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
                    schema_name=schema_name,
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
        """Create an asynchronous PostgreSQL savepoint using native tokens or SQL."""
        valid = validate_savepoint_name(name)
        savepoint_fn = getattr(self.connection, "savepoint", None)
        if callable(savepoint_fn):
            token = await cast("Any", savepoint_fn)()
            if isinstance(token, int):
                self._savepoint_tokens[valid] = token
            return
        await self.connection.execute(f"SAVEPOINT {valid}")

    async def release_savepoint(self, name: str) -> None:
        """Release an asynchronous PostgreSQL savepoint created via `create_savepoint`."""
        valid = validate_savepoint_name(name)
        token = self._savepoint_tokens.pop(valid, None)
        release_fn = getattr(self.connection, "release_savepoint", None)
        if token is not None and callable(release_fn):
            await cast("Any", release_fn)(token)
            return
        await self.connection.execute(f"RELEASE SAVEPOINT {valid}")

    async def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to an asynchronous PostgreSQL savepoint created via `create_savepoint`."""
        valid = validate_savepoint_name(name)
        token = self._savepoint_tokens.get(valid)
        rollback_fn = getattr(self.connection, "rollback_to_savepoint", None)
        if token is not None and callable(rollback_fn):
            await cast("Any", rollback_fn)(token)
            return
        await self.connection.execute(f"ROLLBACK TO SAVEPOINT {valid}")

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> Any:
        """Asynchronously execute batched `$1..$N` statements directly on the native cursor."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        if not prepared:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        await cursor.executemany(sql, prepared)
        return self.create_execution_result(
            cursor,
            rowcount_override=BaseSyncDriver._normalize_rowcount(cursor.rowcount),
            is_many_result=True,
        )

    async def apply_tenant_predicate(self, predicate: TenantPredicate) -> str:
        """Asynchronously bind PostgreSQL session tenant GUC via `set_config` and return predicate SQL."""
        sql, parameters = PostgreSQLCore.build_session_guc_sql(str(predicate.tenant_value), predicate.config)
        await self.connection.execute(sql, parameters)
        return PostgreSQLCore.build_tenant_predicate_sql(predicate, param_index=1)
