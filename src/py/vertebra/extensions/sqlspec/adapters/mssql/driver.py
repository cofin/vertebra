"""SQL Server (MSSQL) synchronous and asynchronous SQLSpec drivers over Vertebra TDS 7.4."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from sqlspec.core import SQL, ParameterStyle, StatementConfig
from sqlspec.driver import ExecutionResult, validate_savepoint_name

from vertebra.extensions.sqlspec._driver import BaseAsyncDriver, BaseSyncDriver
from vertebra.extensions.sqlspec.adapters.mssql.core import (
    default_statement_config,
    map_mssql_exception,
)
from vertebra.extensions.sqlspec.adapters.mssql.data_dictionary import (
    MSSQLAsyncDataDictionary,
    MSSQLSyncDataDictionary,
)

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.adapters.mssql._typing import MSSQLDriverFeatures

__all__ = ("MSSQLAsyncDriver", "MSSQLSyncDriver")


class MSSQLSyncDriver(BaseSyncDriver):
    """Synchronous SQL Server SQLSpec driver backed by Vertebra `SyncMSSQLDriver` (`tsql`)."""

    __slots__ = ("_data_dictionary",)

    dialect: str = "tsql"
    _execution_parameter_style = ParameterStyle.QMARK

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: MSSQLDriverFeatures | dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize the synchronous SQL Server SQLSpec driver with `?` -> `@P1` compilation."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=cast("dict[str, Any] | None", driver_features),
            observability=observability,
            shared_pool=shared_pool,
        )
        self._data_dictionary: MSSQLSyncDataDictionary | None = None

    @property
    def data_dictionary(self) -> MSSQLSyncDataDictionary:
        """Return lazily instantiated synchronous SQL Server data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = MSSQLSyncDataDictionary()
        return self._data_dictionary

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default SQL Server `StatementConfig` targeting `tsql` and `ParameterStyle.QMARK`."""
        return default_statement_config()

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_mssql_exception(exc)

    def create_savepoint(self, name: str) -> None:
        """Create a T-SQL savepoint via `SAVE TRANSACTION <name>`."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "savepoint", None)
        if callable(fn):
            fn(valid)
            return
        self.connection.execute(f"SAVE TRANSACTION {valid}")

    def release_savepoint(self, name: str) -> None:
        """Validate a savepoint name (releasing a savepoint is a no-op on T-SQL)."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "release_savepoint", None)
        if callable(fn):
            fn(valid)

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a T-SQL savepoint via `ROLLBACK TRANSACTION <name>`."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "rollback_to_savepoint", None)
        if callable(fn):
            fn(valid)
            return
        self.connection.execute(f"ROLLBACK TRANSACTION {valid}")

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a T-SQL statement repeatedly directly on the native cursor."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        if not prepared:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        cursor.executemany(sql, prepared)
        return self.create_execution_result(
            cursor, rowcount_override=BaseSyncDriver._normalize_rowcount(cursor.rowcount), is_many_result=True
        )


class MSSQLAsyncDriver(BaseAsyncDriver):
    """Asynchronous SQL Server SQLSpec driver backed by Vertebra `AsyncMSSQLDriver` (`tsql`)."""

    __slots__ = ("_data_dictionary",)

    dialect: str = "tsql"
    _execution_parameter_style = ParameterStyle.QMARK

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: MSSQLDriverFeatures | dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize the asynchronous SQL Server SQLSpec driver with `?` -> `@P1` compilation."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=cast("dict[str, Any] | None", driver_features),
            observability=observability,
            shared_pool=shared_pool,
        )
        self._data_dictionary: MSSQLAsyncDataDictionary | None = None

    @property
    def data_dictionary(self) -> MSSQLAsyncDataDictionary:
        """Return lazily instantiated asynchronous SQL Server data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = MSSQLAsyncDataDictionary()
        return self._data_dictionary

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default SQL Server `StatementConfig` targeting `tsql` and `ParameterStyle.QMARK`."""
        return default_statement_config()

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_mssql_exception(exc)

    async def create_savepoint(self, name: str) -> None:
        """Create a T-SQL savepoint asynchronously via `SAVE TRANSACTION <name>`."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "savepoint", None)
        if callable(fn):
            await cast("Any", fn)(valid)
            return
        await self.connection.execute(f"SAVE TRANSACTION {valid}")

    async def release_savepoint(self, name: str) -> None:
        """Validate a savepoint name asynchronously (releasing a savepoint is a no-op on T-SQL)."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "release_savepoint", None)
        if callable(fn):
            await cast("Any", fn)(valid)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a T-SQL savepoint asynchronously via `ROLLBACK TRANSACTION <name>`."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "rollback_to_savepoint", None)
        if callable(fn):
            await cast("Any", fn)(valid)
            return
        await self.connection.execute(f"ROLLBACK TRANSACTION {valid}")

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a T-SQL statement repeatedly asynchronously directly on the native cursor."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        if not prepared:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        await cursor.executemany(sql, prepared)
        return self.create_execution_result(
            cursor, rowcount_override=BaseSyncDriver._normalize_rowcount(cursor.rowcount), is_many_result=True
        )
