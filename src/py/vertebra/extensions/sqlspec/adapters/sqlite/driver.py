"""SQLite synchronous and asynchronous SQLSpec driver adapters."""

from typing import Any

from sqlspec.core import SQL, ParameterStyle, StatementConfig
from sqlspec.driver import ExecutionResult

from vertebra.extensions.sqlspec._driver import BaseAsyncDriver, BaseSyncDriver, unwrap_typed_parameters
from vertebra.extensions.sqlspec.adapters.sqlite.core import (
    default_statement_config,
    map_sqlite_exception,
    normalize_sqlite_many_parameters,
    normalize_sqlite_parameters,
)
from vertebra.extensions.sqlspec.adapters.sqlite.data_dictionary import (
    SQLiteAsyncDataDictionary,
    SQLiteSyncDataDictionary,
)

__all__ = ("SQLiteAsyncDriver", "SQLiteSyncDriver")


class SQLiteSyncDriver(BaseSyncDriver):
    """Synchronous SQLite SQLSpec driver over Vertebra `SyncSQLiteDriver`."""

    dialect: str = "sqlite"
    _execution_parameter_style = ParameterStyle.QMARK

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize the synchronous SQLite SQLSpec driver and data dictionary."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=driver_features,
            observability=observability,
            shared_pool=shared_pool,
        )
        self._data_dictionary: SQLiteSyncDataDictionary | None = None

    @property
    def data_dictionary(self) -> SQLiteSyncDataDictionary:
        """Return lazily constructed synchronous SQLite metadata data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = SQLiteSyncDataDictionary()
        return self._data_dictionary

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default SQLite `StatementConfig` supporting `?` and `:name` parameters."""
        return default_statement_config()

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_sqlite_exception(exc)

    def _prepare_native_parameters(self, sql: str, params: Any, statement: SQL) -> tuple[str, Any]:
        _ = statement
        return sql, normalize_sqlite_parameters(sql, params)

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a statement across multiple parameter sets normalizing `:name` binds for `_sqlite`."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        unwrapped = unwrap_typed_parameters(prepared)
        param_sets = normalize_sqlite_many_parameters(sql, unwrapped)
        if not param_sets:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        exec_many_fn = getattr(cursor, "execute_many", getattr(cursor, "executemany", None))
        if callable(exec_many_fn):
            res = exec_many_fn(sql, param_sets)
            rowcount = int(res) if res is not None else 0
        else:
            rowcount = 0
            for item in param_sets:
                res = cursor.execute(sql, item)
                rowcount += len(res) if isinstance(res, list) else (int(res) if res is not None else 0)
        return self.create_execution_result(cursor, rowcount_override=rowcount, is_many_result=True)


class SQLiteAsyncDriver(BaseAsyncDriver):
    """Asynchronous SQLite SQLSpec driver over Vertebra `AsyncSQLiteDriver`."""

    dialect: str = "sqlite"
    _execution_parameter_style = ParameterStyle.QMARK

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize the asynchronous SQLite SQLSpec driver and data dictionary."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=driver_features,
            observability=observability,
            shared_pool=shared_pool,
        )
        self._data_dictionary: SQLiteAsyncDataDictionary | None = None

    @property
    def data_dictionary(self) -> SQLiteAsyncDataDictionary:
        """Return lazily constructed asynchronous SQLite metadata data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = SQLiteAsyncDataDictionary()
        return self._data_dictionary

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default SQLite `StatementConfig` supporting `?` and `:name` parameters."""
        return default_statement_config()

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_sqlite_exception(exc)

    def _prepare_native_parameters(self, sql: str, params: Any, statement: SQL) -> tuple[str, Any]:
        _ = statement
        return sql, normalize_sqlite_parameters(sql, params)

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Asynchronously execute a statement across multiple parameter sets for `_sqlite`."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        unwrapped = unwrap_typed_parameters(prepared)
        param_sets = normalize_sqlite_many_parameters(sql, unwrapped)
        if not param_sets:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        exec_many_fn = getattr(cursor, "execute_many", getattr(cursor, "executemany", None))
        if callable(exec_many_fn):
            res = await exec_many_fn(sql, param_sets)
            rowcount = int(res) if res is not None else 0
        else:
            rowcount = 0
            for item in param_sets:
                res = await cursor.execute(sql, item)
                rowcount += len(res) if isinstance(res, list) else (int(res) if res is not None else 0)
        return self.create_execution_result(cursor, rowcount_override=rowcount, is_many_result=True)
