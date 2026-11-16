"""MySQL synchronous and asynchronous SQLSpec driver adapters over Vertebra `vertebra._mysql`."""

from typing import Any

from sqlspec.core import SQL, ParameterStyle, StatementConfig
from sqlspec.driver import ExecutionResult

from vertebra.extensions.sqlspec._driver import BaseAsyncDriver, BaseSyncDriver
from vertebra.extensions.sqlspec.adapters.mysql.core import (
    default_statement_config,
    map_mysql_exception,
    normalize_mysql_many_parameters,
    normalize_mysql_parameters,
)
from vertebra.extensions.sqlspec.adapters.mysql.data_dictionary import (
    MySQLAsyncDataDictionary,
    MySQLSyncDataDictionary,
)

__all__ = ("MySQLAsyncDriver", "MySQLSyncDriver")


class MySQLSyncDriver(BaseSyncDriver):
    """Synchronous MySQL/MariaDB SQLSpec driver backed by Vertebra `SyncMySQLDriver`."""

    dialect: str = "mysql"
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
        """Initialize the synchronous MySQL SQLSpec driver with `?` binds and catalog dictionary."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=driver_features,
            observability=observability,
            shared_pool=shared_pool,
        )
        self._data_dictionary: MySQLSyncDataDictionary | None = None

    @property
    def data_dictionary(self) -> MySQLSyncDataDictionary:
        """Return lazily instantiated MySQL/MariaDB synchronous data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = MySQLSyncDataDictionary()
        return self._data_dictionary

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default MySQL `StatementConfig` targeting `ParameterStyle.QMARK` `?` placeholders."""
        return default_statement_config()

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_mysql_exception(exc)

    def _prepare_native_parameters(self, sql: str, params: Any, statement: SQL) -> tuple[str, Any]:
        _ = statement
        return sql, normalize_mysql_parameters(params)

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a MySQL statement repeatedly across normalized positional parameter tuples."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        param_sets = normalize_mysql_many_parameters(prepared)
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


class MySQLAsyncDriver(BaseAsyncDriver):
    """Asynchronous MySQL/MariaDB SQLSpec driver backed by Vertebra `AsyncMySQLDriver`."""

    dialect: str = "mysql"
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
        """Initialize the asynchronous MySQL SQLSpec driver with `?` binds and catalog dictionary."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=driver_features,
            observability=observability,
            shared_pool=shared_pool,
        )
        self._data_dictionary: MySQLAsyncDataDictionary | None = None

    @property
    def data_dictionary(self) -> MySQLAsyncDataDictionary:
        """Return lazily instantiated MySQL/MariaDB asynchronous data dictionary."""
        if self._data_dictionary is None:
            self._data_dictionary = MySQLAsyncDataDictionary()
        return self._data_dictionary

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build default MySQL `StatementConfig` targeting `ParameterStyle.QMARK` `?` placeholders."""
        return default_statement_config()

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_mysql_exception(exc)

    def _prepare_native_parameters(self, sql: str, params: Any, statement: SQL) -> tuple[str, Any]:
        _ = statement
        return sql, normalize_mysql_parameters(params)

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a MySQL statement repeatedly asynchronously across normalized parameter tuples."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        param_sets = normalize_mysql_many_parameters(prepared)
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
