"""MySQL synchronous and asynchronous SQLSpec driver adapters over Vertebra `vertebra._mysql`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from sqlspec.core import SQL, ParameterStyle, StatementConfig
from sqlspec.driver import ExecutionResult

from vertebra.extensions.sqlspec._driver import BaseAsyncDriver, BaseSyncDriver
from vertebra.extensions.sqlspec.adapters.mysql.core import (
    default_statement_config,
    map_mysql_exception,
)
from vertebra.extensions.sqlspec.adapters.mysql.data_dictionary import (
    MySQLAsyncDataDictionary,
    MySQLSyncDataDictionary,
)

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.adapters.mysql._typing import MySQLDriverFeatures

__all__ = ("MySQLAsyncDriver", "MySQLSyncDriver")


class MySQLSyncDriver(BaseSyncDriver):
    """Synchronous MySQL/MariaDB SQLSpec driver backed by Vertebra `SyncMySQLDriver`."""

    __slots__ = ("_data_dictionary",)

    dialect: str = "mysql"
    _execution_parameter_style = ParameterStyle.QMARK

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: MySQLDriverFeatures | dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize the synchronous MySQL SQLSpec driver with `?` binds and catalog dictionary."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=cast("dict[str, Any] | None", driver_features),
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

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a MySQL statement repeatedly directly on the native cursor."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        if not prepared:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        cursor.executemany(sql, prepared)
        return self.create_execution_result(cursor, rowcount_override=max(cursor.rowcount, 0), is_many_result=True)


class MySQLAsyncDriver(BaseAsyncDriver):
    """Asynchronous MySQL/MariaDB SQLSpec driver backed by Vertebra `AsyncMySQLDriver`."""

    __slots__ = ("_data_dictionary",)

    dialect: str = "mysql"
    _execution_parameter_style = ParameterStyle.QMARK

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: MySQLDriverFeatures | dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize the asynchronous MySQL SQLSpec driver with `?` binds and catalog dictionary."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=cast("dict[str, Any] | None", driver_features),
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

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a MySQL statement repeatedly asynchronously directly on the native cursor."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        if not prepared:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        await cursor.executemany(sql, prepared)
        return self.create_execution_result(cursor, rowcount_override=max(cursor.rowcount, 0), is_many_result=True)
