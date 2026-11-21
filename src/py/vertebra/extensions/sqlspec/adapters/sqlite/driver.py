"""SQLite synchronous and asynchronous SQLSpec driver adapters."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from sqlspec.core import ParameterStyle, StatementConfig
from sqlspec.driver._common import cache_hit_hooks_unchanged

from vertebra.extensions.sqlspec._driver import BaseAsyncDriver, BaseSyncDriver
from vertebra.extensions.sqlspec.adapters.sqlite.core import (
    default_statement_config,
    map_sqlite_exception,
)
from vertebra.extensions.sqlspec.adapters.sqlite.data_dictionary import (
    SQLiteAsyncDataDictionary,
    SQLiteSyncDataDictionary,
)

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.adapters.sqlite._typing import SQLiteDriverFeatures

__all__ = ("SQLiteAsyncDriver", "SQLiteSyncDriver")


class SQLiteSyncDriver(BaseSyncDriver):
    """Synchronous SQLite SQLSpec driver over Vertebra `SyncSQLiteDriver`."""

    __slots__ = ("_data_dictionary",)

    dialect: str = "sqlite"
    _execution_parameter_style = ParameterStyle.QMARK

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: SQLiteDriverFeatures | dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize the synchronous SQLite SQLSpec driver and data dictionary."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=cast("dict[str, Any] | None", driver_features),
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

    def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
        """Return the client-bound callables the native driver holds, or `None` to register no executor."""
        if not cache_hit_hooks_unchanged(type(self), SQLiteSyncDriver):
            return None
        return self._single_call_pair(self.native_driver, "_fetch_rows", "_run_dml")

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_sqlite_exception(exc)


class SQLiteAsyncDriver(BaseAsyncDriver):
    """Asynchronous SQLite SQLSpec driver over Vertebra `AsyncSQLiteDriver`."""

    __slots__ = ("_data_dictionary",)

    dialect: str = "sqlite"
    _execution_parameter_style = ParameterStyle.QMARK

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: SQLiteDriverFeatures | dict[str, Any] | None = None,
        observability: Any = None,
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize the asynchronous SQLite SQLSpec driver and data dictionary."""
        super().__init__(
            connection,
            statement_config=statement_config,
            driver_features=cast("dict[str, Any] | None", driver_features),
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

    def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
        """Return the client-bound callables the native async driver holds, or `None` to register no executor."""
        if not cache_hit_hooks_unchanged(type(self), SQLiteAsyncDriver):
            return None
        return BaseSyncDriver._single_call_pair(self.native_driver, "_fetch_rows", "_run_dml")

    def _map_exception(self, exc: BaseException) -> Exception | None:
        return map_sqlite_exception(exc)
