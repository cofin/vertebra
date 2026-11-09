"""SQL Server drivers that require the native ``vertebra._mssql`` module from ``vertebra-core[mssql]``.

Construction and every public method raise ``MissingDependencyError`` when ``vertebra._mssql`` is absent.
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast

from vertebra.db.adapters.mssql.config import MSSQLConfig
from vertebra.db.adapters.mssql.core import MSSQL_DIALECT_TAG
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
)
from vertebra.db.driver import SyncDatabaseDriver, ThreadOffloadAsyncDatabaseDriver
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from vertebra.db.dialer.cloudsql import SyncCloudSQLDialer

__all__ = (
    "AsyncMSSQLDriver",
    "SyncMSSQLDriver",
)


class SyncMSSQLDriver(SyncDatabaseDriver[MSSQLConfig]):
    """Synchronous SQL Server driver backed by the native ``vertebra._mssql`` module from ``vertebra-core[mssql]``."""

    dialect: ClassVar[str] = "mssql"
    dialect_tag: ClassVar[int] = MSSQL_DIALECT_TAG

    def __init__(
        self,
        config: MSSQLConfig | None = None,
        *,
        dialer: SyncCloudSQLDialer | None = None,
        broker: Any = None,
    ) -> None:
        _require_mssql_native()
        super().__init__(config or MSSQLConfig(), broker=broker)
        self._dialer = dialer

    @property
    def dialer(self) -> SyncCloudSQLDialer | None:
        """Return the attached Cloud SQL dialer if present."""
        return self._dialer

    def connect(self) -> Self:
        """Establish or validate the SQL Server connection; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        return super().connect()

    def close(self) -> None:
        """Close the SQL Server driver; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        super().close()

    def begin(self) -> None:
        """Begin an explicit SQL Server transaction; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        super().begin()

    def commit(self) -> None:
        """Commit the active SQL Server transaction; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        super().commit()

    def rollback(self) -> None:
        """Roll back the active SQL Server transaction; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        super().rollback()

    def cancel(self) -> None:
        """Cancel an in-flight SQL Server query; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        super().cancel()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        return super().execute(sql, parameters, **kwargs)

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional tuples; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        return super().execute_tuples(sql, parameters, **kwargs)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return ``(rows, column_names)``; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        return super().execute_with_description(sql, parameters, **kwargs)

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a ``SELECT`` query; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        return super().select(sql, parameters, schema_type=schema_type, **kwargs)

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query into an ``ArrowCursor``; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        return super().select_to_arrow(sql, parameters, threshold_rows=threshold_rows, **kwargs)

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-ingest Arrow data into ``table_name``; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        return super().load_from_arrow(table_name, data, columns=columns, **kwargs)


class AsyncMSSQLDriver(ThreadOffloadAsyncDatabaseDriver[MSSQLConfig, SyncMSSQLDriver]):
    """Asynchronous SQL Server driver offloading to ``SyncMSSQLDriver``; requires ``vertebra._mssql`` from ``vertebra-core[mssql]``. The Cloud SQL dialer runs on the sync driver's worker thread."""

    dialect: ClassVar[str] = "mssql"
    dialect_tag: ClassVar[int] = MSSQL_DIALECT_TAG

    def __init__(
        self,
        config: MSSQLConfig | None = None,
        *,
        dialer: SyncCloudSQLDialer | None = None,
        broker: Any = None,
    ) -> None:
        _require_mssql_native()
        resolved = config or MSSQLConfig()
        sync_driver = SyncMSSQLDriver(resolved, dialer=dialer, broker=broker)
        super().__init__(resolved, sync_driver, broker=sync_driver.broker)

    @property
    def dialer(self) -> SyncCloudSQLDialer | None:
        """Return the Cloud SQL dialer attached to the paired sync driver."""
        return self._sync.dialer


def _require_mssql_native() -> ModuleType:
    """Resolve the native SQL Server extension module.

    Returns:
        The imported ``vertebra._mssql`` module.

    Raises:
        MissingDependencyError: When ``vertebra._mssql`` is not installed.
    """
    return cast(
        "ModuleType",
        ensure_module("vertebra._mssql", package="vertebra-core[mssql]", install_package="vertebra-core[mssql]"),
    )
