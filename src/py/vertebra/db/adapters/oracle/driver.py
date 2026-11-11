"""Oracle drivers that require the native ``vertebra._oracle`` module from ``vertebra-core[oracle]``.

Construction and every public method raise ``MissingDependencyError`` when ``vertebra._oracle`` is absent.
"""

from __future__ import annotations

import asyncio
from types import ModuleType
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast

from vertebra.db.adapters.oracle.config import OracleConfig
from vertebra.db.adapters.oracle.core import ORACLE_DIALECT_TAG
from vertebra.db.driver import SyncDatabaseDriver, ThreadOffloadAsyncDatabaseDriver
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = (
    "AsyncOracleDriver",
    "SyncOracleDriver",
)


class SyncOracleDriver(SyncDatabaseDriver[OracleConfig]):
    """Synchronous Oracle driver backed by the native ``vertebra._oracle`` module from ``vertebra-core[oracle]``."""

    __slots__ = ()

    dialect: ClassVar[str] = "oracle"
    dialect_tag: ClassVar[int] = ORACLE_DIALECT_TAG

    def __init__(self, config: OracleConfig | None = None, *, broker: Any = None) -> None:
        _require_oracle_native()
        super().__init__(config or OracleConfig(), broker=broker)

    def connect(self) -> Self:
        """Establish or validate the Oracle connection; raises ``MissingDependencyError`` without ``vertebra._oracle``."""
        _require_oracle_native()
        return super().connect()

    def close(self) -> None:
        """Close the Oracle driver; raises ``MissingDependencyError`` without ``vertebra._oracle``."""
        _require_oracle_native()
        super().close()

    def begin(self) -> None:
        """Begin an explicit Oracle transaction; raises ``MissingDependencyError`` without ``vertebra._oracle``."""
        _require_oracle_native()
        super().begin()

    def commit(self) -> None:
        """Commit the active Oracle transaction; raises ``MissingDependencyError`` without ``vertebra._oracle``."""
        _require_oracle_native()
        super().commit()

    def rollback(self) -> None:
        """Roll back the active Oracle transaction; raises ``MissingDependencyError`` without ``vertebra._oracle``."""
        _require_oracle_native()
        super().rollback()

    def cancel(self) -> None:
        """Cancel an in-flight Oracle query; raises ``MissingDependencyError`` without ``vertebra._oracle``."""
        _require_oracle_native()
        super().cancel()

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a batch statement; raises ``MissingDependencyError`` without ``vertebra._oracle``."""
        _require_oracle_native()
        return super().execute_many(sql, parameter_sets, **kwargs)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return ``(rows, column_names)``; raises ``MissingDependencyError`` without ``vertebra._oracle``."""
        _require_oracle_native()
        return super().execute_with_description(sql, parameters, **kwargs)

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        direct_path: bool | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-ingest Arrow data into ``table_name``; raises ``MissingDependencyError`` without ``vertebra._oracle``."""
        _require_oracle_native()
        _ = direct_path
        return super().load_from_arrow(table_name, data, columns=columns, **kwargs)


class AsyncOracleDriver(ThreadOffloadAsyncDatabaseDriver[OracleConfig, SyncOracleDriver]):
    """Asynchronous Oracle driver offloading to ``SyncOracleDriver``; requires ``vertebra._oracle`` from ``vertebra-core[oracle]``."""

    __slots__ = ()

    dialect: ClassVar[str] = "oracle"
    dialect_tag: ClassVar[int] = ORACLE_DIALECT_TAG

    def __init__(self, config: OracleConfig | None = None, *, broker: Any = None) -> None:
        _require_oracle_native()
        resolved = config or OracleConfig()
        sync_driver = SyncOracleDriver(resolved, broker=broker)
        super().__init__(resolved, sync_driver, broker=sync_driver.broker)

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        direct_path: bool | None = None,
        **kwargs: Any,
    ) -> int:
        """Asynchronously bulk-ingest Arrow data into ``table_name``; raises ``MissingDependencyError`` without ``vertebra._oracle``."""
        _require_oracle_native()
        return await asyncio.to_thread(
            self._sync.load_from_arrow,
            table_name,
            data,
            columns=columns,
            direct_path=direct_path,
            **kwargs,
        )


def _require_oracle_native() -> ModuleType:
    """Resolve the native Oracle extension module.

    Returns:
        The imported ``vertebra._oracle`` module.

    Raises:
        MissingDependencyError: When ``vertebra._oracle`` is not installed.
    """
    return cast(
        "ModuleType",
        ensure_module("vertebra._oracle", package="vertebra-core[oracle]", install_package="vertebra-core[oracle]"),
    )
