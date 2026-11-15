"""MySQL and MariaDB adapter package."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.adapters.mysql.config import (
        MySQLConfig,
        MySQLPoolConfig,
        MySQLStatementConfig,
    )
    from vertebra.db.adapters.mysql.core import (
        MYSQL_TRANSIENT_ERRNOS,
        MySQLCore,
        build_mysql_connect_kwargs,
        coerce_mysql_param,
        compile_mysql_statement,
        map_mysql_exception,
        normalize_mysql_rows,
    )
    from vertebra.db.adapters.mysql.driver import AsyncMySQLDriver, SyncMySQLDriver
    from vertebra.db.adapters.mysql.pipeline import AsyncMySQLPipeline, SyncMySQLPipeline
    from vertebra.db.arrow import ArrowCursor

__all__ = (
    "MYSQL_TRANSIENT_ERRNOS",
    "ArrowCursor",
    "AsyncMySQLDriver",
    "AsyncMySQLPipeline",
    "MySQLConfig",
    "MySQLCore",
    "MySQLPoolConfig",
    "MySQLStatementConfig",
    "SyncMySQLDriver",
    "SyncMySQLPipeline",
    "build_mysql_connect_kwargs",
    "coerce_mysql_param",
    "compile_mysql_statement",
    "map_mysql_exception",
    "normalize_mysql_rows",
)

__lazy_modules__ = [
    "vertebra.db.adapters.mysql.config",
    "vertebra.db.adapters.mysql.core",
    "vertebra.db.adapters.mysql.driver",
    "vertebra.db.adapters.mysql.pipeline",
    "vertebra.db.arrow",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "MYSQL_TRANSIENT_ERRNOS": ("vertebra.db.adapters.mysql.core", "MYSQL_TRANSIENT_ERRNOS"),
    "ArrowCursor": ("vertebra.db.arrow", "ArrowCursor"),
    "AsyncMySQLDriver": ("vertebra.db.adapters.mysql.driver", "AsyncMySQLDriver"),
    "AsyncMySQLPipeline": ("vertebra.db.adapters.mysql.pipeline", "AsyncMySQLPipeline"),
    "MySQLConfig": ("vertebra.db.adapters.mysql.config", "MySQLConfig"),
    "MySQLCore": ("vertebra.db.adapters.mysql.core", "MySQLCore"),
    "MySQLPoolConfig": ("vertebra.db.adapters.mysql.config", "MySQLPoolConfig"),
    "MySQLStatementConfig": ("vertebra.db.adapters.mysql.config", "MySQLStatementConfig"),
    "SyncMySQLDriver": ("vertebra.db.adapters.mysql.driver", "SyncMySQLDriver"),
    "SyncMySQLPipeline": ("vertebra.db.adapters.mysql.pipeline", "SyncMySQLPipeline"),
    "build_mysql_connect_kwargs": ("vertebra.db.adapters.mysql.core", "build_mysql_connect_kwargs"),
    "coerce_mysql_param": ("vertebra.db.adapters.mysql.core", "coerce_mysql_param"),
    "compile_mysql_statement": ("vertebra.db.adapters.mysql.core", "compile_mysql_statement"),
    "map_mysql_exception": ("vertebra.db.adapters.mysql.core", "map_mysql_exception"),
    "normalize_mysql_rows": ("vertebra.db.adapters.mysql.core", "normalize_mysql_rows"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
