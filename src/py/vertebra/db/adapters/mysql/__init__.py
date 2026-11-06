"""MySQL and MariaDB driver and Cloud SQL (:3307) configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.adapters.mysql.config import MySQLConfig
    from vertebra.db.adapters.mysql.core import MySQLCore
    from vertebra.db.adapters.mysql.driver import AsyncMySQLDriver, SyncMySQLDriver
    from vertebra.db.arrow import ArrowCursor

__all__ = (
    "ArrowCursor",
    "AsyncMySQLDriver",
    "MySQLConfig",
    "MySQLCore",
    "SyncMySQLDriver",
)

__lazy_modules__ = [
    "vertebra.db.adapters.mysql.config",
    "vertebra.db.adapters.mysql.core",
    "vertebra.db.adapters.mysql.driver",
    "vertebra.db.arrow",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "ArrowCursor": ("vertebra.db.arrow", "ArrowCursor"),
    "AsyncMySQLDriver": ("vertebra.db.adapters.mysql.driver", "AsyncMySQLDriver"),
    "MySQLConfig": ("vertebra.db.adapters.mysql.config", "MySQLConfig"),
    "MySQLCore": ("vertebra.db.adapters.mysql.core", "MySQLCore"),
    "SyncMySQLDriver": ("vertebra.db.adapters.mysql.driver", "SyncMySQLDriver"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
