"""SQLite WAL driver with SQLite 3.45+ binary JSONB, SHM broker leasing, and Arrow IPC support."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.adapters.sqlite.config import SQLiteConfig
    from vertebra.db.adapters.sqlite.core import SQLiteCore
    from vertebra.db.adapters.sqlite.driver import AsyncSQLiteDriver, SyncSQLiteDriver
    from vertebra.db.adapters.sqlite.pipeline import AsyncSQLitePipeline, SyncSQLitePipeline
    from vertebra.db.arrow import ArrowCursor

__all__ = (
    "ArrowCursor",
    "AsyncSQLiteDriver",
    "AsyncSQLitePipeline",
    "SQLiteConfig",
    "SQLiteCore",
    "SyncSQLiteDriver",
    "SyncSQLitePipeline",
)

__lazy_modules__ = [
    "vertebra.db.adapters.sqlite.config",
    "vertebra.db.adapters.sqlite.core",
    "vertebra.db.adapters.sqlite.driver",
    "vertebra.db.adapters.sqlite.pipeline",
    "vertebra.db.arrow",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "ArrowCursor": ("vertebra.db.arrow", "ArrowCursor"),
    "AsyncSQLiteDriver": ("vertebra.db.adapters.sqlite.driver", "AsyncSQLiteDriver"),
    "AsyncSQLitePipeline": ("vertebra.db.adapters.sqlite.pipeline", "AsyncSQLitePipeline"),
    "SQLiteConfig": ("vertebra.db.adapters.sqlite.config", "SQLiteConfig"),
    "SQLiteCore": ("vertebra.db.adapters.sqlite.core", "SQLiteCore"),
    "SyncSQLiteDriver": ("vertebra.db.adapters.sqlite.driver", "SyncSQLiteDriver"),
    "SyncSQLitePipeline": ("vertebra.db.adapters.sqlite.pipeline", "SyncSQLitePipeline"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
