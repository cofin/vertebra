"""Pure-Rust-backed SQL Server (TDS 7.4) driver, `@P1` parameter rewriter, and Arrow cursor support."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.adapters.mssql.config import MSSQLConfig
    from vertebra.db.adapters.mssql.core import (
        MSSQL_DIALECT_TAG,
        MSSQLCore,
        rewrite_mssql_placeholders,
    )
    from vertebra.db.adapters.mssql.driver import AsyncMSSQLDriver, SyncMSSQLDriver
    from vertebra.db.adapters.mssql.pipeline import AsyncMSSQLPipeline, SyncMSSQLPipeline
    from vertebra.db.arrow import ArrowCursor

__all__ = (
    "MSSQL_DIALECT_TAG",
    "ArrowCursor",
    "AsyncMSSQLDriver",
    "AsyncMSSQLPipeline",
    "MSSQLConfig",
    "MSSQLCore",
    "SyncMSSQLDriver",
    "SyncMSSQLPipeline",
    "rewrite_mssql_placeholders",
)

__lazy_modules__ = [
    "vertebra.db.adapters.mssql.config",
    "vertebra.db.adapters.mssql.core",
    "vertebra.db.adapters.mssql.driver",
    "vertebra.db.adapters.mssql.pipeline",
    "vertebra.db.arrow",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "MSSQL_DIALECT_TAG": ("vertebra.db.adapters.mssql.core", "MSSQL_DIALECT_TAG"),
    "ArrowCursor": ("vertebra.db.arrow", "ArrowCursor"),
    "AsyncMSSQLDriver": ("vertebra.db.adapters.mssql.driver", "AsyncMSSQLDriver"),
    "AsyncMSSQLPipeline": ("vertebra.db.adapters.mssql.pipeline", "AsyncMSSQLPipeline"),
    "MSSQLConfig": ("vertebra.db.adapters.mssql.config", "MSSQLConfig"),
    "MSSQLCore": ("vertebra.db.adapters.mssql.core", "MSSQLCore"),
    "SyncMSSQLDriver": ("vertebra.db.adapters.mssql.driver", "SyncMSSQLDriver"),
    "SyncMSSQLPipeline": ("vertebra.db.adapters.mssql.pipeline", "SyncMSSQLPipeline"),
    "rewrite_mssql_placeholders": ("vertebra.db.adapters.mssql.core", "rewrite_mssql_placeholders"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
