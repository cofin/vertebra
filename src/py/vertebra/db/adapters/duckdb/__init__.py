"""DuckDB driver over the `duckdb` package with bootstrap statements and shared-memory Arrow IPC helpers."""

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.adapters.duckdb.config import (
        DuckDBConfig,
        DuckDBSecretConfig,
        duckdb_bootstrap_statements,
    )
    from vertebra.db.adapters.duckdb.core import DUCKDB_DIALECT_TAG, DuckDBCore
    from vertebra.db.adapters.duckdb.driver import AsyncDuckDBDriver, SyncDuckDBDriver
    from vertebra.db.adapters.duckdb.pipeline import (
        AsyncDuckDBPipeline,
        SyncDuckDBPipeline,
    )
    from vertebra.db.arrow import (
        ArrowCursor,
        read_arrow_shm,
        write_arrow_shm,
    )

__all__ = (
    "DUCKDB_DIALECT_TAG",
    "ArrowCursor",
    "AsyncDuckDBDriver",
    "AsyncDuckDBPipeline",
    "DuckDBConfig",
    "DuckDBCore",
    "DuckDBSecretConfig",
    "SyncDuckDBDriver",
    "SyncDuckDBPipeline",
    "duckdb_bootstrap_statements",
    "read_arrow_shm",
    "write_arrow_shm",
)

__lazy_modules__ = [
    "vertebra.db.adapters.duckdb.config",
    "vertebra.db.adapters.duckdb.core",
    "vertebra.db.adapters.duckdb.driver",
    "vertebra.db.adapters.duckdb.pipeline",
    "vertebra.db.arrow",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "DUCKDB_DIALECT_TAG": ("vertebra.db.adapters.duckdb.core", "DUCKDB_DIALECT_TAG"),
    "ArrowCursor": ("vertebra.db.arrow", "ArrowCursor"),
    "AsyncDuckDBDriver": ("vertebra.db.adapters.duckdb.driver", "AsyncDuckDBDriver"),
    "AsyncDuckDBPipeline": ("vertebra.db.adapters.duckdb.pipeline", "AsyncDuckDBPipeline"),
    "DuckDBConfig": ("vertebra.db.adapters.duckdb.config", "DuckDBConfig"),
    "DuckDBCore": ("vertebra.db.adapters.duckdb.core", "DuckDBCore"),
    "DuckDBSecretConfig": ("vertebra.db.adapters.duckdb.config", "DuckDBSecretConfig"),
    "SyncDuckDBDriver": ("vertebra.db.adapters.duckdb.driver", "SyncDuckDBDriver"),
    "SyncDuckDBPipeline": ("vertebra.db.adapters.duckdb.pipeline", "SyncDuckDBPipeline"),
    "duckdb_bootstrap_statements": ("vertebra.db.adapters.duckdb.config", "duckdb_bootstrap_statements"),
    "read_arrow_shm": ("vertebra.db.arrow", "read_arrow_shm"),
    "write_arrow_shm": ("vertebra.db.arrow", "write_arrow_shm"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
