"""Embedded DuckDB OLAP driver with SHM single-writer/multi-reader coordination and Arrow IPC streaming."""

from __future__ import annotations

from vertebra.db._arrow import (
    ArrowCursor,
    read_arrow_shm,
    write_arrow_shm,
)
from vertebra.db.duckdb.config import (
    DuckDBConfig,
    DuckDBSecretConfig,
    duckdb_bootstrap_statements,
)
from vertebra.db.duckdb.core import DUCKDB_DIALECT_TAG, DuckDBCore
from vertebra.db.duckdb.driver import AsyncDuckDBDriver, SyncDuckDBDriver

__all__ = (
    "DUCKDB_DIALECT_TAG",
    "ArrowCursor",
    "AsyncDuckDBDriver",
    "DuckDBConfig",
    "DuckDBCore",
    "DuckDBSecretConfig",
    "SyncDuckDBDriver",
    "duckdb_bootstrap_statements",
    "read_arrow_shm",
    "write_arrow_shm",
)
