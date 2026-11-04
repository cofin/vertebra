"""Embedded DuckDB OLAP driver with SHM single-writer/multi-reader coordination and Arrow IPC streaming."""

from __future__ import annotations

from vertebra.db.adapters.duckdb.config import (
    DuckDBConfig,
    DuckDBSecretConfig,
    duckdb_bootstrap_statements,
)
from vertebra.db.adapters.duckdb.core import DUCKDB_DIALECT_TAG, DuckDBCore
from vertebra.db.adapters.duckdb.driver import AsyncDuckDBDriver, SyncDuckDBDriver
from vertebra.db.arrow import (
    ArrowCursor,
    read_arrow_shm,
    write_arrow_shm,
)

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
