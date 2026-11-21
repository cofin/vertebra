"""Synchronous and asynchronous DuckDB analytical drivers."""

from __future__ import annotations

from vertebra.db.adapters.duckdb._async import AsyncDuckDBDriver
from vertebra.db.adapters.duckdb._sync import SyncDuckDBDriver

__all__ = (
    "AsyncDuckDBDriver",
    "SyncDuckDBDriver",
)
