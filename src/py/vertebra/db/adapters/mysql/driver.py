"""Synchronous and asynchronous MySQL and MariaDB drivers."""

from __future__ import annotations

from vertebra.db.adapters.mysql._async import AsyncMySQLDriver
from vertebra.db.adapters.mysql._sync import SyncMySQLDriver, _mysql_ext
from vertebra.db.adapters.mysql.pipeline import AsyncMySQLPipeline, SyncMySQLPipeline

__all__ = (
    "AsyncMySQLDriver",
    "AsyncMySQLPipeline",
    "SyncMySQLDriver",
    "SyncMySQLPipeline",
    "_mysql_ext",
)
