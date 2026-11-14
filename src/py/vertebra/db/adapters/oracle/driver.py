"""Synchronous and asynchronous Oracle 26ai Thin drivers and connection pools."""

from __future__ import annotations

from vertebra.db.adapters.oracle._async import AsyncOracleDriver, AsyncOraclePool
from vertebra.db.adapters.oracle._sync import SyncOracleDriver, SyncOraclePool
from vertebra.db.adapters.oracle.pipeline import AsyncOraclePipeline, SyncOraclePipeline

try:
    from vertebra._oracle import ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED
except ImportError:
    ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED = False

__all__ = (
    "ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED",
    "AsyncOracleDriver",
    "AsyncOraclePipeline",
    "AsyncOraclePool",
    "SyncOracleDriver",
    "SyncOraclePipeline",
    "SyncOraclePool",
)
