"""Synchronous and asynchronous Oracle 26ai Thin drivers and connection pools."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.db.adapters.oracle._async import AsyncOracleDriver, AsyncOraclePool
from vertebra.db.adapters.oracle._sync import SyncOracleDriver, SyncOraclePool
from vertebra.db.adapters.oracle.pipeline import AsyncOraclePipeline, SyncOraclePipeline
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED: bool

__all__ = (
    "ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED",
    "AsyncOracleDriver",
    "AsyncOraclePipeline",
    "AsyncOraclePool",
    "SyncOracleDriver",
    "SyncOraclePipeline",
    "SyncOraclePool",
)


def __getattr__(name: str) -> Any:
    """Resolve optional native extension flags lazily without eager C-extension import."""
    if name == "ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED":
        oracle_mod = import_optional("vertebra._oracle")
        return bool(getattr(oracle_mod, "ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED", False))
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
