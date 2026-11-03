"""Pure-Rust-backed SQL Server (TDS 7.4) driver, `@P1` parameter rewriter, and Arrow cursor support."""

from __future__ import annotations

from vertebra.db._arrow import ArrowCursor
from vertebra.db.mssql.config import MSSQLConfig
from vertebra.db.mssql.core import (
    MSSQL_DIALECT_TAG,
    TDS_PKT_ATTENTION,
    TDS_PKT_BULK_LOAD,
    TDS_PKT_PRE_LOGIN,
    TDS_PKT_RPC,
    TDS_PKT_SQL_BATCH,
    TDS_PKT_TABULAR_RESULT,
    TDS_PKT_TDS7_LOGIN,
    TDS_PKT_TRANS_MGR_REQ,
    MSSQLCore,
    decode_tds_packet,
    encode_tds_packet,
    encode_tds_sql_batch,
    rewrite_mssql_placeholders,
)
from vertebra.db.mssql.driver import AsyncMSSQLDriver, SyncMSSQLDriver

__all__ = (
    "MSSQL_DIALECT_TAG",
    "TDS_PKT_ATTENTION",
    "TDS_PKT_BULK_LOAD",
    "TDS_PKT_PRE_LOGIN",
    "TDS_PKT_RPC",
    "TDS_PKT_SQL_BATCH",
    "TDS_PKT_TABULAR_RESULT",
    "TDS_PKT_TDS7_LOGIN",
    "TDS_PKT_TRANS_MGR_REQ",
    "ArrowCursor",
    "AsyncMSSQLDriver",
    "MSSQLConfig",
    "MSSQLCore",
    "SyncMSSQLDriver",
    "decode_tds_packet",
    "encode_tds_packet",
    "encode_tds_sql_batch",
    "rewrite_mssql_placeholders",
)
