"""SQL Server configuration, TDS packet helpers, and `@P1` placeholder rewriter; the driver requires the native `vertebra._mssql` module."""

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.adapters.mssql.config import MSSQLConfig
    from vertebra.db.adapters.mssql.core import (
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
        rewrite_mssql_placeholders,
    )
    from vertebra.db.adapters.mssql.driver import AsyncMSSQLDriver, SyncMSSQLDriver
    from vertebra.db.arrow import ArrowCursor

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
    "rewrite_mssql_placeholders",
)

__lazy_modules__ = [
    "vertebra.db.adapters.mssql.config",
    "vertebra.db.adapters.mssql.core",
    "vertebra.db.adapters.mssql.driver",
    "vertebra.db.arrow",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "MSSQL_DIALECT_TAG": ("vertebra.db.adapters.mssql.core", "MSSQL_DIALECT_TAG"),
    "TDS_PKT_ATTENTION": ("vertebra.db.adapters.mssql.core", "TDS_PKT_ATTENTION"),
    "TDS_PKT_BULK_LOAD": ("vertebra.db.adapters.mssql.core", "TDS_PKT_BULK_LOAD"),
    "TDS_PKT_PRE_LOGIN": ("vertebra.db.adapters.mssql.core", "TDS_PKT_PRE_LOGIN"),
    "TDS_PKT_RPC": ("vertebra.db.adapters.mssql.core", "TDS_PKT_RPC"),
    "TDS_PKT_SQL_BATCH": ("vertebra.db.adapters.mssql.core", "TDS_PKT_SQL_BATCH"),
    "TDS_PKT_TABULAR_RESULT": ("vertebra.db.adapters.mssql.core", "TDS_PKT_TABULAR_RESULT"),
    "TDS_PKT_TDS7_LOGIN": ("vertebra.db.adapters.mssql.core", "TDS_PKT_TDS7_LOGIN"),
    "TDS_PKT_TRANS_MGR_REQ": ("vertebra.db.adapters.mssql.core", "TDS_PKT_TRANS_MGR_REQ"),
    "ArrowCursor": ("vertebra.db.arrow", "ArrowCursor"),
    "AsyncMSSQLDriver": ("vertebra.db.adapters.mssql.driver", "AsyncMSSQLDriver"),
    "MSSQLConfig": ("vertebra.db.adapters.mssql.config", "MSSQLConfig"),
    "MSSQLCore": ("vertebra.db.adapters.mssql.core", "MSSQLCore"),
    "SyncMSSQLDriver": ("vertebra.db.adapters.mssql.driver", "SyncMSSQLDriver"),
    "decode_tds_packet": ("vertebra.db.adapters.mssql.core", "decode_tds_packet"),
    "encode_tds_packet": ("vertebra.db.adapters.mssql.core", "encode_tds_packet"),
    "rewrite_mssql_placeholders": ("vertebra.db.adapters.mssql.core", "rewrite_mssql_placeholders"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
