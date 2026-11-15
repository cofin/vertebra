"""Oracle configuration, TNS descriptor and wallet parsing, and 26ai VECTOR/OSON codecs; the driver requires the native `vertebra._oracle` module."""

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.adapters.oracle.config import (
        OracleConfig,
        OracleConnectDescriptor,
        OracleDriverFeatures,
        OracleSessionAttrs,
        OracleTopologyConfig,
        OracleWallet,
        parse_tnsnames_ora,
        resolve_tns_alias,
    )
    from vertebra.db.adapters.oracle.core import (
        ORACLE_DIALECT_TAG,
        ORACLE_VECTOR_FMT_BINARY,
        ORACLE_VECTOR_FMT_F32,
        ORACLE_VECTOR_FMT_F64,
        ORACLE_VECTOR_FMT_I8,
        ORACLE_VECTOR_MAGIC,
        ORACLE_VECTOR_VERSION,
        OSON_MAGIC,
        OracleCore,
    )
    from vertebra.db.adapters.oracle.driver import (
        AsyncOracleDriver,
        AsyncOraclePool,
        SyncOracleDriver,
        SyncOraclePool,
    )
    from vertebra.db.adapters.oracle.embeddings import OracleEmbeddingPushdown
    from vertebra.db.adapters.oracle.pipeline import (
        AsyncOraclePipeline,
        SyncOraclePipeline,
    )
    from vertebra.db.arrow import ArrowCursor

__all__ = (
    "OracleEmbeddingPushdown",
    "ORACLE_DIALECT_TAG",
    "ORACLE_VECTOR_FMT_BINARY",
    "ORACLE_VECTOR_FMT_F32",
    "ORACLE_VECTOR_FMT_F64",
    "ORACLE_VECTOR_FMT_I8",
    "ORACLE_VECTOR_MAGIC",
    "ORACLE_VECTOR_VERSION",
    "OSON_MAGIC",
    "ArrowCursor",
    "AsyncOracleDriver",
    "AsyncOraclePipeline",
    "AsyncOraclePool",
    "OracleConfig",
    "OracleConnectDescriptor",
    "OracleCore",
    "OracleDriverFeatures",
    "OracleSessionAttrs",
    "OracleTopologyConfig",
    "OracleWallet",
    "SyncOracleDriver",
    "SyncOraclePipeline",
    "SyncOraclePool",
    "parse_tnsnames_ora",
    "resolve_tns_alias",
)

__lazy_modules__ = [
    "vertebra.db.adapters.oracle.embeddings",
    "vertebra.db.adapters.oracle.config",
    "vertebra.db.adapters.oracle.core",
    "vertebra.db.adapters.oracle.driver",
    "vertebra.db.adapters.oracle.pipeline",
    "vertebra.db.arrow",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "OracleEmbeddingPushdown": ("vertebra.db.adapters.oracle.embeddings", "OracleEmbeddingPushdown"),
    "ORACLE_DIALECT_TAG": ("vertebra.db.adapters.oracle.core", "ORACLE_DIALECT_TAG"),
    "ORACLE_VECTOR_FMT_BINARY": ("vertebra.db.adapters.oracle.core", "ORACLE_VECTOR_FMT_BINARY"),
    "ORACLE_VECTOR_FMT_F32": ("vertebra.db.adapters.oracle.core", "ORACLE_VECTOR_FMT_F32"),
    "ORACLE_VECTOR_FMT_F64": ("vertebra.db.adapters.oracle.core", "ORACLE_VECTOR_FMT_F64"),
    "ORACLE_VECTOR_FMT_I8": ("vertebra.db.adapters.oracle.core", "ORACLE_VECTOR_FMT_I8"),
    "ORACLE_VECTOR_MAGIC": ("vertebra.db.adapters.oracle.core", "ORACLE_VECTOR_MAGIC"),
    "ORACLE_VECTOR_VERSION": ("vertebra.db.adapters.oracle.core", "ORACLE_VECTOR_VERSION"),
    "OSON_MAGIC": ("vertebra.db.adapters.oracle.core", "OSON_MAGIC"),
    "ArrowCursor": ("vertebra.db.arrow", "ArrowCursor"),
    "AsyncOracleDriver": ("vertebra.db.adapters.oracle.driver", "AsyncOracleDriver"),
    "AsyncOraclePipeline": ("vertebra.db.adapters.oracle.pipeline", "AsyncOraclePipeline"),
    "AsyncOraclePool": ("vertebra.db.adapters.oracle.driver", "AsyncOraclePool"),
    "OracleConfig": ("vertebra.db.adapters.oracle.config", "OracleConfig"),
    "OracleConnectDescriptor": ("vertebra.db.adapters.oracle.config", "OracleConnectDescriptor"),
    "OracleCore": ("vertebra.db.adapters.oracle.core", "OracleCore"),
    "OracleDriverFeatures": ("vertebra.db.adapters.oracle.config", "OracleDriverFeatures"),
    "OracleSessionAttrs": ("vertebra.db.adapters.oracle.config", "OracleSessionAttrs"),
    "OracleTopologyConfig": ("vertebra.db.adapters.oracle.config", "OracleTopologyConfig"),
    "OracleWallet": ("vertebra.db.adapters.oracle.config", "OracleWallet"),
    "SyncOracleDriver": ("vertebra.db.adapters.oracle.driver", "SyncOracleDriver"),
    "SyncOraclePipeline": ("vertebra.db.adapters.oracle.pipeline", "SyncOraclePipeline"),
    "SyncOraclePool": ("vertebra.db.adapters.oracle.driver", "SyncOraclePool"),
    "parse_tnsnames_ora": ("vertebra.db.adapters.oracle.config", "parse_tnsnames_ora"),
    "resolve_tns_alias": ("vertebra.db.adapters.oracle.config", "resolve_tns_alias"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
