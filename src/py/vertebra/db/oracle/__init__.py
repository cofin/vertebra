"""Oracle Database (19c / 21c / 23ai / 26ai, Exadata, RAC, Autonomous Database) subsystem."""

from __future__ import annotations

from vertebra.db.oracle.config import OracleConfig, OracleConnectDescriptor, OraclePoolSettings
from vertebra.db.oracle.containers import OracleAdbFreeContainer, OrdsApexContainer
from vertebra.db.oracle.driver import (
    AsyncOracleDriver,
    OracleDataFrame,
    OraclePoolRegistry,
    SyncOracleDriver,
    encode_oracle_vector,
)
from vertebra.db.oracle.wallet import (
    ExtractedWallet,
    build_wallet_ssl_context,
    extract_wallet_archive,
    parse_tnsnames_ora,
    resolve_tns_alias,
)

__all__ = (
    "AsyncOracleDriver",
    "ExtractedWallet",
    "OracleAdbFreeContainer",
    "OracleConfig",
    "OracleConnectDescriptor",
    "OracleDataFrame",
    "OraclePoolRegistry",
    "OraclePoolSettings",
    "OrdsApexContainer",
    "SyncOracleDriver",
    "build_wallet_ssl_context",
    "encode_oracle_vector",
    "extract_wallet_archive",
    "parse_tnsnames_ora",
    "resolve_tns_alias",
)
