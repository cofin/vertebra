//! `vertebra-db` — Database wire codecs, Cloud SQL and AlloyDB mTLS dialers, shared transport, and
//! shared-memory Arrow framing.
//!
//! Provides:
//! - Zero-sidecar Google Cloud SQL (`:3307`) and AlloyDB (`:5433`) mTLS dialers, ephemeral keypair
//!   generation (`rcgen`), and `MetadataExchangeRequest` / `MetadataExchangeResponse` framing (`dialer`).
//! - Shared async transport stream (TCP, `tokio-rustls`, Unix socket) and the `Connection` /
//!   `CancelHandle` traits (`connection`).
//! - Shared-memory `VTBARROW` Arrow IPC framing and multi-batch stream writer (`arrow_shm`).
//! - PostgreSQL v3 frontend frame encoders, pooler topology rules (`Direct`, `PgBouncerTransaction`,
//!   `PgCat`, `Supavisor`), binary `JSONB` header stripping, `PGCOPY` binary codec, and pgvector codecs (`postgres`).
//! - SQLite 3.45+ WAL / `mmap_size` / `busy_timeout` initialization pragmas (`sqlite`).
//! - Oracle `tnsnames.ora` parser and alias resolver (`oracle::tns`), TTC piggybacked session
//!   attributes, and 26ai `VECTOR` (`FLOAT32`, `FLOAT64`, `INT8`, `BINARY`) and `OSON` codecs (`oracle`).
//! - SQL Server single-pass `@P1` placeholder rewriter (`mssql`).
//! - DuckDB 2.0-safe `SET` bootstrap statements and `ShmRegion` Arrow IPC frame re-exports (`duckdb`).
//! - Redis 7.2+/8.0+ and Valkey 8.0+/9.0+ RESP3 wire framing, recursive parser, `invalidate`
//!   push frame extractor, Sentinel master discovery, and Cluster CRC16-XMODEM slot routing (`resp3`).
//! - Shared credential and TLS wiring for the official Google Cloud Rust clients (`gcp`).

pub mod arrow_shm;
pub mod connection;
pub mod dialer;
pub mod duckdb;
#[cfg(feature = "gcp")]
pub mod gcp;
pub mod mssql;
pub mod oracle;
pub mod postgres;
#[cfg(feature = "pyo3")]
pub mod py_async;
pub mod resp3;
pub mod sqlite;

pub use arrow_shm::{
    ACTIVE_ARROW_SHM_REGIONS, ARROW_IPC_EOS_MARKER, SHM_ARROW_MAGIC, ShmArrowFrameHeader,
    ShmArrowStreamWriter, normalize_shm_name, open_shm_arrow_region, read_shm_arrow_frame,
    read_shm_arrow_frame_with, release_shm_arrow_frame, write_shm_arrow_frame,
    write_shm_arrow_region, write_shm_arrow_region_unregistered,
};
pub use connection::{
    BoxFuture, CancelHandle, ConnectOptions, Connection, EndpointTarget, TlsMode, TransportError,
    TransportStream, install_rustls_ring_provider,
};
pub use dialer::{
    AlloyDbClusterUri, AlloyDbDialer, AuthType, CloudSqlDialer, CloudSqlInstanceUri, IpType,
    decode_alloydb_metadata_response, encode_alloydb_metadata_exchange,
    encode_alloydb_metadata_response, exchange_alloydb_metadata_over_stream,
    generate_ephemeral_keypair_pem,
};
pub use duckdb::DuckdbConfig;
#[cfg(feature = "gcp")]
pub use gcp::{
    GcpErrorInfo, GcpTokenCredentials, code_name, code_number, gax_error_info, http_code,
    official_credentials,
};
pub use mssql::rewrite_mssql_placeholders;
pub use oracle::{
    OSON_MAGIC, OracleSessionAttrs, OracleVectorFormat, decode_oracle_vector_binary,
    decode_oracle_vector_f32, decode_oracle_vector_f64, decode_oracle_vector_i8,
    decode_oson_payload, encode_oracle_vector_binary, encode_oracle_vector_f32,
    encode_oracle_vector_f64, encode_oracle_vector_i8, encode_oson_payload,
};
pub use postgres::{
    PGCOPY_SIGNATURE, PoolerMode, compute_worker_pool_budget, decode_pgcopy_binary_rows,
    encode_bind_frame, encode_execute_frame, encode_parse_frame, encode_pg_jsonb,
    encode_pgcopy_binary_rows, encode_query_frame, encode_startup_message, encode_sync_frame,
    strip_pg_jsonb_header,
};
pub use resp3::{
    ClusterRedirect, RedirectKind, Resp3Frame, crc16_xmodem, encode_hello3_handshake,
    encode_resp_command, encode_sentinel_get_master_cmd, extract_invalidation_keys,
    parse_cluster_redirect, parse_resp3_frame, redis_hash_slot,
};
pub use sqlite::SqliteConfig;

use std::sync::{Arc, LazyLock};

use dashmap::DashMap;

/// Pluggable native database engine backend registered with the shared-memory broker by dialect tag.
pub trait BrokerEngineBackend: Send + Sync {
    /// Canonical dialect tag (`vertebra_core::broker::DIALECT_TAG_*`).
    fn dialect_tag(&self) -> u16;

    /// Canonical lowercase dialect identifier (for example `"mysql"`).
    fn dialect_name(&self) -> &'static str;

    /// Normalize SQL placeholders for `ShmStatementCache` plan storage, returning `(normalized_sql, param_count)`.
    fn normalize_statement(&self, sql: &str) -> (String, usize);
}

static BROKER_ENGINE_BACKENDS: LazyLock<DashMap<u16, Arc<dyn BrokerEngineBackend>>> =
    LazyLock::new(DashMap::new);

/// Register a [`BrokerEngineBackend`] for its [`BrokerEngineBackend::dialect_tag`].
pub fn register_broker_engine_backend(backend: Arc<dyn BrokerEngineBackend>) {
    BROKER_ENGINE_BACKENDS.insert(backend.dialect_tag(), backend);
}

/// Look up a registered [`BrokerEngineBackend`] by dialect tag.
pub fn lookup_broker_engine_backend(dialect_tag: u16) -> Option<Arc<dyn BrokerEngineBackend>> {
    BROKER_ENGINE_BACKENDS
        .get(&dialect_tag)
        .map(|entry| Arc::clone(entry.value()))
}
