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
//! - Oracle `tnsnames.ora` parser and alias resolver (`oracle::tns`), TTC piggybacked session
//!   attributes, and 26ai `VECTOR` (`FLOAT32`, `FLOAT64`, `INT8`, `BINARY`) and `OSON` codecs (`oracle`).
//! - SQL Server single-pass `@P1` placeholder rewriter (`mssql`).
//! - DuckDB 2.0-safe `SET` bootstrap statements and `ShmRegion` Arrow IPC frame re-exports (`duckdb`).
//! - Shared credential and TLS wiring for the official Google Cloud Rust clients (`gcp`).

#[cfg(feature = "arrow-shm")]
pub mod arrow_shm;
#[cfg(feature = "broker")]
pub mod broker_capsule;
#[cfg(feature = "transport")]
pub mod connection;
#[cfg(feature = "dialer")]
pub mod dialer;
#[cfg(feature = "codecs-duckdb")]
pub mod duckdb;
#[cfg(feature = "gcp")]
pub mod gcp;
#[cfg(feature = "pyo3")]
pub mod lease_guard;
#[cfg(feature = "codecs-mssql")]
pub mod mssql;
#[cfg(feature = "codecs-oracle")]
pub mod oracle;
#[cfg(feature = "codecs-pg")]
pub mod postgres;
#[cfg(feature = "pyo3")]
pub mod py_async;
#[cfg(feature = "pyo3")]
pub mod py_broker;
#[cfg(feature = "pyo3")]
pub mod py_cursor;
#[cfg(feature = "pyo3")]
pub mod py_errors;

/// Install `rustls`' `ring` `CryptoProvider` as the process default if none is installed yet.
#[cfg(feature = "rustls-ring")]
pub fn install_rustls_ring_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[cfg(feature = "arrow-shm")]
pub use arrow_shm::{
    ACTIVE_ARROW_SHM_REGIONS, ARROW_IPC_EOS_MARKER, SHM_ARROW_MAGIC, ShmArrowFrameHeader,
    ShmArrowStreamWriter, normalize_shm_name, open_shm_arrow_region, read_shm_arrow_frame,
    read_shm_arrow_frame_with, release_shm_arrow_frame, write_shm_arrow_frame,
    write_shm_arrow_region, write_shm_arrow_region_unregistered,
};
#[cfg(all(feature = "broker", feature = "pyo3"))]
pub use broker_capsule::new_broker_engine_pycapsule;
#[cfg(feature = "broker")]
pub use broker_capsule::{
    BROKER_ENGINE_ABI_VERSION, BROKER_ENGINE_CAPSULE_NAME, BrokerBytesVisitFn, BrokerEngineBackend,
    BrokerEngineCapsule, BrokerPoolBackend, DbRpcDecodedRequest, DbRpcEncodedResponse, DbRpcScalar,
    ForeignEngineCapsule, PlaceholderStyle, decode_dbrpc_request_frame,
    encode_dbrpc_response_frame, new_broker_engine_capsule, normalize_placeholders,
};
#[cfg(feature = "transport")]
pub use connection::{
    BoxFuture, CancelHandle, ConnectOptions, Connection, EndpointTarget, TlsMode, TransportError,
    TransportStream,
};
#[cfg(feature = "dialer")]
pub use dialer::{
    AlloyDbClusterUri, AlloyDbDialer, AuthType, CloudSqlDialer, CloudSqlInstanceUri, IpType,
    decode_alloydb_metadata_response, encode_alloydb_metadata_exchange,
    encode_alloydb_metadata_response, exchange_alloydb_metadata_over_stream,
    generate_ephemeral_keypair_pem,
};
#[cfg(feature = "codecs-duckdb")]
pub use duckdb::DuckdbConfig;
#[cfg(feature = "gcp")]
pub use gcp::{
    GcpErrorInfo, GcpTokenCredentials, code_name, code_number, gax_error_info, http_code,
    official_credentials,
};
#[cfg(feature = "pyo3")]
pub use lease_guard::LeaseGuard;
#[cfg(feature = "codecs-mssql")]
pub use mssql::rewrite_mssql_placeholders;
#[cfg(feature = "codecs-oracle")]
pub use oracle::{
    OSON_MAGIC, OracleSessionAttrs, OracleVectorFormat, decode_oracle_vector_binary,
    decode_oracle_vector_f32, decode_oracle_vector_f64, decode_oracle_vector_i8,
    decode_oson_payload, encode_oracle_vector_binary, encode_oracle_vector_f32,
    encode_oracle_vector_f64, encode_oracle_vector_i8, encode_oson_payload,
};
#[cfg(feature = "codecs-pg")]
pub use postgres::{
    PGCOPY_SIGNATURE, PoolerMode, compute_worker_pool_budget, decode_pgcopy_binary_rows,
    encode_pg_jsonb, encode_pgcopy_binary_rows, strip_pg_jsonb_header,
};
