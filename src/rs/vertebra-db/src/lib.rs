//! `vertebra-db` — Native Rust database wire protocols, cloud mTLS dialers, and shared-memory engines.
//!
//! Provides:
//! - Zero-sidecar Google Cloud SQL (`:3307`) and AlloyDB (`:5433`) mTLS dialers, ephemeral keypair
//!   generation (`rcgen`), and `MetadataExchangeRequest` / `MetadataExchangeResponse` framing (`dialer`).
//! - Native PostgreSQL v3 wire framing, pooler topology rules (`Direct`, `PgBouncerTransaction`,
//!   `PgCat`, `Supavisor`), zero-copy binary `JSONB` header stripping, and `PGCOPY` binary codec (`postgres`).
//! - Native MySQL 4.1+/8.0+/9.0+ packet framing, `caching_sha2_password` scramble, and binary command
//!   codecs (`mysql`).
//! - Native SQLite 3.45+ WAL / `mmap_size` / `busy_timeout` initialization pragmas and single-writer /
//!   multi-reader SHM pool (`sqlite`).
//! - Pure-Rust Oracle `tnsnames.ora` / `sqlnet.ora` / Autonomous Database Wallet parser (`tns`) and
//!   TNS/TTC wire framing, zero-RTT piggybacked session attributes, 26ai `VECTOR` (`FLOAT32`, `FLOAT64`,
//!   `INT8`, `BINARY`), and `OSON` binary codecs (`oracle`).
//! - Pure-Rust SQL Server Tabular Data Stream (TDS 7.4) packet/batch framing and single-pass `@P1`
//!   placeholder rewriter (`mssql`).
//! - SHM-brokered single-writer DuckDB analytical engine, DuckDB 2.0-safe `SET` bootstrap statements,
//!   and `ShmRegion` Arrow IPC framing (`duckdb`).
//! - Pure-Rust Redis 7.2+/8.0+ and Valkey 8.0+/9.0+ RESP3 wire framing, recursive parser, `invalidate`
//!   push frame extractor, Sentinel master discovery, and Cluster CRC16-XMODEM slot routing (`resp3`).
//! - Shared credential and TLS wiring for the official Google Cloud Rust clients (`gcp`).

pub mod arrow_shm;
pub mod bigquery;
pub mod dialer;
pub mod duckdb;
pub mod gcp;
pub mod mssql;
pub mod mysql;
pub mod oracle;
pub mod postgres;
pub mod resp3;
pub mod spanner;
pub mod sqlite;
pub mod tns;

pub use arrow_shm::*;
pub use bigquery::*;
pub use dialer::{
    AlloyDbClusterUri, AlloyDbDialer, AuthType, CloudSqlDialer, CloudSqlInstanceUri, IpType,
    decode_alloydb_metadata_response, encode_alloydb_metadata_exchange,
    encode_alloydb_metadata_response, exchange_alloydb_metadata_over_stream,
    generate_ephemeral_keypair_pem,
};
pub use duckdb::{DuckdbBrokerPool, DuckdbConfig};
pub use mssql::{
    MssqlPool, TdsPacketType, decode_tds_packet, encode_tds_packet, encode_tds_sql_batch,
    rewrite_mssql_placeholders,
};
pub use mysql::{
    MysqlPool, MysqlPoolConfig, compute_caching_sha2_scramble, decode_mysql_packet,
    encode_com_query, encode_com_stmt_close, encode_com_stmt_prepare, encode_mysql_packet,
};
pub use oracle::{
    OSON_MAGIC, OraclePool, OracleSessionAttrs, OracleVectorFormat, TnsPacketType,
    decode_oracle_vector_binary, decode_oracle_vector_f32, decode_oracle_vector_f64,
    decode_oracle_vector_i8, decode_oson_payload, decode_tns_packet, encode_oracle_vector_binary,
    encode_oracle_vector_f32, encode_oracle_vector_f64, encode_oracle_vector_i8,
    encode_oson_payload, encode_tns_connect_packet, encode_tns_data_packet,
};
pub use postgres::{
    PGCOPY_SIGNATURE, PoolerMode, PostgresPool, PostgresPoolConfig, compute_worker_pool_budget,
    decode_pgcopy_binary_rows, encode_bind_frame, encode_execute_frame, encode_parse_frame,
    encode_pg_jsonb, encode_pgcopy_binary_rows, encode_query_frame, encode_startup_message,
    encode_sync_frame, strip_pg_jsonb_header,
};
pub use resp3::{
    ClusterRedirect, RedirectKind, Resp3Frame, crc16_xmodem, encode_hello3_handshake,
    encode_resp_command, encode_sentinel_get_master_cmd, extract_invalidation_keys,
    parse_cluster_redirect, parse_resp3_frame, redis_hash_slot,
};
pub use spanner::*;
pub use sqlite::{SqliteConfig, SqlitePool};
pub use tns::{
    OracleConnectDescriptor, OracleWalletMaterial, parse_tnsnames_ora, resolve_tns_alias,
};
