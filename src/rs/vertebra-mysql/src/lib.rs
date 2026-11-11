//! `vertebra-mysql` — Standalone PyO3 extension (`vertebra._mysql`) for MySQL 8.4/9 and MariaDB.

pub mod arrow_ipc;
pub mod client;
pub mod codecs;
pub mod config;
pub mod errors;
pub mod loopback;
pub mod protocol;

use std::sync::Arc;

use pyo3::prelude::*;
use vertebra_core::broker::DIALECT_TAG_MYSQL;
use vertebra_db::{BrokerEngineBackend, register_broker_engine_backend};

pub use arrow_ipc::{
    DEFAULT_MYSQL_ARROW_SHM_CAPACITY, MYSQL_ARROW_DECIMAL_SCALE, ShmArrowBatchHandle,
    decode_arrow_ipc_to_mysql_rows, encode_result_set_to_arrow_ipc, encode_result_set_to_shm,
    encode_result_set_to_shm_batches, next_mysql_shm_name, open_mysql_shm_writer,
    write_mysql_shm_frame,
};
pub use client::{AsyncMySQLClient, MYSQL_COMPLETION_DRAINER_ATTR, SyncMySQLClient};
pub use config::{MySQLConnectConfig, MySQLSslMode};
pub use errors::{MySQLError, MySQLErrorKind, classify_mysql_error, map_mysql_error};
pub use loopback::MySQLWireTestServer;
pub use protocol::{
    HandshakeV10, PlaceholderPlan, compute_caching_sha2_scramble,
    compute_caching_sha2_scramble_bytes, compute_mysql_native_scramble,
    compute_mysql_native_scramble_bytes, decode_mysql_packet, encode_mysql_packet,
    encrypt_caching_sha2_rsa, encrypt_caching_sha2_rsa_password, normalize_mysql_placeholders,
    split_sql_statements,
};

#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;

/// Shared-memory broker backend registration for MySQL (`DIALECT_TAG_MYSQL = 2`).
pub struct MySQLBrokerBackend;

impl BrokerEngineBackend for MySQLBrokerBackend {
    fn dialect_tag(&self) -> u16 {
        DIALECT_TAG_MYSQL
    }

    fn dialect_name(&self) -> &'static str {
        "mysql"
    }

    fn normalize_statement(&self, sql: &str) -> (String, usize) {
        let (norm, plan) = normalize_mysql_placeholders(sql);
        let param_count = match plan {
            PlaceholderPlan::Named(names) => names.len(),
            PlaceholderPlan::Positional => norm.bytes().filter(|&b| b == b'?').count(),
        };
        (norm, param_count)
    }
}

/// Register the MySQL engine backend with the shared-memory statement broker.
pub fn register_mysql_broker_backend() {
    register_broker_engine_backend(Arc::new(MySQLBrokerBackend));
}

#[pymodule(gil_used = false)]
fn _mysql(m: &Bound<'_, PyModule>) -> PyResult<()> {
    vertebra_db::install_rustls_ring_provider();
    register_mysql_broker_backend();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", false)?;
    m.add_class::<SyncMySQLClient>()?;
    m.add_class::<AsyncMySQLClient>()?;
    m.add_class::<MySQLWireTestServer>()?;
    m.add_function(wrap_pyfunction!(compute_caching_sha2_scramble, m)?)?;
    m.add_function(wrap_pyfunction!(compute_mysql_native_scramble, m)?)?;
    m.add_function(wrap_pyfunction!(encrypt_caching_sha2_rsa_password, m)?)?;
    m.add_function(wrap_pyfunction!(map_mysql_error, m)?)?;
    m.add_function(wrap_pyfunction!(next_mysql_shm_name, m)?)?;
    Ok(())
}
